"""
Project: Czech Surname Restorator
File: main.py
Description: Main script to correct Czech surnames in CSV files using a reference database and fuzzy matching.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""

import logging
import pandas as pd
import ray
import time
import unicodedata
import regex as re
from rapidfuzz import process, fuzz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('name_correction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Ray
ray.init(ignore_reinit_error=True, num_cpus=8, log_to_driver=True)


def remove_diacritics(s: str) -> str:
    """
    Remove diacritics from a string.
    
    Args:
        s (str): Input string with potential diacritics
        
    Returns:
        str: String with diacritics removed
    """
    return ''.join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))


def dopln_diakritiku(puvodni: str, opravena: str) -> str:
    """
    Preserve as much diacritics from 'opravena' as possible without
    dropping additional characters from the original text.
    
    Args:
        puvodni (str): Original text
        opravena (str): Corrected text with proper diacritics
        
    Returns:
        str: Text with combined diacritics from both inputs
    """
    orig_nfd = unicodedata.normalize("NFD", puvodni)
    ref_nfd = unicodedata.normalize("NFD", opravena)
    orig_clusters = re.findall(r'\X', orig_nfd)
    ref_clusters = re.findall(r'\X', ref_nfd)
    corrected_clusters = []
    min_len = min(len(orig_clusters), len(ref_clusters))
    for i in range(min_len):
        corrected_clusters.append(union_diacritics(orig_clusters[i], ref_clusters[i]))
    if len(orig_clusters) > min_len:
        corrected_clusters.extend(orig_clusters[min_len:])
    corrected = "".join(corrected_clusters)
    return unicodedata.normalize("NFC", corrected)


def union_diacritics(orig_cluster: str, ref_cluster: str) -> str:
    """
    Combine diacritics (combining characters) for matching base characters.
    
    Args:
        orig_cluster (str): Original character cluster
        ref_cluster (str): Reference character cluster
        
    Returns:
        str: Combined character cluster with merged diacritics
    """
    o_nfd = unicodedata.normalize("NFD", orig_cluster)
    r_nfd = unicodedata.normalize("NFD", ref_cluster)
    if not o_nfd:
        return ref_cluster
    if not r_nfd:
        return orig_cluster
    base_o = o_nfd[0]
    diacs_o = o_nfd[1:]
    base_r = r_nfd[0]
    diacs_r = r_nfd[1:]
    # If base characters differ, keep the original to preserve the root letter
    if unicodedata.normalize("NFD", base_o)[0] != unicodedata.normalize("NFD", base_r)[0]:
        return orig_cluster
    new_diacs = list(diacs_o)
    for c in diacs_r:
        if c not in new_diacs:
            new_diacs.append(c)
    combined_nfd = base_o + ''.join(new_diacs)
    return unicodedata.normalize("NFC", combined_nfd)


def get_grapheme_count(text: str) -> int:
    """
    Count the number of graphemes in a text string.
    
    Args:
        text (str): Input text string
        
    Returns:
        int: Number of graphemes in the text
    """
    return len(re.findall(r'\X', unicodedata.normalize("NFD", text)))


######################################################################
# Index creation (run once before chunking)
######################################################################
def vytvor_index(prijmeni_list: list) -> dict:
    """
    Create an index from a reference surname list (e.g., from CSV).
    Builds index based on (grapheme_count, without_diacritics.lower()).
    
    Args:
        prijmeni_list (list): List of reference surnames
        
    Returns:
        dict: Index dictionary with (grapheme_count, normalized_name) as keys
    """
    dict_index = {}
    for item in prijmeni_list:
        if not isinstance(item, str):
            continue
        key = (get_grapheme_count(item), remove_diacritics(item).lower())
        dict_index.setdefault(key, []).append(item)
    return dict_index


######################################################################
# Surname correction using the index
######################################################################
@ray.remote
def oprav_prijmeni_ray(prijmeni_batch: list, dict_index: dict, dict_index_values: list) -> list:
    """
    Correct surnames in a batch using Ray for parallel processing.
    
    Args:
        prijmeni_batch (list): Batch of surnames to correct
        dict_index (dict): Reference index for surname matching
        dict_index_values (list): Flattened list of all reference surnames for fuzzy search
        
    Returns:
        list: List of corrected surnames
    """
    opravene_prijmeni = []

    for prijmeni in prijmeni_batch:
        if not isinstance(prijmeni, str):
            opravene_prijmeni.append(prijmeni)
            continue

        pocet = get_grapheme_count(prijmeni)
        nd = remove_diacritics(prijmeni).lower()
        key = (pocet, nd)

        # 1) If key exists and has a single candidate, use it directly
        if key in dict_index:
            candidates = dict_index[key]
            if len(candidates) == 1:
                opravene = dopln_diakritiku(prijmeni, candidates[0])
                opravene_prijmeni.append(opravene)
                continue
            else:
                # 2) Multiple candidates: run fuzzy match only on this small subset
                best_match = process.extractOne(
                    prijmeni,
                    candidates,
                    scorer=fuzz.token_set_ratio
                )
                if best_match and best_match[1] >= 80:
                    opravene = dopln_diakritiku(prijmeni, best_match[0])
                else:
                    opravene = prijmeni
                opravene_prijmeni.append(opravene)
                continue

        # 3) Key missing in index: fallback to fuzzy search against full dictionary
        # Note: This can be performance intensive because we search the entire list
        
        # Find top matches
        found = process.extract(prijmeni, dict_index_values, limit=5)
        if not found:
            opravene_prijmeni.append(prijmeni)
            continue

        # Select first match with high confidence
        candidate = None
        for cand in found:
            if cand[1] >= 75:
                candidate = cand[0]
                break
        if candidate:
            opravene = dopln_diakritiku(prijmeni, candidate)
        else:
            opravene = prijmeni

        opravene_prijmeni.append(opravene)

    return opravene_prijmeni


if __name__ == "__main__":
    # Configuration
    # REPLACE THESE VALUES WITH YOUR ACTUAL FILE PATHS
    REFERENCE_CSV = "surnames_all.csv"  # The file containing the correct list of surnames
    INPUT_CSV = "input_names.csv"             # The file containing names to specific correction
    OUTPUT_CSV = "output_corrected.csv"       # Where to save the results
    
    CHUNK_SIZE = 10000
    BATCH_SIZE = 1000
    
    start_time = time.time()
    logger.info("Starting CSV name correction process")

    # Load reference CSV surnames
    logger.info(f"Loading reference surnames from {REFERENCE_CSV}")
    prijmeni_df = pd.read_csv(REFERENCE_CSV, dtype=str)
    prijmeni_list = prijmeni_df["column1"].dropna().tolist()
    prijmeni_list.sort() # Sort alphabetically as requested
    logger.info(f"Loaded {len(prijmeni_list)} reference surnames")

    # Create index
    logger.info("Creating surname index")
    dict_index = vytvor_index(prijmeni_list)
    logger.info(f"Created index with {len(dict_index)} unique keys")

    # Create global list for fallback fuzzy search
    dict_index_values = list(dict_index.values())
    # Flatten the list of lists into a single list
    dict_index_values = [x for sub in dict_index_values for x in sub]
    logger.info(f"Prepared {len(dict_index_values)} surnames for fallback search")

    # Put heavy data into Ray Object Store
    dict_index_ref = ray.put(dict_index)
    dict_index_values_ref = ray.put(dict_index_values)


    # Processing only; no file output configured

    first_chunk = True
    processed_chunks = 0

    logger.info(f"Processing input file {INPUT_CSV} in chunks of {CHUNK_SIZE}")
    for chunk in pd.read_csv(INPUT_CSV, dtype=str, low_memory=False, chunksize=CHUNK_SIZE):
        chunk["krestni_jmeno"] = chunk["Person_Name"].str.extract(r"^(\S+)")
        chunk["prijmeni"] = chunk["Person_Name"].str.extract(r"(\S+)$")

        # Split into smaller batches for Ray parallelism
        prijmeni_batches = [
            chunk["prijmeni"].iloc[i: i + BATCH_SIZE].tolist()
            for i in range(0, len(chunk), BATCH_SIZE)
        ]


        tasks = []
        for batch in prijmeni_batches:
            tasks.append(oprav_prijmeni_ray.remote(batch, dict_index_ref, dict_index_values_ref))



        results = ray.get(tasks)


        chunk["opravene_prijmeni"] = [item for sublist in results for item in sublist]
        chunk["opravene_jmeno"] = chunk["krestni_jmeno"] + " " + chunk["opravene_prijmeni"]

        # Output disabled
        first_chunk = False
        processed_chunks += 1
        
        logger.info(f"Processed chunk {processed_chunks} with {len(chunk)} records")

    elapsed_time = time.time() - start_time
    logger.info(f"Total processing time: {elapsed_time:.2f} seconds")
    logger.info("Processing completed - no file output")
    ray.shutdown()
