# Czech Surname Restorator
![License](https://img.shields.io/badge/license-MIT-blue) ![Python](https://img.shields.io/badge/python-3.10+-blue) ![Framework](https://img.shields.io/badge/framework-Ray-blue)

## 1. Overview
This tool restores diacritics to Czech surnames in large datasets. It uses a reference database and a multi-stage fuzzy matching algorithm to correct names corrupted by ASCII-only entry or typos.

## 2. Motivation
Large legacy datasets often lack diacritics (e.g., "Dvorak" vs "Dvořák"). Manual correction is impossible at scale. Simple dictionary lookups fail on typos. This project automates restoration with high precision using context-aware fuzzy matching.

## 3. What This Project Does
- **Ingests** CSV files with a `Person_Name` column.
- **Normalizes** input text to handle inconsistent casing and formatting.
- **Matches** names against a reference database.
- **Restores** diacritics using best-match candidates.
- **Parallelizes** processing for high throughput.

## 4. Architecture
The system uses a distributed batch processing model:
1.  **Indexing**: Reference data is loaded into memory. An optimized index maps (grapheme count, normalized string) to correct forms.
2.  **Shared Memory**: The index is stored in Ray's Object Store to minimize serialization overhead.
3.  **Parallel Execution**: Input is chunked and distributed to worker nodes.
4.  **Matching Strategy**:
    -   **Exact Match**: Fast lookup in the pre-computed index.
    -   **Local Fuzzy**: If index has ambiguous matches, fuzzy search runs on that subset.
    -   **Global Fuzzy**: Fallback to full-database fuzzy search for unknown names.

## 5. Tech Stack
-   **Python 3.10**
-   **Ray**: Distributed parallel processing.
-   **Pandas**: Data manipulation and chunking.
-   **RapidFuzz**: High-performance string matching.
-   **Regex**: Text normalization.

## 6. Data Sources
-   **Reference CSV**: `surnames_all.csv` (Source of truth).
-   **Input CSV**: `input_names.csv` (Requires `Person_Name` column).

## 7. Key Design Decisions
-   **3-Stage Matching**: Prioritizes O(1) index lookups. Expensive O(N) fuzzy search is only used as a last resort.
-   **Ray Object Store**: Prevents copying the large reference dictionary to every worker process.
-   **Diacritic Union**: When matching, the algorithm merges diacritics rather than blindly replacing, preserving base letter differences in ambiguous cases.

## 8. Limitations
-   **No File Output**: The script currently calculates corrections but **does not write them to a file**. Results are only logged or held in memory.
-   **Hardcoded Paths**: File paths are hardcoded in `main.py`.
-   **Memory Usage**: Requires sufficient RAM to hold the entire reference dataset and its index.
-   **Reference Dependency**: Unknown names cannot be corrected without a generic fallback (which is not implemented).

## 9. How to Run
1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Ensure data files (`surnames_all.csv`, `input_names.csv`) are present.
3.  Run the script:
    ```bash
    python main.py
    ```

## 10. Example Usage
**Input:**
```text
Novak
Dvorak
Kucera
```

**Reference Database:**
```text
Novák
Dvořák
Kučera
```

**Result:**
```text
Novák
Dvořák
Kučera
```

## 11. Future Improvements
-   **Implement Output**: serialization of results to CSV/Parquet.
-   **CLI Arguments**: Replace hardcoded paths with `argparse`.
-   **Performance**: Implement blocking for the global fuzzy search to reduce search space.

## 12. Author
Jan Alexandr Kopřiva
jan.alexandr.kopriva@gmail.com

## 13. License
MIT
