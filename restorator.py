#
# Project: czech-surname-restorator
# File:    restorator.py
#
# Description:
# Restores diacritics to a Czech surname, by index lookup first and a fuzzy search over the whole reference list second.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT
#

import unicodedata

import regex as re
from rapidfuzz import process

# Score a candidate has to reach when it comes from the full reference list. The
# search is wider, so a weaker match is still the best signal available.
GLOBAL_MATCH_THRESHOLD = 75

# Candidates pulled from the full reference list before the threshold applies.
GLOBAL_SEARCH_LIMIT = 5

# Every letter Czech writes. Slovak adds a, l and r forms this set leaves out,
# which is exactly how the Slovak spelling of Kral was winning.
CZECH_LETTERS = frozenset("aábcčdďeéěfghiíjklmnňoópqrřsštťuúůvwxyýzž")


def remove_diacritics(text: str) -> str:
    """Strip combining marks, so that Dvořák and Dvorak compare equal."""
    return "".join(c for c in unicodedata.normalize("NFD", text) if not unicodedata.combining(c))


def grapheme_count(text: str) -> int:
    """Number of user-visible characters. A base letter and its mark count as one."""
    return len(re.findall(r"\X", unicodedata.normalize("NFD", text)))


def index_key(name: str) -> tuple[int, str]:
    """Key that groups every spelling of one name: length plus its ASCII form."""
    return grapheme_count(name), remove_diacritics(name).lower()


def merge_marks(original: str, reference: str) -> str:
    """Merge the combining marks of two graphemes that share a base letter.

    The original wins on the base letter. A mark present in either input ends up
    in the result, so a mark the user typed is never lost.
    """
    original_nfd = unicodedata.normalize("NFD", original)
    reference_nfd = unicodedata.normalize("NFD", reference)
    if not original_nfd:
        return reference
    if not reference_nfd:
        return original

    base = original_nfd[0]
    if base != reference_nfd[0]:
        # Different letters entirely. Keep what the user wrote.
        return original

    marks = list(original_nfd[1:])
    marks.extend(m for m in reference_nfd[1:] if m not in marks)
    return unicodedata.normalize("NFC", base + "".join(marks))


def apply_diacritics(original: str, reference: str) -> str:
    """Copy the marks of the reference spelling onto the original name.

    Characters past the end of the reference stay as the user typed them, so a
    longer original is never truncated to the length of the reference.
    """
    original_clusters = re.findall(r"\X", unicodedata.normalize("NFD", original))
    reference_clusters = re.findall(r"\X", unicodedata.normalize("NFD", reference))

    merged = [
        merge_marks(o, r)
        for o, r in zip(original_clusters, reference_clusters, strict=False)
    ]
    merged.extend(original_clusters[len(reference_clusters):])
    return unicodedata.normalize("NFC", "".join(merged))


def build_index(reference_names) -> dict[tuple[int, str], list[str]]:
    """Group the reference list by index key. Values keep every known spelling."""
    index: dict[tuple[int, str], list[str]] = {}
    for name in reference_names:
        if isinstance(name, str):
            index.setdefault(index_key(name), []).append(name)
    return index


def mark_count(name: str) -> int:
    """How many combining marks a spelling carries."""
    return sum(1 for c in unicodedata.normalize("NFD", name) if unicodedata.combining(c))


def is_czech_spelling(name: str) -> bool:
    """True when every letter of the name exists in the Czech alphabet.

    The reference list carries Slovak and Polish spellings of shared surnames.
    They are not wrong, they are just not Czech, and this tool restores Czech.
    """
    return all(not ch.isalpha() or ch.lower() in CZECH_LETTERS for ch in name)


def richest_spelling(candidates: list[str]) -> str:
    """The Czech candidate carrying the most combining marks.

    Every candidate under one index key shares the same ASCII form and the same
    length, so they are spellings of one name and differ only in marks and case.
    Scoring them against each other adds nothing. About a quarter of the
    reference list is written without diacritics, so the plain spelling would
    otherwise win against the input by exact match and restore nothing.

    Mark count alone is not enough. The list holds Kral, Král, Kráĺ and Kráľ, and
    the two Slovak spellings carry the most marks, so the most common Czech
    surname came back Slovak. Czech spellings are therefore preferred first, and
    mark count only decides between them. Ties fall back to the string, so the
    answer does not depend on the order the reference list arrived in.
    """
    return max(candidates, key=lambda cand: (is_czech_spelling(cand), mark_count(cand), cand))


def correct_name(name, index, all_reference_names) -> str:
    """Restore diacritics in one surname.

    Two stages, cheapest first. A key hit means the reference list already holds
    this name, and the richest spelling under that key is the answer. A key that
    is missing falls back to a fuzzy search over the whole reference list, which
    is the expensive path and the only one that needs scoring.
    """
    if not isinstance(name, str):
        return name

    candidates = index.get(index_key(name))
    if candidates:
        return apply_diacritics(name, richest_spelling(candidates))

    for candidate, score, _ in process.extract(
        name, all_reference_names, limit=GLOBAL_SEARCH_LIMIT
    ):
        if score >= GLOBAL_MATCH_THRESHOLD:
            return apply_diacritics(name, candidate)
    return name


def correct_batch(names, index, all_reference_names) -> list:
    """Restore diacritics in a list of surnames.

    Kept free of Ray so that it can be called and tested directly. The worker
    wrapper is built in main.py, where Ray is actually running.
    """
    return [correct_name(name, index, all_reference_names) for name in names]
