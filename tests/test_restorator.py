#
# Project: czech-surname-restorator
# File:    test_restorator.py
#
# Description:
# Tests for the matching logic and for the surnames the shipped reference list has to cover.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT
#

"""Tests for the surname diacritic restoration logic.

Nothing here needs Ray. correct_batch is a plain function and the CLI builds the
Ray worker around it, so the matching logic is testable on its own.
"""

import csv
from pathlib import Path

import pytest

from restorator import (
    apply_diacritics,
    build_index,
    correct_batch,
    correct_name,
    grapheme_count,
    index_key,
    mark_count,
    merge_marks,
    remove_diacritics,
    richest_spelling,
)

REFERENCE = [
    "Dvořák",
    "Dvorak",
    "Kučera",
    "Kopřiva",
    "Novák",
    "Nováková",
    "Černý",
    "Šťastný",
]


@pytest.fixture
def index():
    return build_index(REFERENCE)


@pytest.fixture
def flat():
    return list(REFERENCE)


# --- normalization helpers --------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Dvořák", "Dvorak"),
        ("Šťastný", "Stastny"),
        ("Kopřiva", "Kopriva"),
        ("Smith", "Smith"),
        ("", ""),
    ],
)
def test_remove_diacritics(text, expected):
    assert remove_diacritics(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [("Dvořák", 6), ("Dvorak", 6), ("Šťastný", 7), ("", 0)],
)
def test_grapheme_count_ignores_combining_marks(text, expected):
    assert grapheme_count(text) == expected


def test_index_key_is_identical_for_both_spellings():
    assert index_key("Dvořák") == index_key("Dvorak") == (6, "dvorak")


# --- merge_marks ------------------------------------------------------------


def test_merge_marks_takes_the_mark_from_the_reference():
    assert merge_marks("r", "ř") == "ř"


def test_merge_marks_keeps_a_mark_the_user_already_typed():
    assert merge_marks("ř", "r") == "ř"


def test_merge_marks_keeps_the_original_when_the_base_letter_differs():
    assert merge_marks("k", "ř") == "k"


@pytest.mark.parametrize(("original", "reference"), [("", "ř"), ("ř", "")])
def test_merge_marks_handles_an_empty_side(original, reference):
    assert merge_marks(original, reference) == (reference or original)


# --- apply_diacritics -------------------------------------------------------


def test_apply_diacritics_restores_a_plain_ascii_name():
    assert apply_diacritics("Dvorak", "Dvořák") == "Dvořák"


def test_apply_diacritics_keeps_characters_past_the_reference():
    """A longer original must not be cut down to the length of the reference."""
    assert apply_diacritics("Dvorakova", "Dvořák") == "Dvořákova"


def test_apply_diacritics_does_not_drop_marks_the_user_typed():
    assert apply_diacritics("Dvořák", "Dvorak") == "Dvořák"


def test_apply_diacritics_leaves_an_unrelated_name_alone():
    assert apply_diacritics("Smith", "Dvořák") == "Smith"


# --- build_index ------------------------------------------------------------


def test_build_index_groups_both_spellings_under_one_key(index):
    assert sorted(index[(6, "dvorak")]) == ["Dvorak", "Dvořák"]


def test_build_index_skips_non_string_entries():
    assert build_index(["Novák", None, 42, float("nan")]) == {(5, "novak"): ["Novák"]}


# --- correct_name -----------------------------------------------------------


def test_a_single_candidate_is_applied_directly(index, flat):
    assert correct_name("Kucera", index, flat) == "Kučera"


def test_a_tie_between_two_spellings_goes_to_the_one_with_diacritics(index, flat):
    """Dvorak and Dvořák share a key and both score 100 against a plain input.

    Breaking the tie by score alone leaves the answer up to list order, and half
    the time it returns the input unchanged. Restoring the marks is the job.
    """
    assert correct_name("Dvorak", index, flat) == "Dvořák"


def test_the_choice_is_stable_whatever_the_reference_order():
    forward = build_index(["Dvorak", "Dvořák"])
    backward = build_index(["Dvořák", "Dvorak"])
    assert correct_name("Dvorak", forward, []) == correct_name("Dvorak", backward, []) == "Dvořák"


def test_richest_spelling_picks_the_most_marked_variant():
    assert richest_spelling(["Cerny", "Černy", "Černý"]) == "Černý"
    assert richest_spelling(["Smith"]) == "Smith"


def test_mark_count_counts_combining_marks():
    assert mark_count("Dvorak") == 0
    assert mark_count("Dvořák") == 2
    assert mark_count("Šťastný") == 3


def test_a_name_missing_from_the_index_falls_back_to_the_full_list(index, flat):
    """Novakov has no key of its own, so only the global search can find Novák."""
    assert (6, "novakov") not in index
    assert correct_name("Novakov", index, flat).startswith("Nov")


def test_an_unknown_name_is_returned_unchanged(index, flat):
    assert correct_name("Zzzzqqq", index, flat) == "Zzzzqqq"


def test_a_non_string_passes_through(index, flat):
    assert correct_name(None, index, flat) is None
    assert correct_name(42, index, flat) == 42


def test_an_already_correct_name_survives(index, flat):
    assert correct_name("Kopřiva", index, flat) == "Kopřiva"


# --- correct_batch ----------------------------------------------------------


def test_correct_batch_preserves_order_and_length(index, flat):
    names = ["Kucera", "Kopriva", None, "Zzzzqqq"]
    result = correct_batch(names, index, flat)
    assert len(result) == len(names)
    assert result[0] == "Kučera"
    assert result[1] == "Kopřiva"
    assert result[2] is None
    assert result[3] == "Zzzzqqq"


def test_correct_batch_on_an_empty_list(index, flat):
    assert correct_batch([], index, flat) == []


# --- the shipped reference list ---------------------------------------------


REFERENCE_CSV = Path(__file__).resolve().parent.parent / "surnames_all.csv"


def shipped_names():
    with REFERENCE_CSV.open(encoding="utf-8") as f:
        return [row["column1"] for row in csv.DictReader(f) if row.get("column1")]


# The most common Czech surnames, male and female. A reference list that cannot
# restore these cannot restore a real dataset either.
COMMON = [
    "Novák", "Nováková", "Svoboda", "Svobodová", "Novotný", "Novotná",
    "Dvořák", "Dvořáková", "Černý", "Černá", "Procházka", "Procházková",
    "Kučera", "Kučerová", "Veselý", "Veselá", "Horák", "Horáková",
    "Němec", "Němcová", "Marek", "Marková", "Král", "Králová",
]


@pytest.mark.parametrize("name", COMMON)
def test_a_common_surname_is_in_the_reference(name):
    assert name in set(shipped_names())


@pytest.mark.parametrize(
    ("stripped", "expected"),
    [
        ("Novak", "Novák"),
        ("Novakova", "Nováková"),
        ("Dvorak", "Dvořák"),
        ("Kucera", "Kučera"),
        ("Svobodova", "Svobodová"),
        ("Cerny", "Černý"),
    ],
)
def test_a_common_surname_is_restored_from_its_ascii_form(stripped, expected):
    """The reference used to hold Novak and Novaková but neither correct form,
    so the two most common Czech surnames came back still wrong."""
    names = shipped_names()
    assert correct_name(stripped, build_index(names), names) == expected


def test_the_reference_holds_no_duplicate_rows():
    names = shipped_names()
    assert len(names) == len(set(names))
