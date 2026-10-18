// Project: czech-surname-restorator
// File: tests/restorator.rs
// Description: Equivalents of every original matching and reference regression test.
// Author: Jan Alexandr Kopřiva
// License: MIT

use czech_surname_restorator::*;
use std::{collections::HashSet, path::Path};
fn reference() -> Vec<String> {
    [
        "Dvořák",
        "Dvorak",
        "Kučera",
        "Kopřiva",
        "Novák",
        "Nováková",
        "Černý",
        "Šťastný",
    ]
    .map(String::from)
    .to_vec()
}
fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}
fn shipped() -> Vec<String> {
    csv::Reader::from_path(Path::new(env!("CARGO_MANIFEST_DIR")).join("surnames_all.csv"))
        .unwrap()
        .records()
        .map(|r| r.unwrap()[0].to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}
const COMMON: &[&str] = &[
    "Novák",
    "Nováková",
    "Svoboda",
    "Svobodová",
    "Novotný",
    "Novotná",
    "Dvořák",
    "Dvořáková",
    "Černý",
    "Černá",
    "Procházka",
    "Procházková",
    "Kučera",
    "Kučerová",
    "Veselý",
    "Veselá",
    "Horák",
    "Horáková",
    "Němec",
    "Němcová",
    "Marek",
    "Marková",
    "Král",
    "Králová",
];
#[test]
fn remove_marks() {
    for (s, e) in [
        ("Dvořák", "Dvorak"),
        ("Šťastný", "Stastny"),
        ("Kopřiva", "Kopriva"),
        ("Smith", "Smith"),
        ("", ""),
    ] {
        assert_eq!(remove_diacritics(s), e);
    }
}
#[test]
fn graphemes_and_keys() {
    for (s, n) in [("Dvořák", 6), ("Dvorak", 6), ("Šťastný", 7), ("", 0)] {
        assert_eq!(grapheme_count(s), n);
    }
    assert_eq!(index_key("Dvořák"), index_key("Dvorak"));
    assert_eq!(index_key("Dvorak"), (6, "dvorak".into()));
    assert_eq!(grapheme_count("👩‍👩‍👧‍👧"), 1);
}
#[test]
fn merging() {
    for (a, b, e) in [
        ("r", "ř", "ř"),
        ("ř", "r", "ř"),
        ("k", "ř", "k"),
        ("", "ř", "ř"),
        ("ř", "", "ř"),
    ] {
        assert_eq!(merge_marks(a, b), e);
    }
}
#[test]
fn applying() {
    for (a, b, e) in [
        ("Dvorak", "Dvořák", "Dvořák"),
        ("Dvorakova", "Dvořák", "Dvořákova"),
        ("Dvořák", "Dvorak", "Dvořák"),
        ("Smith", "Dvořák", "Smith"),
    ] {
        assert_eq!(apply_diacritics(a, b), e);
    }
}
#[test]
fn grouping_and_non_strings() {
    let index = build_index(&reference());
    let mut variants = index[&index_key("Dvorak")].clone();
    variants.sort();
    assert_eq!(variants, strings(&["Dvorak", "Dvořák"]));
    assert_eq!(
        build_value_index(&[
            Value::Text("Novák".into()),
            Value::Null,
            Value::Number(42.0),
            Value::Number(f64::NAN)
        ]),
        build_index(&["Novák"])
    );
}
#[test]
fn direct_ties_order_and_existing_marks() {
    let names = reference();
    let index = build_index(&names);
    for (a, e) in [
        ("Kucera", "Kučera"),
        ("Dvorak", "Dvořák"),
        ("Kopřiva", "Kopřiva"),
    ] {
        assert_eq!(correct_name(a, &index, &names), e);
    }
    let forward = build_index(&["Dvorak", "Dvořák"]);
    let backward = build_index(&["Dvořák", "Dvorak"]);
    assert_eq!(correct_name("Dvorak", &forward, &[]), "Dvořák");
    assert_eq!(
        correct_name("Dvorak", &forward, &[]),
        correct_name("Dvorak", &backward, &[])
    );
}
#[test]
fn richest_and_marks() {
    assert_eq!(
        richest_spelling(&strings(&["Cerny", "Černy", "Černý"])),
        "Černý"
    );
    assert_eq!(richest_spelling(&strings(&["Smith"])), "Smith");
    for (s, n) in [("Dvorak", 0), ("Dvořák", 2), ("Šťastný", 3)] {
        assert_eq!(mark_count(s), n);
    }
}
#[test]
fn fuzzy_unknown_and_passthrough() {
    let all = reference();
    let index = build_index(&all);
    assert!(!index.contains_key(&index_key("Novakov")));
    assert!(correct_name("Novakov", &index, &all).starts_with("Nov"));
    assert_eq!(correct_name("Zzzzqqq", &index, &all), "Zzzzqqq");
    assert_eq!(correct_value(&Value::Null, &index, &all), Value::Null);
    assert_eq!(
        correct_value(&Value::Number(42.0), &index, &all),
        Value::Number(42.0)
    );
    // A missing index really must restore marks, rather than only preserve a prefix.
    assert_eq!(
        correct_name("Dvořakova", &Index::new(), &strings(&["Dvořák"])),
        "Dvořákova"
    );
    assert_eq!(weighted_ratio("abc", "abc"), 100.0);
    assert_eq!(weighted_ratio("abc", "xabcx"), 90.0);
    assert_eq!(weighted_ratio("abc def", "def abc"), 95.0);
    assert_eq!(weighted_ratio("abc", ""), 0.0);
}
#[test]
fn batches() {
    let all = reference();
    let index = build_index(&all);
    let names = vec![
        Value::Text("Kucera".into()),
        Value::Text("Kopriva".into()),
        Value::Null,
        Value::Text("Zzzzqqq".into()),
    ];
    assert_eq!(
        correct_batch(&names, &index, &all),
        vec![
            Value::Text("Kučera".into()),
            Value::Text("Kopřiva".into()),
            Value::Null,
            Value::Text("Zzzzqqq".into())
        ]
    );
    assert!(correct_batch(&[], &index, &all).is_empty());
}
#[test]
fn reference_members_and_duplicates() {
    let names = shipped();
    let unique: HashSet<_> = names.iter().collect();
    for name in COMMON {
        assert!(unique.contains(&name.to_string()), "missing {name}");
    }
    assert_eq!(names.len(), unique.len());
}
#[test]
fn common_restorations() {
    let names = shipped();
    let index = build_index(&names);
    for (a, e) in [
        ("Novak", "Novák"),
        ("Novakova", "Nováková"),
        ("Dvorak", "Dvořák"),
        ("Dvorakova", "Dvořáková"),
        ("Kucera", "Kučera"),
        ("Kucerova", "Kučerová"),
        ("Svobodova", "Svobodová"),
        ("Cerny", "Černý"),
        ("Cerna", "Černá"),
        ("Kral", "Král"),
        ("Kralova", "Králová"),
        ("Ruzicka", "Růžička"),
        ("Kolar", "Kolář"),
        ("Kolarova", "Kolářová"),
        ("Cermak", "Čermák"),
        ("Benes", "Beneš"),
        ("Dolezal", "Doležal"),
        ("Sedlacek", "Sedláček"),
        ("Vlcek", "Vlček"),
        ("Pospisil", "Pospíšil"),
        ("Navratil", "Navrátil"),
        ("Jelinek", "Jelínek"),
        ("Hajek", "Hájek"),
        ("Nemcova", "Němcová"),
    ] {
        assert_eq!(correct_name(a, &index, &names), e, "{a}");
    }
    for expected in COMMON {
        let got = correct_name(&remove_diacritics(expected), &index, &names);
        assert!(is_czech_spelling(&got), "{expected}: {got}");
    }
}
#[test]
fn czech_and_slovak() {
    for (s, b) in [
        ("Král", true),
        ("Kráľ", false),
        ("Kráĺ", false),
        ("Králová", true),
        ("Kráľová", false),
        ("Dvořák", true),
        ("Świerczyk", false),
        ("Novák", true),
        ("Smith", true),
    ] {
        assert_eq!(is_czech_spelling(s), b);
    }
    for (names, e) in [
        (&["Kral", "Král", "Kráĺ", "Kráľ"][..], "Král"),
        (&["Kralová", "Králová", "Kráĺová", "Kráľová"][..], "Králová"),
        (&["Kráľ"][..], "Kráľ"),
        (&["Cerny", "Černy", "Černý"][..], "Černý"),
    ] {
        assert_eq!(richest_spelling(&strings(names)), e);
    }
}

#[test]
fn combining_category_case_and_fuzzy_ties() {
    assert!(is_czech_spelling("\u{0345}"));
    assert_eq!(apply_diacritics("DVORAK", "Dvořák"), "DVORAK");
    assert_eq!(merge_marks("r\u{0301}", "ř"), "ŕ\u{030c}");
    assert_eq!(
        correct_name("Dvořakova", &Index::new(), &strings(&["Dvořák", "Dvořak"])),
        "Dvořakova"
    );
    // Equal scores retain the reference's first candidate.
    assert_eq!(
        correct_name("Dvorak", &Index::new(), &strings(&["Dvořak", "Dvorák"])),
        "Dvořak"
    );
    assert_eq!(
        correct_name("Dvorak", &Index::new(), &strings(&["Dvorák", "Dvořak"])),
        "Dvorák"
    );
}

#[test]
fn fuzzy_threshold_and_long_alignments() {
    assert_eq!(weighted_ratio("abcd", "ábcd"), 75.0);
    assert_eq!(
        correct_name("abcd", &Index::new(), &strings(&["ábcd"])),
        "ábcd"
    );
    assert_eq!(
        correct_name("abcd", &Index::new(), &strings(&["ábçd"])),
        "abcd"
    );
    let needle = "a".repeat(80);
    let haystack = format!("{}{}{}", "x".repeat(80), needle, "x".repeat(80));
    assert_eq!(weighted_ratio(&needle, &haystack), 90.0);
}

#[test]
fn empty_index_bucket_uses_fuzzy_fallback() {
    let mut index = Index::new();
    index.insert(index_key("Novak"), Vec::new());
    assert_eq!(correct_name("Novak", &index, &strings(&["Novák"])), "Novák");
}

#[test]
fn long_identical_fuzzy_strings_and_recent_unicode() {
    let name = "a".repeat(100_000);
    assert_eq!(weighted_ratio(&name, &name), 100.0);
    // The Rust Unicode database recognizes this letter added after Python 3.11's database.
    assert!(!is_czech_spelling("\u{1c89}"));
}
