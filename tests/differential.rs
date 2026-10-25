// Project: czech-surname-restorator
// File: tests/differential.rs
// Description: Frozen RapidFuzz oracle scores for adversarial Unicode and token inputs.
// Author: Jan Alexandr Kopřiva
// License: AGPL-3.0-or-later
use czech_surname_restorator::weighted_ratio;
#[test]
fn rapidfuzz_oracle_scores() {
    let mut reader = csv::Reader::from_reader(include_bytes!("fixtures/scores.csv").as_slice());
    let mut failures = Vec::new();
    for row in reader.records() {
        let row = row.unwrap();
        let expected: f64 = row[2].parse().unwrap();
        let actual = weighted_ratio(&row[0], &row[1]);
        if (expected - actual).abs() > 1e-9 {
            failures.push(format!(
                "{:?} / {:?}: Python {expected}, Rust {actual}",
                &row[0], &row[1]
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} differences:\n{}",
        failures.len(),
        failures
            .iter()
            .take(30)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn unicode_helper_oracle() {
    use czech_surname_restorator::*;
    let mut reader =
        csv::Reader::from_reader(include_bytes!("fixtures/unicode-oracle.csv").as_slice());
    for row in reader.records() {
        let row = row.unwrap();
        let (a, b) = (&row[0], &row[1]);
        assert_eq!(remove_diacritics(a), row[2], "{a:?}");
        assert_eq!(grapheme_count(a), row[3].parse::<usize>().unwrap(), "{a:?}");
        assert_eq!(index_key(a).1, row[4], "{a:?}");
        assert_eq!(merge_marks(a, b), row[5], "{a:?} / {b:?}");
        assert_eq!(apply_diacritics(a, b), row[6], "{a:?} / {b:?}");
        assert_eq!(mark_count(a), row[7].parse::<usize>().unwrap(), "{a:?}");
        assert_eq!(is_czech_spelling(a), row[8] == *"true", "{a:?}");
    }
}

#[test]
fn correction_oracle() {
    use czech_surname_restorator::*;
    let mut reader =
        csv::Reader::from_reader(include_bytes!("fixtures/corrections-oracle.csv").as_slice());
    for row in reader.records() {
        let row = row.unwrap();
        let names: Vec<String> = row[1].split('\n').map(String::from).collect();
        let index = if row[2] == *"true" {
            build_index(&names)
        } else {
            Index::new()
        };
        assert_eq!(
            correct_name(&row[0], &index, &names),
            row[3],
            "{:?}",
            &row[0]
        );
    }
}
