// Project: czech-surname-restorator
// File: src/lib.rs
// Description: Unicode restoration and deterministic fuzzy surname matching.
// Author: Jan Alexandr Kopřiva
// License: MIT

use std::collections::HashMap;
use unicode_general_category::{get_general_category, GeneralCategory};
use unicode_normalization::{char::canonical_combining_class, UnicodeNormalization};
use unicode_segmentation::UnicodeSegmentation;

pub const GLOBAL_MATCH_THRESHOLD: f64 = 75.0;
pub const GLOBAL_SEARCH_LIMIT: usize = 5;
pub type Index = HashMap<(usize, String), Vec<String>>;

pub fn remove_diacritics(text: &str) -> String {
    text.nfd()
        .filter(|&c| canonical_combining_class(c) == 0)
        .collect()
}
pub fn grapheme_count(text: &str) -> usize {
    text.nfd().collect::<String>().graphemes(true).count()
}
pub fn index_key(name: &str) -> (usize, String) {
    (grapheme_count(name), remove_diacritics(name).to_lowercase())
}
pub fn merge_marks(original: &str, reference: &str) -> String {
    let mut o = original.nfd();
    let mut r = reference.nfd();
    let Some(base) = o.next() else {
        return reference.into();
    };
    let Some(other) = r.next() else {
        return original.into();
    };
    if base != other {
        return original.into();
    }
    let mut marks: Vec<char> = o.collect();
    for mark in r {
        if !marks.contains(&mark) {
            marks.push(mark);
        }
    }
    std::iter::once(base).chain(marks).nfc().collect()
}
pub fn apply_diacritics(original: &str, reference: &str) -> String {
    let o: String = original.nfd().collect();
    let r: String = reference.nfd().collect();
    let refs: Vec<_> = r.graphemes(true).collect();
    o.graphemes(true)
        .enumerate()
        .map(|(i, g)| {
            refs.get(i)
                .map_or_else(|| g.to_owned(), |r| merge_marks(g, r))
        })
        .collect::<String>()
        .nfc()
        .collect()
}
pub fn build_index<S: AsRef<str>>(names: &[S]) -> Index {
    let mut index = Index::new();
    for name in names {
        let name = name.as_ref();
        index.entry(index_key(name)).or_default().push(name.into());
    }
    index
}

// Explicit variants retain the original library's non-string passthrough behavior.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Text(String),
    Null,
    Number(f64),
}
pub fn build_value_index(names: &[Value]) -> Index {
    build_index(
        &names
            .iter()
            .filter_map(|v| match v {
                Value::Text(s) => Some(s),
                _ => None,
            })
            .collect::<Vec<_>>(),
    )
}
pub fn mark_count(name: &str) -> usize {
    name.nfd()
        .filter(|&c| canonical_combining_class(c) != 0)
        .count()
}
pub fn is_czech_spelling(name: &str) -> bool {
    name.chars().all(|c| {
        !matches!(
            get_general_category(c),
            GeneralCategory::UppercaseLetter
                | GeneralCategory::LowercaseLetter
                | GeneralCategory::TitlecaseLetter
                | GeneralCategory::ModifierLetter
                | GeneralCategory::OtherLetter
        ) || c
            .to_lowercase()
            .all(|c| "aábcčdďeéěfghiíjklmnňoópqrřsštťuúůvwxyýzž".contains(c))
    })
}
pub fn richest_spelling(candidates: &[String]) -> &str {
    candidates
        .iter()
        .max_by_key(|s| (is_czech_spelling(s), mark_count(s), s.as_str()))
        .expect("nonempty candidates")
}

fn ratio(a: &[char], b: &[char]) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 100.0;
    }
    let mut row = vec![0usize; b.len() + 1];
    for x in a {
        let mut diagonal = 0;
        for (j, y) in b.iter().enumerate() {
            let old = row[j + 1];
            row[j + 1] = if x == y {
                diagonal + 1
            } else {
                row[j + 1].max(row[j])
            };
            diagonal = old;
        }
    }
    (1.0 - (a.len() + b.len() - 2 * row[b.len()]) as f64 / (a.len() + b.len()) as f64) * 100.0
}
fn partial_ratio(a: &[char], b: &[char]) -> f64 {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if short.is_empty() {
        return if long.is_empty() { 100.0 } else { 0.0 };
    }
    let mut best: f64 = 0.0;
    // An optimal local alignment may overlap either edge of the longer string.
    for start in -(short.len() as isize) + 1..long.len() as isize {
        let lo = start.max(0) as usize;
        let hi = (start + short.len() as isize).min(long.len() as isize) as usize;
        best = best.max(ratio(short, &long[lo..hi]));
    }
    if short.len() == long.len() {
        for start in -(short.len() as isize) + 1..long.len() as isize {
            let lo = start.max(0) as usize;
            let hi = (start + short.len() as isize).min(long.len() as isize) as usize;
            best = best.max(ratio(long, &short[lo..hi]));
        }
    }
    best
}
fn chars(s: &str) -> Vec<char> {
    s.chars().collect()
}
fn token_scores(a: &str, b: &str, partial: bool) -> f64 {
    let mut a: Vec<_> = a
        .split(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
        .filter(|s| !s.is_empty())
        .collect();
    let mut b: Vec<_> = b
        .split(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
        .filter(|s| !s.is_empty())
        .collect();
    a.sort();
    b.sort();
    let score = |a: &str, b: &str| {
        if partial {
            partial_ratio(&chars(a), &chars(b))
        } else {
            ratio(&chars(a), &chars(b))
        }
    };
    let mut best = score(&a.join(" "), &b.join(" "));
    a.dedup();
    b.dedup();
    let common = a
        .iter()
        .filter(|t| b.contains(t))
        .copied()
        .collect::<Vec<_>>();
    if !common.is_empty() && partial {
        return 100.0;
    }
    let left = a
        .iter()
        .filter(|t| !b.contains(t))
        .copied()
        .collect::<Vec<_>>();
    let right = b
        .iter()
        .filter(|t| !a.contains(t))
        .copied()
        .collect::<Vec<_>>();
    let c = common.join(" ");
    let l = common
        .iter()
        .chain(left.iter())
        .copied()
        .collect::<Vec<_>>()
        .join(" ");
    let r = common
        .iter()
        .chain(right.iter())
        .copied()
        .collect::<Vec<_>>()
        .join(" ");
    if !a.is_empty() && !b.is_empty() {
        best = best.max(score(&l, &r));
        if !c.is_empty() {
            best = best.max(score(&c, &l)).max(score(&c, &r));
        }
    }
    best
}
pub fn weighted_ratio(a: &str, b: &str) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let ac = chars(a);
    let bc = chars(b);
    let base = ratio(&ac, &bc);
    let lengths = ac.len().max(bc.len()) as f64 / ac.len().min(bc.len()) as f64;
    if lengths < 1.5 {
        base.max(token_scores(a, b, false) * 0.95)
    } else {
        let scale = if lengths > 8.0 { 0.6 } else { 0.9 };
        base.max(partial_ratio(&ac, &bc) * scale)
            .max(token_scores(a, b, true) * 0.95 * scale)
    }
}
pub fn correct_name(name: &str, index: &Index, all: &[String]) -> String {
    if let Some(candidates) = index.get(&index_key(name)) {
        return apply_diacritics(name, richest_spelling(candidates));
    }
    if name.is_empty() || all.is_empty() {
        return name.into();
    }
    let mut best = GLOBAL_MATCH_THRESHOLD;
    let mut found = None;
    for candidate in all {
        let score = weighted_ratio(name, candidate);
        if score >= best && (found.is_none() || score > best) {
            best = score;
            found = Some(candidate);
        }
    }
    found.map_or_else(|| name.into(), |r| apply_diacritics(name, r))
}
pub fn correct_value(value: &Value, index: &Index, all: &[String]) -> Value {
    match value {
        Value::Text(s) => Value::Text(correct_name(s, index, all)),
        _ => value.clone(),
    }
}
pub fn correct_batch(values: &[Value], index: &Index, all: &[String]) -> Vec<Value> {
    values
        .iter()
        .map(|v| correct_value(v, index, all))
        .collect()
}
