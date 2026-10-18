# czech-surname-restorator

Restores diacritics to Czech surnames in CSV files.

[![CI](https://github.com/koprjaa/czech-surname-restorator/actions/workflows/ci.yml/badge.svg)](https://github.com/koprjaa/czech-surname-restorator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-A31F34)](LICENSE)

## What it does

Uses a reference surname list to restore spellings such as `Dvorak` to `Dvořák`. Names without a direct match go through a fuzzy search and stay unchanged if no candidate meets the threshold.

Only the last whitespace-separated token counts as the surname; the first becomes the given name. Ambiguous spellings need review: `Horák` and `Hořák` share the same form without diacritics. The tool assumes surnames need marks restored, even when an unaccented spelling is valid.

## Install

Install Rust and Cargo, then run from the repository directory:

```bash
cargo install --path . --locked
```

Prepare UTF-8, comma-separated CSV files with headers. The input uses `Person_Name` for full names. The reference uses `column1` for surnames and defaults to `surnames_all.csv` in your working directory.

## Use

Write corrections to the default `output_corrected.csv`:

```bash
czech-surname-restorator input_names.csv
```

Choose another reference, column names and output path:

```bash
czech-surname-restorator input_names.csv --reference reference.csv --reference-column surname --name-column full_name --output corrected.csv
```

The output keeps input columns and writes `given_name`, `surname`, `corrected_surname` and `corrected_name`, replacing those columns if they already exist. Rows retain their input order. Processing logs go to stderr.

See all options:

```bash
czech-surname-restorator --help
```

## License

[MIT](LICENSE)
