# czech-surname-restorator

Restores diacritics to Czech surnames in large legacy datasets. `Dvorak` becomes `Dvořák`, `Kucera` becomes `Kučera`, `Kopriva` becomes `Kopřiva`. It uses Ray for distribution and rapidfuzz for the fuzzy stages.

![python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)
![status](https://img.shields.io/badge/status-prototype-lightgrey?style=flat-square)

## What it does

Banks, insurers, and public sector databases often store Czech names without diacritics. Either the form accepted ASCII only, or the data came from a system that did. Restoring the marks by hand across millions of rows is not practical. A plain dictionary lookup fails on typos, abbreviations, and hyphenated compounds.

This tool runs a three stage match. Each stage is more expensive than the one before it, so most names never reach the last stage.

```
index key:  (grapheme_count, normalized) -> {correct_form_1, correct_form_2}

input name
    |
    v
Stage 1  Exact.         O(1) lookup in the index. One candidate ends the search.
    |    ambiguous
    v
Stage 2  Local fuzzy.   rapidfuzz over the small candidate set from stage 1.
    |    no match
    v
Stage 3  Global fuzzy.  rapidfuzz over the full reference database. Last resort.
```

The index goes into the Ray object store once. Every worker reads the same copy, so the reference dictionary is not copied per process.

## Install

```bash
uv venv
uv pip install -r requirements.txt
```

## Use

Place `surnames_all.csv` and `input_names.csv` in the project root. The input file needs a `Person_Name` column.

```bash
python main.py
```

The reference database ships with the repository. It holds about 60,000 surnames in 1.2 MB, compiled from open Czech onomastic sources.

## How it works

`multiprocessing.Pool` would also work, but it copies the index into every worker. That wastes memory and adds fork time. The Ray object store gives every worker a zero copy shared memory reference instead. For a 60,000 row index this is a convenience. For a production sized index it is the reason the design works.

## Limits

- No file output yet. The pipeline computes the corrections and logs them, but it does not write them anywhere. Add a `.to_csv()` or `.to_parquet()` call at the end of `main.py` before you use this in a data pipeline.
- Paths are fixed in the source. There is no argument parser. Edit the file names in `main.py`.
- The full index must fit in memory. A much larger reference database needs an on-disk structure.
- Only names that exist in the reference database get corrected. There is no character level fallback for unknown names.
- No tests.

## License

[MIT](LICENSE)
