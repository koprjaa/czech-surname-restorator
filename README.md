# czech-surname-restorator

Restores diacritics to Czech surnames in large legacy datasets. `Dvorak` becomes `Dvořák`, `Kucera` becomes `Kučera`, `Kopriva` becomes `Kopřiva`. It uses Ray for distribution and rapidfuzz for the fallback search.

![python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)
![status](https://img.shields.io/badge/status-beta-22863A?style=flat-square)
[![ci](https://github.com/koprjaa/czech-surname-restorator/actions/workflows/ci.yml/badge.svg)](https://github.com/koprjaa/czech-surname-restorator/actions/workflows/ci.yml)

## What it does

Banks, insurers, and public sector databases often store Czech names without diacritics. Either the form accepted ASCII only, or the data came from a system that did. Restoring the marks by hand across millions of rows is not practical. A plain dictionary lookup fails on typos and on names the reference list does not hold.

The tool runs two stages. The first is cheap and answers almost every name. The second is expensive and only runs when the first finds nothing.

```
index key:  (grapheme_count, ascii_lowercase) -> [every known spelling]

input name
    |
    v
Stage 1  Index hit.     O(1) lookup. Take the spelling with the most marks.
    |    key not found
    v
Stage 2  Global fuzzy.  rapidfuzz over the whole reference list. Last resort.
```

Every spelling under one index key shares the same ASCII form and the same length, so they are the same name written with different marks. Scoring them against each other adds nothing, and the tool takes the richest spelling instead. That choice matters, because about a quarter of the reference list carries no marks at all. Scoring would let the plain spelling win against a plain input by exact match, and restore nothing.

The index goes into the Ray object store once. Every worker reads the same copy, so the reference list is not copied per process.

## Install

```bash
uv venv
uv pip install -r requirements.txt
```

## Use

```bash
python main.py input_names.csv -o corrected.csv
```

The input needs one column with the full name, `Person_Name` by default. The output keeps the original column and adds `given_name`, `surname`, `corrected_surname`, and `corrected_name`.

```
Person_Name,given_name,surname,corrected_surname,corrected_name
Jan Dvorak,Jan,Dvorak,Dvořák,Jan Dvořák
Petr Kucera,Petr,Kucera,Kučera,Petr Kučera
```

## Options

| Flag | Default | Effect |
|---|---|---|
| `-o, --output` | `output_corrected.csv` | Where to write the result. |
| `-r, --reference` | `surnames_all.csv` | Reference surname list. |
| `--name-column` | `Person_Name` | Column holding the full name. |
| `--reference-column` | `column1` | Column holding the reference surnames. |
| `--chunk-size` | 10000 | Rows read at a time. |
| `--batch-size` | 1000 | Names per Ray task. |
| `--num-cpus` | all | CPUs given to Ray. |
| `--log-file` | none | Also write the log to this file. |

## How it works

`multiprocessing.Pool` would also work, but it copies the index into every worker. That wastes memory and adds fork time. The Ray object store gives every worker a zero copy shared memory reference instead. For a reference list this size it is a convenience. For a production sized one it is the reason the design works.

The matching logic lives in `restorator.py` and imports nothing from Ray. `main.py` wraps it with `ray.remote` at run time. That keeps the library usable, and testable, without a cluster.

## Limits

The shipped reference list is `surnames_all.csv`, with 119,239 entries compiled from open Czech onomastic sources.

About 24 percent of the entries carry no diacritic at all. Some of those are genuine spellings and some are gaps in the source. The tool cannot tell the two apart, so it assumes the input is stripped legacy data and prefers the marked spelling. That assumption is what makes the tool work, and it is also its main limit: a name that really is spelled without marks gets marks added.

The list is not authoritative. It once held `Novak` and `Novaková` but neither `Novák` nor `Nováková`, so the two most common Czech surnames came back still wrong. That is fixed and there is a test over the twenty-four most common names, but a rarer name can carry the same kind of error. Check a sample against the list before trusting a run.

Other limits:

- The whole index must fit in memory. A much larger reference list needs an on-disk structure.
- A name missing from the reference list stays as it came in. There is no character level model to guess from.
- Only the last whitespace separated token is treated as the surname. A compound surname written with a space is not handled.

## Development

```bash
uv run --extra dev ruff check .
uv run --extra dev pytest -q
```

CI runs both on Python 3.10, 3.11, and 3.12, on Linux and Windows. The test suite covers the matching logic and needs no Ray cluster.

## License

[MIT](LICENSE)
