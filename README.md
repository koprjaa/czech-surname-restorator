# czech-surname-restorator

**Restores diacritics to Czech surnames in large legacy datasets — Ray-distributed, rapidfuzz-backed, with a three-stage matching strategy.**

![python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)
![status](https://img.shields.io/badge/status-prototype-lightgrey?style=flat-square)
![ray](https://img.shields.io/badge/Ray-distributed-028CF0?style=flat-square)
![pandas](https://img.shields.io/badge/pandas-1.5+-150458?style=flat-square&logo=pandas&logoColor=white)
![rapidfuzz](https://img.shields.io/badge/rapidfuzz-string%20match-555?style=flat-square)

`Dvorak` → `Dvořák`. `Kucera` → `Kučera`. `Kopriva` → `Kopřiva`.

Banks, insurance companies, and legacy public-sector databases routinely store Czech names without diacritics — either because the form only accepted ASCII or because the data was migrated from a system that did. Manually restoring diacritics across millions of rows is a non-starter. A plain dictionary lookup fails on typos, abbreviations, and hyphenated compounds. This project does it with a three-stage matching pipeline that prioritises speed and falls back to fuzzy search only when it has to.

## Matching strategy

```
                ┌────────────────────────────────┐
                │ (grapheme_count, normalized)   │ ← index key
                │   →  {correct_form_1, form_2}  │
                └────────────┬───────────────────┘
 input name ─────────────────┤
                             ▼
               ┌─────── Stage 1: Exact ─────────┐
               │  O(1) dict lookup on index      │
               │  single candidate?  →  done     │
               └────────────┬────────────────────┘
                            │ ambiguous
                            ▼
               ┌─── Stage 2: Local fuzzy ───────┐
               │  rapidfuzz on the small        │
               │  candidate set from stage 1     │
               └────────────┬────────────────────┘
                            │ no match
                            ▼
               ┌─── Stage 3: Global fuzzy ──────┐
               │  rapidfuzz across full DB       │
               │  (expensive; last resort)       │
               └─────────────────────────────────┘
```

The index is stored once in Ray's Object Store and referenced by every worker, so the big reference dictionary (surnames_all.csv) isn't copied per-process.

## Install + run

```bash
uv venv
uv pip install -r requirements.txt
# ensure surnames_all.csv and input_names.csv (with 'Person_Name' column) exist
python main.py
```

The ref DB (`surnames_all.csv`) ships with the repo — ~1.2 MB, compiled from open Czech onomastic sources.

## Known limits

- **No file output yet** — the pipeline computes corrections and logs them but doesn't serialise to a file. Wire up a `.to_csv()` or `.to_parquet()` at the tail of `main.py` before relying on this in a data pipeline.
- **Hardcoded paths** — no `argparse`; edit the file names in `main.py`.
- **RAM-bound** — the full index must fit in memory. For the included 60k-surname ref this is trivial; for a much larger DB you'd switch to an on-disk FST.
- **Unknown names stay unknown** — no generic diacritic-restoration fallback (e.g. character-level ML). Only names in the reference DB get corrected.

## Why Ray instead of multiprocessing

`multiprocessing.Pool` would work but copies the index per worker, wasting RAM and adding fork-time latency. Ray's Object Store gives you a zero-copy shared-memory reference that all workers read. For a 60k-row index it's a nice-to-have; for a production-sized one it's load-bearing.

## License

[MIT](LICENSE)
