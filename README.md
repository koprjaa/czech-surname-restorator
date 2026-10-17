# czech-surname-restorator

Restores diacritics to Czech surnames in CSV files.

[![ci](https://github.com/koprjaa/czech-surname-restorator/actions/workflows/ci.yml/badge.svg)](https://github.com/koprjaa/czech-surname-restorator/actions/workflows/ci.yml)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)

`Dvorak` becomes `Dvořák`, `Kucera` becomes `Kučera`, `Kopriva` becomes `Kopřiva`. Legacy databases at banks, insurers and public bodies often hold names as plain ASCII, and fixing millions of rows by hand is not practical.

The tool assumes the input is stripped data, so a surname that really is spelled without marks gets marks added. Two real surnames with one ASCII form (`Horák` and `Hořák`) cannot be told apart. A name missing from the reference list stays unchanged. Only the last whitespace-separated token counts as the surname.

## Install

```bash
uv venv
uv pip install -r requirements.txt
```

Python 3.10 or newer. On Windows, Ray wheels exist only for Python 3.10 to 3.12.

## Use

```bash
python main.py input_names.csv -o corrected.csv
```

The input needs one column with the full name (`Person_Name` by default, change it with `--name-column`). The output keeps it and adds `given_name`, `surname`, `corrected_surname` and `corrected_name`.

```
Person_Name,given_name,surname,corrected_surname,corrected_name
Jan Dvorak,Jan,Dvorak,Dvořák,Jan Dvořák
```

Run `python main.py --help` for the other options.

## License

[MIT](LICENSE)
