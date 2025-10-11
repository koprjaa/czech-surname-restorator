# Czech Surname Corrector

Nástroj pro automatickou opravu českých příjmení v CSV souborech. Porovnává příjmení s referenční databází a doplňuje chybějící diakritiku pomocí fuzzy matching algoritmů. Používá Ray pro paralelní zpracování velkých datových souborů a rapidfuzz pro inteligentní vyhledávání podobných příjmení. Užitečný pro čištění dat z různých zdrojů, kde může chybět správná diakritika.

## Instalace

```bash
pip install -r requirements.txt
```

## Použití

```python
# Načtení referenčních příjmení a vytvoření indexu
prijmeni_df = pd.read_csv("prijmeni.csv")
prijmeni_list = prijmeni_df["column1"].dropna().tolist()
dict_index = vytvor_index(prijmeni_list)

# Oprava příjmení v batch
opravene = oprav_prijmeni_ray.remote(batch, dict_index)
```

Výstup: Příjmení s doplněnou diakritikou podle referenční databáze.

## Licence

MIT License - viz [LICENSE](LICENSE) soubor.
