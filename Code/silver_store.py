# silver_store.py
import sqlite3
import pandas as pd
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = PROJECT_ROOT / "Bronze"
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
DB_PATH.parent.mkdir(exist_ok=True)

def clean_name(name: str) -> str:
    """
    Nettoie les noms de fichiers pour en faire des noms SQL propres
    """
    name = name.lower()
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = re.sub(r"[éèêë]", "e", name)
    name = re.sub(r"[àâ]", "a", name)
    name = re.sub(r"[ùû]", "u", name)
    name = re.sub(r"[ô]", "o", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    return name

conn = sqlite3.connect(DB_PATH)

tables_grouped = {}

for csv_file in BRONZE_PATH.glob("*.csv"):
    try:
        df = pd.read_csv(
            csv_file,
            dtype=str,
            low_memory=False,
            encoding="utf-8",
            sep=","
        )

        base_name = clean_name(csv_file.stem)

        # Classification intelligente
        if "pauvrete" in base_name:
            table_name = "pauvrete_normalized"
        elif "presidentielle" in base_name:
            table_name = "presidentielle"
        elif "legislatives" in base_name:
            table_name = "legislatives"
        elif "municipales" in base_name:
            table_name = "municipales"
        elif "emploi" in base_name:
            table_name = "emploi"
        elif "population" in base_name:
            table_name = "population"
        elif "naissance" in base_name:
            table_name = "naissances"
        elif "deces" in base_name:
            table_name = "deces"
        else:
            table_name = base_name

        if table_name not in tables_grouped:
            tables_grouped[table_name] = []

        tables_grouped[table_name].append(df)

        print(f"✔ {csv_file.name} préparé pour table {table_name}")

    except Exception as e:
        print(f"Erreur lecture {csv_file.name} : {e}")

# Fusion et stockage
for table_name, df_list in tables_grouped.items():
    df_final = pd.concat(df_list, ignore_index=True)
    df_final.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Table finale créée : {table_name} ({len(df_final)} lignes)")

conn.close()
print("SILVER terminé")