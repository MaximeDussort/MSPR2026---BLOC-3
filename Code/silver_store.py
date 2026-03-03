# silver_store.py
import sqlite3, pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = PROJECT_ROOT / "Bronze"
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
DB_PATH.parent.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# Exemple simple: lecture tous les CSV Bronze et insertion dans la DB
for csv_file in BRONZE_PATH.glob("*.csv"):
    df = pd.read_csv(csv_file)
    table_name = csv_file.stem.lower()
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"✔ {csv_file.name} stocké dans table {table_name}")

conn.close()