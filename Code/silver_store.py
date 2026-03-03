# silver_store.py
import sqlite3
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = PROJECT_ROOT / "Bronze"
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
DB_PATH.parent.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH)


def normalize_pauvrete_2021_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns (after bronze_clean normalization):
      geo, geo_object, filosofi_measure, unit_measure, unit_mult,
      conf_status, obs_status, time_period, obs_value
    Output canonical columns for Gold:
      date, geo, geo_object, measure, valeur, unit_measure, unit_mult
    """
    required = {"geo", "geo_object", "filosofi_measure", "time_period", "obs_value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns for pauvrete normalization: {sorted(missing)}")

    out = df.rename(
        columns={
            "time_period": "date",
            "obs_value": "valeur",
            "filosofi_measure": "measure",
        }
    ).copy()

    keep = ["date", "geo", "geo_object", "measure"]
    for c in ["unit_measure", "unit_mult"]:
        if c in out.columns:
            keep.append(c)
    keep.append("valeur")
    out = out[keep]

    out["date"] = out["date"].astype(str)
    out["valeur"] = pd.to_numeric(out["valeur"], errors="coerce")

    # drop missing values
    out = out.dropna(subset=["valeur"])

    return out


for csv_file in BRONZE_PATH.glob("*.csv"):
    df = pd.read_csv(csv_file, low_memory=False)

    # raw table
    table_name = csv_file.stem.lower()
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"✔ {csv_file.name} stocké dans table {table_name}")

    # poverty canonical table
    try:
        df_norm = normalize_pauvrete_2021_schema(df)
        df_norm.to_sql("pauvrete_normalized", conn, if_exists="replace", index=False)
        print(f"✔ Table canonique créée: pauvrete_normalized (depuis {csv_file.name})")
    except Exception as e:
        print(f"ℹ {csv_file.name}: pas de normalisation pauvrete_normalized ({e})")

conn.close()