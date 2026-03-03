import os
import pandas as pd
import unicodedata
import re
from pathlib import Path

# ----------------------------
# PATH CONFIG
# ----------------------------

# Chemin racine du projet (un niveau au-dessus de /Code/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "normalized"

OUTPUT_PATH.mkdir(exist_ok=True)

print(f"Projet détecté : {PROJECT_ROOT}")
print(f"Dossier de sortie : {OUTPUT_PATH}")

# ----------------------------
# UTILS
# ----------------------------

def normalize_columns(df):
    def clean(col):
        col = str(col).strip().lower()
        col = unicodedata.normalize('NFD', col)
        col = col.encode('ascii', 'ignore').decode('utf-8')
        col = re.sub(r'\s+', '_', col)
        col = re.sub(r'[^a-z0-9_]', '', col)
        return col
    
    df.columns = [clean(c) for c in df.columns]
    return df


def is_year(col):
    return re.fullmatch(r"\d{4}", str(col)) is not None


def is_month(col):
    return re.fullmatch(r"\d{4}-\d{2}", str(col)) is not None


def clean_numeric(df):
    for col in df.columns:
        df[col] = df[col].replace(",", ".", regex=True)
    return df


# ----------------------------
# PROCESS FILE
# ----------------------------

def process_file(file_path):
    print(f"Traitement : {file_path}")

    try:
        df = pd.read_csv(file_path, sep=";", encoding="utf-8")
    except:
        df = pd.read_csv(file_path, sep=";", encoding="latin1")

    df = normalize_columns(df)
    df = clean_numeric(df)

    filename = Path(file_path).stem

    # ----------------------------
    # ELECTIONS
    # ----------------------------
    if "municipales" in str(file_path).lower():
        df = df[[c for c in df.columns if not c.startswith("ratio_")]]
        df.to_csv(OUTPUT_PATH / f"{filename}_normalized.csv", index=False)
        return

    # ----------------------------
    # PAUVRETE (déjà long)
    # ----------------------------
    if "filosofi" in str(file_path).lower():
        df.to_csv(OUTPUT_PATH / f"{filename}_normalized.csv", index=False)
        return

    # ----------------------------
    # DEMOGRAPHIE (wide → long)
    # ----------------------------

    year_cols = [c for c in df.columns if is_year(c)]
    month_cols = [c for c in df.columns if is_month(c)]

    if year_cols:
        id_vars = [c for c in df.columns if c not in year_cols]
        df_long = df.melt(
            id_vars=id_vars,
            value_vars=year_cols,
            var_name="annee",
            value_name="valeur"
        )
        df_long["date"] = pd.to_datetime(df_long["annee"], errors="coerce")
        df_long.drop(columns=["annee"], inplace=True)
        df_long.to_csv(OUTPUT_PATH / f"{filename}_normalized.csv", index=False)
        return

    if month_cols:
        id_vars = [c for c in df.columns if c not in month_cols]
        df_long = df.melt(
            id_vars=id_vars,
            value_vars=month_cols,
            var_name="mois",
            value_name="valeur"
        )
        df_long["date"] = pd.to_datetime(df_long["mois"], errors="coerce")
        df_long.drop(columns=["mois"], inplace=True)
        df_long.to_csv(OUTPUT_PATH / f"{filename}_normalized.csv", index=False)
        return

    # ----------------------------
    # DEFAULT
    # ----------------------------
    df.to_csv(OUTPUT_PATH / f"{filename}_normalized.csv", index=False)


# ----------------------------
# LOOP ALL CSV (EXCEPT /Code/)
# ----------------------------

for root, dirs, files in os.walk(PROJECT_ROOT):
    
    # Ignorer le dossier Code
    if "Code" in root:
        continue

    for file in files:
        if file.endswith(".csv"):
            full_path = Path(root) / file
            process_file(full_path)

print("\nTous les fichiers ont été nettoyés et normalisés.")