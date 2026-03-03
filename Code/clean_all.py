import os
import zipfile
import pandas as pd
import unicodedata
import re
from pathlib import Path

# ----------------------------
# CONFIG
# ----------------------------

ZIP_PATH = "MSPR2026---BLOC-3.zip"
EXTRACT_PATH = "data_raw"
OUTPUT_PATH = "data_normalized"

os.makedirs(EXTRACT_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ----------------------------
# EXTRACTION ZIP
# ----------------------------

with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
    zip_ref.extractall(EXTRACT_PATH)

print("ZIP extrait.")

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
    if "municipales" in file_path.lower():
        # Supprimer ratios recalculables
        df = df[[c for c in df.columns if not c.startswith("ratio_")]]
        df.to_csv(f"{OUTPUT_PATH}/{filename}_normalized.csv", index=False)
        return

    # ----------------------------
    # PAUVRETE (déjà long)
    # ----------------------------
    if "filosofi" in file_path.lower():
        df.to_csv(f"{OUTPUT_PATH}/{filename}_normalized.csv", index=False)
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
        df_long.to_csv(f"{OUTPUT_PATH}/{filename}_normalized.csv", index=False)
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
        df_long.to_csv(f"{OUTPUT_PATH}/{filename}_normalized.csv", index=False)
        return

    # ----------------------------
    # DEFAULT
    # ----------------------------
    df.to_csv(f"{OUTPUT_PATH}/{filename}_normalized.csv", index=False)


# ----------------------------
# LOOP ALL FILES
# ----------------------------

for root, dirs, files in os.walk(EXTRACT_PATH):
    for file in files:
        if file.endswith(".csv"):
            full_path = os.path.join(root, file)
            process_file(full_path)

print("Tous les fichiers ont été nettoyés et normalisés.")