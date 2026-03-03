import os
import pandas as pd
import unicodedata
import re
from pathlib import Path

# ============================
# PATH CONFIG
# ============================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "CleanedCSVs"
OUTPUT_PATH.mkdir(exist_ok=True)

print(f"Projet : {PROJECT_ROOT}")
print(f"Sortie : {OUTPUT_PATH}")

# ============================
# UTILS
# ============================

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

def read_csv_smart(path):
    """Lit un CSV en essayant plusieurs encodages et séparateurs"""
    for encoding in ["utf-8", "latin1"]:
        for sep in [";", ","]:
            try:
                return pd.read_csv(path, sep=sep, encoding=encoding, engine='python')
            except Exception:
                continue
    # Si toujours impossible, lever exception
    raise ValueError(f"Impossible de lire le CSV : {path}")

# ============================
# COLLECT DATA
# ============================

demographie_frames = []
election_frames = []
pauvrete_data = []
pauvrete_meta = []

for root, dirs, files in os.walk(PROJECT_ROOT):
    # Ignorer le dossier Code et l'environnement virtuel
    if "Code" in root or ".venv" in root:
        continue

    for file in files:
        if not file.endswith(".csv"):
            continue

        full_path = Path(root) / file
        print(f"Lecture : {full_path}")

        try:
            df = read_csv_smart(full_path)
        except Exception as e:
            print(f"⚠ Erreur lecture {full_path} : {e}")
            continue

        df = normalize_columns(df)
        df = clean_numeric(df)
        cols = set(df.columns)

        # ----------------------------
        # DETECTION PAR STRUCTURE
        # ----------------------------

        # ELECTIONS
        if {"inscrits", "votants", "exprimes"}.issubset(cols):
            df = df[[c for c in df.columns if not c.startswith("ratio_")]]
            election_frames.append(df)
            continue

        # PAUVRETE DATA
        if "obs_value" in cols and "time_period" in cols:
            pauvrete_data.append(df)
            continue

        # PAUVRETE META
        if {"cod_var", "lib_var"}.issubset(cols):
            pauvrete_meta.append(df)
            continue

        # DEMOGRAPHIE (wide)
        year_cols = [c for c in df.columns if is_year(c)]
        month_cols = [c for c in df.columns if is_month(c)]

        if year_cols or month_cols:
            if year_cols:
                id_vars = [c for c in df.columns if c not in year_cols]
                df = df.melt(
                    id_vars=id_vars,
                    value_vars=year_cols,
                    var_name="annee",
                    value_name="valeur"
                )
                df["date"] = pd.to_datetime(df["annee"], errors="coerce")
                df.drop(columns=["annee"], inplace=True)

            if month_cols:
                id_vars = [c for c in df.columns if c not in month_cols]
                df = df.melt(
                    id_vars=id_vars,
                    value_vars=month_cols,
                    var_name="mois",
                    value_name="valeur"
                )
                df["date"] = pd.to_datetime(df["mois"], errors="coerce")
                df.drop(columns=["mois"], inplace=True)

            demographie_frames.append(df)
            continue

# ============================
# FUSIONS
# ============================

# 1️⃣ DEMOGRAPHIE
if demographie_frames:
    df_demo = pd.concat(demographie_frames, ignore_index=True)
    df_demo.to_csv(OUTPUT_PATH / "demographie_normalized.csv", index=False)
    print("✔ demographie_normalized.csv créé")

# 2️⃣ ELECTIONS
if election_frames:
    df_elec = pd.concat(election_frames, ignore_index=True)
    if "id_election" in df_elec.columns:
        df_elec["annee"] = df_elec["id_election"].astype(str).str.extract(r"(\d{4})")
    df_elec.to_csv(OUTPUT_PATH / "elections_municipales_normalized.csv", index=False)
    print("✔ elections_municipales_normalized.csv créé")

# 3️⃣ PAUVRETE
if pauvrete_data:
    df_pauv = pd.concat(pauvrete_data, ignore_index=True)
    if pauvrete_meta:
        df_meta = pd.concat(pauvrete_meta, ignore_index=True)
        df_pauv = df_pauv.merge(
            df_meta,
            left_on="filosofi_measure",
            right_on="cod_mod",
            how="left"
        )
    df_pauv.to_csv(OUTPUT_PATH / "pauvrete_normalized.csv", index=False)
    print("✔ pauvrete_normalized.csv créé")

print("\nETL terminé proprement.")