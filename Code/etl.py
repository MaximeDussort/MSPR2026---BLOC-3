"""
ETL POC - Indicateurs socio-économiques & électoraux
Périmètre :
    - Niveau : Département
    - Période : 2016-2018
Clé principale : (code_departement, annee)
"""

import os
import pandas as pd
from sqlalchemy import create_engine


# =========================
# CONFIGURATION
# =========================

DATABASE_URL = "postgresql://postgres:password@localhost:5432/elections_db"

DATA_PATH = "data_raw"

START_YEAR = 2016
END_YEAR = 2018


# =========================
# EXTRACT
# =========================

def extract_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(DATA_PATH, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} introuvable")

    print(f"Extraction : {filename}")
    return pd.read_csv(path, encoding="utf-8")


# =========================
# TRANSFORM
# =========================

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def filter_years(df: pd.DataFrame) -> pd.DataFrame:
    if "annee" in df.columns:
        df = df[(df["annee"] >= START_YEAR) & (df["annee"] <= END_YEAR)]
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = filter_years(df)

    # ======================
    # TODO TRANSFORMATIONS
    # ======================
    # - Harmoniser code_departement (format string, padding si besoin)
    # - Gérer les valeurs manquantes
    # - Convertir types numériques
    # - Agréger données mensuelles en annuel
    # - Supprimer colonnes inutiles
    # - Vérifier doublons (code_departement, annee)

    return df


def merge_datasets(datasets: dict) -> pd.DataFrame:
    """
    Fusionne tous les datasets sur :
    (code_departement, annee)
    """

    base = datasets["emploi"]

    for key, df in datasets.items():
        if key == "emploi":
            continue

        base = base.merge(
            df,
            on=["code_departement", "annee"],
            how="left"
        )

    return base


# =========================
# LOAD
# =========================

def load(df: pd.DataFrame, table_name: str):
    print("Connexion base de données...")

    engine = create_engine(DATABASE_URL)

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",  # TODO: passer en append/incremental plus tard
        index=False
    )

    print(f"Chargement terminé dans la table : {table_name}")


# =========================
# PIPELINE PRINCIPAL
# =========================

def run_etl():
    print("===== ETL START =====")

    # --- Extraction ---
    datasets = {
        "emploi": extract_csv("emploi.csv"),
        "pauvrete": extract_csv("pauvrete.csv"),
        "demographie": extract_csv("demographie.csv"),
        "economie": extract_csv("economie.csv"),
        "securite": extract_csv("securite.csv"),
        "elections": extract_csv("elections.csv"),
    }

    # --- Transformation ---
    for key in datasets:
        datasets[key] = transform(datasets[key])

    # --- Merge ---
    final_df = merge_datasets(datasets)

    # --- Load ---
    load(final_df, "indicateurs_annuels")

    print("===== ETL END =====")


if __name__ == "__main__":
    run_etl()