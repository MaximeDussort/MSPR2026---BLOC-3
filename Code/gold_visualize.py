# gold_visualize.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
GOLD_PATH = PROJECT_ROOT / "Gold"
GOLD_PATH.mkdir(exist_ok=True)

def table_exists(conn, table_name):
    query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
    """
    return pd.read_sql(query, conn, params=(table_name,)).shape[0] > 0


def get_possible_columns(df):
    """
    Détecte automatiquement une colonne région et une colonne valeur
    """
    region_candidates = [col for col in df.columns if "region" in col.lower()]
    value_candidates = [col for col in df.columns if "valeur" in col.lower()
                        or "taux" in col.lower()
                        or "niveau" in col.lower()]

    region_col = region_candidates[0] if region_candidates else None
    value_col = value_candidates[0] if value_candidates else None

    return region_col, value_col


def main():
    if not DB_PATH.exists():
        print("Base de données introuvable :", DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)

    table_name = "pauvrete_normalized"

    if not table_exists(conn, table_name):
        print(f"Table '{table_name}' inexistante.")
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'", conn
        )
        print("Tables disponibles :")
        print(tables)
        conn.close()
        return

    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        if df.empty:
            print("Table vide.")
            return

        region_col, value_col = get_possible_columns(df)

        if not region_col or not value_col:
            print("Impossible de détecter automatiquement les colonnes région/valeur.")
            print("Colonnes disponibles :", df.columns.tolist())
            return

        # Nettoyage
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        df = df.dropna(subset=[region_col, value_col])

        df_group = (
            df.groupby(region_col)[value_col]
            .mean()
            .reset_index()
            .sort_values(by=value_col, ascending=False)
        )

        plt.figure(figsize=(12, 6))
        sns.barplot(data=df_group, x=region_col, y=value_col)
        plt.xticks(rotation=45)
        plt.title("Valeur moyenne de pauvreté par région")
        plt.tight_layout()

        output_path = GOLD_PATH / "pauvrete_region.png"
        plt.savefig(output_path)
        plt.close()

        print("Graphique généré :", output_path)

    except Exception as e:
        print("Erreur :", e)

    conn.close()


if __name__ == "__main__":
    main()