# gold_visualize.py
import sqlite3, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
GOLD_PATH = PROJECT_ROOT / "Gold"
GOLD_PATH.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# Exemple: visualiser la pauvreté par région
try:
    df = pd.read_sql("SELECT * FROM pauvrete_normalized", conn)
    df_group = df.groupby("region")["valeur"].mean().reset_index()

    plt.figure(figsize=(10, 6))
    sns.barplot(data=df_group, x="region", y="valeur")
    plt.xticks(rotation=45)
    plt.title("Valeur moyenne de pauvreté par région")
    plt.tight_layout()
    plt.savefig(GOLD_PATH / "pauvrete_region.png")
    print("✔ Graphique pauvreté par région généré")
except Exception as e:
    print(f"⚠ Pas de table pauvrete_normalized: {e}")

conn.close()