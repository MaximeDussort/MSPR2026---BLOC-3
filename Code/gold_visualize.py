# gold_visualize.py
import sqlite3, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
GOLD_PATH = PROJECT_ROOT / "Gold"
GOLD_PATH.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# Choisir une mesure (code FILOSOFI_MEASURE) et un niveau géo
MEASURE = "S_HH_TAX"      # <-- exemple, à adapter
GEO_OBJECT = "DEP"        # DEP=Département (sinon COM, REG, etc.)

try:
    df = pd.read_sql("SELECT * FROM pauvrete_normalized", conn)

    df = df[(df["measure"] == MEASURE) & (df["geo_object"] == GEO_OBJECT)]

    # moyenne par geo (département) sur l'année
    df_group = df.groupby("geo")["valeur"].mean().reset_index()

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_group, x="geo", y="valeur")
    plt.xticks(rotation=90)
    plt.title(f"{MEASURE} - valeur moyenne par {GEO_OBJECT}")
    plt.tight_layout()
    plt.savefig(GOLD_PATH / f"pauvrete_{MEASURE}_{GEO_OBJECT}.png")
    print("✔ Graphique généré")
except Exception as e:
    print(f"⚠ Erreur gold_visualize: {e}")

conn.close()