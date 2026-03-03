# gold_predict.py
import sqlite3, pandas as pd
from pathlib import Path
from sklearn.model_selection import TimeSeriesSplit
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "Silver" / "data.db"
GOLD_PATH = PROJECT_ROOT / "Gold"
GOLD_PATH.mkdir(exist_ok=True)

MEASURE = "S_HH_TAX"   # <-- exemple, à adapter

conn = sqlite3.connect(DB_PATH)

try:
    df = pd.read_sql("SELECT * FROM pauvrete_normalized", conn)
    df = df[df["measure"] == MEASURE]

    df["annee"] = pd.to_datetime(df["date"], errors="coerce").dt.year
    df = df.dropna(subset=["annee", "valeur"])

    df_group = df.groupby("annee")["valeur"].mean().reset_index()

    X = df_group["annee"].values.reshape(-1, 1)
    y = df_group["valeur"].values

    tscv = TimeSeriesSplit(n_splits=3)
    plt.figure(figsize=(10, 6))

    for i, (train_idx, test_idx) in enumerate(tscv.split(X)):
        model = LinearRegression().fit(X[train_idx], y[train_idx])
        pred = model.predict(X[test_idx])
        plt.plot(X[test_idx], pred, label=f"Split {i + 1} prediction")

    plt.plot(X, y, "o-", label="Réel")
    plt.title(f"Prédiction ({MEASURE}) moyenne par année")
    plt.xlabel("Année")
    plt.ylabel("Valeur moyenne")
    plt.legend()
    plt.savefig(GOLD_PATH / f"pauvrete_prediction_{MEASURE}.png")
    print("✔ Modèle prédictif et graphique généré")
except Exception as e:
    print(f"⚠ Erreur modèle prédictif: {e}")

conn.close()