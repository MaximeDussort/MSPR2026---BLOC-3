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

conn = sqlite3.connect(DB_PATH)

# Exemple: prédiction pauvreté moyenne par année
try:
    df = pd.read_sql("SELECT * FROM pauvrete_normalized", conn)
    df["annee"] = pd.to_datetime(df["date"]).dt.year
    df_group = df.groupby("annee")["valeur"].mean().reset_index()

    X = df_group["annee"].values.reshape(-1, 1)
    y = df_group["valeur"].values

    tscv = TimeSeriesSplit(n_splits=3)
    plt.figure(figsize=(10, 6))

    for i, (train_idx, test_idx) in enumerate(tscv.split(X)):
        model = LinearRegression().fit(X[train_idx], y[train_idx])
        pred = model.predict(X[test_idx])
        plt.plot(X[test_idx], pred, label=f"Split {i + 1} prediction")

    plt.plot(X, y, 'o-', label="Réel")
    plt.title("Prédiction pauvreté moyenne (CV temporelle)")
    plt.xlabel("Année")
    plt.ylabel("Valeur moyenne")
    plt.legend()
    plt.savefig(GOLD_PATH / "pauvrete_prediction.png")
    print("Modèle prédictif et graphique généré")
except Exception as e:
    print(f"Erreur modèle prédictif: {e}")

conn.close()