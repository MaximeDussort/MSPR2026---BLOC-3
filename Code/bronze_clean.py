# bronze_clean.py
import os, pandas as pd, unicodedata, re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = PROJECT_ROOT / "Bronze"
BRONZE_PATH.mkdir(exist_ok=True)

def normalize_columns(df):
    def clean(col):
        col = str(col).strip().lower()
        col = unicodedata.normalize('NFD', col)
        col = col.encode('ascii','ignore').decode('utf-8')
        col = re.sub(r'\s+','_',col)
        col = re.sub(r'[^a-z0-9_]','',col)
        return col
    df.columns = [clean(c) for c in df.columns]
    return df

def read_csv_smart(path):
    for encoding in ["utf-8","latin1"]:
        for sep in [";",","]:
            try: return pd.read_csv(path, sep=sep, encoding=encoding, engine='python')
            except: continue
    raise ValueError(f"Impossible de lire {path}")

for root, dirs, files in os.walk(PROJECT_ROOT):
    if "Code" in root or ".venv" in root: continue
    for file in files:
        if not file.endswith(".csv"): continue
        full_path = Path(root)/file
        try:
            df = read_csv_smart(full_path)
        except Exception as e:
            print(f"Erreur lecture {full_path}: {e}")
            continue
        df = normalize_columns(df)
        df.to_csv(BRONZE_PATH / file, index=False)
        print(f"{file} nettoyé et stocké en Bronze")