# run_pipeline.py

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CODE_PATH = PROJECT_ROOT / "Code"

SCRIPTS = [
    "bronze_clean.py",
    "silver_store.py",
    "gold_visualize.py",
    "gold_predict.py"
]


def run_script(script_name):
    script_path = CODE_PATH / script_name
    print(f"\nLancement : {script_name}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Erreur dans {script_name}")
        print(result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)
        print(f"{script_name} terminé avec succès")


def main():
    print("====================================")
    print("      PIPELINE BRONZE → GOLD")
    print("====================================")

    for script in SCRIPTS:
        run_script(script)

    print("\nPipeline complet exécuté avec succès !")


if __name__ == "__main__":
    main()