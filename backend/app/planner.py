import pandas as pd
from pathlib import Path

# Path setup
BASE = Path(__file__).resolve().parents[2]  # goes two levels up from backend/app
NORMALIZED_DIR = BASE / "normalized"

def list_csv_headers():
    if not NORMALIZED_DIR.exists():
        print(f"❌ Directory not found: {NORMALIZED_DIR}")
        return

    csv_files = [f for f in NORMALIZED_DIR.glob("*.csv") if f.name != "header_summary.csv"]

    if not csv_files:
        print("⚠️ No CSV files found in normalized folder.")
        return

    print(f"✅ Found {len(csv_files)} CSV files in {NORMALIZED_DIR}\n")

    summary_lines = []

    for file in csv_files:
        try:
            df = pd.read_csv(file, nrows=2)  # read first 2 rows for speed
            cols = ", ".join(df.columns)
            print(f"📄 {file.name}")
            print(f"   → Columns ({len(df.columns)}): {cols}\n")
            summary_lines.append(f"{file.name}: {cols}")
        except Exception as e:
            print
