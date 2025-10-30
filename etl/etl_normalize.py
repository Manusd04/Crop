import os
import logging
import pandas as pd
import duckdb
from pathlib import Path

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data"
OUT_DIR = BASE / "normalized"
OUT_DIR.mkdir(exist_ok=True)
DB_PATH = BASE / "agri_climate_duckdb"

ENCODINGS = ["utf-8", "latin1", "windows-1252"]

FILES = [
    "groundwater_raw.csv",
    "crop_production_raw.csv",
    "rainfall_raw.csv",
    "temperature.csv",
    "Market_Prices.csv"
]


# --- Helper functions ---
def try_read_csv(path):
    """Try multiple encodings until the file reads successfully."""
    for enc in ENCODINGS:
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False)
            logging.info(f"Read {path.name} with encoding {enc}")
            return df, enc
        except UnicodeDecodeError:
            logging.warning(f"Encoding {enc} failed for {path.name}, trying next...")
    raise UnicodeDecodeError(f"All encodings failed for {path.name}")


def normalize_headers(df):
    """Clean up column headers."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[()\-./]", "_", regex=True)
    )
    return df


def clean_dataframe(df):
    """Clean numeric and string columns."""
    for col in df.columns:
        if df[col].dtype == "object":
            df.loc[:, col] = df[col].astype(str).str.strip().replace({"nan": pd.NA})
        else:
            try:
                df.loc[:, col] = pd.to_numeric(df[col], errors="coerce")
            except Exception:
                continue
    return df


# --- Main ETL ---
def main():
    summary = []

    for file in FILES:
        path = DATA_DIR / file
        if not path.exists():
            logging.warning(f"{file} not found in data/. Skipping.")
            continue

        try:
            df, enc = try_read_csv(path)
        except Exception as e:
            logging.error(f"Failed to read {file}: {e}")
            continue

        orig_headers = "|".join(df.columns)
        df = normalize_headers(df)
        df = clean_dataframe(df)

        # Save normalized file
        normalized_path = OUT_DIR / file
        df.to_csv(normalized_path, index=False)
        logging.info(f"Processed: {file} → normalized and saved.")

        normalized_sample = "|".join(df.columns[:5])
        summary.append([file, enc, orig_headers, normalized_sample])

    # Write header summary
    pd.DataFrame(
        summary,
        columns=["file", "encoding", "original_headers", "normalized_sample"]
    ).to_csv(OUT_DIR / "header_summary.csv", index=False)

    # --- Load into DuckDB ---
    conn = duckdb.connect(DB_PATH)
    for csv_file in OUT_DIR.glob("*.csv"):
        if csv_file.name == "header_summary.csv":
            continue
        table_name = csv_file.stem
        logging.info(f"Loading {table_name} into DuckDB...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_file}', header=True);
        """)

    conn.close()
    logging.info(f"ETL complete. Summary: {OUT_DIR / 'header_summary.csv'}")
    logging.info(f"DuckDB ready at: {DB_PATH}")


if __name__ == "__main__":
    main()
