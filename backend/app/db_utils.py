import duckdb
import logging
from pathlib import Path

DB_PATH = Path("/Users/manusd/Crop/agri_climate_db.duckdb")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def get_connection():
    try:
        conn = duckdb.connect(str(DB_PATH))
        logging.info(f"✅ Connected to DuckDB at {DB_PATH}")
        return conn
    except Exception as e:
        logging.error(f"❌ Failed to connect to DuckDB: {e}")
        raise

def run_sql_query(query: str):
    conn = None
    try:
        conn = get_connection()
        logging.info("🧩 Executing SQL query...")
        result = conn.execute(query)

        columns = [desc[0] for desc in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

        logging.info(f"✅ Query executed successfully. Rows fetched: {len(rows)}")
        return rows

    except Exception as e:
        logging.error(f"⚠️ SQL Execution error: {e}")
        return []
    finally:
        if conn:
            conn.close()
            logging.info("🔒 DuckDB connection closed.")

# ============================================================
# 🔹 Updated Database Initialization (points to /data/processed)
# ============================================================
def initialize_database():
    """Auto-load CSVs from /data/processed into DuckDB if not already loaded."""
    csv_dir = Path("/Users/manusd/Crop/data/processed")
    conn = duckdb.connect(str(DB_PATH))

    csv_files = {
        "crop_production_raw": csv_dir / "crop_production_raw.csv",
        "groundwater_raw": csv_dir / "groundwater_raw.csv",
        "rainfall_raw": csv_dir / "rainfall_raw.csv",
    }

    for table, path in csv_files.items():
        if path.exists():
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} AS
                SELECT * FROM read_csv_auto('{path}');
            """)
            logging.info(f"📥 Loaded {table} from {path.name}")
        else:
            logging.warning(f"⚠️ Missing CSV for {table}: {path}")

    tables = conn.execute("SHOW TABLES;").fetchall()
    logging.info("📊 Available tables in database: " + ", ".join([t[0] for t in tables]))

    conn.close()

initialize_database()
