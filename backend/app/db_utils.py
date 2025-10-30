import duckdb
import logging
from pathlib import Path

# --------------------------------------------
# ✅ Dynamically locate your project root
# --------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
DB_PATH = BASE_DIR / "data" / "agri_climate_db.duckdb"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_connection():
    """Connect to the DuckDB database."""
    try:
        conn = duckdb.connect(str(DB_PATH))
        logging.info(f"✅ Connected to DuckDB at {DB_PATH}")
        return conn
    except Exception as e:
        logging.error(f"❌ Failed to connect to DuckDB: {e}")
        raise


def run_sql_query(query: str):
    """Execute a SQL query and return the results as a list of dicts."""
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
# 🔹 Database Initialization (auto-loads CSVs if DB is missing)
# ============================================================
def initialize_database():
    """Auto-load CSVs from /data/processed into DuckDB if not already loaded."""
    csv_files = {
        "crop_production_raw": DATA_DIR / "crop_production_raw.csv",
        "groundwater_raw": DATA_DIR / "groundwater_raw.csv",
        "rainfall_raw": DATA_DIR / "rainfall_raw.csv",
        "market_price": DATA_DIR / "market_price.csv",
        "temperature": DATA_DIR / "temperature.csv",
    }

    conn = duckdb.connect(str(DB_PATH))
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
    if tables:
        logging.info("📊 Available tables: " + ", ".join([t[0] for t in tables]))
    else:
        logging.warning("⚠️ No tables found in DuckDB!")

    conn.close()


# Initialize once at startup
initialize_database()
