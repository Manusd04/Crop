import duckdb
from pathlib import Path
from contextlib import contextmanager

# Dynamically detect path — works on both local and Render
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "agri_climate_db.duckdb"

def get_connection():
    """Reusable DuckDB connection"""
    return duckdb.connect(str(DB_PATH))

@contextmanager
def db_connection():
    """Context manager for auto-closing DuckDB connections"""
    conn = duckdb.connect(str(DB_PATH))
    try:
        yield conn
    finally:
        conn.close()
