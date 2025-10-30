import duckdb
from pathlib import Path
from contextlib import contextmanager

# Path to your main DuckDB database
DB_PATH = Path("/Users/manusd/Crop/agri_climate_db.duckdb")

def get_connection():
    """
    Return a reusable DuckDB connection.
    Use this when you need to keep the connection open for multiple queries.
    """
    return duckdb.connect(str(DB_PATH))

@contextmanager
def db_connection():
    """
    Context manager for auto-closing DuckDB connections.
    Example:
        with db_connection() as conn:
            df = conn.execute("SELECT * FROM crop_production_raw LIMIT 5").fetchdf()
    """
    conn = duckdb.connect(str(DB_PATH))
    try:
        yield conn
    finally:
        conn.close()
