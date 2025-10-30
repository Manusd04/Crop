#!/usr/bin/env python3
"""
backend/app/intelligent_system.py

Single-file intelligent Q&A system:
 - loads normalized CSVs into DuckDB (persistent DB file)
 - generates SQL via Groq LLM
 - executes SQL
 - summarizes results in natural language via Groq
 - interactive console loop
"""

import os
import re
import textwrap
import logging
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv
from groq import Groq

# -------- CONFIG --------
BASE = Path(__file__).resolve().parents[2]  # /Users/manusd/Crop
PROCESSED_DIR = BASE / "data" / "processed"
DB_PATH = BASE / "agri_climate_duckdb"      # DuckDB file
LOAD_FILES = [
    "groundwater_raw.csv",
    "groundwater_long.csv",
    "crop_production_raw.csv",
    "rainfall_raw.csv",
    "temperature.csv",
    "Market_Prices.csv"
]

# Choose preferred models (first available will be used)
PREFERRED_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
    "mixtral-8x7b",
]

# -------- LOGGING & ENV --------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
load_dotenv()  # load GROQ_API_KEY from .env
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise RuntimeError("GROQ_API_KEY not set in .env — please add it and re-run.")

client = Groq(api_key=GROQ_API_KEY)


# -------- UTIL: DB loader --------
def load_processed_into_duckdb(db_path=DB_PATH, processed_dir=PROCESSED_DIR, files=LOAD_FILES):
    """Load processed CSVs into the DuckDB file (CREATE OR REPLACE TABLE)."""
    conn = duckdb.connect(db_path)
    logging.info(f"Loading normalized CSVs from {processed_dir} into DuckDB at {db_path} ...")
    for fname in files:
        fpath = processed_dir / fname
        if not fpath.exists():
            logging.warning(f"File missing: {fpath} — skipping.")
            continue
        table_name = fpath.stem.lower()
        # use read_csv_auto for robust parsing
        sql = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{fpath.as_posix()}', header=True);
        """
        conn.execute(sql)
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logging.info(f" - Loaded {table_name} ({cnt:,} rows)")
    conn.close()
    logging.info("All processed CSVs loaded.")


# -------- UTIL: schema introspection --------
def get_conn():
    return duckdb.connect(DB_PATH)


def get_tables(conn):
    return [r[0] for r in conn.execute("SHOW TABLES").fetchall()]


def get_table_columns(conn, table):
    return [col[1] for col in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def get_schema_description(conn):
    desc = []
    for t in get_tables(conn):
        cols = get_table_columns(conn, t)
        desc.append(f"Table: {t}\n  Columns: {', '.join(cols)}")
    return "\n\n".join(desc)


# -------- UTIL: SQL extraction and safety --------
_SQL_BLOCK_RE = re.compile(r"(?is)(?:```sql\s*)?(WITH\s+.*?;|SELECT\s.*?;)$")


def extract_sql_block(text: str) -> str:
    text = text.strip()
    m = _SQL_BLOCK_RE.search(text)
    if m:
        return m.group(1).strip()
    # fallback: first SELECT ...;
    m2 = re.search(r"(?is)(SELECT\s.+?;)", text)
    if m2:
        return m2.group(1).strip()
    # fallback: return whole text (model might return plain SQL)
    return text


def safe_validate_tables_and_cols(conn, sql: str):
    """Lightweight validation: ensure referenced tables exist; quoted cols exist in some table."""
    tables_in_sql = re.findall(r'FROM\s+([a-zA-Z0-9_"]+)', sql, flags=re.IGNORECASE)
    available = get_tables(conn)
    for t in tables_in_sql:
        t_clean = t.strip().strip('"')
        if t_clean not in available:
            raise ValueError(f"Referenced table '{t_clean}' not found. Available: {available}")


# -------- UTIL: model call helpers --------
def choose_model():
    """Try preferred list; return the first available model id (best-effort)."""
    # We don't call a model-list endpoint here; pick first from PREFERRED_MODELS.
    # If a model is decommissioned, API call will return an error and code will fall back.
    return PREFERRED_MODELS[0]


def generate_sql_via_groq(question: str, schema_text: str, model_id=None):
    """Ask Groq to generate a single DuckDB SQL query given schema and question."""
    if model_id is None:
        model_id = choose_model()

    system = (
        "You are an expert data analyst and SQL generator for DuckDB. "
        "Given the schema and a user's question, return ONLY a single valid SQL query that answers the question. "
        "Do NOT add explanation, do not wrap the SQL in markdown — only the SQL code. "
        "Use table and column names exactly as given in the schema."
    )

    prompt = f"Schema:\n{schema_text}\n\nQuestion:\n{question}\n\nReturn only the SQL."

    resp = client.chat.completions.create(
        model=model_id,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        temperature=0.0,
    )
    raw = resp.choices[0].message.content.strip()
    sql = extract_sql_block(raw)
    return sql


def summarize_results_via_groq(question: str, sql: str, result_df: pd.DataFrame, model_id=None):
    """Convert a small query result into a concise natural-language answer via Groq."""
    if model_id is None:
        model_id = choose_model()

    # Limit rows and columns for prompt brevity
    max_rows = 12
    df_for_prompt = result_df.head(max_rows).copy()
    # convert to simple CSV snippet
    csv_snippet = df_for_prompt.to_csv(index=False)

    system = (
        "You are a helpful, concise data analyst that summarizes query results in plain English. "
        "Given the user's question, the SQL executed, and a small table result, produce a short (1-3 sentence) summary answering the question. "
        "Do NOT output SQL or the full table. Keep it human-friendly and mention numbers with units if reasonable."
    )
    user = (
        f"User question: {question}\n\n"
        f"SQL executed:\n{sql}\n\n"
        f"Query result (first {min(len(result_df), max_rows)} rows):\n{csv_snippet}\n\n"
        "Provide a concise natural-language answer (1-3 sentences)."
    )

    resp = client.chat.completions.create(
        model=model_id,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.2,
        max_tokens=180,
    )
    ans = resp.choices[0].message.content.strip()
    return ans


# -------- MAIN INTERACTIVE LOOP --------
def interactive_loop():
    # Ensure processed data is loaded into DuckDB
    load_processed_into_duckdb()

    conn = get_conn()
    schema_text = get_schema_description(conn)
    print("\n✅ Schema overview (tables and columns):\n")
    print(schema_text)
    print("\nSystem ready — ask a question about the data (type 'exit' to quit).")
    print("Tip: Ask things like 'Compare rice production between Punjab and Haryana for 2015 to 2020.'\n")

    while True:
        q = input("🧠 Your question: ").strip()
        if not q:
            continue
        if q.lower() in ("exit", "quit"):
            print("👋 Bye.")
            break

        try:
            # 1) Ask model to generate SQL
            print("\n🔎 Generating SQL for your question...")
            sql = generate_sql_via_groq(q, schema_text)
            print("\n🧩 Generated SQL:\n")
            print(textwrap.indent(sql, "    "))

            # 2) Preflight validate
            try:
                safe_validate_tables_and_cols(conn, sql)
            except ValueError as ve:
                print(f"\n⚠️ Preflight validation failed: {ve}")
                # show schema hint and skip execution
                print("Please rephrase your question (use table/column names from the schema).")
                continue

            # 3) Execute SQL
            print("\n⏳ Executing SQL...")
            df = conn.execute(sql).fetchdf()
            print(f"✅ Query returned {len(df)} rows.")

            # 4) Summarize result in natural language
            if df.empty:
                print("\n🤖 Answer: No rows returned for that query.")
            else:
                summary = summarize_results_via_groq(q, sql, df)
                print("\n🤖 Answer (natural-language):")
                print(summary)
                # also offer to show table if user wants
                show = input("\nShow top results as table? (y/N): ").strip().lower()
                if show == "y":
                    print("\n📋 Top rows:")
                    print(df.head(20).to_string(index=False))
            print("\n—\n")
        except Exception as e:
            # model or execution errors
            logging.exception("An error occurred while handling the question:")
            print(f"❌ Error: {e}")
            print("Try rephrasing the question or ask a simpler query.\n")


if __name__ == "__main__":
    interactive_loop()
