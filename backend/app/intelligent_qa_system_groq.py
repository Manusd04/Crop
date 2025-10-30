#!/usr/bin/env python3
"""
AgriClimate Intelligent Q&A System - Complete Final Version
Handles both simple and complex multi-step queries with human-like answers
"""
import duckdb
import re
import os
import logging
from dotenv import load_dotenv
from groq import Groq
from schema import SCHEMA
from pathlib import Path

# -----------------------------------------------------------------------------
# SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables (.env for local, Render env for production)
load_dotenv()

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
# ✅ Dynamically resolve DB path
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"

# First try environment variable (Render will set this), else local fallback
DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "agri_climate_db.duckdb"))
MODEL = "llama-3.3-70b-versatile"

# Groq API Key
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("❌ GROQ_API_KEY not found. Add it in .env or Render dashboard.")

# Initialize Groq client
client = Groq(api_key=GROQ_API_KEY)

# -----------------------------------------------------------------------------
# COMPLEXITY DETECTION
# -----------------------------------------------------------------------------
def is_complex_query(question: str) -> bool:
    """Detect if query requires multi-step processing"""
    complex_indicators = [
        r'compare.*with', r'comparison', r'difference between',
        r'highest.*lowest', r'maximum.*minimum', r'max.*min',
        r'both.*and', r'versus', r'\bvs\b',
        r'calculate.*difference', r'percent difference',
        r'latest year.*compare', r'which.*highest.*which.*lowest'
    ]
    question_lower = question.lower()
    return any(re.search(ind, question_lower) for ind in complex_indicators)

# -----------------------------------------------------------------------------
# SQL UTILITIES
# -----------------------------------------------------------------------------
def clean_sql(query: str) -> str:
    """Extract, sanitize, and normalize SQL query for DuckDB compatibility"""
    import re

    # 🧹 Step 1: Basic cleanup (your original logic)
    query = query.strip()
    query = re.sub(r'^```sql\s*', '', query, flags=re.IGNORECASE)
    query = re.sub(r'^```|\s*```$', '', query)
    query = query.strip()
    
    if re.search(r'^[a-z_]+\s+AS\s+\(', query, re.IGNORECASE) and not query.upper().startswith('WITH'):
        query = 'WITH ' + query
    
    match = re.search(r'((?:WITH|SELECT)\s+.*)', query, re.IGNORECASE | re.DOTALL)
    if match:
        query = match.group(1)

    # 🧠 Step 2: Replace PostgreSQL-style functions with DuckDB equivalents
    replacements = {
        r"\bTO_DATE\s*\(": "STRPTIME(",
        r"\bto_date\s*\(": "STRPTIME(",
        r"'YYYY-MM-DD'": "'%Y-%m-%d'",
        r"ILIKE": "LIKE",  # DuckDB doesn’t have ILIKE (case-insensitive LIKE)
        r"::DATE": "",      # remove Postgres-style type casting
    }
    for pattern, replacement in replacements.items():
        query = re.sub(pattern, replacement, query)

    # 🧩 Step 3: Final cleanup
    return query.rstrip(';').strip()


def execute_sql(sql: str):
    """Execute SQL query and return results"""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        result_df = con.execute(sql).df()
        con.close()
        return result_df.to_dict('records')
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        raise

# -----------------------------------------------------------------------------
# SIMPLE QUERY HANDLER
# -----------------------------------------------------------------------------
def execute_simple_query(question: str) -> dict:
    """Handle simple single-step queries"""
    schema_desc = "\n".join([f"- {t}: {', '.join(c)}" for t, c in SCHEMA.items()])

    sql_prompt = f"""You are an expert SQL analyst. Convert this question into ONE valid DuckDB SQL query.

Database schema:
{schema_desc}

Rules:
1. Columns are lowercase with underscores.
2. Use ILIKE for text (e.g., WHERE state ILIKE '%punjab%')
3. Use = for numbers (e.g., WHERE crop_year = 2020)
4. For rice: (crop ILIKE '%rice%' OR crop ILIKE '%paddy%')
5. If using CTEs, start with WITH keyword
6. Return ONLY SQL, no explanation
7. LIMIT 100 rows

Question: {question}
SQL:"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": sql_prompt}],
        temperature=0.2,
        max_tokens=800
    )
    
    sql_query = clean_sql(response.choices[0].message.content)
    logger.info(f"Generated SQL: {sql_query}")

    sql_query = re.sub(
        r'(crop_year|s_no|sr__no_|year)\s+ILIKE\s+[\'"]%(\d+)%[\'"]',
        r'\1 = \2', sql_query, flags=re.IGNORECASE
    )

    rows = execute_sql(sql_query)
    answer = generate_simple_answer(question, rows)
    
    return {"answer": answer, "sql": sql_query, "rows": rows}

def generate_simple_answer(question: str, rows: list) -> str:
    if not rows:
        return "No data found for that question. Try including location or year."

    data_summary = str(rows[:10]) if len(rows) <= 10 else f"First 10 of {len(rows)} rows"
    answer_prompt = f"""You are an agricultural analyst. Answer conversationally.

Question: {question}
Data: {data_summary}

Use:
- Natural language (friendly tone)
- 2-4 sentences
- Mention specific values if visible
Answer:"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": answer_prompt}],
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Answer generation failed: {e}")
        return f"Found {len(rows)} result(s)."

# -----------------------------------------------------------------------------
# COMPLEX QUERY HANDLER
# -----------------------------------------------------------------------------
def execute_complex_query(question: str) -> dict:
    plan = create_query_plan(question)
    results = execute_plan_steps(plan)
    answer = generate_complex_answer(question, results)
    return {"answer": answer, "sql": f"Multi-step ({plan['num_steps']} steps)", "rows": results.get('final_data', [])}

def create_query_plan(question: str) -> dict:
    schema_desc = "\n".join([f"- {t}: {', '.join(c)}" for t, c in SCHEMA.items()])
    prompt = f"""Break this complex question into SQL steps.

Database: crop_production_raw(state, district, crop, crop_year, season, area, production, yield)
Question: {question}

Each step should look like:
STEP 1: ...
SQL: SELECT ... FROM crop_production_raw WHERE ...
"""
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=1000
    )
    text = response.choices[0].message.content
    steps = [m.group(1).strip().rstrip(';') for m in re.finditer(r'SQL:\s*(SELECT.*?)(?=\n(?:STEP|\Z))', text, re.DOTALL)]
    return {"num_steps": len(steps), "steps": steps, "plan_text": text}

def execute_plan_steps(plan: dict) -> dict:
    results, all_data = {}, []
    for i, sql in enumerate(plan['steps'], 1):
        for key, value in results.items():
            sql = sql.replace(f"{{{{{key}}}}}", str(value))
        rows = execute_sql(sql)
        all_data.extend(rows)
        if rows:
            for k, v in rows[0].items():
                results[k] = v
    return {"variables": results, "final_data": all_data, "num_steps": plan['num_steps']}

def generate_complex_answer(question: str, results: dict) -> str:
    prompt = f"""You are an agricultural analyst.

Question: {question}
Results: {results['variables']}

Explain in plain English:
1. Mention both regions/crops compared
2. Show their production values
3. Give percent difference
4. Keep it concise (3–4 lines)
Answer:"""
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=400
    )
    return response.choices[0].message.content.strip()

# -----------------------------------------------------------------------------
# MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def run_intelligent_query(question: str) -> dict:
    if is_complex_query(question):
        logger.info("→ Complex query detected")
        return execute_complex_query(question)
    else:
        logger.info("→ Simple query detected")
        return execute_simple_query(question)

# -----------------------------------------------------------------------------
# LOCAL TEST
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("🧪 Testing AgriClimate Q&A System\n")
    tests = [
        "What is rice production in Punjab?",
        "Compare Punjab and West Bengal rice production in the latest year"
    ]
    for q in tests:
        print("=" * 80)
        print(f"Q: {q}")
        try:
            res = run_intelligent_query(q)
            print(f"\nSQL: {res['sql']}")
            print(f"Rows: {len(res['rows'])}")
            print(f"\nAnswer:\n{res['answer']}\n")
        except Exception as e:
            print(f"❌ Error: {e}")
