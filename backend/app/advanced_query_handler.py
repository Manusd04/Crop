#!/usr/bin/env python3
"""
AgriClimate Intelligent Q&A System Core Logic
Converts natural language to SQL and generates human-like answers
"""
import duckdb
import re
import os
import logging
from dotenv import load_dotenv
from groq import Groq
from schema import SCHEMA

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === LOAD ENVIRONMENT VARIABLES ===
load_dotenv()

# === CONFIG ===
# Use DB_PATH from environment if available, else fallback to local path
DB_PATH = os.getenv("DB_PATH", "/Users/manusd/Crop/data/processed/agri_climate_db.duckdb")
MODEL = "llama-3.3-70b-versatile"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in environment. Please set it in Render or .env file.")

# === INIT GROQ ===
client = Groq(api_key=GROQ_API_KEY)

# === HELPER: Clean SQL ===
def clean_sql(query: str) -> str:
    """Extract and clean SQL query from LLM response"""
    query = query.strip()
    
    # Remove markdown code blocks
    query = re.sub(r'^```sql\s*', '', query, flags=re.IGNORECASE)
    query = re.sub(r'^```\s*', '', query)
    query = re.sub(r'```$', '', query)
    query = query.strip()
    
    # Extract SELECT statement if embedded in text
    if not query.upper().startswith('SELECT'):
        match = re.search(r'(SELECT\s+.*?;?)\s*$', query, re.IGNORECASE | re.DOTALL)
        if match:
            query = match.group(1)
    
    # Remove trailing semicolon
    query = query.rstrip(';').strip()
    return query

# === GENERATE HUMAN ANSWER ===
def generate_human_answer(question: str, sql: str, rows: list) -> str:
    """
    Use LLM to generate a natural, conversational answer from query results
    """
    if len(rows) == 0:
        return (
            "I couldn't find any data matching your question. "
            "This could mean the data doesn't exist in our database, "
            "or it might be recorded under different terms. "
            "Try rephrasing your question or being more specific about the location, crop, or time period."
        )
    
    # Prepare data summary for LLM
    data_summary = str(rows) if len(rows) <= 10 else f"First 10 rows: {str(rows[:10])}... (Total {len(rows)} rows)"
    
    # Create prompt for natural answer generation
    answer_prompt = f"""You are a helpful agricultural data analyst. A user asked you a question about agricultural data, and you've retrieved the relevant information from the database.

User's Question: {question}

Data Retrieved:
{data_summary}

Your task: Write a natural, conversational answer that:
1. Directly addresses the user's question
2. Presents key insights from the data in plain English
3. Uses a friendly, professional tone (like talking to a farmer or researcher)
4. Includes specific numbers and facts from the data
5. Keeps the response concise (2-4 sentences for simple queries, more for complex ones)
6. If there are trends or patterns, mention them
7. Avoid technical jargon - explain like you're having a conversation

DO NOT:
- Say "based on the data" or "according to the database"
- Mention SQL or technical terms
- Just list numbers without context
- Be overly formal or robotic

Write a helpful, human answer:"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": answer_prompt}],
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return f"I found {len(rows)} result(s) for your question. The data shows various records matching your criteria."

# === MAIN QUERY FUNCTION ===
def run_intelligent_query(question: str) -> dict:
    """
    Takes a natural language question and returns:
    {
        "answer": "Human-like conversational response",
        "sql": "Generated SQL query",
        "rows": [...] # List of dictionaries
    }
    """
    schema_desc = "\n".join([f"- {table}: {', '.join(cols)}" for table, cols in SCHEMA.items()])
    
    sql_prompt = f"""You are an expert SQL analyst. Convert this question into a valid DuckDB SQL query.

Available tables and columns:
{schema_desc}

IMPORTANT RULES:
1. Return ONLY the SQL query, no explanation
2. Use exact column names from the schema (case-sensitive)
3. For crop_production_raw: Use "State", "District", "Crop", "Crop_Year", "Season", "Area", "Production", "Yield"
4. For groundwater_raw: Use columns like "State_Name_With_LGD_Code", "District_Name_With_LGD_Code", etc.
5. For rainfall_raw: Use "District", "Period_Actual(mm)", "Period_Normal(mm)", etc.
6. For market_prices: Use "State", "District", "Commodity", "Modal_x0020_Price", "Arrival_Date", etc.
7. For temperature: Use "YEAR", "ANNUAL", "JAN-FEB", "MAR-MAY", "JUN-SEP", "OCT-DEC"
8. Use ILIKE ONLY for TEXT/VARCHAR columns (e.g., WHERE "Crop" ILIKE '%rice%')
9. For NUMERIC columns, use = or comparison operators (e.g., WHERE "Crop_Year" = 2020)
10. Always wrap column names with special characters in double quotes
11. Limit results to 100 rows maximum using LIMIT 100
12. For averages, use AVG() and ROUND() to 2 decimal places
13. Order results logically (e.g., by year DESC, by value DESC)

CRITICAL:
- Text columns: Use ILIKE for matching
- Numeric columns: Use = for exact match

User question: {question}

SQL Query:"""

    try:
        # Step 1: Generate SQL using Groq
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": sql_prompt}],
            temperature=0.2,
            max_tokens=500
        )
        
        sql_query = clean_sql(response.choices[0].message.content)
        logger.info(f"Generated SQL: {sql_query}")

        if not sql_query or not sql_query.upper().startswith('SELECT'):
            raise ValueError(f"Invalid SQL generated: {sql_query}")
        
        # Step 2: Execute query
        con = duckdb.connect(DB_PATH, read_only=True)
        result_df = con.execute(sql_query).df()
        con.close()
        
        rows = result_df.to_dict('records')
        answer = generate_human_answer(question, sql_query, rows)
        
        return {"answer": answer, "sql": sql_query, "rows": rows}
    
    except duckdb.Error as db_err:
        raise ValueError(f"Database error: {str(db_err)}")
    except Exception as e:
        raise ValueError(f"Query processing error: {str(e)}")

# === TEST FUNCTION (for debugging) ===
if __name__ == "__main__":
    print("🧪 Testing qa_core with human-like answers...\n")
    test_questions = [
        "What is the rice production in Punjab in 2020?",
        "Show me rainfall data for Kerala",
        "What's the average market price of onion in Maharashtra?"
    ]
    for q in test_questions:
        print(f"Q: {q}")
        try:
            result = run_intelligent_query(q)
            print(f"SQL: {result['sql']}")
            print(f"Rows: {len(result['rows'])}")
            print(f"Answer: {result['answer']}\n")
            print("-" * 80 + "\n")
        except Exception as e:
            print(f"Error: {e}\n")
