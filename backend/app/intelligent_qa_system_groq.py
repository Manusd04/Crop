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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "/Users/manusd/Crop/data/processed/agri_climate_db.duckdb"
MODEL = "llama-3.3-70b-versatile"  # Latest working model
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in .env file")

# Initialize Groq client
client = Groq(api_key=GROQ_API_KEY)


# ============================================================================
# COMPLEXITY DETECTION
# ============================================================================

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
    for indicator in complex_indicators:
        if re.search(indicator, question_lower):
            logger.info(f"Complex query detected: matched '{indicator}'")
            return True
    
    return False


# ============================================================================
# SQL UTILITIES
# ============================================================================

def clean_sql(query: str) -> str:
    """Extract and clean SQL query from LLM response"""
    query = query.strip()
    
    # Remove markdown code blocks
    query = re.sub(r'^```sql\s*', '', query, flags=re.IGNORECASE)
    query = re.sub(r'^```\s*', '', query)
    query = re.sub(r'```$', '', query)
    query = query.strip()
    
    # Fix missing WITH keyword for CTEs
    if re.search(r'^[a-z_]+\s+AS\s+\(', query, re.IGNORECASE) and not query.upper().startswith('WITH'):
        query = 'WITH ' + query
        logger.info("Added missing WITH keyword to CTE")
    
    # Extract query if embedded in text
    if not query.upper().startswith(('SELECT', 'WITH')):
        match = re.search(r'((?:WITH|SELECT)\s+.*?)(?:\n\n|$)', query, re.IGNORECASE | re.DOTALL)
        if match:
            query = match.group(1)
    
    # Remove trailing semicolon
    query = query.rstrip(';').strip()
    
    return query


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


# ============================================================================
# SIMPLE QUERY HANDLER
# ============================================================================

def execute_simple_query(question: str) -> dict:
    """Handle simple single-step queries"""
    schema_desc = "\n".join([
        f"- {table}: {', '.join(cols)}"
        for table, cols in SCHEMA.items()
    ])
    
    sql_prompt = f"""You are an expert SQL analyst. Convert this question into ONE valid DuckDB SQL query.

Database schema:
{schema_desc}

CRITICAL RULES:
1. ALL columns are lowercase with underscores: crop_year, period_actual_mm_, etc.
2. Use ILIKE for text: WHERE state ILIKE '%punjab%'
3. Use = for numbers: WHERE crop_year = 2020
4. For rice: (crop ILIKE '%rice%' OR crop ILIKE '%paddy%')
5. If using CTEs, MUST start with WITH keyword
6. Return ONLY the SQL query, no explanation
7. LIMIT 100 rows

CORRECT CTE EXAMPLE:
WITH latest_year AS (SELECT MAX(crop_year) as max_year FROM crop_production_raw)
SELECT * FROM crop_production_raw WHERE crop_year = (SELECT max_year FROM latest_year) LIMIT 100

Question: {question}

SQL:"""

    try:
        # Generate SQL
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": sql_prompt}],
            temperature=0.2,
            max_tokens=800
        )
        
        sql_query = clean_sql(response.choices[0].message.content)
        logger.info(f"Generated SQL: {sql_query}")
        
        # Validate
        if not sql_query or not sql_query.upper().startswith(('SELECT', 'WITH')):
            raise ValueError(f"Invalid SQL generated")
        
        # Fix ILIKE on numeric columns
        sql_query = re.sub(
            r'(crop_year|s_no|sr__no_|year)\s+ILIKE\s+[\'"]%(\d+)%[\'"]',
            r'\1 = \2',
            sql_query,
            flags=re.IGNORECASE
        )
        
        # Execute
        rows = execute_sql(sql_query)
        
        # Generate answer
        answer = generate_simple_answer(question, rows)
        
        return {
            "answer": answer,
            "sql": sql_query,
            "rows": rows
        }
        
    except Exception as e:
        logger.error(f"Simple query error: {e}")
        raise ValueError(f"Query failed: {str(e)}")


def generate_simple_answer(question: str, rows: list) -> str:
    """Generate human-like answer for simple queries"""
    if len(rows) == 0:
        return "I couldn't find any data matching your question. Try rephrasing or being more specific about the location, crop, or time period."
    
    data_summary = str(rows[:10]) if len(rows) <= 10 else f"First 10 of {len(rows)} rows: {str(rows[:10])}"
    
    answer_prompt = f"""You are a helpful agricultural analyst. Give a natural, conversational answer.

Question: {question}
Data: {data_summary}

Requirements:
- Direct, friendly tone (like talking to a farmer)
- Include specific numbers and facts
- 2-4 sentences for simple data, more for complex
- NO technical jargon or phrases like "based on the data"
- If patterns exist, mention them

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
        return f"Found {len(rows)} result(s) matching your query."


# ============================================================================
# COMPLEX QUERY HANDLER
# ============================================================================

def execute_complex_query(question: str) -> dict:
    """Handle complex multi-step queries"""
    logger.info("Processing complex multi-step query")
    
    # Step 1: Create execution plan
    plan = create_query_plan(question)
    logger.info(f"Query plan created with {plan['num_steps']} steps")
    
    # Step 2: Execute steps
    results = execute_plan_steps(plan)
    
    # Step 3: Generate final answer
    answer = generate_complex_answer(question, results)
    
    return {
        "answer": answer,
        "sql": f"Multi-step query ({plan['num_steps']} steps)",
        "rows": results.get('final_data', [])
    }


def create_query_plan(question: str) -> dict:
    """Break complex question into executable steps"""
    schema_desc = "\n".join([f"- {table}: {', '.join(cols)}" for table, cols in SCHEMA.items()])
    
    plan_prompt = f"""Break this complex question into simple SQL steps. Each step should be ONE simple SELECT query.

Database: crop_production_raw has: state, district, crop, crop_year, season, area, production, yield
All columns are lowercase. Use ILIKE for text, = for numbers. Rice: (crop ILIKE '%rice%' OR crop ILIKE '%paddy%')

Question: {question}

Format EXACTLY like this (number each step):

STEP 1: Find the latest year
SQL: SELECT MAX(crop_year) as latest_year FROM crop_production_raw WHERE crop ILIKE '%rice%' OR crop ILIKE '%paddy%'

STEP 2: Find Punjab highest rice production
SQL: SELECT district, SUM(production) as total FROM crop_production_raw WHERE state ILIKE '%punjab%' AND (crop ILIKE '%rice%' OR crop ILIKE '%paddy%') AND crop_year = {{{{latest_year}}}} GROUP BY district ORDER BY total DESC LIMIT 1

STEP 3: Find West Bengal lowest rice production  
SQL: SELECT district, SUM(production) as total FROM crop_production_raw WHERE state ILIKE '%west bengal%' AND (crop ILIKE '%rice%' OR crop ILIKE '%paddy%') AND crop_year = {{{{latest_year}}}} GROUP BY district ORDER BY total ASC LIMIT 1

Create the plan:"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": plan_prompt}],
        temperature=0.3,
        max_tokens=1200
    )
    
    plan_text = response.choices[0].message.content
    
    # Parse steps
    steps = []
    for match in re.finditer(r'STEP \d+:.*?SQL:\s*(SELECT.*?)(?=\n(?:STEP|\n|$))', plan_text, re.DOTALL | re.IGNORECASE):
        sql = match.group(1).strip().rstrip(';')
        steps.append(sql)
    
    return {
        "num_steps": len(steps),
        "steps": steps,
        "plan_text": plan_text
    }


def execute_plan_steps(plan: dict) -> dict:
    """Execute each step sequentially, passing results between steps"""
    results = {}
    all_data = []
    
    for i, sql in enumerate(plan['steps'], 1):
        # Replace placeholders from previous steps
        for key, value in results.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in sql:
                sql = sql.replace(placeholder, str(value))
        
        logger.info(f"Executing step {i}/{plan['num_steps']}: {sql[:100]}...")
        
        try:
            rows = execute_sql(sql)
            logger.info(f"Step {i} returned {len(rows)} rows")
            
            all_data.extend(rows)
            
            # Store results for next steps
            if rows and len(rows) > 0:
                for key, value in rows[0].items():
                    results[key] = value
                    
        except Exception as e:
            logger.error(f"Step {i} failed: {e}")
            raise ValueError(f"Query step {i} failed: {str(e)}")
    
    return {
        "variables": results,
        "final_data": all_data,
        "num_steps": plan['num_steps']
    }


def generate_complex_answer(question: str, results: dict) -> str:
    """Generate human answer for complex multi-step query"""
    answer_prompt = f"""You are an agricultural data analyst presenting findings.

Question: {question}

Analysis Results:
{results['variables']}

Provide a clear answer that:
1. Names both districts
2. Shows both production values
3. Calculates absolute difference
4. Calculates percent difference
5. Uses natural, conversational language

Answer:"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": answer_prompt}],
            temperature=0.7,
            max_tokens=400
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Complex answer generation failed: {e}")
        return f"Analysis completed with {results['num_steps']} steps. Key findings: {results['variables']}"


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def run_intelligent_query(question: str) -> dict:
    """
    Main entry point - routes to simple or complex handler
    
    Returns:
    {
        "answer": "Human-readable answer",
        "sql": "SQL query or description",
        "rows": [list of result dictionaries]
    }
    """
    if is_complex_query(question):
        logger.info("→ Routing to COMPLEX query handler")
        return execute_complex_query(question)
    else:
        logger.info("→ Routing to SIMPLE query handler")
        return execute_simple_query(question)


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    print("🧪 Testing AgriClimate Q&A System\n")
    
    test_questions = [
        "What is rice production in Punjab?",
        "In the latest year, which district in Punjab had highest rice production? Compare with West Bengal's lowest."
    ]
    
    for q in test_questions:
        print(f"\n{'='*80}")
        print(f"Q: {q}")
        print('='*80)
        try:
            result = run_intelligent_query(q)
            print(f"\nSQL: {result['sql']}")
            print(f"Rows: {len(result['rows'])}")
            print(f"\nANSWER:\n{result['answer']}")
        except Exception as e:
            print(f"\n❌ Error: {e}")