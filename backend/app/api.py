#!/usr/bin/env python3
"""
AgriClimate Intelligent Q&A API Wrapper
⚡ Safely exposes your existing query engine via FastAPI
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from backend.app.intelligent_qa_system_groq import run_intelligent_query

# Initialize FastAPI
app = FastAPI(
    title="AgriClimate Intelligent Q&A System",
    description="Query and analyze agricultural datasets intelligently using Groq + DuckDB",
    version="1.0.0"
)

# Request body schema
class QueryRequest(BaseModel):
    question: str

# Root endpoint
@app.get("/")
def home():
    return {"message": "🌾 Welcome to AgriClimate Intelligent Q&A System API!"}

# Core query endpoint
@app.post("/query")
def query_endpoint(req: QueryRequest):
    """
    Accepts a natural-language question and returns:
      - Human-readable answer
      - SQL query (or steps for complex)
      - Result rows
    """
    try:
        result = run_intelligent_query(req.question)
        return {
            "status": "success",
            "question": req.question,
            "answer": result.get("answer"),
            "sql": result.get("sql"),
            "rows": result.get("rows", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
