#!/usr/bin/env python3
"""
FastAPI server for AgriClimate Q&A System
(Deploy-ready for Render and local environments)
"""

import os
import sys
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ✅ Fix Python import path (so it works both locally and on Render)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from intelligent_qa_system_groq import run_intelligent_query


# -------------------- Logging Setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------- FastAPI App --------------------
app = FastAPI(
    title="AgriClimate Intelligent Q&A System",
    description="Natural language interface for agricultural data queries",
    version="1.0.0"
)

# -------------------- CORS Configuration --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "http://127.0.0.1:5501",
        "http://localhost:5501",
        "http://127.0.0.1",
        "http://localhost",
        "http://localhost:3000",
        "https://your-frontend-domain.vercel.app",  # for production frontend
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- Pydantic Models --------------------
class QuestionRequest(BaseModel):
    question: str


class QuestionResponse(BaseModel):
    answer: str
    sql: str
    rows: list


# -------------------- Routes --------------------
@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "message": "🌾 AgriClimate Q&A System is running!",
        "status": "healthy",
        "version": "1.0.0",
        "endpoints": {
            "ask": "/ask (POST)",
            "health": "/health (GET)",
            "docs": "/docs (GET)",
        },
    }


@app.get("/health")
def health_check():
    """Detailed health check"""
    return {"status": "ok", "service": "agriclimate-qa", "version": "1.0.0"}


@app.post("/ask", response_model=QuestionResponse)
async def ask_endpoint(request: QuestionRequest):
    """
    Process natural language question and return SQL results
    """
    question = request.question.strip()

    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    if len(question) < 5:
        raise HTTPException(
            status_code=400,
            detail="Question too short. Please provide more details."
        )

    logger.info(f"Received question: {question}")

    try:
        result = run_intelligent_query(question)
        logger.info(f"Query successful. Returned {len(result['rows'])} rows")
        return result

    except ValueError as ve:
        logger.error(f"ValueError: {str(ve)}")
        raise HTTPException(status_code=400, detail=f"Query error: {str(ve)}")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error. Please try rephrasing your question."
        )


# -------------------- Global Exception Handler --------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500, content={"detail": "An unexpected error occurred"}
    )


# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    import uvicorn

    env = "Render" if "RENDER" in os.environ else "Local"
    logger.info(f"Starting AgriClimate Q&A Server ({env})...")
    logger.info("Docs available at: http://localhost:8000/docs")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        log_level="info"
    )
