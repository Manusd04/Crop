#!/usr/bin/env python3
"""
🌾 AgriClimate Intelligent Q&A System (Full Stack)
Serves both backend API (FastAPI) and static frontend (HTML, JS, CSS)
"""

import os
import sys
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ✅ Ensure backend imports work in all environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from intelligent_qa_system_groq import run_intelligent_query


# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------- FastAPI App --------------------
app = FastAPI(
    title="🌾 AgriClimate Intelligent Q&A System",
    description="Ask natural questions about agricultural data — powered by Groq + DuckDB",
    version="1.0.0"
)


# -------------------- CORS --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*",  # 🔓 Easier for prototyping — restrict later if needed
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "https://crop-dcdl.onrender.com",  # Render production
        "https://your-frontend-domain.vercel.app"
    ],
    allow_credentials=True,
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


# -------------------- API Endpoints --------------------
@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "message": "🌾 AgriClimate Q&A System is running!",
        "status": "healthy",
        "version": "1.0.0",
        "endpoints": {
            "ask": "/ask (POST)",
            "health": "/health (GET)",
            "docs": "/docs (GET)"
        }
    }


@app.post("/ask", response_model=QuestionResponse)
async def ask_endpoint(request: QuestionRequest):
    """Handles natural language → SQL → response pipeline"""
    question = request.question.strip()

    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    if len(question) < 5:
        raise HTTPException(status_code=400, detail="Please ask a more complete question.")

    logger.info(f"🧠 Received question: {question}")

    try:
        result = run_intelligent_query(question)
        logger.info(f"✅ Query executed successfully ({len(result['rows'])} rows).")
        return result

    except ValueError as ve:
        logger.error(f"⚠️ Query error: {ve}")
        raise HTTPException(status_code=400, detail=f"Query error: {ve}")

    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error.")


# -------------------- FRONTEND SERVING --------------------
# Automatically detect frontend directory
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "../../frontend")

if os.path.exists(FRONTEND_DIR):
    logger.info(f"📂 Serving static files from: {FRONTEND_DIR}")
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_frontend_root():
        """Serve index.html for root path"""
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        logger.warning("⚠️ index.html not found in frontend folder.")
        return JSONResponse(content={"detail": "Frontend not found"}, status_code=404)

    @app.get("/{path_name}", include_in_schema=False)
    async def serve_frontend_files(path_name: str):
        """Catch-all route for frontend navigation"""
        file_path = os.path.join(FRONTEND_DIR, path_name)
        if os.path.exists(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
else:
    logger.warning("⚠️ No frontend directory found — only API endpoints will be available.")


# -------------------- Global Exception Handler --------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred."}
    )


# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    import uvicorn

    env = "Render" if "RENDER" in os.environ else "Local"
    logger.info(f"🚀 Starting AgriClimate Q&A Server ({env})...")
    logger.info("📘 Docs available at: http://localhost:8000/docs")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        log_level="info"
    )
