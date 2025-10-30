#!/usr/bin/env python3
"""
FastAPI server for AgriClimate Q&A System
Now serves both backend API and frontend UI
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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------- FastAPI App --------------------
app = FastAPI(
    title="AgriClimate Intelligent Q&A System",
    description="Natural language interface for agricultural data queries",
    version="1.0.0"
)

# -------------------- CORS --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "https://your-frontend-domain.vercel.app",  # Replace if needed
        "https://crop-dcdl.onrender.com",  # Render production
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- Models --------------------
class QuestionRequest(BaseModel):
    question: str


class QuestionResponse(BaseModel):
    answer: str
    sql: str
    rows: list


# -------------------- API Routes --------------------
@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "ok", "service": "agriclimate-qa", "version": "1.0.0"}


@app.post("/ask", response_model=QuestionResponse)
async def ask_endpoint(request: QuestionRequest):
    """Handles natural language → SQL → answer generation"""
    question = request.question.strip()

    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    if len(question) < 5:
        raise HTTPException(status_code=400, detail="Please provide a more detailed question.")

    logger.info(f"Received question: {question}")

    try:
        result = run_intelligent_query(question)
        logger.info(f"Query executed successfully. Rows returned: {len(result['rows'])}")
        return result

    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise HTTPException(status_code=400, detail=f"Query error: {str(ve)}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error. Please try again later.")


# -------------------- FRONTEND SERVING --------------------
# Serve static files (HTML, CSS, JS)
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "../../frontend")

if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_frontend():
        """Serve the main frontend page"""
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return {"detail": "Frontend not found. Please check deployment paths."}
else:
    logger.warning("⚠️ Frontend directory not found — only API endpoints will work.")


# -------------------- Global Exception Handler --------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500, content={"detail": "An unexpected error occurred"}
    )


# -------------------- Entry Point --------------------
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
