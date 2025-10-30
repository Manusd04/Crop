import os
from pathlib import Path
from dotenv import load_dotenv

def load_env():
    """Load environment variables from .env at project root."""
    base_dir = Path(__file__).resolve().parents[2]
    dotenv_path = base_dir / ".env"

    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        print(f"✅ Loaded .env file from: {dotenv_path}")
    else:
        print(f"⚠️ .env file not found at: {dotenv_path}")

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("❌ GROQ_API_KEY not found in .env file.")
    else:
        print("🔑 GROQ_API_KEY successfully loaded!")

    return api_key
