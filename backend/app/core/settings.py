# backend/app/core/settings.py
import os
from pathlib import Path
from pydantic import BaseModel
from dotenv import load_dotenv

# Resolve backend directory and load .env explicitly
BACKEND_DIR = Path(__file__).resolve().parents[2]  # .../backend
load_dotenv(BACKEND_DIR / ".env")  # do NOT set override=True; shell exports still win

class Settings(BaseModel):
    env: str = os.getenv("APP_ENV", "dev")
    port: int = int(os.getenv("PORT", "8000"))
    finnhub_api_key: str = os.getenv("FINNHUB_API_KEY", "")
    finnhub_rpm: int = int(os.getenv("FINNHUB_RPM", "55"))
    http_timeout_s: float = float(os.getenv("HTTP_TIMEOUT_S", "10"))

settings = Settings()
