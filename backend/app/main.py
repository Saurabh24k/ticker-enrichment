# backend/app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app.core.settings import settings
from app.api.routes_files import router as files_router
from app.api.routes_symbols import router as symbols_router
from app.api.routes_enrich import router as enrich_router

app = FastAPI(title="Powder Ticker Enrichment API", version="0.3.0")

# CORS for local dev frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers (no duplicates)
app.include_router(files_router, prefix="/files", tags=["files"])
app.include_router(symbols_router, prefix="/symbols", tags=["symbols"])
app.include_router(enrich_router, prefix="/enrich", tags=["enrich"])

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/health")
def health():
    logger.info("Health check ok")
    return {"status": "ok", "env": settings.env}

@app.get("/config/check")
def config_check():
    return {"finnhub_key_present": bool(settings.finnhub_api_key)}
