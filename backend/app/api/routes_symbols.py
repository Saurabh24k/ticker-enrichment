from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any
from loguru import logger
from app.services.finnhub_client import get_finnhub

router = APIRouter()

def _normalize_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Finnhub result fields typically: symbol, description, displaySymbol, type
    return {
        "symbol": raw.get("symbol") or raw.get("displaySymbol") or "",
        "name": raw.get("description") or "",
        "type": raw.get("type") or "",
    }

@router.get("/search")
def search_symbols(query: str = Query(..., min_length=1, description="Company name or keyword")):
    try:
        fh = get_finnhub()
        data = fh.search(query)
        results = data.get("result") or []
        normalized = [_normalize_result(r) for r in results]
        # Return top 10 to keep payload small
        top = normalized[:10]
        logger.info(f"Search '{query}' -> {len(top)} candidates (of {len(normalized)})")
        return {"query": query, "candidates": top, "total": len(normalized)}
    except Exception as e:
        logger.exception(f"Search error for '{query}': {e}")
        raise HTTPException(status_code=500, detail=str(e))
