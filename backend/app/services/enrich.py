from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Dict, Optional

import httpx

# Fields we’ll try to hydrate when a symbol is present
ENRICH_FIELDS = [
    "Name", "Exchange", "Currency", "Country", "AssetType",
    "ISIN", "FIGI", "MIC", "CIK", "Sector", "Industry",
    "LastPrice", "PriceAsOf",
]


class EnrichmentProvider:
    """
    Lightweight async metadata hydrator.
    Tries Finnhub then Polygon, and shallow-merges results.
    """
    def __init__(self, finnhub_key: str = "", polygon_key: str = "", timeout_s: float = 12.0, max_concurrency: int = 6):
        self.finnhub_key = finnhub_key or ""
        self.polygon_key = polygon_key or ""
        self.client = httpx.AsyncClient(timeout=timeout_s)
        self.sem = asyncio.Semaphore(max_concurrency)
        # in-memory cache: symbol -> dict
        self._cache: Dict[str, Dict] = {}

    async def close(self):
        await self.client.aclose()

    async def _get(self, url: str, params: Dict) -> Optional[Dict]:
        async with self.sem:
            r = await self.client.get(url, params=params)
            if r.status_code >= 400:
                return None
            try:
                return r.json()
            except Exception:
                return None

    async def _from_finnhub(self, symbol: str) -> Optional[Dict]:
        if not self.finnhub_key:
            return None
        base = "https://finnhub.io/api/v1"
        prof = await self._get(f"{base}/stock/profile2", {"symbol": symbol, "token": self.finnhub_key})
        if not prof or not (prof.get("name") or prof.get("ticker")):
            return None
        quote = await self._get(f"{base}/quote", {"symbol": symbol, "token": self.finnhub_key}) or {}
        return {
            "Name": prof.get("name"),
            "Exchange": prof.get("exchange"),
            "Country": prof.get("country"),
            "Currency": prof.get("currency"),
            "ISIN": prof.get("isin"),
            "MIC": prof.get("mic"),
            "Sector": prof.get("finnhubIndustry"),
            "CIK": prof.get("cik"),
            "AssetType": "ETF" if str(prof.get("type", "")).upper() == "ETF" else (prof.get("type") or "Common Stock"),
            "LastPrice": quote.get("c"),
            "PriceAsOf": quote.get("t"),  # epoch seconds
            "_source": "Finnhub",
            "_confidence": 0.95,
        }

    async def _from_polygon(self, symbol: str) -> Optional[Dict]:
        if not self.polygon_key:
            return None
        base = "https://api.polygon.io"
        res = await self._get(f"{base}/v3/reference/tickers/{symbol}", {"apiKey": self.polygon_key})
        if not res or not res.get("results"):
            return None
        r = res["results"]
        out = {
            "Name": r.get("name"),
            "Exchange": r.get("primary_exchange"),
            "Currency": r.get("currency_name"),
            "Country": r.get("locale"),
            "FIGI": r.get("composite_figi"),
            "AssetType": r.get("type"),
            "_source": "Polygon",
            "_confidence": 0.85,
        }
        # Sector/Industry sometimes in polygon via supplemental endpoints; keep basic here.
        return out

    def _merge(self, a: Optional[Dict], b: Optional[Dict]) -> Optional[Dict]:
        if not a and not b:
            return None
        if not a:
            return b
        if not b:
            return a
        # Prefer a’s values and use b to fill missing
        out = dict(a)
        for k, v in b.items():
            if k not in out or out[k] in (None, "", 0):
                out[k] = v
        # Merge provenance conservatively: favor higher confidence if both present
        if a.get("_confidence", 0) >= (b.get("_confidence", 0) or 0):
            out["_source"] = a.get("_source")
            out["_confidence"] = a.get("_confidence")
        else:
            out["_source"] = b.get("_source")
            out["_confidence"] = b.get("_confidence")
        return out

    async def enrich_by_symbol(self, symbol: str) -> Optional[Dict]:
        sym = (symbol or "").strip().upper()
        if not sym:
            return None
        if sym in self._cache:
            return self._cache[sym]

        futs = []
        if self.finnhub_key:
            futs.append(self._from_finnhub(sym))
        if self.polygon_key:
            futs.append(self._from_polygon(sym))
        if not futs:
            return None

        results = await asyncio.gather(*futs, return_exceptions=True)
        final = None
        for r in results:
            if isinstance(r, dict):
                final = self._merge(final, r)
        if final:
            self._cache[sym] = final
        return final


# Simple module-level factory with memoization so routers can reuse the client
@lru_cache(maxsize=1)
def get_provider(finnhub_key: str = "", polygon_key: str = "") -> EnrichmentProvider:
    return EnrichmentProvider(finnhub_key=finnhub_key, polygon_key=polygon_key)
