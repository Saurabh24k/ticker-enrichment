import time
import random
from typing import Dict, Any, List, Optional
import httpx
from loguru import logger
from app.core.settings import settings
from app.services.rate_limiter import TokenBucket
from app.services.cache import JsonCache

SEARCH_URL = "https://finnhub.io/api/v1/search"
PROFILE_URL = "https://finnhub.io/api/v1/stock/profile2"

class FinnhubClient:
    def __init__(self, api_key: str, rpm: int, timeout_s: float):
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.bucket = TokenBucket(rpm)
        self.search_cache = JsonCache("backend/.cache/finnhub_search.json")
        self.profile_cache = JsonCache("backend/.cache/finnhub_profile.json")
        self.client = httpx.Client(timeout=self.timeout_s)

    def _request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("FINNHUB_API_KEY missing")
    
        self.bucket.acquire()
        p = dict(params); p["token"] = self.api_key
    
        delay = 0.5
        for attempt in range(6):
            try:
                resp = self.client.get(url, params=p)
                status = resp.status_code
                # retry only on 429 and 5xx
                if status == 429 or (500 <= status < 600):
                    raise httpx.HTTPStatusError(f"{status}", request=resp.request, response=resp)
    
                if 400 <= status < 500:
                    # non-retryable client error (e.g., 422 for "Class B" queries)
                    # return empty payload compatible with /search & profile callers
                    return {}
                resp.raise_for_status()
                return resp.json()
    
            except httpx.HTTPError as e:
                if attempt == 5:
                    logger.error(f"HTTP error after retries: {e}")
                    raise
                sleep_s = delay + random.random() * 0.3
                logger.warning(f"HTTP error ({e}); retrying in {sleep_s:.2f}s...")
                time.sleep(sleep_s)
                delay = min(delay * 2, 8.0)
    
    def search(self, query: str) -> Dict[str, Any]:
        key = query.strip().lower()
        cached = self.search_cache.get(key)
        if cached is not None:
            return cached
        data = self._request(SEARCH_URL, {"q": query})
        # Shape from Finnhub: {"count": n, "result": [{symbol, description, ...}]}
        self.search_cache.set(key, data)
        return data

    def profile(self, symbol: str) -> Dict[str, Any]:
        key = symbol.strip().upper()
        cached = self.profile_cache.get(key)
        if cached is not None:
            return cached
        data = self._request(PROFILE_URL, {"symbol": symbol})
        # Shape: {"country": "...", "currency": "...", "exchange": "...", "name": "...", "ticker": "...", ...}
        self.profile_cache.set(key, data)
        return data

# Singleton accessor
_client: Optional[FinnhubClient] = None

def get_finnhub() -> FinnhubClient:
    global _client
    if _client is None:
        _client = FinnhubClient(
            api_key=settings.finnhub_api_key,
            rpm=settings.finnhub_rpm,
            timeout_s=settings.http_timeout_s,
        )
    return _client
