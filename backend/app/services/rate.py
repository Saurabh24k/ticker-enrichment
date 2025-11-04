# backend/app/services/rate.py
from __future__ import annotations
import time, threading, hashlib, json
from typing import Any, Dict, Optional

# ---- Token bucket limiter (process-wide, thread-safe) ----
class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = max(0.01, rate_per_sec)
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.t = time.monotonic()
        self.mu = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        # Block until we can take 'tokens'
        while True:
            with self.mu:
                now = time.monotonic()
                # refill
                self.tokens = min(self.capacity, self.tokens + (now - self.t) * self.rate)
                self.t = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                # compute wait needed
                need = (tokens - self.tokens) / self.rate
            # sleep outside lock
            time.sleep(min(need, 1.0))

# ---- Simple circuit breaker per host ----
class CircuitBreaker:
    def __init__(self, fail_threshold: int = 5, cooldown_sec: float = 10.0):
        self.fail_threshold = fail_threshold
        self.cooldown_sec = cooldown_sec
        self._failures = 0
        self._open_until = 0.0
        self.mu = threading.Lock()

    def allow(self) -> bool:
        with self.mu:
            if time.monotonic() < self._open_until:
                return False
            return True

    def record_success(self) -> None:
        with self.mu:
            self._failures = 0
            self._open_until = 0.0

    def record_failure(self, severe: bool = False) -> None:
        with self.mu:
            self._failures += 1 if not severe else 2
            if self._failures >= self.fail_threshold:
                self._open_until = time.monotonic() + self.cooldown_sec
                # soft reset counter to avoid instant re-open
                self._failures = 0

# ---- Tiny TTL memo (in-memory) ----
class TTLCache:
    def __init__(self, ttl_sec: float = 3600.0, max_items: int = 4096):
        self.ttl = ttl_sec
        self.max = max_items
        self.mu = threading.Lock()
        self.store: Dict[str, tuple[float, Any]] = {}

    def _prune(self) -> None:
        if len(self.store) <= self.max:
            return
        # drop oldest 10%
        items = sorted(self.store.items(), key=lambda kv: kv[1][0])
        for k, _ in items[: max(1, len(items) // 10)]:
            self.store.pop(k, None)

    def _key(self, url: str, params: Dict[str, Any]) -> str:
        blob = json.dumps([url, params], sort_keys=True, ensure_ascii=False)
        return hashlib.sha1(blob.encode("utf-8")).hexdigest()

    def get(self, url: str, params: Dict[str, Any]) -> Optional[Any]:
        k = self._key(url, params)
        with self.mu:
            v = self.store.get(k)
            if not v:
                return None
            ts, data = v
            if (time.monotonic() - ts) > self.ttl:
                self.store.pop(k, None)
                return None
            return data

    def set(self, url: str, params: Dict[str, Any], data: Any) -> None:
        k = self._key(url, params)
        with self.mu:
            self.store[k] = (time.monotonic(), data)
            self._prune()

# ---- Registry of per-host controls ----
class ProviderControls:
    def __init__(self, qps: float, burst: int, fail_threshold: int, cooldown_sec: float):
        self.bucket = TokenBucket(qps, burst)
        self.breaker = CircuitBreaker(fail_threshold, cooldown_sec)

# singleton-ish
_ttl = TTLCache()
_providers: Dict[str, ProviderControls] = {}

def get_controls(host: str, qps: float = 2.0, burst: int = 2,
                 fail_threshold: int = 5, cooldown_sec: float = 10.0) -> ProviderControls:
    if host not in _providers:
        _providers[host] = ProviderControls(qps, burst, fail_threshold, cooldown_sec)
    return _providers[host]

def ttl_get(url: str, params: Dict[str, Any]) -> Optional[Any]:
    return _ttl.get(url, params)

def ttl_set(url: str, params: Dict[str, Any], data: Any) -> None:
    _ttl.set(url, params, data)
