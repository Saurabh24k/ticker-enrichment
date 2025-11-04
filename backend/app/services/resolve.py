from __future__ import annotations

import os, re, csv, json, time, random, difflib, unicodedata, uuid, datetime
from functools import lru_cache
from typing import List, Tuple, Optional, Dict, Any, Set, Iterable
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from loguru import logger

from app.services.rate import get_controls, ttl_get, ttl_set

# Public constants (used by routers)
RESOLVER_VERSION = os.getenv("RESOLVER_VERSION", "2025.11.03")
Candidate = Tuple[str, str, str, float, str]   # (symbol, name/desc, type, score, source_tag)

# =============================================================================
# Config
# =============================================================================
PREFER_US_LISTINGS = os.getenv("PREFER_US_LISTINGS", "1").lower() not in {"0","false"}
PREFER_OTC         = os.getenv("PREFER_OTC", "1").lower() not in {"0","false"}

SECOND_PASS_ENABLED       = os.getenv("SECOND_PASS_ENABLED", "1").lower() not in {"0","false"}
SECOND_PASS_TOPK          = int(os.getenv("SECOND_PASS_TOPK", "1"))
MAX_VARIANTS_PER_NAME     = int(os.getenv("MAX_VARIANTS_PER_NAME", "8"))
MAX_SECOND_PASS_QUERIES   = int(os.getenv("MAX_SECOND_PASS_QUERIES", "6"))
EARLY_EXIT_US_SCORE       = float(os.getenv("EARLY_EXIT_US_SCORE", "0.92"))
TOPK_RETURN               = int(os.getenv("TOPK_RETURN", "10"))

# Local maps are **OFF by default**; UI can opt-in per request
USE_LOCAL_MAPS_DEFAULT    = os.getenv("USE_LOCAL_MAPS_DEFAULT", "0").lower() not in {"0","false"}
LOCAL_FIRST               = os.getenv("LOCAL_FIRST", "0").lower() not in {"0","false"}  # only if use_local_maps is True
LOCAL_ACCEPT_SCORE        = float(os.getenv("LOCAL_ACCEPT_SCORE", "0.90"))
BATCH_MAX_WORKERS         = int(os.getenv("BATCH_MAX_WORKERS", "8"))
PARALLEL_PROVIDERS        = os.getenv("PARALLEL_PROVIDERS","1").lower() not in {"0","false"}

HTTP_QPS           = float(os.getenv("HTTP_QPS", "0.8"))
HTTP_BURST         = int(os.getenv("HTTP_BURST", "2"))
CB_FAIL_THRESHOLD  = int(os.getenv("CB_FAIL_THRESHOLD", "14"))
CB_COOLDOWN_SEC    = float(os.getenv("CB_COOLDOWN_SEC", "18.0"))
HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "4.0"))
HTTP_MAX_RETRIES   = int(os.getenv("HTTP_MAX_RETRIES", "2"))
NEG_CACHE_TTL      = float(os.getenv("NEG_CACHE_TTL", "180.0"))

CACHE_VERSION = os.getenv("SYMBOL_CACHE_VERSION", "4")
_CACHE_PATH = os.getenv("SYMBOL_CACHE_PATH", f".cache/symbol_resolve_v{CACHE_VERSION}.json")

CACHE_READ  = os.getenv("RESOLVE_CACHE_READ","1").lower() not in {"0","false"}
CACHE_WRITE = os.getenv("RESOLVE_CACHE_WRITE","1").lower() not in {"0","false"}
CACHE_CLEAR = os.getenv("RESOLVE_CACHE_CLEAR_ON_START","0").lower() not in {"0","false"}

_MASTER_PATH     = os.getenv("MASTER_PATH","assets/securities_master.csv")
ETF_CANON_PATH   = os.getenv("ETF_CANON_PATH","assets/etf_canon.json")
ALIASES_PATH     = os.getenv("ALIASES_PATH","assets/aliases.json")

_FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN","") or os.getenv("FINNHUB_API_KEY","")
ENABLE_POLYGON = os.getenv("ENABLE_POLYGON","1").lower() not in {"0","false"}
_POLYGON_KEY   = os.getenv("POLYGON_API_KEY","")

# ---- new knobs (safe defaults) ----
HTTP_POOL = int(os.getenv("HTTP_POOL", "64"))
VARIANT_CONCURRENCY = int(os.getenv("VARIANT_CONCURRENCY", "0"))  # 0 = off (preserves behavior)
CANDIDATE_CACHE_SIZE = int(os.getenv("CANDIDATE_CACHE_SIZE", "4096"))

# =============================================================================
# HTTP client + negative cache
# =============================================================================
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
})
_adapter = HTTPAdapter(pool_connections=HTTP_POOL, pool_maxsize=HTTP_POOL, max_retries=0)
_SESSION.mount("https://", _adapter)
_SESSION.mount("http://", _adapter)

_NEG_CACHE: Dict[tuple, float] = {}

def _neg_key(url: str, params: Dict[str, Any]) -> tuple:
    return (url, tuple(sorted((k, str(v)) for k, v in params.items())))

def _neg_hit(url: str, params: Dict[str, Any]) -> bool:
    t = _NEG_CACHE.get(_neg_key(url, params))
    return bool(t and (time.time() - t) < NEG_CACHE_TTL)

def _neg_set(url: str, params: Dict[str, Any]) -> None:
    _NEG_CACHE[_neg_key(url, params)] = time.time()

def _http_get_json(url: str, params: Dict[str, Any], max_retries: Optional[int]=None) -> Optional[Dict[str, Any]]:
    if max_retries is None:
        max_retries = HTTP_MAX_RETRIES

    cached = ttl_get(url, params)
    if cached is not None:
        return cached

    if _neg_hit(url, params):
        logger.debug(f"neg_cache_skip url={url} params={params}")
        return None

    host = urlparse(url).netloc
    ctl = get_controls(host, qps=HTTP_QPS, burst=HTTP_BURST,
                       fail_threshold=CB_FAIL_THRESHOLD, cooldown_sec=CB_COOLDOWN_SEC)

    if not ctl.breaker.allow():
        logger.warning(f"circuit_open host={host} url={url} params={params}")
        return None

    for attempt in range(max_retries):
        ctl.bucket.acquire(1.0)
        try:
            r = _SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
            code = r.status_code

            if code == 429:
                ctl.breaker.record_failure(severe=True)
                ra = r.headers.get("Retry-After")
                if ra:
                    try: time.sleep(min(float(ra), 3.0))
                    except Exception: time.sleep(0.6)
                else:
                    time.sleep(0.35*(attempt+1)+random.uniform(0,0.25))
                logger.error(f"HTTP 429 for {url} params={params}")
                continue

            if code == 422:
                ctl.breaker.record_failure(severe=False)
                _neg_set(url, params)
                logger.error(f"HTTP 422 for {url} params={params}")
                return None

            if 400 <= code < 600:
                ctl.breaker.record_failure(severe=False)
                logger.error(f"HTTP {code} for {url} params={params}")
                time.sleep(0.22*(attempt+1)+random.uniform(0,0.15))
                continue

            ctl.breaker.record_success()
            data = r.json()
            ttl_set(url, params, data)
            return data

        except requests.RequestException as e:
            ctl.breaker.record_failure(severe=False)
            logger.error(f"HTTP error for {url} params={params}: {e}")
            time.sleep(0.25*(attempt+1)+random.uniform(0,0.2))

    return None

# =============================================================================
# Normalization + scoring helpers
# =============================================================================
_STOPWORDS = {
    "inc","inc.","corporation","corp","co","company","plc","sa","nv","ag","se",
    "the","ltd","limited","holdings","holding","group","class"
}
_GENERIC = {"bank","group","holdings","holding","plc","company","corporation","sa","nv","ag","se"}

_NON_US_SUFFIXES = (
    ".TO",".V",".SA",".L",".AS",".PA",".SW",".F",".DE",".HK",".SS",".SZ",".AX",".NZ",".BK",
    ".TW",".T",".KL",".IS",".ME",".MI",".MC",".VI",".SG",".JK",".KS",".KQ",".SR",".CR",".NE",".NS",".BO"
)

BANK_WORDS: Set[str] = {"bank","banking","financial","finance","wealth","lending","credit","capital"}
CRUISE_WORDS: Set[str] = {"cruise","cruises","cruiseline","cruiselines"}
BOTTLER_WORDS: Set[str] = {"bottl","bottler","bottling","embonor","femsa","hbc"}

# --- precompiled regexes (hot paths) ---
_CLASS_DOT_RE = re.compile(r"^[A-Z]{1,5}\.[AB]$")
_US_TICK_RE   = re.compile(r"^[A-Z]{1,5}$")
_OTC_RE       = re.compile(r"^[A-Z]{5}$")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE        = re.compile(r"\s+")
_CLASS_WORD_RE = re.compile(r"\bclass\s*([abc])\b")

@lru_cache(maxsize=16384)
def _unaccent(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii","ignore").decode("ascii")

@lru_cache(maxsize=16384)
def _tokenize(s: str) -> List[str]:
    s = _unaccent(s).lower()
    s = re.sub(r"[^a-z0-9]+"," ",s)
    return [t for t in s.split() if t]

@lru_cache(maxsize=16384)
def _simplify_name(name: str) -> str:
    toks = _tokenize(name)
    out = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in _STOPWORDS:
            if t == "class" and i+1 < len(toks) and toks[i+1] in {"a","b","c"}:
                out.append(f"class{toks[i+1]}")
                i += 2
                continue
            i += 1
            continue
        out.append(t)
        i += 1
    return " ".join(out)

@lru_cache(maxsize=16384)
def _company_family_key(desc: str) -> str:
    s = _simplify_name(desc)
    s = re.sub(r"\b(sp|spon|sponsored|adr|ads|pref|preferred|share|shares)\b"," ",s)
    s = re.sub(r"\b(class[abc])\b"," ",s)
    s = re.sub(r"\s+"," ",s).strip()
    return s

def _jaccard(a: str, b: str) -> float:
    A, B = set(_tokenize(a)), set(_tokenize(b))
    if not A or not B: return 0.0
    return len(A & B) / len(A | B)

@lru_cache(maxsize=65536)
def _fuzzy_score(a: str, b: str) -> float:
    ja = _jaccard(a, b)
    sm = difflib.SequenceMatcher(None, _simplify_name(a), _simplify_name(b)).ratio()
    score = 0.62*ja + 0.38*sm
    return 0.0 if score < 0 else (1.0 if score > 1 else score)

def _is_us_like_symbol(sym: str) -> bool:
    if not sym: return False
    if _US_TICK_RE.fullmatch(sym): return True
    if _CLASS_DOT_RE.fullmatch(sym): return True  # BRK.A / BRK.B
    if PREFER_OTC and _OTC_RE.fullmatch(sym) and sym[-1] in {"Y","F"}: return True  # ADR/FO
    return False

def _contains_prefix(words: Set[str], prefixes: Set[str]) -> bool:
    for w in words:
        for p in prefixes:
            if w.startswith(p):
                return True
    return False

def _has_contradiction(input_name: str, cand_name: str) -> bool:
    a, b = set(_tokenize(input_name)), set(_tokenize(cand_name))
    if ("bank" in a) and (_contains_prefix(b, CRUISE_WORDS) or b & {"brew","brewer","beer","drinks"}):
        return True
    if {"coca","cola"} <= a and _contains_prefix(b, BOTTLER_WORDS):
        return True
    strong_in = {t for t in a if t not in _GENERIC and t not in _STOPWORDS}
    strong_c  = {t for t in b if t not in _GENERIC and t not in _STOPWORDS}
    if strong_in and strong_in.isdisjoint(strong_c):
        return True
    return False

def _infer_expected_type(name: str) -> Optional[str]:
    t = set(_tokenize(name))
    if "etf" in t or "trust" in t or "fund" in t:
        return "ETF"
    return "Common Stock"

def _apply_biases(sym: str,
                  base_score: float,
                  simplified_name: str,
                  candidate_type: Optional[str],
                  expected_type: Optional[str],
                  *,
                  input_name: Optional[str]=None,
                  candidate_name: Optional[str]=None) -> float:
    score = base_score

    if input_name and candidate_name and _has_contradiction(input_name, candidate_name):
        return 0.0

    if input_name and "bank" in _tokenize(input_name):
        if candidate_name and not (set(_tokenize(candidate_name)) & BANK_WORDS):
            score -= 0.60

    if base_score < 0.40:
        score -= 0.35
        if base_score < 0.30:
            return 0.0

    if base_score >= 0.55:
        if "classa" in simplified_name and re.fullmatch(r".*\.A$", sym): score += 0.06
        if "classb" in simplified_name and re.fullmatch(r".*\.B$", sym): score += 0.06
        if "classc" in simplified_name and re.search(r"\bclass\s*c\b", (candidate_name or "").lower()): score += 0.06

    if expected_type and candidate_type:
        e_is_etf = expected_type.upper().startswith("ETF")
        c_is_etf = candidate_type.upper().startswith("ETF")
        score += 0.12 if e_is_etf == c_is_etf else -0.40

    if PREFER_US_LISTINGS and base_score >= 0.55:
        if _is_us_like_symbol(sym): score += 0.10
        for suf in _NON_US_SUFFIXES:
            if sym.endswith(suf):
                score -= 0.20
                break
        if "." in sym and not re.fullmatch(r"[A-Z]{1,5}\.[AB]", sym):
            score -= 0.35

    return max(0.0, min(1.0, score))

# =============================================================================
# Query variants (with alias expansion)
# =============================================================================
_ABBREV = {
    "mfg":"manufacturing","tech":"technology","intl":"international","int'l":"international",
    "grp":"group","co":"company","corp":"corporation"
}

@lru_cache(maxsize=4096)
def _expand_abbrev(s: str) -> str:
    toks = _tokenize(s)
    return " ".join(_ABBREV.get(t, t) for t in toks)

@lru_cache(maxsize=4096)
def _acronym(s: str) -> str:
    toks = _tokenize(s)
    if len(toks) < 2: return ""
    ac = "".join(t[0] for t in toks if t and t[0].isalnum())
    return ac if 3 <= len(ac) <= 8 else ""

@lru_cache(maxsize=32768)
def _sanitize_query_for_api(q: str) -> str:
    q = _unaccent(q).lower()
    q = _NON_ALNUM_RE.sub(" ", q)
    q = _WS_RE.sub(" ", q).strip()
    if len(q) > 48:
        q = " ".join(q.split()[:8])
    return q

def _filter_generic_head_tail(tokens: List[str]) -> List[str]:
    if tokens and tokens[-1] in _GENERIC: tokens = tokens[:-1]
    if tokens and tokens[0]  in _GENERIC: tokens = tokens[1:]
    return tokens

# ===== External canons (ETF + aliases) =======================================
_ETF_CANON_EXT: Dict[str, str] = {}
_ALIAS_CANON: Dict[str, List[str]] = {}          # name -> list of symbols (preferred)
_ALIAS_QUERY_EXPAND: Dict[str, List[str]] = {}   # name -> extra terms to inject

def _load_canons() -> None:
    # ETF canonical (normalized name -> ticker)
    try:
        if os.path.exists(ETF_CANON_PATH):
            with open(ETF_CANON_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for k, v in raw.items():
                    if not isinstance(v, str): continue
                    _ETF_CANON_EXT[_simplify_name(k)] = v.upper()
    except Exception as e:
        logger.warning(f"Failed loading ETF canon {ETF_CANON_PATH}: {e}")

    # Aliases: {"google":{"symbols":["GOOGL","GOOG"], "expand":["alphabet"]}}, or {"square":"SQ"}
    try:
        if os.path.exists(ALIASES_PATH):
            with open(ALIASES_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for k, v in raw.items():
                    nk = _simplify_name(k)
                    if isinstance(v, dict):
                        if "symbols" in v and isinstance(v["symbols"], list):
                            _ALIAS_CANON[nk] = [str(s).upper() for s in v["symbols"] if s]
                        if "expand" in v and isinstance(v["expand"], list):
                            _ALIAS_QUERY_EXPAND[nk] = [str(s) for s in v["expand"] if s]
                    elif isinstance(v, str):
                        _ALIAS_CANON[nk] = [v.upper()]
    except Exception as e:
        logger.warning(f"Failed loading aliases {ALIASES_PATH}: {e}")

_load_canons()

@lru_cache(maxsize=4096)
def _query_variants_for_name(name: str) -> List[str]:
    raw = (name or "").strip()
    expanded = _expand_abbrev(raw)
    simple = _simplify_name(raw)
    simple_noclass = re.sub(r"\bclass[abc]\b"," ", simple).strip()

    toks = _filter_generic_head_tail(_tokenize(raw))
    compact3 = " ".join(toks[:3])
    compact2 = " ".join(toks[:2]) if len(toks[:2]) >= 2 and not set(toks[:2]) & _GENERIC else ""

    ac = _acronym(raw)

    variants: List[str] = []
    base_variants = (expanded, simple, simple_noclass, compact3, compact2, ac)
    for v in base_variants:
        v = _sanitize_query_for_api(v)
        if v and v not in variants:
            variants.append(v)

    # Alias-based query expansions (e.g., Google -> add "alphabet")
    sname = _simplify_name(raw)
    if sname in _ALIAS_QUERY_EXPAND:
        for extra in _ALIAS_QUERY_EXPAND[sname]:
            sv = _sanitize_query_for_api(extra)
            if sv and sv not in variants:
                variants.append(sv)

    if not variants:
        b = _sanitize_query_for_api(raw)
        if b: variants.append(b)

    return variants[:MAX_VARIANTS_PER_NAME]

def get_search_variants(name: str) -> List[str]:
    """Public helper (used by routers)"""
    return _query_variants_for_name(name)

# =============================================================================
# Persistent cache (name→symbol)
# =============================================================================
_CACHE: Dict[str, str] = {}
_CACHE_LOADED = False

def _ensure_cache_loaded() -> None:
    global _CACHE_LOADED
    if _CACHE_LOADED: return
    _CACHE_LOADED = True
    try:
        if CACHE_CLEAR and os.path.exists(_CACHE_PATH):
            try: os.remove(_CACHE_PATH)
            except Exception: pass
        if CACHE_READ and os.path.exists(_CACHE_PATH):
            with open(_CACHE_PATH,"r",encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict): _CACHE.update(data)
    except Exception as e:
        logger.warning(f"Could not load cache {_CACHE_PATH}: {e}")

def _cache_get(name: str) -> Optional[str]:
    if not CACHE_READ:
        return None
    _ensure_cache_loaded()
    return _CACHE.get(_simplify_name(name))

def _cache_put(name: str, symbol: str) -> None:
    if not CACHE_WRITE:
        return
    try:
        _ensure_cache_loaded()
        _CACHE[_simplify_name(name)] = symbol.upper()
        os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
        with open(_CACHE_PATH,"w",encoding="utf-8") as f:
            json.dump(_CACHE,f,ensure_ascii=False,indent=2)
    except Exception as e:
        logger.warning(f"Could not write cache {_CACHE_PATH}: {e}")

def cache_put(name: str, symbol: str) -> None:
    _cache_put(name, symbol)

# =============================================================================
# Optional local master + canonical safety net
# =============================================================================
_MASTER_ROWS: List[Dict[str,str]] = []
_MASTER_LOADED = False

def _maybe_load_master() -> None:
    global _MASTER_LOADED, _MASTER_ROWS
    if _MASTER_LOADED: return
    _MASTER_LOADED = True
    if not os.path.exists(_MASTER_PATH):
        logger.info(f"Local securities master not found at {_MASTER_PATH} (optional).")
        return
    try:
        with open(_MASTER_PATH,"r",encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                sym = (r.get("symbol") or r.get("Symbol") or "").strip().upper()
                nm  = (r.get("name")   or r.get("Name")   or "").strip()
                tp  = (r.get("type") or r.get("Type") or "").strip() or "Common Stock"
                if sym and nm:
                    _MASTER_ROWS.append({"symbol":sym,"name":nm,"type":tp})
        logger.info(f"Loaded local master: {_MASTER_PATH} with {len(_MASTER_ROWS)} rows.")
    except Exception as e:
        logger.warning(f"Failed to load local master {_MASTER_PATH}: {e}")

def _local_master_candidates(name: str) -> List[Candidate]:
    _maybe_load_master()
    if not _MASTER_ROWS: return []
    sname = _simplify_name(name)
    expected = _infer_expected_type(name)
    out: List[Candidate] = []
    for r in _MASTER_ROWS:
        nm, sym = r["name"], r["symbol"]
        base = _fuzzy_score(name, nm)
        score = _apply_biases(sym, base, sname, r.get("type") or "Common Stock",
                              expected, input_name=name, candidate_name=nm)
        out.append((sym, nm, r.get("type") or "Common Stock", round(score,2), "Local"))
    out.sort(key=lambda t: (-t[3], t[0]))
    return [c for c in out if c[3] >= 0.55][:TOPK_RETURN]

# Built-in minimal canon (kept small; most coverage comes from JSONs)
_CANON_COMMON: Dict[str, str] = {
    "royal bank": "RY",
    "shell": "SHEL",
    "sony": "SONY",
    "toyota motor corp": "TM",
    "hsbc holdings": "HSBC",
    "tencent holdings": "TCEHY",
    "bhp group": "BHP",
    "rio tinto": "RIO",
    "nestle": "NSRGY",
    "novo nordisk": "NVO",
    "taiwan semiconductor mfg": "TSM",
    "asml holding": "ASML",
    "sap se": "SAP",
    "totalenergies": "TTE",
    "petrobras": "PBR",
    "santander": "SAN",
    "nintendo": "NTDOY",
    "lvmh": "LVMUY",
    "roche holding": "RHHBY",
    "unilever": "UL",
    "astrazeneca": "AZN",
    "canadian national railway": "CNI",
    "palantir technologies": "PLTR",
    "coca cola": "KO",
    "johnson johnson": "JNJ",
    "air canada": "AC.TO",
    "berkshire hathaway inc class b": "BRK.B",
}
_ETF_CANON_BUILTIN: Dict[str, str] = {
    "spdr s p 500 etf trust": "SPY",
    "vanguard s p 500 etf": "VOO",
    "vanguard sp 500 etf": "VOO",
    "ishares core s p 500 etf": "IVV",
    "invesco qqq trust": "QQQ",
    "ishares russell 2000 etf": "IWM",
    "vanguard total stock market etf": "VTI",
    "schwab u s broad market etf": "SCHB",
    "ishares msci eafe etf": "EFA",
    "ishares msci emerging markets etf": "EEM",
    "ishares 20 year treasury bond etf": "TLT",
    "spdr gold trust": "GLD",
    "vaneck gold miners etf": "GDX",
    "ishares i boxx investment grade corporate bond etf": "LQD",
    "ishares i boxx high yield corporate bond etf": "HYG",
    "technology select sector spdr fund": "XLK",
    "financial select sector spdr fund": "XLF",
}

def _canonical_candidates(name: str, use_local_maps: bool) -> List[Candidate]:
    if not use_local_maps:
        return []
    s = _simplify_name(name)

    # 1) Aliases with explicit symbols (e.g., "google" -> ["GOOGL","GOOG"])
    if s in _ALIAS_CANON:
        out: List[Candidate] = []
        for sym in _ALIAS_CANON[s]:
            out.append((sym, name, "Common Stock", 0.99, "Alias"))
        out.sort(key=lambda t: (-t[3], t[0]))
        return out

    # 2) External ETF canon first, then built-in
    if s in _ETF_CANON_EXT:
        sym = _ETF_CANON_EXT[s].upper()
        return [(sym, name, "ETF", 0.99, "CanonETF")]
    if s in _ETF_CANON_BUILTIN:
        sym = _ETF_CANON_BUILTIN[s].upper()
        return [(sym, name, "ETF", 0.98, "CanonETF")]

    # 3) Built-in common canon
    if s in _CANON_COMMON:
        sym = _CANON_COMMON[s].upper()
        return [(sym, name, "Common Stock", 0.98, "Canon")]

    return []

# =============================================================================
# FAST LOCAL INDEX
# =============================================================================
class _LocalIndex:
    _built = False
    _rows: List[Tuple[str,str,str]] = []  # (symbol, name, type)
    _postings: Dict[str, List[int]] = {}

    @classmethod
    def _ensure(cls):
        if cls._built:
            return
        _maybe_load_master()
        for r in _MASTER_ROWS:
            cls._rows.append((r["symbol"], r["name"], r.get("type") or "Common Stock"))
        for k,v in _CANON_COMMON.items():
            cls._rows.append((v, k, "Common Stock"))
        for k,v in _ETF_CANON_BUILTIN.items():
            cls._rows.append((v, k, "ETF"))
        for k,v in _ETF_CANON_EXT.items():
            cls._rows.append((v, k, "ETF"))
        for i, (_sym, nm, _tp) in enumerate(cls._rows):
            for t in set(_tokenize(nm)):
                if t in _STOPWORDS or t in _GENERIC:
                    continue
                cls._postings.setdefault(t, []).append(i)
        cls._built = True

    @classmethod
    def fast_candidates(cls, name: str) -> List[Candidate]:
        cls._ensure()
        toks = [t for t in _tokenize(name) if t not in _STOPWORDS and t not in _GENERIC]
        if not toks:
            return []
        idxs: Set[int] = set()
        for t in toks:
            for i in cls._postings.get(t, []):
                idxs.add(i)
        if not idxs:
            return []
        expected = _infer_expected_type(name)
        sname = _simplify_name(name)
        cands: List[Candidate] = []
        for i in idxs:
            sym, nm, tp = cls._rows[i]
            base = _fuzzy_score(name, nm)
            sc   = _apply_biases(sym, base, sname, tp, expected, input_name=name, candidate_name=nm)
            cands.append((sym, nm, tp, round(sc,2), "LocalFast"))
        cands.sort(key=lambda x: (-x[3], x[0]))
        return cands[:TOPK_RETURN]

# =============================================================================
# Finnhub
# =============================================================================
@lru_cache(maxsize=512)
def _finnhub_search_raw(q: str) -> List[Dict[str, Any]]:
    if not _FINNHUB_TOKEN: return []
    url = "https://finnhub.io/api/v1/search"
    data = _http_get_json(url, {"q": q, "token": _FINNHUB_TOKEN})
    if not data: return []
    return data.get("result",[]) or []

def _add_us_hypotheses(cands: List[Candidate]) -> List[Candidate]:
    best: Dict[str, Candidate] = {}
    for c in cands:
        if c[0] not in best or c[3] > best[c[0]][3]:
            best[c[0]] = c
    new = dict(best)
    for sym,nm,typ,sc,src in list(best.values()):
        if re.fullmatch(r"[A-Z]{1,5}\.[AB]", sym):
            continue
        if re.fullmatch(r"[A-Z]{5}", sym) and sym[-1] in {"Y","F"}:
            continue
        for suf in _NON_US_SUFFIXES:
            if sym.endswith(suf):
                stem = sym[: -len(suf)]
                if stem and _is_us_like_symbol(stem) and stem not in new:
                    adj = _apply_biases(stem, sc-0.02, _simplify_name(nm), typ, typ,
                                        input_name=nm, candidate_name=nm)
                    new[stem] = (stem, nm, typ, round(adj,2), f"{src}+USHyp")
                break
    return list(new.values())

def _add_shareclass_hypotheses(cands: List[Candidate]) -> List[Candidate]:
    best: Dict[str, Candidate] = {}
    for c in cands:
        if c[0] not in best or c[3] > best[c[0]][3]:
            best[c[0]] = c
    new = dict(best)
    for sym, nm, typ, sc, src in list(best.values()):
        m = re.fullmatch(r"([A-Z]{1,5})\.([AB])", sym)
        if not m:
            continue
        base, cls = m.group(1), m.group(2)
        other = f"{base}.B" if cls == "A" else f"{base}.A"
        if other in new:
            continue
        adj = sc - 0.03
        adj = _apply_biases(other, adj, _simplify_name(nm), typ, typ, input_name=nm, candidate_name=nm)
        new[other] = (other, nm, typ, round(adj, 2), f"{src}+ClassHyp")
    return list(new.values())

def _finnhub_candidates(name: str, tag: str="Finnhub") -> List[Candidate]:
    if not _FINNHUB_TOKEN: return []
    sname = _simplify_name(name)
    expected = _infer_expected_type(name)
    out: List[Candidate] = []
    seen: set[str] = set()
    best_sym, best_score = "", 0.0

    def _one_query(q: str) -> List[Candidate]:
        rows = _finnhub_search_raw(q)
        if not rows: return []
        loc: List[Candidate] = []
        for r in rows:
            sym = (r.get("symbol") or "").upper()
            if not sym or sym in seen: continue
            desc = r.get("description") or r.get("displaySymbol") or sym
            qt  = (r.get("type") or "Common Stock").strip() or "Common Stock"
            typ = "ETF" if str(qt).upper() == "ETF" else "Common Stock"
            base = _fuzzy_score(name, desc)
            score = _apply_biases(sym, base, sname, typ, expected,
                                  input_name=name, candidate_name=desc)
            sc = round(score, 2)
            loc.append((sym, desc, typ, sc, tag))
        return loc

    variants = _query_variants_for_name(name)
    if VARIANT_CONCURRENCY > 0:
        with ThreadPoolExecutor(max_workers=min(VARIANT_CONCURRENCY, len(variants))) as ex:
            for res in ex.map(_one_query, variants):
                for c in res:
                    if c[0] in seen: continue
                    seen.add(c[0]); out.append(c)
                    if c[3] > best_score and _is_us_like_symbol(c[0]):
                        best_sym, best_score = c[0], c[3]
    else:
        for q in variants:
            for c in _one_query(q):
                if c[0] in seen: continue
                seen.add(c[0]); out.append(c)
                if c[3] > best_score and _is_us_like_symbol(c[0]):
                    best_sym, best_score = c[0], c[3]
            if best_sym and best_score >= EARLY_EXIT_US_SCORE:
                break

    out = _add_us_hypotheses(out)
    out = _add_shareclass_hypotheses(out)
    out.sort(key=lambda t: (-t[3], t[0]))
    return out[:TOPK_RETURN]

# =============================================================================
# Polygon (US/OTC filter)
# =============================================================================
def _polygon_us_ok(r: Dict[str, Any]) -> bool:
    market = (r.get("market") or "").lower()
    locale = (r.get("locale") or "").lower()
    ex     = (r.get("primary_exchange") or "").upper()
    mic    = (r.get("primary_exchange_mic") or "").upper()

    if locale and locale != "us":
        return False

    us_exchanges = {"XNAS","XNYS","ARCX","BATS","IEXG","LTSE","XASE","XPHL","EDGA","EDGX"}
    otc_exchanges = {"OTC","OTCQX","OTCQB","PINX"}

    if market == "stocks":
        return True
    if PREFER_OTC and (market == "otc" or ex in otc_exchanges or mic in otc_exchanges):
        return True
    if ex in us_exchanges or mic in us_exchanges:
        return True
    return False

@lru_cache(maxsize=512)
def _polygon_search_raw(q: str) -> List[Dict[str, Any]]:
    if not (ENABLE_POLYGON and _POLYGON_KEY): return []
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {"search": q, "active": "true", "limit": 30, "apiKey": _POLYGON_KEY}
    data = _http_get_json(url, params)
    if not data: return []
    return data.get("results",[]) or []

def _polygon_candidates(name: str, tag: str="Polygon") -> List[Candidate]:
    if not (ENABLE_POLYGON and _POLYGON_KEY): return []
    sname = _simplify_name(name)
    expected = _infer_expected_type(name)
    out: List[Candidate] = []
    seen: set[str] = set()
    best_sym, best_score = "", 0.0

    def _one_query(q: str) -> List[Candidate]:
        rows = _polygon_search_raw(q)
        if not rows: return []
        loc: List[Candidate] = []
        for r in rows:
            if not _polygon_us_ok(r): continue
            sym = (r.get("ticker") or "").upper()
            if not sym or sym in seen: continue
            longname = r.get("name") or sym
            t = (r.get("type") or "").upper()
            typ = "ETF" if t == "ETF" else "Common Stock"
            base = _fuzzy_score(name, longname)
            score = _apply_biases(sym, base, sname, typ, expected,
                                  input_name=name, candidate_name=longname)
            sc = round(score, 2)
            loc.append((sym, longname, typ, sc, tag))
        return loc

    variants = _query_variants_for_name(name)
    if VARIANT_CONCURRENCY > 0:
        with ThreadPoolExecutor(max_workers=min(VARIANT_CONCURRENCY, len(variants))) as ex:
            for res in ex.map(_one_query, variants):
                for c in res:
                    if c[0] in seen: continue
                    seen.add(c[0]); out.append(c)
                    if c[3] > best_score and _is_us_like_symbol(c[0]):
                        best_sym, best_score = c[0], c[3]
    else:
        for q in variants:
            for c in _one_query(q):
                if c[0] in seen: continue
                seen.add(c[0]); out.append(c)
                if c[3] > best_score and _is_us_like_symbol(c[0]):
                    best_sym, best_score = c[0], c[3]
            if best_sym and best_score >= EARLY_EXIT_US_SCORE:
                break

    out = _add_us_hypotheses(out)
    out = _add_shareclass_hypotheses(out)
    out.sort(key=lambda t: (-t[3], t[0]))
    return out[:TOPK_RETURN]

# =============================================================================
# Aggregation + second pass
# =============================================================================
def _merge_best(*lists: List[Candidate]) -> List[Candidate]:
    best: Dict[str, Candidate] = {}
    for L in lists:
        for c in L:
            sym = c[0].upper()
            if sym not in best or c[3] > best[sym][3]:
                best[sym] = c
    out = list(best.values())
    out.sort(key=lambda t: (-t[3], t[0]))
    return out

def _group_by_family(cands: List[Candidate]) -> Dict[str, List[Candidate]]:
    fam: Dict[str, List[Candidate]] = {}
    for c in cands:
        key = _company_family_key(c[1] or c[0])
        fam.setdefault(key, []).append(c)
    return fam

def _within_family_prefer_us(cands: List[Candidate]) -> Candidate:
    cands = sorted(cands, key=lambda x: (-x[3], x[0]))
    best = cands[0]
    for c in cands[1:]:
        if abs(c[3]-best[3]) <= 0.04:
            if _is_us_like_symbol(c[0]) and not _is_us_like_symbol(best[0]):
                best = c
    return best

def _need_second_pass(cands: List[Candidate]) -> bool:
    if not SECOND_PASS_ENABLED or not cands: return False
    sym, _nm, _t, sc, _ = cands[0]
    foreignish = any(sym.endswith(suf) for suf in _NON_US_SUFFIXES)
    weak = sc < 0.88
    return PREFER_US_LISTINGS and (foreignish or weak)

def _second_pass(name: str, first_pass: List[Candidate]) -> List[Candidate]:
    fam = _group_by_family(first_pass)
    reps = sorted((_within_family_prefer_us(v) for v in fam.values()), key=lambda t: (-t[3], t[0]))
    qs = _second_pass_queries(reps[:SECOND_PASS_TOPK])
    addl: List[Candidate] = []
    for q in qs:
        addl += _finnhub_candidates(q, tag="Finnhub2")
        addl += _polygon_candidates(q, tag="Polygon2")
        if addl:
            addl.sort(key=lambda t: (-t[3], t[0]))
            if _is_us_like_symbol(addl[0][0]) and addl[0][3] >= EARLY_EXIT_US_SCORE:
                break
    combined = _merge_best(first_pass, addl)
    fam2 = _group_by_family(combined)
    collapsed = [ _within_family_prefer_us(v) for v in fam2.values() ]
    collapsed.sort(key=lambda t: (-t[3], t[0]))
    return collapsed[:TOPK_RETURN]

def _second_pass_queries(top_hits: List[Candidate]) -> List[str]:
    qs: List[str] = []
    for sym, nm, _t, _sc, _src in top_hits[:SECOND_PASS_TOPK]:
        base = nm or ""
        base2 = re.sub(r"\b(plc|sa|ag|nv|se)\b"," ", _simplify_name(base)).strip()
        for v in {base, base2}:
            v = _sanitize_query_for_api(v)
            if v and v not in qs:
                qs.append(v)
        for suf in _NON_US_SUFFIXES:
            if sym.endswith(suf):
                stem = sym[: -len(suf)]
                if stem and stem not in qs:
                    qs.append(_sanitize_query_for_api(stem))
                break
    return qs[:MAX_SECOND_PASS_QUERIES]

# =============================================================================
# Public API (accelerated)
# =============================================================================
def _should_query_more(after: List[Candidate], expected: Optional[str]) -> bool:
    if not after: return True
    top = after[0]
    if expected and expected.upper().startswith("ETF"):
        return not (top[2].upper().startswith("ETF") and top[3] >= 0.94)
    return not (_is_us_like_symbol(top[0]) and top[3] >= 0.95)

def _search_candidates_impl(name: str, use_locals: bool) -> List[Candidate]:
    """Inner implementation used by the memoized wrapper."""
    if not name:
        return []

    # Disk cache hit → single perfect candidate (preserve original behavior)
    hit = _cache_get(name)
    if hit:
        return [(hit, name, _infer_expected_type(name) or "Common Stock", 1.0, "Cache")]

    expected = _infer_expected_type(name)

    # Canonical early-accept ONLY if locals enabled
    canon: List[Candidate] = _canonical_candidates(name, use_locals)
    if use_locals and canon and canon[0][3] >= 0.96:
        canon_sorted = sorted(canon, key=lambda t: (-t[3], t[0]))[:TOPK_RETURN]
        return canon_sorted

    api_cands: List[Candidate] = []

    if PARALLEL_PROVIDERS:
        futs = []
        with ThreadPoolExecutor(max_workers=2) as ex:
            if _FINNHUB_TOKEN:
                futs.append(ex.submit(_finnhub_candidates, name))
            if ENABLE_POLYGON and _POLYGON_KEY:
                futs.append(ex.submit(_polygon_candidates, name))
            for f in as_completed(futs):
                try:
                    api_cands = _merge_best(api_cands, f.result())
                except Exception as e:
                    logger.error(f"provider error: {e}")
    else:
        fh = _finnhub_candidates(name) if _FINNHUB_TOKEN else []
        api_cands = _merge_best(api_cands, fh)
        if _should_query_more(api_cands, expected):
            api_cands = _merge_best(api_cands, _polygon_candidates(name))

    # Optional local fast accept (only if locals enabled and LOCAL_FIRST)
    if use_locals and LOCAL_FIRST:
        fast = _LocalIndex.fast_candidates(name)
        if fast and fast[0][3] >= LOCAL_ACCEPT_SCORE:
            return [fast[0]]

    # Local fallbacks ONLY if locals enabled
    local_cands: List[Candidate] = []
    if use_locals and canon:
        local_cands = _merge_best(local_cands, canon)
    if use_locals:
        lm = _local_master_candidates(name)
        if lm:
            local_cands = _merge_best(local_cands, lm)

    merged = _merge_best(api_cands, local_cands)
    fam = _group_by_family(merged)
    collapsed = [ _within_family_prefer_us(v) for v in fam.values() ]
    collapsed.sort(key=lambda t: (-t[3], t[0]))

    if _need_second_pass(collapsed):
        collapsed = _second_pass(name, collapsed)

    return collapsed

@lru_cache(maxsize=CANDIDATE_CACHE_SIZE)
def _search_memo(name: str, use_locals: bool) -> Tuple[Candidate, ...]:
    return tuple(_search_candidates_impl(name, use_locals))

def search_candidates(name: str, *, use_local_maps: Optional[bool] = None) -> List[Candidate]:
    """Deterministic, rounded, sorted candidates. APIs-first; local maps only if enabled."""
    name = (name or "").strip()
    if not name: return []
    use_locals = USE_LOCAL_MAPS_DEFAULT if use_local_maps is None else bool(use_local_maps)
    return list(_search_memo(name, use_locals))

# --- profile/quote helpers for enrichment ---
def _finnhub_profile(sym: str) -> Dict[str, Any]:
    if not _FINNHUB_TOKEN:
        return {}
    url = "https://finnhub.io/api/v1/stock/profile2"
    data = _http_get_json(url, {"symbol": sym, "token": _FINNHUB_TOKEN}) or {}
    out = {}
    if data:
        out["name"] = data.get("name") or data.get("ticker") or ""
        out["type"] = "ETF" if str(data.get("finnhubIndustry","")).lower() == "etf" else "Common Stock"
        out["source"] = "FinnhubProfile"
    return out

def _finnhub_quote(sym: str) -> Dict[str, Any]:
    if not _FINNHUB_TOKEN:
        return {}
    url = "https://finnhub.io/api/v1/quote"
    data = _http_get_json(url, {"symbol": sym, "token": _FINNHUB_TOKEN}) or {}
    out = {}
    # c = current price, pc = previous close
    price = None
    try:
        price = float(data.get("c") or 0.0) or float(data.get("pc") or 0.0)
    except Exception:
        price = None
    if price:
        out["price"] = price
        out["price_source"] = "FinnhubQuote"
    return out

def _polygon_ticker(sym: str) -> Dict[str, Any]:
    if not (ENABLE_POLYGON and _POLYGON_KEY):
        return {}
    url = f"https://api.polygon.io/v3/reference/tickers/{sym.upper()}"
    data = _http_get_json(url, {"apiKey": _POLYGON_KEY}) or {}
    r = data.get("results") or {}
    out = {}
    if r:
        out["name"] = r.get("name") or ""
        typ = r.get("type") or ""
        out["type"] = "ETF" if str(typ).upper() == "ETF" else "Common Stock"
        out["source"] = "PolygonTicker"
    return out

def enrich_symbol(sym: str) -> Dict[str, Any]:
    """
    Best-effort metadata for an already-known symbol:
    returns {'name', 'type', 'price', 'source', 'price_source'} (subset may be present).
    """
    sym = (sym or "").upper().strip()
    if not sym:
        return {}

    prof = {}
    if ENABLE_POLYGON and _POLYGON_KEY:
        prof = _polygon_ticker(sym)
    if not prof:  # fall back
        prof = _finnhub_profile(sym)

    quote = _finnhub_quote(sym)

    out = {}
    out["name"] = (prof.get("name") or "").strip()
    out["type"] = prof.get("type") or "Common Stock"
    if quote.get("price"):
        out["price"] = quote["price"]
        out["price_source"] = quote.get("price_source")
    out["source"] = prof.get("source") or quote.get("price_source") or ""
    return out


def search_with_meta(name: str, *, use_local_maps: Optional[bool] = None) -> Tuple[List[Candidate], Dict[str, Any]]:
    """Like search_candidates, but returns audit meta: search variants + api latency."""
    t0 = time.time()
    cands = search_candidates(name, use_local_maps=use_local_maps)
    latency_ms = int((time.time() - t0) * 1000)
    return cands, {
        "search_variants": get_search_variants(name),
        "api_latency_ms": latency_ms,
        "use_local_maps": USE_LOCAL_MAPS_DEFAULT if use_local_maps is None else bool(use_local_maps),
        "resolver_version": RESOLVER_VERSION,
    }

def choose_symbol(name: str, candidates: List[Candidate]) -> Tuple[Optional[str], str]:
    """
    Selection:
      - Respect share class hints (A/B/C), + hard rule for Alphabet.
      - If Berkshire with no class, default to BRK.B if present.
      - Else accept single / high-confidence; otherwise AMBIGUOUS.
    """
    if not candidates: return None, "no_candidates"
    sname = _simplify_name(name)
    tokens_in = set(_tokenize(name))

    def _encodes_class(sym: str, longname: str, hint: str) -> bool:
        ln = (longname or "").lower()
        # Hard rule: Alphabet
        if "alphabet" in tokens_in:
            if hint == "c" and sym.upper() == "GOOG": return True
            if hint == "a" and sym.upper() == "GOOGL": return True
        if hint == "a":
            return bool(re.fullmatch(r".*\.A$", sym) or re.search(r"\bclass\s*a\b", ln))
        if hint == "b":
            return bool(re.fullmatch(r".*\.B$", sym) or re.search(r"\bclass\s*b\b", ln))
        if hint == "c":
            return bool(re.search(r"\bclass\s*c\b", ln))
        return False

    class_hint = "a" if "classa" in sname else "b" if "classb" in sname else "c" if "classc" in sname else None

    if class_hint:
        matches = [c for c in candidates if _encodes_class(c[0], c[1] or "", class_hint)]
        if len(matches) == 1:
            sym = matches[0][0]; _cache_put(name, sym); return sym, f"class_match:{matches[0][3]:.2f}"
        if len(matches) > 1:
            matches.sort(key=lambda t: (-t[3], t[0]))
            sym = matches[0][0]; _cache_put(name, sym); return sym, f"class_match_top:{matches[0][3]:.2f}"
        return None, "ambiguous_class_hint"

    if "berkshire" in sname:
        brkb = [c for c in candidates if c[0].upper().endswith(".B")]
        if brkb:
            brkb.sort(key=lambda t: (-t[3], t[0]))
            sym = brkb[0][0]; _cache_put(name, sym); return sym, f"berkshire_default_B:{brkb[0][3]:.2f}"

    if len(candidates) == 1:
        sym = candidates[0][0]; _cache_put(name, sym); return sym, f"single_candidate:{candidates[0][3]:.2f}"

    top = candidates[0]
    if top[3] >= 0.90:
        _cache_put(name, top[0]); return top[0], f"top>=0.90:{top[3]:.2f}"

    return None, "ambiguous"

def is_generic_name(_: str) -> bool:
    return False

# =============================================================================
# Batch API
# =============================================================================
def resolve_one(name: str, *, use_local_maps: Optional[bool] = None) -> Tuple[str, Optional[str], str, Dict[str, Any], List[Candidate]]:
    cands, meta = search_with_meta(name, use_local_maps=use_local_maps)
    sym, reason = choose_symbol(name, cands)
    return name, sym, reason, meta, cands

def resolve_many(names: Iterable[str], *, use_local_maps: Optional[bool] = None) -> Dict[str, Tuple[Optional[str], str]]:
    use_locals = USE_LOCAL_MAPS_DEFAULT if use_local_maps is None else bool(use_local_maps)
    unique = list(dict.fromkeys([n.strip() for n in names if n and n.strip()]))
    out: Dict[str, Tuple[Optional[str], str]] = {}

    if use_locals and LOCAL_FIRST:
        for n in unique:
            hit = _cache_get(n)
            if hit:
                out[n] = (hit, "cache"); continue
            fast = _LocalIndex.fast_candidates(n)
            if fast and fast[0][3] >= LOCAL_ACCEPT_SCORE:
                _cache_put(n, fast[0][0])
                out[n] = (fast[0][0], "local_fast")
    pending = [n for n in unique if n not in out]
    if not pending:
        return out

    with ThreadPoolExecutor(max_workers=max(1, BATCH_MAX_WORKERS)) as ex:
        futs = {ex.submit(resolve_one, n, use_local_maps=use_locals): n for n in pending}
        for f in as_completed(futs):
            n, sym, reason, _meta, _cands = f.result()
            out[n] = (sym, reason)
    return out

def new_run_id() -> str:
    return str(uuid.uuid4())

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
