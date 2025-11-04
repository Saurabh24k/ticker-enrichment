# backend/app/services/io_utils.py
from __future__ import annotations

import io
import re
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from loguru import logger

# ---------- Canonical header aliases ----------
_CANONICAL_ALIASES: Dict[str, set[str]] = {
    "name": {
        "name", "security", "security name", "company", "company name", "description",
        "holding", "issuer", "asset", "instrument", "equity", "fund name",
        "security description", "security_desc", "long name",
    },
    "symbol": {
        "symbol", "ticker", "ticker symbol", "code", "security id", "secid", "ric",
        "isin symbol", "ticker_code",
    },
    "price": {
        "price", "last", "last price", "close", "close price", "unit price",
        "market price", "cost", "unit cost", "avg price",
    },
    "# of shares": {
        "# of shares", "shares", "qty", "quantity", "units", "position",
        "position size", "amount", "share qty",
    },
    "market value": {
        "market value", "marketvalue", "market val", "value", "mv",
        "position value", "current value", "total value", "valuation",
    },
}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

_REV: Dict[str, str] = {}
for canon, aliases in _CANONICAL_ALIASES.items():
    for a in aliases:
        _REV[_norm(a)] = canon

_REGEX_RULES: list[tuple[str, str]] = [
    (r"\b(name|security|company|description|issuer|holding|asset|instrument|equity|fund)\b", "name"),
    (r"\b(symbol|ticker|code|secid|ric)\b", "symbol"),
    (r"\b(price|last|close|unit\s*price|market\s*price|avg|cost)\b", "price"),
    (r"\b(shares?|qty|quantity|units|position(?!.*value)|amount)\b", "# of shares"),
    (r"(market.*value|position.*value|total.*value|\bvalue\b|\bmv\b)", "market value"),
]

def _map_headers(cols: List[Any]) -> List[str]:
    mapped: List[str] = []
    for c in cols:
        raw = str(c)
        key = _REV.get(_norm(raw))
        if not key:
            s = raw.strip().lower()
            for pat, canon in _REGEX_RULES:
                if re.search(pat, s):
                    key = canon
                    break
        mapped.append(key if key else raw)
    return mapped

def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For duplicated column names, keep the leftmost copy, fill its blanks from the
    right copies, then drop those right-hand duplicates by *position*.
    """
    name_to_idxs: dict[Any, list[int]] = {}
    for idx, col_name in enumerate(df.columns):
        name_to_idxs.setdefault(col_name, []).append(idx)

    keep_indices: list[int] = list(range(df.shape[1]))

    for _, idxs in name_to_idxs.items():
        if len(idxs) <= 1:
            continue

        sub = df.iloc[:, idxs].copy()
        # per Series -> map (avoid deprecated applymap)
        sub = sub.apply(lambda s: s.map(
            lambda v: None if (pd.isna(v) or (isinstance(v, str) and v.strip() == "")) else v
        ))
        combined = sub.bfill(axis=1).iloc[:, 0]
        df.iloc[:, idxs[0]] = combined

        for j in idxs[1:]:
            if j in keep_indices:
                keep_indices.remove(j)

    df = df.iloc[:, keep_indices].copy()
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def _drop_header_echo_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows that look like header rows mistakenly left in the data.
    We require >=3 header-name matches in the same row to avoid false positives.
    """
    if df.empty:
        return df

    def pretty(s: str) -> str:
        return str(s).strip().replace("_", " ").casefold()

    col_pretty = [pretty(c) for c in df.columns]

    matches = []
    for i, c in enumerate(df.columns):
        col_match = df[c].astype(str).map(lambda v: pretty(v) == col_pretty[i])
        matches.append(col_match)

    match_sum = matches[0].astype(int)
    for m in matches[1:]:
        match_sum = match_sum + m.astype(int)

    mask = match_sum >= 3
    return df.loc[~mask].reset_index(drop=True)

def _coerce_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(",", "").str.strip(),
                errors="coerce",
            )
    return df

def _dedupe_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop exact duplicate rows across the canonical columns.
    Keep the first occurrence to preserve order.
    """
    cols = [c for c in ["Name", "Symbol", "Price", "Shares", "MarketValue"] if c in df.columns]
    if not cols:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=cols, keep="first").reset_index(drop=True)
    after = len(df)
    if after != before:
        logger.info(f"De-duplicated identical rows: {before} -> {after}")
    return df

def _looks_like_symbol(val: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z.\-]{1,8}", (val or "").strip()))

def _looks_like_name(val: str) -> bool:
    if not val:
        return False
    if _looks_like_symbol(val):
        return False
    if re.fullmatch(r"\d+(\.\d+)?", val.strip()):
        return False
    return bool(re.search(r"[A-Za-z]", val)) and len(val.strip()) >= 3

def _infer_columns_by_content(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()

    # infer Symbol if missing/empty
    if "Symbol" not in df2.columns:
        need_symbol = True
    else:
        sym = df2["Symbol"].astype(str).fillna("").str.strip()
        need_symbol = sym.eq("").all()

    if need_symbol:
        best_col, best_score = None, -1.0
        for c in df2.columns:
            vals = df2[c].astype(str).fillna("")
            score = (vals.apply(_looks_like_symbol).sum()) / max(1, len(vals))
            if score > best_score:
                best_col, best_score = c, score
        if best_col and best_score >= 0.4:
            df2["Symbol"] = df2[best_col].astype(str).str.strip().replace({"": None})

    # infer Name if missing/empty
    if "Name" not in df2.columns:
        need_name = True
    else:
        nm = df2["Name"].astype(str).fillna("").str.strip()
        need_name = nm.eq("").all()

    if need_name:
        best_col, best_score = None, -1.0
        for c in df2.columns:
            vals = df2[c].astype(str).fillna("")
            score = (vals.apply(_looks_like_name).sum()) / max(1, len(vals))
            if score > best_score:
                best_col, best_score = c, score
        if best_col and best_score >= 0.4:
            df2["Name"] = df2[best_col].astype(str).str.strip().replace({"": None})

    return df2

def load_table(content: bytes, filename: str) -> list[dict]:
    """Read CSV/XLSX bytes, canonicalize headers, coalesce duplicates, infer missing columns, sanitize rows."""
    # 1) Read
    try:
        if filename.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content))
        else:
            # sep=None -> auto-detect; python engine handles weird delimiters
            df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig", sep=None, engine="python")
    except Exception as e:
        logger.exception(f"Failed to read {filename}: {e}")
        raise

    # 2) If first row looks like headers, promote it
    promoted = False
    if df.shape[0] > 0:
        first = df.iloc[0].astype(str).tolist()
        first_mapped = _map_headers(first)
        if any(x in first_mapped for x in ["name", "symbol", "price", "# of shares", "market value"]):
            df = df[1:].reset_index(drop=True)
            df.columns = first_mapped
            promoted = True

    # 3) Map headers and log
    original_cols = list(df.columns)
    df.columns = _map_headers(df.columns)

    # 4) Canonical internal names
    rename = {
        "name": "Name",
        "symbol": "Symbol",
        "price": "Price",
        "# of shares": "Shares",
        "market value": "MarketValue",
    }
    df = df.rename(columns=rename)

    # 5) Coalesce duplicate columns (e.g., Name/Symbol twice)
    df = _coalesce_duplicate_columns(df)

    # 6) Drop obvious header-echo rows
    df = _drop_header_echo_rows(df)

    logger.info(
        f"Loaded {len(df)} rows from {filename}; "
        f"{'promoted first row as header; ' if promoted else ''}"
        f"headers {original_cols} -> {list(df.columns)}"
    )

    # 7) Clean empties and infer missing cols by content
    df = df.replace({pd.NA: None})
    df = df.dropna(how="all")
    df = _infer_columns_by_content(df)

    # 8) Ensure required columns exist
    for col in ["Name", "Symbol", "Price", "Shares", "MarketValue"]:
        if col not in df.columns:
            df[col] = None

    # 9) Strip whitespace
    for c in ["Name", "Symbol"]:
        df[c] = df[c].astype(object).where(df[c].notna(), None)
        df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # 10) Coerce numerics and de-duplicate rows
    df = _coerce_numeric_cols(df, ["Price", "Shares", "MarketValue"])
    df = df.replace({np.nan: None})
    df = _dedupe_rows(df)

    return df.to_dict(orient="records")
