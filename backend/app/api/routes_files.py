from __future__ import annotations

import csv
import io
import json
import os
import asyncio
from typing import List, Dict, Any, Optional, Tuple

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse

from app.services.io_utils import load_table
from app.services.resolve import (
    search_candidates,
    search_with_meta,
    choose_symbol,
    is_generic_name,
    new_run_id,
    RESOLVER_VERSION,
)
from app.models.records import HoldingRow
from app.services.enrich import get_provider, ENRICH_FIELDS

router = APIRouter()  # no prefix

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "") or os.getenv("FINNHUB_TOKEN", "")
POLYGON_KEY = os.getenv("POLYGON_API_KEY", "")

# -------------------- helpers --------------------
def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip()
        return s in ("", "—", "(blank)", "None")
    return False

def _safe_num(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _cand_tuple_to_dict(t) -> Dict[str, Any]:
    """
    Accepts 4- or 5-tuple candidates; adds 'source' when present.
    (symbol, name, type, score, source?)
    """
    if not isinstance(t, (list, tuple)) or len(t) < 4:
        return {}
    d = {"symbol": t[0], "name": t[1], "type": t[2], "score": float(t[3])}
    if len(t) >= 5:
        d["source"] = t[4]
    return d

def _top3_json(cand_objs: List[Dict[str, Any]]) -> str:
    top = [{
        "symbol": c.get("symbol"),
        "name": c.get("name"),
        "type": c.get("type"),
        "score": round(float(c.get("score", 0.0)), 2),
        "source": c.get("source", "")
    } for c in cand_objs[:3]]
    return json.dumps(top, ensure_ascii=False)

def _has_fillable_blanks(row: Dict[str, Any], proposed: Dict[str, Any]) -> bool:
    """Whether proposed enrichments would actually fill any blank tracked fields."""
    if not proposed:
        return False
    if _is_missing(row.get("Name")) and proposed.get("Name"):
        return True
    for f in ENRICH_FIELDS:
        if f == "Name":
            continue
        if _is_missing(row.get(f)) and proposed.get(f):
            return True
    return False

def _apply_proposals(row: Dict[str, Any], proposed: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Return (filled_row, audit_map).
    filled_row: shallow copy of row with only blank fields filled from proposed.
    audit_map: {f_prev, f_new, f_source, f_confidence} entries for changed fields.
    """
    if not proposed:
        return dict(row), {}
    out = dict(row)
    audit: Dict[str, Any] = {}
    for f in ENRICH_FIELDS:
        prev = out.get(f)
        newv = proposed.get(f)
        if (prev is None or str(prev).strip() in {"", "—", "(blank)", "None"}) and (newv not in (None, "")):
            out[f] = newv
            audit[f"{f}_prev"] = prev or ""
            audit[f"{f}_new"] = newv
            audit[f"{f}_source"] = proposed.get("_source", "")
            audit[f"{f}_confidence"] = proposed.get("_confidence", "")
    return out, audit

def _audit_header() -> List[str]:
    cols: List[str] = []
    for f in ENRICH_FIELDS:
        cols += [f"{f}_prev", f"{f}_new", f"{f}_source", f"{f}_confidence"]
    return cols

# -------------------- preview --------------------
@router.post("/preview-file")
async def preview_file(
    file: UploadFile = File(...),
    use_local_maps: bool = Form(False),
) -> List[Dict[str, Any]]:
    try:
        content = await file.read()
        raw_rows: List[Dict[str, Any]] = load_table(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    provider = get_provider(FINNHUB_KEY, POLYGON_KEY)

    # enrichment for rows that already have a symbol and are missing fields.
    tasks: List[asyncio.Task] = []
    sym_rows_idx: List[int] = []
    for i, r in enumerate(raw_rows):
        row = HoldingRow.model_validate(r)
        name = (row.Name or "").strip() if row.Name is not None else ""
        symbol_in = (row.Symbol or "").strip() if row.Symbol is not None else ""

        if not _is_missing(symbol_in):
            price_in = _safe_num(row.Price)
            shares_in = _safe_num(row.Shares)
            mv_in = _safe_num(row.MarketValue)
            need_name = _is_missing(name)
            need_price = price_in is None
            need_mv = mv_in is None and (price_in is not None and shares_in is not None)
            if (need_name or need_price or need_mv) and (FINNHUB_KEY or POLYGON_KEY):
                tasks.append(asyncio.create_task(provider.enrich_by_symbol(symbol_in)))
                sym_rows_idx.append(i)

    proposals: Dict[int, Dict[str, Any]] = {}
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, res in zip(sym_rows_idx, results):
            if isinstance(res, dict):
                proposals[idx] = res

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(raw_rows):
        row = HoldingRow.model_validate(r)
        name = (row.Name or "").strip() if row.Name is not None else ""
        symbol_in = (row.Symbol or "").strip() if row.Symbol is not None else ""

        # SYMBOL PRESENT - populate enrichments
        if not _is_missing(symbol_in):
            proposed = proposals.get(i) or {}
            status = "ENRICHED" if _has_fillable_blanks(r, proposed) else "UNCHANGED"

            filled, _audit = _apply_proposals(
                {"Name": row.Name, "Symbol": row.Symbol, "Price": row.Price, "MarketValue": row.MarketValue, "Shares": row.Shares},
                proposed
            )

            mv_in = _safe_num(row.MarketValue)
            price_in = _safe_num(filled.get("Price"))
            shares_in = _safe_num(filled.get("Shares"))
            mv_final = mv_in
            if mv_final is None and price_in is not None and shares_in is not None:
                mv_final = round(price_in * shares_in, 2)

            out.append({
                "index": i,
                "status": status,
                "candidates": [],
                "notes": "symbol_present",
                "input": {"Name": row.Name, "Symbol": row.Symbol},
                "Name": filled.get("Name"),
                "Symbol": filled.get("Symbol"),
                "Price": price_in,
                "Shares": shares_in,
                "MarketValue": mv_final,
            })
            continue

        if _is_missing(name):
            out.append({
                "index": i,
                "status": "NOT_FOUND",
                "candidates": [],
                "notes": "missing_name",
                "input": {"Name": row.Name, "Symbol": row.Symbol},
            })
            continue

        if is_generic_name(name):
            out.append({
                "index": i,
                "status": "NOT_FOUND",
                "candidates": [],
                "notes": "generic_name",
                "input": {"Name": row.Name, "Symbol": row.Symbol},
            })
            continue

        cands = search_candidates(name, use_local_maps=use_local_maps) or []
        cand_objs = [_cand_tuple_to_dict(c) for c in cands]
        chosen, reason = choose_symbol(name, cands)
        status = "FILLED" if chosen else ("AMBIGUOUS" if cands else "NOT_FOUND")

        out.append({
            "index": i,
            "status": status,
            "candidates": cand_objs,
            "notes": reason,
            "input": {"Name": row.Name, "Symbol": row.Symbol},
        })

    return out

# -------------------- commit --------------------
@router.post("/commit-file")
async def commit_file(
    file: UploadFile = File(...),
    overrides_json: UploadFile | None = File(None),
    use_local_maps: bool = Form(False),
):
    try:
        content = await file.read()
        parsed_rows: List[Dict[str, Any]] = load_table(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read table: {e}")

    overrides: Dict[int, str] = {}
    if overrides_json is not None:
        try:
            raw = await overrides_json.read()
            if raw:
                overrides = {int(k): str(v) for k, v in json.loads(raw.decode("utf-8")).items()}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid overrides_json: {e}")

    provider = get_provider(FINNHUB_KEY, POLYGON_KEY)

    head_base = [
        "Name", "Symbol", "Price", "Shares", "MarketValue",
        "ResolveStatus", "ResolvedSymbol", "ResolveSource", "ResolveScore", "ResolveReason",
        "TopCandidatesJSON", "WasOverridden", "OverrideSymbol",
        "SearchVariantsJSON", "ApiLatencyMs",
        "RunId", "ResolverVersion",
    ]
    header = head_base + _audit_header()

    run_id = new_run_id()
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)

    async def _enrich(symbol: str) -> Optional[Dict[str, Any]]:
        try:
            return await provider.enrich_by_symbol(symbol)
        except Exception:
            return None

    for i, r in enumerate(parsed_rows):
        row = HoldingRow.model_validate(r)

        name = (row.Name or "").strip() if row.Name is not None else ""
        symbol_in = (row.Symbol or "").strip() if row.Symbol is not None else ""

        price = _safe_num(row.Price)
        shares = _safe_num(row.Shares)
        mv = _safe_num(row.MarketValue)
        if mv is None and price is not None and shares is not None:
            mv = round(price * shares, 2)

        # ---------- Case 1: symbol already present → attempt enrichment ----------
        if not _is_missing(symbol_in):
            proposed = await _enrich(symbol_in) if (FINNHUB_KEY or POLYGON_KEY) else None
            filled_dict, audit_map = _apply_proposals(
                {"Name": row.Name, "Symbol": row.Symbol, "Price": row.Price, "MarketValue": row.MarketValue, "Shares": row.Shares},
                proposed or {}
            )

            price_f = _safe_num(filled_dict.get("Price"))
            shares_f = _safe_num(filled_dict.get("Shares"))
            mv_in = _safe_num(filled_dict.get("MarketValue"))
            mv_final = mv_in
            if mv_final is None and price_f is not None and shares_f is not None:
                mv_final = round(price_f * shares_f, 2)

            audit_cells: List[Any] = []
            for f in ENRICH_FIELDS:
                audit_cells += [
                    audit_map.get(f"{f}_prev", ""),
                    audit_map.get(f"{f}_new", ""),
                    audit_map.get(f"{f}_source", ""),
                    audit_map.get(f"{f}_confidence", ""),
                ]

            status = "ENRICHED" if any(audit_map.values()) else "UNCHANGED"

            writer.writerow([
                filled_dict.get("Name"), filled_dict.get("Symbol"), price_f, shares_f, mv_final,
                status, "", "", "", "symbol_present",
                "[]", False, "",
                "[]", 0,
                run_id, RESOLVER_VERSION,
                *audit_cells
            ])
            continue

        # ---------- Case 2: missing name ----------
        if _is_missing(name):
            writer.writerow([
                row.Name, "", row.Price, row.Shares, mv,
                "NOT_FOUND", "", "", "", "missing_name",
                "[]", False, "",
                "[]", 0,
                run_id, RESOLVER_VERSION,
                *([""] * len(_audit_header()))
            ])
            continue

        # ---------- Case 3: generic ----------
        if is_generic_name(name):
            writer.writerow([
                row.Name, "", row.Price, row.Shares, mv,
                "NOT_FOUND", "", "", "", "generic_name",
                "[]", False, "",
                "[]", 0,
                run_id, RESOLVER_VERSION,
                *([""] * len(_audit_header()))
            ])
            continue

        # ---------- Case 4: resolve then (optionally) enrich ----------
        cands, meta = search_with_meta(name, use_local_maps=use_local_maps)
        cand_objs = [_cand_tuple_to_dict(c) for c in cands]

        was_over = i in overrides
        if was_over:
            chosen = overrides[i]
            reason = "override"
            src = ""
            score = ""
            for co in cand_objs:
                if co.get("symbol", "").upper() == chosen.upper():
                    src = co.get("source", "") or ""
                    score = f"{float(co.get('score', 0.0)):.2f}"
                    break
        else:
            chosen, reason = choose_symbol(name, cands)
            src = ""
            score = ""
            if chosen:
                for co in cand_objs:
                    if co.get("symbol", "").upper() == chosen.upper():
                        src = co.get("source", "") or ""
                        score = f"{float(co.get('score', 0.0)):.2f}"
                        break

        top_json = _top3_json(cand_objs)
        search_variants_json = json.dumps(meta.get("search_variants", []), ensure_ascii=False)
        api_latency_ms = int(meta.get("api_latency_ms", 0))

        if chosen:
            proposed = await _enrich(chosen) if (FINNHUB_KEY or POLYGON_KEY) else None
            filled_dict, audit_map = _apply_proposals(
                {"Name": row.Name, "Symbol": chosen, "Price": row.Price, "MarketValue": row.MarketValue, "Shares": row.Shares},
                proposed or {}
            )

            # compute MV as needed
            price_f = _safe_num(filled_dict.get("Price"))
            shares_f = _safe_num(filled_dict.get("Shares"))
            mv_in = _safe_num(filled_dict.get("MarketValue"))
            mv_final = mv_in
            if mv_final is None and price_f is not None and shares_f is not None:
                mv_final = round(price_f * shares_f, 2)

            audit_cells: List[Any] = []
            for f in ENRICH_FIELDS:
                audit_cells += [
                    audit_map.get(f"{f}_prev", ""),
                    audit_map.get(f"{f}_new", ""),
                    audit_map.get(f"{f}_source", ""),
                    audit_map.get(f"{f}_confidence", ""),
                ]

            writer.writerow([
                filled_dict.get("Name"), filled_dict.get("Symbol"), price_f, shares_f, mv_final,
                "FILLED", chosen, src, score, reason,
                top_json, bool(was_over), overrides.get(i, ""),
                search_variants_json, api_latency_ms,
                run_id, RESOLVER_VERSION,
                *audit_cells
            ])
        else:
            writer.writerow([
                row.Name, "", row.Price, row.Shares, mv,
                ("AMBIGUOUS" if cand_objs else "NOT_FOUND"), "", "", "", reason,
                top_json, False, "",
                search_variants_json, api_latency_ms,
                run_id, RESOLVER_VERSION,
                *([""] * len(_audit_header()))
            ])

    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename=\"enriched_holdings.csv\"'},
    )
