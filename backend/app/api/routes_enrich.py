from typing import List, Dict, Optional
from fastapi import APIRouter
from pydantic import BaseModel

from app.services.resolve import search_candidates
from app.services.decision import choose_symbol
from app.models.records import HoldingRow

router = APIRouter(prefix="/enrich", tags=["enrich"])

class EnrichCommitInput(BaseModel):
    rows: List[HoldingRow]
    overrides: Dict[int, str] | None = None  # index -> symbol

class EnrichedRow(BaseModel):
    Name: Optional[str] = None
    Symbol: Optional[str] = None
    Price: Optional[float] = None
    Shares: Optional[float] = None
    MarketValue: Optional[float] = None
    ResolveStatus: str
    ResolvedSymbol: Optional[str] = None
    ResolveNotes: Optional[str] = None
    CandidatesTop3: Optional[str] = None

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _is_blank(s: Optional[str]) -> bool:
    return (s is None) or (str(s).strip() == "")

@router.post("/commit", response_model=List[EnrichedRow])
def commit(payload: EnrichCommitInput):
    out: List[EnrichedRow] = []
    overrides = payload.overrides or {}

    for i, row in enumerate(payload.rows):
        name = (row.Name or "").strip()
        symbol_in = (row.Symbol or "").strip() if row.Symbol else ""

        # UNCHANGED if symbol present
        if symbol_in:
            out.append(EnrichedRow(
                Name=row.Name, Symbol=symbol_in, Price=row.Price,
                Shares=row.Shares, MarketValue=row.MarketValue,
                ResolveStatus="UNCHANGED", ResolvedSymbol=None,
                ResolveNotes="symbol_present", CandidatesTop3=None
            ))
            continue

        # No name -> NOT_FOUND
        if _is_blank(name):
            out.append(EnrichedRow(
                Name=row.Name, Symbol=None, Price=row.Price,
                Shares=row.Shares, MarketValue=row.MarketValue,
                ResolveStatus="NOT_FOUND", ResolvedSymbol=None,
                ResolveNotes="missing_name", CandidatesTop3=None
            ))
            continue

        # API
        cands = search_candidates(name)
        top3 = "; ".join([f"{c[0]}:{c[3]:.2f}" for c in cands[:3]]) if cands else None

        # Applying override or chooser
        if i in overrides:
            chosen, reason = overrides[i], "override"
        else:
            chosen, reason = choose_symbol(name, cands)

        if chosen:
            price = _safe_float(row.Price)
            shares = _safe_float(row.Shares)
            mv = row.MarketValue
            if mv is None and price is not None and shares is not None:
                mv = round(price * shares, 2)

            out.append(EnrichedRow(
                Name=row.Name, Symbol=chosen, Price=row.Price,
                Shares=row.Shares, MarketValue=mv,
                ResolveStatus="FILLED", ResolvedSymbol=chosen,
                ResolveNotes=reason, CandidatesTop3=top3
            ))
        else:
            out.append(EnrichedRow(
                Name=row.Name, Symbol=None, Price=row.Price,
                Shares=row.Shares, MarketValue=row.MarketValue,
                ResolveStatus=("AMBIGUOUS" if cands else "NOT_FOUND"),
                ResolvedSymbol=None, ResolveNotes=reason, CandidatesTop3=top3
            ))
    return out
