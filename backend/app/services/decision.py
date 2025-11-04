# app/services/decision.py
from __future__ import annotations

from typing import List, Tuple, Optional
from app.services.resolve import _simplify_name, cache_put, is_generic_name, Candidate

def choose_symbol(name: str, candidates: List[Candidate]) -> Tuple[Optional[str], str]:
    """
    Auto-pick policy:
      - 0 candidates          -> (None, "no_candidates")
      - 1 candidate           -> pick only if score >= threshold (0.90; 0.96 if generic)
      - Share-class hints     -> prefer GOOGL/GOOG or BRK.A/BRK.B
      - Non-generic names     -> top score >= 0.90 -> pick
      - Generic names         -> top score >= 0.96 -> pick
      - Otherwise             -> (None, "ambiguous")
    """
    if not candidates:
        return None, "no_candidates"

    # ensure sorted by score desc (defensive)
    cands = sorted(candidates, key=lambda x: float(x[3]), reverse=True)

    sname = _simplify_name(name)
    generic = is_generic_name(name)
    threshold = 0.96 if generic else 0.90

    # share-class shortcuts
    if "classa" in sname:
        for c in cands:
            if c[0].upper() in {"GOOGL", "BRK.A"}:
                cache_put(name, c[0])
                return c[0], f"class_match>=0.85:{c[3]:.2f}"
    if "classb" in sname:
        for c in cands:
            if c[0].upper() in {"BRK.B"}:
                cache_put(name, c[0])
                return c[0], f"class_match>=0.85:{c[3]:.2f}"
    if "classc" in sname:
        for c in cands:
            if c[0].upper() in {"GOOG"}:
                cache_put(name, c[0])
                return c[0], f"class_match>=0.85:{c[3]:.2f}"

    # single-candidate gating (this is what was biting you)
    if len(cands) == 1:
        top = cands[0]
        if float(top[3]) >= threshold:
            cache_put(name, top[0])
            reason = f"single_candidate>={threshold:.2f}:{float(top[3]):.2f}"
            if generic:
                reason += ":generic_name"
            return top[0], reason
        return None, f"single_low:{float(top[3]):.2f}" + (":generic_name" if generic else "")

    # multi-candidate thresholding
    top = cands[0]
    if float(top[3]) >= threshold:
        cache_put(name, top[0])
        reason = f"top>={threshold:.2f}:{float(top[3]):.2f}"
        if generic:
            reason += ":generic_name"
        return top[0], reason

    return None, "ambiguous" + (":generic_name" if generic else "")
