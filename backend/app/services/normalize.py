import re
from typing import Optional, Tuple

_CORP_SUFFIXES = [
    r"\bincorporated\b", r"\binc\b\.?", r"\bcorp\b\.?", r"\bcorporation\b",
    r"\bco\b\.?", r"\bcompany\b", r"\bltd\b\.?", r"\bplc\b", r"\bsa\b", r"\bag\b",
    r"\bnv\b", r"\boyj\b", r"\bspa\b", r"\bollc\b"
]

_SUFFIX_RE = re.compile("|".join(_CORP_SUFFIXES), re.IGNORECASE)

_CLASS_RE = re.compile(r"\bclass\s+([ABCDEF])\b", re.IGNORECASE)
_SERIES_RE = re.compile(r"\bseries\s+([A-Z0-9]+)\b", re.IGNORECASE)
_ADR_RE = re.compile(r"\badr\b", re.IGNORECASE)

def normalize_name(name: Optional[str]) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Returns (base_name, class_hint, adr_hint)
    - base_name: lowercased, corp suffixes removed, punctuation collapsed
    - class_hint: 'A','B','C' if present
    - adr_hint: True if ADR present
    """
    if not name or not name.strip():
        return None, None, False

    s = name.strip()

    # extract hints
    class_hint = None
    m = _CLASS_RE.search(s)
    if m:
        class_hint = m.group(1).upper()

    adr_hint = bool(_ADR_RE.search(s))

    # remove class/series markers from search base
    s = _CLASS_RE.sub("", s)
    s = _SERIES_RE.sub("", s)

    # strip corp suffixes
    s = _SUFFIX_RE.sub("", s)

    # strip extra tokens like "class", "series" remnants and punctuation
    s = re.sub(r"[^A-Za-z0-9\s&\-\.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()

    return s, class_hint, adr_hint
