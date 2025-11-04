from io import BytesIO
from typing import List, Dict, Any
import pandas as pd

def to_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    df = pd.DataFrame(rows)
    # Column order: original + audit
    preferred = ["Name","Symbol","Price","# of Shares","Market Value",
                 "ResolveStatus","ResolvedSymbol","ResolveNotes","CandidatesTop3"]
    # handle aliases in EnrichedRow
    if "Shares" in df.columns and "# of Shares" not in df.columns:
        df["# of Shares"] = df["Shares"]
    if "MarketValue" in df.columns and "Market Value" not in df.columns:
        df["Market Value"] = df["MarketValue"]

    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    bio = BytesIO()
    df.to_csv(bio, index=False)
    return bio.getvalue()
