from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

class HoldingRow(BaseModel):
    # canonical, but accept spreadsheet aliases via Field(..., alias=...)
    Name: Optional[str] = None
    Symbol: Optional[str] = None
    Price: Optional[float] = None
    Shares: Optional[float] = Field(default=None, alias="# of Shares")
    MarketValue: Optional[float] = Field(default=None, alias="Market Value")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")
