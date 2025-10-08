from typing import List, Optional
from pydantic import BaseModel


class Meta(BaseModel):
    data_vintage: Optional[str] = None
    config_version: Optional[str] = None
    git_hash: Optional[str] = None
    warnings: Optional[List[str]] = None


class MarketSummaryItem(BaseModel):
    id: str
    last: Optional[float] = None
    ma50: Optional[float] = None
    spark: Optional[List[float]] = None
    warning: Optional[str] = None


class MarketSummaryResponse(BaseModel):
    items: List[MarketSummaryItem]
    meta: Meta


class RegimePoint(BaseModel):
    date: str
    label: str


class RegimeHistoryAsset(BaseModel):
    ticker: str
    points: List[RegimePoint]


class RegimeHistoryResponse(BaseModel):
    assets: List[RegimeHistoryAsset]
    meta: Meta


class ProbPoint(BaseModel):
    date: str
    p: Optional[float]


class ProbSeries(BaseModel):
    state: str
    points: List[ProbPoint]


class HMMItem(BaseModel):
    ticker: str
    labels: List[RegimePoint]
    probs: List[ProbSeries]
    latest: Optional[RegimePoint] = None
    state_labels: Optional[List[str]] = None


class HMMResponse(BaseModel):
    items: List[HMMItem]
    meta: Meta


class TransitionMatrixItem(BaseModel):
    ticker: str
    states: List[str]
    matrix: List[List[float]]
    labels: Optional[List[str]] = None


class TransitionMatrixResponse(BaseModel):
    items: List[TransitionMatrixItem]
    meta: Meta

