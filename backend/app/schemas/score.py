from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class ScoreSummary(BaseModel):
    epci_id: str
    epci_label: str
    department_code: Optional[str] = None
    region_code: Optional[str] = None
    global_score: Optional[float] = None
    indicator_count: int
    updated_at: Optional[datetime] = None


class ScoreListResponse(BaseModel):
    items: List[ScoreSummary]
    total: int


class AggregatedScore(BaseModel):
    id: Optional[str] = None
    label: Optional[str] = None
    score: Optional[float] = None


class IndicatorScoreDetail(BaseModel):
    indicator_id: str
    indicator_label: str
    indicator_score: Optional[float] = None
    need_id: Optional[str] = None
    need_label: Optional[str] = None
    need_score: Optional[float] = None
    objective_id: Optional[str] = None
    objective_label: Optional[str] = None
    objective_score: Optional[float] = None
    type_id: Optional[str] = None
    type_label: Optional[str] = None
    type_score: Optional[float] = None


class ScoreDetail(BaseModel):
    summary: ScoreSummary
    needs: List[AggregatedScore]
    objectives: List[AggregatedScore]
    types: List[AggregatedScore]
    indicators: List[IndicatorScoreDetail]
