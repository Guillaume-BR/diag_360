from __future__ import annotations

from typing import List, Optional

from sqlalchemy import asc, desc, func, select
from sqlalchemy.orm import Session

from app.models.diag360_raw import Epci, IndicatorScore
from app.models.diag360_ref import Indicator, IndicatorType, Need, Objective
from app.schemas.score import (
    AggregatedScore,
    IndicatorScoreDetail,
    ScoreDetail,
    ScoreListResponse,
    ScoreSummary,
)


def _resolve_year(db: Session, year: Optional[int]) -> int:
    if year is not None:
        return year
    latest_year = db.execute(select(func.max(IndicatorScore.year))).scalar()
    if latest_year is None:
        return 0
    return int(latest_year)


def _to_float(value):
    if value is None:
        return None
    return float(value)


def list_scores(
    db: Session,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    order_by: str = "name",
    year: Optional[int] = None,
) -> ScoreListResponse:
    target_year = _resolve_year(db, year)

    summary_stmt = (
        select(
            IndicatorScore.epci_id.label("epci_id"),
            Epci.label.label("epci_label"),
            Epci.department_code.label("department_code"),
            Epci.region_code.label("region_code"),
            func.avg(IndicatorScore.global_score).label("global_score"),
            func.count(IndicatorScore.indicator_id).label("indicator_count"),
            func.max(IndicatorScore.updated_at).label("updated_at"),
        )
        .join(Epci, Epci.id == IndicatorScore.epci_id)
        .where(IndicatorScore.year == target_year)
        .group_by(
            IndicatorScore.epci_id,
            Epci.label,
            Epci.department_code,
            Epci.region_code,
        )
    )

    if search:
        pattern = f"%{search.lower()}%"
        summary_stmt = summary_stmt.where(
            func.lower(Epci.label).like(pattern)
            | func.lower(IndicatorScore.epci_id).like(pattern)
        )

    summary_subquery = summary_stmt.subquery()
    total = db.execute(select(func.count()).select_from(summary_subquery)).scalar()
    total = int(total or 0)

    if total == 0:
        return ScoreListResponse(items=[], total=0)

    order_expr = summary_subquery.c.epci_label.asc()
    if order_by == "score":
        order_expr = summary_subquery.c.global_score.desc().nullslast()
    elif order_by == "code":
        order_expr = summary_subquery.c.epci_id.asc()

    page_stmt = (
        select(summary_subquery)
        .order_by(order_expr)
        .offset(offset)
        .limit(limit)
    )

    rows = db.execute(page_stmt).mappings().all()
    items: List[ScoreSummary] = [
        ScoreSummary(
            epci_id=row["epci_id"],
            epci_label=row["epci_label"],
            department_code=row["department_code"],
            region_code=row["region_code"],
            global_score=_to_float(row["global_score"]),
            indicator_count=int(row["indicator_count"] or 0),
            updated_at=row["updated_at"],
        )
        for row in rows
    ]

    return ScoreListResponse(items=items, total=total)


def get_score_detail(
    db: Session,
    epci_id: str,
    year: Optional[int] = None,
) -> ScoreDetail:
    target_year = _resolve_year(db, year)

    summary_stmt = (
        select(
            IndicatorScore.epci_id.label("epci_id"),
            Epci.label.label("epci_label"),
            Epci.department_code.label("department_code"),
            Epci.region_code.label("region_code"),
            func.avg(IndicatorScore.global_score).label("global_score"),
            func.count(IndicatorScore.indicator_id).label("indicator_count"),
            func.max(IndicatorScore.updated_at).label("updated_at"),
        )
        .join(Epci, Epci.id == IndicatorScore.epci_id)
        .where(IndicatorScore.year == target_year, IndicatorScore.epci_id == epci_id)
        .group_by(
            IndicatorScore.epci_id,
            Epci.label,
            Epci.department_code,
            Epci.region_code,
        )
    )

    summary_row = db.execute(summary_stmt).mappings().first()
    if not summary_row:
        raise ValueError("EPCI not found")

    summary = ScoreSummary(
        epci_id=summary_row["epci_id"],
        epci_label=summary_row["epci_label"],
        department_code=summary_row["department_code"],
        region_code=summary_row["region_code"],
        global_score=_to_float(summary_row["global_score"]),
        indicator_count=int(summary_row["indicator_count"] or 0),
        updated_at=summary_row["updated_at"],
    )

    needs_rows = db.execute(
        select(
            IndicatorScore.need_id.label("id"),
            Need.label.label("label"),
            func.avg(IndicatorScore.need_score).label("score"),
        )
        .join(Need, Need.id == IndicatorScore.need_id, isouter=True)
        .where(
            IndicatorScore.year == target_year,
            IndicatorScore.epci_id == epci_id,
            IndicatorScore.need_id.is_not(None),
        )
        .group_by(IndicatorScore.need_id, Need.label)
        .order_by(asc(Need.label))
    ).mappings()

    objectives_rows = db.execute(
        select(
            IndicatorScore.objective_id.label("id"),
            Objective.label.label("label"),
            func.avg(IndicatorScore.objective_score).label("score"),
        )
        .join(Objective, Objective.id == IndicatorScore.objective_id, isouter=True)
        .where(
            IndicatorScore.year == target_year,
            IndicatorScore.epci_id == epci_id,
            IndicatorScore.objective_id.is_not(None),
        )
        .group_by(IndicatorScore.objective_id, Objective.label)
        .order_by(asc(Objective.label))
    ).mappings()

    types_rows = db.execute(
        select(
            IndicatorScore.type_id.label("id"),
            IndicatorType.label.label("label"),
            func.avg(IndicatorScore.type_score).label("score"),
        )
        .join(IndicatorType, IndicatorType.id == IndicatorScore.type_id, isouter=True)
        .where(
            IndicatorScore.year == target_year,
            IndicatorScore.epci_id == epci_id,
            IndicatorScore.type_id.is_not(None),
        )
        .group_by(IndicatorScore.type_id, IndicatorType.label)
        .order_by(asc(IndicatorType.label))
    ).mappings()

    indicators_rows = db.execute(
        select(
            IndicatorScore.indicator_id.label("indicator_id"),
            Indicator.label.label("indicator_label"),
            IndicatorScore.indicator_score.label("indicator_score"),
            IndicatorScore.need_id.label("need_id"),
            Need.label.label("need_label"),
            IndicatorScore.need_score.label("need_score"),
            IndicatorScore.objective_id.label("objective_id"),
            Objective.label.label("objective_label"),
            IndicatorScore.objective_score.label("objective_score"),
            IndicatorScore.type_id.label("type_id"),
            IndicatorType.label.label("type_label"),
            IndicatorScore.type_score.label("type_score"),
        )
        .join(Indicator, Indicator.id == IndicatorScore.indicator_id)
        .join(Need, Need.id == IndicatorScore.need_id, isouter=True)
        .join(Objective, Objective.id == IndicatorScore.objective_id, isouter=True)
        .join(IndicatorType, IndicatorType.id == IndicatorScore.type_id, isouter=True)
        .where(IndicatorScore.year == target_year, IndicatorScore.epci_id == epci_id)
        .order_by(asc(Indicator.label))
    ).mappings()

    needs = [
        AggregatedScore(
            id=row["id"],
            label=row["label"],
            score=_to_float(row["score"]),
        )
        for row in needs_rows
    ]
    objectives = [
        AggregatedScore(
            id=row["id"],
            label=row["label"],
            score=_to_float(row["score"]),
        )
        for row in objectives_rows
    ]
    types = [
        AggregatedScore(
            id=row["id"],
            label=row["label"],
            score=_to_float(row["score"]),
        )
        for row in types_rows
    ]
    indicators = [
        IndicatorScoreDetail(
            indicator_id=row["indicator_id"],
            indicator_label=row["indicator_label"],
            indicator_score=_to_float(row["indicator_score"]),
            need_id=row["need_id"],
            need_label=row["need_label"],
            need_score=_to_float(row["need_score"]),
            objective_id=row["objective_id"],
            objective_label=row["objective_label"],
            objective_score=_to_float(row["objective_score"]),
            type_id=row["type_id"],
            type_label=row["type_label"],
            type_score=_to_float(row["type_score"]),
        )
        for row in indicators_rows
    ]

    return ScoreDetail(
        summary=summary,
        needs=needs,
        objectives=objectives,
        types=types,
        indicators=indicators,
    )
