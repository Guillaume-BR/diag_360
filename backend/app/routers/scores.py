from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import ScoreDetail, ScoreListResponse
from app.services import score_service

router = APIRouter(prefix="/scores", tags=["scores"])


@router.get("", response_model=ScoreListResponse, summary="List EPCI scores")
def list_scores(
    search: str | None = Query(default=None, min_length=2, description="Filter by name or SIREN"),
    limit: int = Query(default=50, le=2000),
    offset: int = Query(default=0),
    order_by: str = Query(default="name", pattern="^(name|score|code)$"),
    year: int | None = Query(default=None, description="Target year (defaults to latest)"),
    db: Session = Depends(get_db),
):
    return score_service.list_scores(
        db=db,
        search=search,
        limit=limit,
        offset=offset,
        order_by=order_by,
        year=year,
    )


@router.get(
    "/{epci_id}",
    response_model=ScoreDetail,
    summary="Retrieve detailed scores for an EPCI",
)
def get_score_detail(
    epci_id: str,
    year: int | None = Query(default=None, description="Target year (defaults to latest)"),
    db: Session = Depends(get_db),
):
    try:
        return score_service.get_score_detail(db=db, epci_id=epci_id, year=year)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
