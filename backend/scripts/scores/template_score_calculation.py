#!/usr/bin/env python3
"""Template de calcul de scores à partir de `valeur_indicateur`.

Adapter ce script pour chaque famille d'indicateurs :
1. Sélectionner les valeurs brutes pertinentes (par indicateur, période, source...).
2. Appliquer la formule de normalisation / scoring.
3. Écrire le résultat dans `score_indicateur` en renseignant les colonnes attendues.
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import Select, select

from app.db import SessionLocal
from app.models import IndicatorScore, IndicatorValue

logger = logging.getLogger(__name__)


@dataclass
class ScoreRow:
    epci_id: str
    indicator_id: str
    year: int
    indicator_score: float
    need_id: str | None = None
    need_score: float | None = None
    objective_id: str | None = None
    objective_score: float | None = None
    type_id: str | None = None
    type_score: float | None = None
    global_score: float | None = None
    report: dict | None = None


def base_query(*, indicator_ids: list[str], year: int) -> Select:
    """Construire la requête de base vers `valeur_indicateur`."""

    return select(IndicatorValue).where(
        IndicatorValue.indicator_id.in_(indicator_ids),
        IndicatorValue.year == year,
    )


def fetch_raw_values(session, *, indicator_ids: list[str], year: int) -> dict[str, dict[str, float]]:
    """Retourner un mapping {indicator_id: {epci_id: valeur_brute}}."""

    stmt = base_query(indicator_ids=indicator_ids, year=year)
    rows = session.execute(stmt).scalars().all()
    values: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        if row.value is None:
            continue
        values[row.indicator_id][row.epci_id] = float(row.value)
    logger.info("%s valeurs brutes chargées", sum(len(e) for e in values.values()))
    return values


def compute_scores(raw_values: dict[str, dict[str, float]]) -> Iterable[ScoreRow]:
    """Convertir les valeurs brutes en scores normalisés.

    Remplacer cette logique par la vraie formule (min/max, zscore, barèmes...).
    """

    for indicator_id, epci_map in raw_values.items():
        if not epci_map:
            continue
        min_value = min(epci_map.values())
        max_value = max(epci_map.values())
        span = max(max_value - min_value, 1e-9)
        for epci_id, value in epci_map.items():
            normalized = (value - min_value) / span
            score = round(normalized * 100, 2)
            yield ScoreRow(
                epci_id=epci_id,
                indicator_id=indicator_id,
                year=DEFAULT_YEAR,
                indicator_score=score,
                global_score=score,
                report={"valeur_brute": value, "min": min_value, "max": max_value},
            )


def persist_scores(session, rows: Iterable[ScoreRow]) -> int:
    inserted = 0
    for row in rows:
        record = IndicatorScore(
            epci_id=row.epci_id,
            indicator_id=row.indicator_id,
            year=row.year,
            indicator_score=row.indicator_score,
            need_id=row.need_id,
            need_score=row.need_score,
            objective_id=row.objective_id,
            objective_score=row.objective_score,
            type_id=row.type_id,
            type_score=row.type_score,
            global_score=row.global_score,
            report=row.report,
        )
        session.merge(record)
        inserted += 1
    session.commit()
    return inserted


DEFAULT_YEAR = 2025
DEFAULT_INDICATORS = ["i001", "i002"]


def run(indicator_ids: list[str], year: int) -> None:
    session = SessionLocal()
    try:
        raw_values = fetch_raw_values(session, indicator_ids=indicator_ids, year=year)
        rows = list(compute_scores(raw_values))
        if not rows:
            logger.warning("Aucun score calculé (indicateurs=%s, année=%s)", indicator_ids, year)
            return
        count = persist_scores(session, rows)
        logger.info("%s scores enregistrés dans score_indicateur", count)
    finally:
        session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Template calcul score_indicateur")
    parser.add_argument(
        "--indicator",
        action="append",
        dest="indicators",
        default=None,
        help="Ajouter un ID indicateur (répéter l'argument pour plusieurs valeurs)",
    )
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Année de référence")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    indicator_ids = args.indicators or DEFAULT_INDICATORS
    run(indicator_ids=indicator_ids, year=args.year)


if __name__ == "__main__":
    main()
