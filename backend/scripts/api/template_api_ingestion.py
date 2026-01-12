#!/usr/bin/env python3
"""Template de script API pour alimenter `valeur_indicateur`.

Étapes à personnaliser :
1. Renseigner les constantes (URL, identifiant d'indicateur, année, source...).
2. Implémenter `fetch_api_payload` pour récupérer les données depuis l'API cible.
3. Adapter `transform_payload` afin de retourner des lignes prêtes à être insérées.
4. Ajuster les métadonnées (unités, source, meta JSON) selon le fournisseur.
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Iterator

import requests
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Indicator, IndicatorValue

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.example.com/indicators"  # À mettre à jour
DEFAULT_INDICATOR_ID = "i000"  # Identifiant Diag360 de l'indicateur ciblé
DEFAULT_YEAR = date.today().year
DEFAULT_SOURCE = "API Example"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def fetch_api_payload(*, indicator_id: str, year: int) -> dict:
    """Appeler l'API distante et retourner la charge utile brute.

    Personnaliser cette fonction : authentification, paramètres de requête,
    pagination, gestion des erreurs, etc.
    """

    url = f"{API_BASE_URL}/{indicator_id}?year={year}"
    logger.info("Fetching %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_payload(payload: dict, *, indicator_id: str, year: int) -> Iterator[RawValue]:
    """Convertir la réponse API en objets `RawValue`.

    Adapter la logique en fonction du format réel (JSON/CSV...).
    """

    records = payload.get("results", [])
    for record in records:
        epci_id = record.get("id_epci") or record.get("siren")
        value = record.get("value")
        if not epci_id or value is None:
            continue
        yield RawValue(
            epci_id=str(epci_id),
            indicator_id=indicator_id,
            year=year,
            value=float(value),
            unit=record.get("unit"),
            source=record.get("source", DEFAULT_SOURCE),
            meta={"raw": record},
        )


def persist_values(session, rows: Iterable[RawValue]) -> int:
    """Insérer ou mettre à jour les valeurs brutes."""

    inserted = 0
    for row in rows:
        record = IndicatorValue(
            epci_id=row.epci_id,
            indicator_id=row.indicator_id,
            year=row.year,
            value=row.value,
            unit=row.unit,
            source=row.source or DEFAULT_SOURCE,
            meta=row.meta or {},
        )
        session.merge(record)
        inserted += 1
    session.commit()
    return inserted


def ensure_indicator_exists(session, indicator_id: str) -> None:
    """Optionnel : vérifier que l'indicateur ciblé existe côté base."""

    exists = session.execute(select(Indicator.id).where(Indicator.id == indicator_id)).scalar_one_or_none()
    if not exists:
        raise ValueError(f"L'indicateur {indicator_id} est introuvable en base. Importez d'abord la table de référence.")


def run(indicator_id: str, year: int) -> None:
    session = SessionLocal()
    try:
        ensure_indicator_exists(session, indicator_id)
        payload = fetch_api_payload(indicator_id=indicator_id, year=year)
        rows = list(transform_payload(payload, indicator_id=indicator_id, year=year))
        if not rows:
            logger.warning("Aucune ligne à insérer (indicator=%s, year=%s)", indicator_id, year)
            return
        count = persist_values(session, rows)
        logger.info("%s lignes upsertées dans valeur_indicateur", count)
    finally:
        session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Template d'appel API -> valeur_indicateur")
    parser.add_argument("--indicator", default=DEFAULT_INDICATOR_ID, help="ID indicateur Diag360 (ex: i123)")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Année de référence")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="N'insère rien en base, affiche seulement les lignes qui seraient importées.",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    parser = build_parser()
    args = parser.parse_args()

    if args.dry_run:
        payload = fetch_api_payload(indicator_id=args.indicator, year=args.year)
        rows = list(transform_payload(payload, indicator_id=args.indicator, year=args.year))
        print(json.dumps([row.__dict__ for row in rows], indent=2, ensure_ascii=False))
        return

    run(indicator_id=args.indicator, year=args.year)


if __name__ == "__main__":
    main()
