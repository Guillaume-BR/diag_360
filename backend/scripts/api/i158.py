#!/usr/bin/env python3
"""
Indicateur i158 : Nombre de catastrophes naturelles par km² par EPCI
Source : data.gouv.fr
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, Iterator  # ✅ Correction
import zipfile
from io import BytesIO

import pandas as pd
import duckdb
from sqlalchemy import select

# Remonte de 3 niveaux : api/ -> scripts/ -> backend/
backend_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(backend_path))
from app.db import SessionLocal
from app.models import Indicator, IndicatorValue

# Import de vos fonctions utilitaires existantes
scripts_path = backend_path / "scripts"
sys.path.append(str(scripts_path))
from utils.functions import get_raw_dir, download_file

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/d6fb9e18-b66b-499c-8284-46a3595579cc"
DEFAULT_INDICATOR_ID = "i158"
DEFAULT_YEAR = 2025  # Année fictive car indicateur cumulatif
DEFAULT_SOURCE = (
    "https://www.data.gouv.fr/api/1/datasets/r/d6fb9e18-b66b-499c-8284-46a3595579cc"
)


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def traitement_catnat() -> tuple[pd.DataFrame, Path]:
    """Télécharge, extrait et retourne les données GASPAR + le chemin raw_dir."""

    # Define URLs and file paths
    URL = (
        "https://www.data.gouv.fr/api/1/datasets/r/d6fb9e18-b66b-499c-8284-46a3595579cc"
    )
    zip_content = download_file(URL)
    with zipfile.ZipFile(BytesIO(zip_content)) as z:
        with z.open("catnat_gaspar.csv") as f:
            df_cat_nat = pd.read_csv(f, sep=";", low_memory=False)

    # On modifie les code insee de Paris, Lyon, Marseille pour les faire correspondre à ceux de l'INSEE
    df_cat_nat.loc[df_cat_nat["cod_commune"].str.startswith("75"), "cod_commune"] = (
        "75056"
    )
    df_cat_nat.loc[df_cat_nat["cod_commune"].str.startswith("693"), "cod_commune"] = (
        "69123"
    )
    df_cat_nat.loc[df_cat_nat["cod_commune"].str.startswith("132"), "cod_commune"] = (
        "13055"
    )

    # nombre de cat nat par commune sur 40 ans
    query = """
    SELECT 
        cod_commune AS code_insee, 
        count(*) AS nb_cat_nat
    FROM df_cat_nat
    GROUP BY cod_commune
    """
    df_cat_nat = duckdb.sql(query).df()
    return df_cat_nat


def clean_and_prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Prépare le DataFrame brut pour le traitement."""

    raw_dir = get_raw_dir()

    # Chargement de la table epci
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    # Surface de chaque epci et nb de cat nat par epci sur 40 ans
    query = """
    WITH df_temp AS (
    SELECT 
        df_epci.siren AS siren,
        sum(df_cat_nat.nb_cat_nat) as nb_cat_nat_total,
        sum(df_epci.superficie_km2) as superficie_km2
    FROM df_epci
    LEFT JOIN df_cat_nat
    ON df_epci.code_insee = df_cat_nat.code_insee
    GROUP BY df_epci.siren
    )

    SELECT 
        siren,
        ROUND(nb_cat_nat_total / superficie_km2, 3) AS cat_nat_per_km2
    FROM df_temp
    """

    df_cat_nat_temp = duckdb.sql(query)

    # Ajout du nom des epci
    query_complete = """
    SELECT 
        df_epci.dept_epci as dept_id,
        CAST(df_epci.siren AS VARCHAR) as id_epci,
        df_epci.epci_nom as epci_lib,
        'i158' AS id_indicator,
        df_cat_nat_temp.cat_nat_per_km2 as valeur_brute,
        '2025' AS annee
    FROM df_epci
    LEFT JOIN df_cat_nat_temp
    ON df_cat_nat_temp.siren = df_epci.siren
    GROUP BY df_epci.dept_epci, df_epci.siren, df_epci.epci_nom, df_cat_nat_temp.cat_nat_per_km2
    ORDER BY df_epci.dept_epci, df_epci.siren
    """

    df_cat_nat_final = duckdb.sql(query_complete)

    return df_cat_nat_final.df()


def transform_payload(df: pd.DataFrame) -> Iterator[RawValue]:

    for _, row in df.iterrows():
        if pd.isna(row["valeur_brute"]):
            continue

        yield RawValue(
            epci_id=str(row["id_epci"]),
            indicator_id=str(row["id_indicator"]),
            year=str(row["annee"]),
            value=float(row["valeur_brute"]),
            unit="nb_cat_nat/km2",
            source=DEFAULT_SOURCE,
            meta={"note": "Calculé sur l'historique total GASPAR"},
        )


def persist_values(session, rows: Iterable[RawValue]) -> int:
    """Persiste les valeurs en base de données."""
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
    logger.info(f"Commit de {inserted} valeurs en base")
    return inserted


def ensure_indicator_exists(session, indicator_id: str) -> None:
    """Vérifie que l'indicateur existe, sinon le crée."""
    exists = session.execute(
        select(Indicator.id).where(Indicator.id == indicator_id)
    ).scalar_one_or_none()
    if not exists:
        logger.warning(
            f"L'indicateur {indicator_id} n'existe pas en base. Création d'une entrée générique."
        )
        new_ind = Indicator(id=indicator_id, nom=f"Indicateur {indicator_id}")
        session.add(new_ind)
        session.commit()


def run(indicator_id: str) -> None:
    """Exécution principale."""
    session = SessionLocal()
    try:
        ensure_indicator_exists(session, indicator_id)

        # Téléchargement et extraction
        df_cat_nat = traitement_catnat()
        df_processed = clean_and_prepare_df(df_cat_nat)

        # Transformation
        rows = list(transform_payload(df_processed))

        if not rows:
            logger.warning("Aucune donnée calculée.")
            return

        # Persistance
        count = persist_values(session, rows)
        logger.info(f"✅ {count} lignes traitées pour l'indicateur {indicator_id}")
    finally:
        session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Téléchargement de données -> i158")
    parser.add_argument("--indicator", default=DEFAULT_INDICATOR_ID, help="i158")
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
        df_cat_nat = traitement_catnat()
        df_processed = clean_and_prepare_df(df_cat_nat)
        rows = list(transform_payload(df_processed))
        print(
            json.dumps(
                [row.__dict__ for row in rows[:10]],
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )
        print(f"... (10 premières lignes sur {len(rows)})")
        return

    run(indicator_id=args.indicator)


if __name__ == "__main__":
    main()
