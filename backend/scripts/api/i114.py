#!/usr/bin/env python3
"""
Indicateur i114 : quantité de produit phytosanitaire par hectare de surface agricole utilisée (SAU) par EPCI    
Source : Data.gouv.fr
URL : "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"

"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, Iterator  # ✅ Correction

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
from utils.functions import get_raw_dir

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"
DEFAULT_INDICATOR_ID = "i114"
DEFAULT_YEAR = 2025
DEFAULT_SOURCE = "i114.csv"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def prepare_phyto():
    # Téléchargement de la table phyto
    url_phyto = (
        "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"
    )
    df_phyto = duckdb.read_parquet(url_phyto)

    # Correction de Paris, Lyon, Marseille dans df_phyto
    df_phyto = duckdb.sql(
        """
    SELECT
        *,
        CASE
            WHEN code_insee LIKE '75%'  THEN '75056'
            WHEN code_insee LIKE '132%' THEN '13055'
            WHEN code_insee LIKE '693%' THEN '69123'
            ELSE code_insee
        END AS code_insee
    FROM df_phyto
"""
    )
    return df_phyto


def prepare_sau():
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/b27d31a6-107b-46ee-8427-518799b488f0"
    )
    df_sau = pd.read_csv(url, sep=",")

    # traitement de df_sau : on ajoute des zéros et on corrige les codes insee de Paris, Lyon, Marseille pour les faire correspondre à ceux de l'INSEE
    df_sau = df_sau.dropna(subset=["geocode_commune"]).copy()
    df_sau["geocode_commune"] = df_sau["geocode_commune"].astype("string").str.zfill(5)

    mask = df_sau["geocode_commune"].str.startswith("75")
    df_sau.loc[mask, "geocode_commune"] = "75056"

    mask = df_sau["geocode_commune"].str.startswith("132")
    df_sau.loc[mask, "geocode_commune"] = "13055"

    mask = df_sau["geocode_commune"].str.startswith("693")
    df_sau.loc[mask, "geocode_commune"] = "69123"

    return df_sau


def clean_and_prepare_df(df_phyto: pd.DataFrame, df_sau: pd.DataFrame) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    raw_dir = get_raw_dir()

    # Téléchargement des données epci pour jointure
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    # Préparation de df_sau : on ne garde que 2020
    query_sau = """ 
    SELECT 
        df_epci.siren,
        ROUND(SUM(TRY_CAST(valeur AS DOUBLE)), 2) AS sau_ha
    FROM df_sau
    LEFT JOIN df_epci
    ON df_sau.geocode_commune = df_epci.code_insee
    WHERE date_mesure LIKE '2020%'
    GROUP BY df_epci.siren
    """
    df_sau = duckdb.sql(query_sau)

    # Jointure entre df_epci et df_phyto
    query = """
    SELECT 
        df_phyto.annee,
        df_epci.siren,
        TRY_CAST(df_phyto.quantite_substance AS DOUBLE) AS quantite_substance
    FROM df_epci 
    INNER JOIN df_phyto 
    ON df_epci.code_insee = df_phyto.code_insee
    """

    df_phyto_merged = duckdb.sql(query)

    # Calcul de la moyenne annuelle par EPCI
    query_avg = """ 
    WITH df_temp AS (
        SELECT
            siren,
            COUNT(DISTINCT annee) AS n_years,
            SUM(quantite_substance) AS total_quantite_substance
        FROM df_phyto_merged
        GROUP BY siren
    )

    SELECT
        siren,
        (1.0*total_quantite_substance / n_years) AS avg_annual_phyto
    FROM df_temp
    """

    avg_annual_phyto = duckdb.sql(query_avg)

    query_bdd = """
    WITH epci AS (
    SELECT 
        DISTINCT siren, 
        dept_epci, 
        epci_nom 
    FROM df_epci)
    
    SELECT
        epci.dept_epci AS dept_id,
        epci.siren AS id_epci,
        epci.epci_nom AS epci_lib,
        'i114' AS id_indicator,
        ROUND((1.0 * aap.avg_annual_phyto / ds.sau_ha), 3) AS valeur_brute,
        '2023' AS annee
    FROM epci
    LEFT JOIN avg_annual_phyto AS aap
        ON epci.siren = aap.siren
    LEFT JOIN df_sau AS ds
        ON epci.siren = ds.siren
    ORDER BY epci.dept_epci, epci.siren
    """

    df_final = duckdb.sql(query_bdd).df()
    return df_final


def transform_payload(df: pd.DataFrame) -> Iterator[RawValue]:
    """Transforme le DataFrame en itérable de RawValue"""
    for _, row in df.iterrows():
        if pd.isna(row["valeur_brute"]):
            continue

        yield RawValue(
            epci_id=str(row["id_epci"]),
            indicator_id=str(row["id_indicator"]),
            year=str(row["annee"]),
            value=float(row["valeur_brute"]),
            unit="kg/ha/an",
            source=DEFAULT_SOURCE,
            meta={"raw": row.to_dict()},
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
        df_phyto = prepare_phyto()
        df_sau = prepare_sau()
        df_processed = clean_and_prepare_df(df_phyto, df_sau)

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
    parser = argparse.ArgumentParser(
        description=f"Import des données des zones urbanisées -> {DEFAULT_INDICATOR_ID}"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="i114",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Affiche les résultats sans insérer"
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    parser = build_parser()
    args = parser.parse_args()

    if args.dry_run:
        df_phyto = prepare_phyto()
        df_sau = prepare_sau()
        df_processed = clean_and_prepare_df(df_phyto, df_sau)

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
