#!/usr/bin/env python3
"""
Indicateur i113 : Part de la Surface Agricole Utile sur la superficie totale du territoire
Source : data.gouv.fr
URL : "https://www.data.gouv.fr/api/1/datasets/r/b27d31a6-107b-46ee-8427-518799b488f0"

"""
from __future__ import annotations

import argparse
from io import BytesIO
import json
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, Iterator
import zipfile

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
from utils.functions import download_file, get_raw_dir

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/b27d31a6-107b-46ee-8427-518799b488f0"
DEFAULT_INDICATOR_ID = "i113"
DEFAULT_YEAR = 2025
DEFAULT_SOURCE = "data.gouv.fr"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def traitement_sau() -> pd.DataFrame:
    """Charge le fichier des données principales et retourne le DataFrame"""

    df_sau = pd.read_csv(URL, sep=",")

    # Traitement de la table sau
    df_sau = df_sau[df_sau["date_mesure"].str.startswith("2020")].copy()
    df_sau["geocode_commune"] = df_sau["geocode_commune"].astype(str).str.zfill(5)
    return df_sau


def recup_communes() -> pd.DataFrame:
    url = "https://www.insee.fr/fr/statistiques/fichier/4505239/ODD_PARQUET.zip"
    zip_content = download_file(url)
    with zipfile.ZipFile(BytesIO(zip_content)) as z:
        with z.open("ODD_COM.parquet") as f:
            df_communes = duckdb.read_parquet(f)

        # Traitement de la table des communes pour ne garder que les codes insee et les surfaces
    query = """ 
    SELECT 
        codgeo,
        libgeo,
        A2021 AS surface
    FROM df_communes
    WHERE variable = 'surface'
    """

    df_surf_com = duckdb.sql(query)
    return df_surf_com


def clean_and_prepare_df(
    df_sau: pd.DataFrame, df_surf_com: pd.DataFrame
) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    raw_dir = get_raw_dir()
    # Téléchargement des données epci pour jointure
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    query = """
    SELECT
        dept_epci as dept_id,
        siren as id_epci,
        epci_nom AS lib_epci,
        'i113' AS id_indicator,
        ROUND(sum(df_sau.valeur/100) / sum(surface)  * 100,3) AS valeur_brute,
        '2020' AS annee
    FROM df_epci
    LEFT JOIN df_surf_com 
        ON df_epci.code_insee = df_surf_com.codgeo
    LEFT JOIN df_sau
        ON df_epci.code_insee = df_sau.geocode_commune
    GROUP BY siren, dept_epci, epci_nom
    ORDER BY dept_epci, siren
    """

    df_sau_final = duckdb.sql(query)
    return df_sau_final.df()


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
            unit="%",
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
        df_sau = traitement_sau()
        df_surf_com = recup_communes()
        df_processed = clean_and_prepare_df(df_sau, df_surf_com)

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
        description=f"Import des données des SAU -> {DEFAULT_INDICATOR_ID}"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="i149",
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
        df_sau = traitement_sau()
        df_surf_com = recup_communes()
        df_processed = clean_and_prepare_df(df_sau, df_surf_com)
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
