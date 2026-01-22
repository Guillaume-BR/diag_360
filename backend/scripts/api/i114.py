#!/usr/bin/env python3
"""
Indicateur i114 : Quantité moyenne annuelle d'achats de substances actives rapporté à la SAU du territoire
Source : data.gouv.fr
URL : https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c
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
import requests
from sqlalchemy import select

# Remonte de 3 niveaux : api/ -> scripts/ -> backend/
backend_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(backend_path))
from app.db import SessionLocal
from app.models import Indicator, IndicatorValue

# Import de vos fonctions utilitaires existantes
scripts_path = backend_path / "scripts"
sys.path.append(str(scripts_path))
from utils.functions import *

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/a1fe6b6c-4658-4c24-a8d8-dec530bcfc7c"
DEFAULT_INDICATOR_ID = "i114"
DEFAULT_YEAR = 2023  # Anne fictive car moyenne annuelle
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


def get_raw_dir() -> Path:
    """Retourne le chemin du répertoire source, le crée si nécessaire."""
    base_dir = Path(__file__).resolve().parent.parent
    raw_dir = base_dir / "source"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def dl_data_phyto() -> pd.DataFrame:
    """Charge le fichier des lieux de covoiturage et retourne le DataFrame"""

    raw_dir = get_raw_dir()

    # Téléchargement de la table phyto
    download_file(URL, extract_to=raw_dir, filename="achat_commune_phyto.parquet")
    df_phyto = duckdb.read_parquet(str(raw_dir / "achat_commune_phyto.parquet"))

    logger.info("Téléchargement des données du taux de couverture accueil jeune enfant")
    return df_phyto


def dl_data_sau() -> pd.Dataframe:

    raw_dir = get_raw_dir()
    # Téléchagement de la table de la sau
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/022cb00f-38f2-4fe7-8895-e3467d3d9255"
    )
    download_file(url, extract_to=raw_dir, filename="sau_2025.csv")
    df_sau = pd.read_csv(raw_dir / "sau_2025.csv", sep=",")
    return df_sau


def clean_and_prepare_df(df_phyto: pd.DataFrame, df_sau: pd.DataFrame) -> pd.DataFrame:

    raw_dir = get_raw_dir()
    # Téléchargement de la table epci
    df_epci = create_dataframe_epci(raw_dir)

    # jointure avec epci
    query_merged_phyto = """ 
        SELECT
            phyto.annee,
            TRY_CAST(phyto.quantite_substance AS DOUBLE) AS quantite_substance,
            epci.siren
        FROM data_phyto AS phyto
        INNER JOIN data_epci AS epci
            ON epci.insee = phyto.code_insee
        """

    merged_phyto = duckdb.sql(query_merged_phyto)

    query_avg = """ 
    WITH nb_years AS (
        SELECT
            siren,
            COUNT(DISTINCT annee) AS n_years
        FROM merged_phyto
        GROUP BY siren
    ),

    total_phyto AS (
        SELECT
            siren,
            SUM(quantite_substance) AS total_quantite_substance
        FROM merged_phyto
        GROUP BY siren
    )

    SELECT
        tp.siren as id_epci
        (1.0*tp.total_quantite_substance / ny.n_years) AS avg_annual_phyto,
        '2023' AS annee
    FROM total_phyto AS tp
    INNER JOIN nb_years AS ny
        ON tp.siren = ny.siren
    """

    avg_annual_phyto = duckdb.sql(query_avg)

    # On ramène to ça à la surface agricole utile
    query_bdd = """
    SELECT
        aap.siren as id_epci,
        'i114' AS id_indicator,
        ROUND((1.0 * aap.avg_annual_phyto / ds.sau_ha), 3) AS valeur_brute,
        '2023' AS annee
    FROM avg_annual_phyto AS aap
    INNER JOIN data_sau AS ds
        ON aap.siren = ds.geocode_epci
    """

    return duckdb.sql(query_bdd).df()


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
            unit="kg de substance active /ha de sau",
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
        df_phyto = dl_data_phyto()
        df_sau = dl_data_sau()
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
        description=f"Import des données du taux de couverture -> {DEFAULT_INDICATOR_ID}"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="i130",
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
        df_phyto = dl_data_phyto()
        df_sau = dl_data_sau()
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
