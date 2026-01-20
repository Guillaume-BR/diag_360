#!/usr/bin/env python3
"""
Script pour alimenter `valeur_indicateur` avec les données CatNat (GASPAR).
Calcule le nombre de catastrophes naturelles par km² par EPCI.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import sys
from typing import Iterable, Iterator  # ✅ Correction
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
from utils.functions import *

logger = logging.getLogger(__name__)

# Configuration
URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
)
DEFAULT_INDICATOR_ID = "i066"
DEFAULT_YEAR = 2025  # Année fictive car indicateur cumulatif
DEFAULT_SOURCE = "https://www.data.gouv.fr/api/1/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"

@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def fetch_api_payload() -> tuple[pd.DataFrame, Path]:
    """Télécharge le fichier des pharmacies et retourne le DataFrame + le chemin raw_dir."""

    """Télécharge, extrait et retourne les données GASPAR + le chemin raw_dir."""
    base_dir = Path(__file__).resolve().parent.parent
    raw_dir = base_dir / "source"
    raw_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Téléchargement des données de pharmacies")
    download_file(URL, dl_to=raw_dir, filename="pharmacies.csv")

    # Lire le CSV
    path_pharmacies = raw_dir / "pharmacies.csv"
    if not path_pharmacies.exists():
        raise FileNotFoundError(f"Fichier {path_pharmacies} introuvable après téléchargement")
    
    df_pharmacies = pd.read_csv(path_pharmacies, sep=";", low_memory=False)
    logger.info(f"Chargé {len(df_pharmacies)} lignes de pharmacies")

    return df_pharmacies, raw_dir


def transform_payload(df_pharmacies: pd.DataFrame, raw_dir: Path, indicator_id: str, year: int) -> Iterator[RawValue]:
    """Calcule l'indicateur via DuckDB à partir des données."""
    
    # Chargement de la table epci
    df_epci = create_dataframe_epci(raw_dir)

    # Chargement de la table des communes
    df_com = create_dataframe_communes(raw_dir)

    # Traitement des données de pharmacies
    df_pharma = df_pharma.iloc[:, [15, 19]]
    df_pharma.rename(columns={19: "type", 15: "code_insee"}, inplace=True)
    df_pharma = df_pharma.loc[df_pharma["type"].str.startswith("Phar")].reset_index(
        drop=True
    )

    df_pharma["code_postal"] = df_pharma["code_insee"].apply(lambda x: x.split(" ")[0])
    df_pharma.drop(columns=["code_insee"], inplace=True)

    # Jointure avec les données des communes pour récupérer le nombre de pharma par commune
    query = """
    SELECT
        df_com.epci_code AS id_epci,
        'i066' AS id_indicator,
        COUNT(df_pharma.code_postal) AS valeur_brute,
        '2025' AS annee
    FROM df_pharma
    LEFT JOIN df_com
        ON df_pharma.code_postal = df_com.code_postal
    GROUP BY id_epci
    HAVING id_epci != 'ZZZZZZZZZ'
    """

    result = duckdb.sql(query)

    # On garde la population totale des epci
    query = """ 
    SELECT 
        DISTINCT siren, 
        TRY_CAST(REPLACE(total_pop_tot,' ','') AS INTEGER) as total_pop 
        FROM df_epci
    """
    df_epci_pop_tot = duckdb.sql(query)

    # Calcul du nombre de pharmacie pour 10000 habitants
    query_final = """
    SELECT 
        result.id_epci,
        result.id_indicator,
        ROUND((result.valeur_brute/ df_epci_pop_tot.total_pop) * 10000, 2) AS valeur_brute,
        result.annee
    FROM df_epci_pop_tot
    LEFT JOIN result 
    ON result.id_epci = df_epci_pop_tot.siren
    WHERE result.id_epci IS NOT NULL
    """

    df_densite_pharma = duckdb.sql(query_final)

    results = df_densite_pharma.df()
    logger.info(f"Calculé {len(results)} valeurs d'indicateur")

    for _, row in results.iterrows():
        if pd.isna(row["valeur_brute"]):
            continue

        yield RawValue(
            epci_id=str(row["id_epci"]),
            indicator_id=indicator_id,
            year=year,
            value=float(row["valeur_brute"]),
            unit="nb_pharmacies/10000hab",
            source=DEFAULT_SOURCE,
            meta={"raw": row.to_dict()},
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


def run(indicator_id: str, year: int) -> None:
    """Exécution principale."""
    session = SessionLocal()
    try:
        ensure_indicator_exists(session, indicator_id)
        
        # Téléchargement et extraction
        df_cat_nat, raw_dir = fetch_api_payload()
        
        # Transformation
        rows = list(transform_payload(df_cat_nat, raw_dir, indicator_id=indicator_id, year=year))

        if not rows:
            logger.warning("Aucune donnée calculée.")
            return

        # Persistance
        count = persist_values(session, rows)
        logger.info(f"✅ {count} lignes traitées pour l'indicateur {indicator_id}")
    finally:
        session.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(
        description="Import CatNat GASPAR -> valeur_indicateur"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="ID indicateur (ex: i158)",
    )
    parser.add_argument(
        "--year", type=int, default=DEFAULT_YEAR, help="Année de référence"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Affiche les résultats sans insérer"
    )

    args = parser.parse_args()

    if args.dry_run:
        df_cat_nat, raw_dir = fetch_api_payload()
        rows = list(
            transform_payload(df_cat_nat, raw_dir, indicator_id=args.indicator, year=args.year)
        )
        print(
            json.dumps(
                [row.__dict__ for row in rows[:10]], indent=2, ensure_ascii=False, default=str)
        )
        print(f"... (10 premières lignes sur {len(rows)})")
        return

    run(indicator_id=args.indicator, year=args.year)


if __name__ == "__main__":
    main()