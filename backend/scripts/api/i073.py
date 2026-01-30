#!/usr/bin/env python3
"""
Indicateur i073 : Part du territoire en zones protégées
Source : geoiid
URL : https://geoidd.developpement-durable.gouv.fr/#bbox=-392589,7139776,2153622,1120791&c=indicator&i=i105.surf_prot_tou_pro_id&s=2019&t=A08&view=map45

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
import geopandas as gpd
from shapely import wkb
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
from utils.functions import create_dataframe_communes, get_raw_dir

logger = logging.getLogger(__name__)

# Configuration
URL = "https://geoidd.developpement-durable.gouv.fr/#bbox=-392589,7139776,2153622,1120791&c=indicator&i=i105.surf_prot_tou_pro_id&s=2019&t=A08&view=map45"
DEFAULT_INDICATOR_ID = "i073"
DEFAULT_YEAR = 2019
DEFAULT_SOURCE = "i073.csv"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def load_territoire_protege() -> pd.DataFrame:
    """Charge le fichier des zones protégées et retourne le DataFrame"""

    raw_dir = get_raw_dir()

    # Lire le CSV
    path_file = raw_dir / DEFAULT_SOURCE
    if not path_file.exists():
        raise FileNotFoundError(
            f"Fichier {path_file} introuvable dans le dossier {raw_dir}"
        )
    logger.info("Téléchargement des données des zones protégées")
    return pd.read_csv(path_file, sep=",")


def clean_and_prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    # Téléchargement des données communale
    df_communes = create_dataframe_communes()

    # On arrange les données de df
    df = df.rename(
        columns={
            "Code": "id_epci",
            "Ensemble des surfaces protegées: au moins une mesure de protection 2019": "surface_protegee",
        }
    ).drop(columns=["Libellé"])

    df["id_indicator"] = "i073"
    df["annee"] = 2019
    df["unit"] = None
    df["source"] = "Geoiid - https://geoidd.developpement-durable.gouv.fr/"

    # On calcule la surface totale des EPCI en hectares
    query = """ 
    SELECT
        epci_code AS id_epci,
        SUM(superficie_hectare) AS surface_totale
    FROM df_communes
    GROUP BY epci_code
    WHERE epci_code NOT LIKE 'ZZ%'
    """

    df_superficie_epci = duckdb.sql(query)

    # On joint les deux DataFrames pour calculer la part de surface protégée
    query_bdd = """ 
    SELECT
        p.id_epci,
        p.id_indicator,
        p.annee,
        (p.surface_protegee / s.surface_totale) * 100 AS valeur_brute
    FROM df AS p
    JOIN df_superficie_epci AS s
    ON p.id_epci = s.id_epci
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
        df_territoire_protege = load_territoire_protege()
        df_processed = clean_and_prepare_df(df_territoire_protege)

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
        help="i073",
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
        df_territoire_protege = load_territoire_protege()
        df_processed = clean_and_prepare_df(df_territoire_protege)
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
