#!/usr/bin/env python3
"""
Indicateur i058 : Nombre de kilomètres d'aménagements cyclables par km2 urbanisé
Source : Data.gouv.fr
URL : https://www.data.gouv.fr/api/1/datasets/r/f5d6ae97-b62e-46a7-ad5e-736c8084cee8

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
URL = "https://www.data.gouv.fr/api/1/datasets/r/f5d6ae97-b62e-46a7-ad5e-736c8084cee8"
DEFAULT_INDICATOR_ID = "i058"
DEFAULT_YEAR = 2025
DEFAULT_SOURCE = "i058.csv"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def prepare_zone_urb():
    # Chargement local des données mais sinon utiliser :
    # https://docs.google.com/spreadsheets/d/1y6yy7_XCmhSUIqBmzZ200mgMo93YuZS8/edit?usp=sharing&ouid=108793438427721456504&rtpof=true&sd=true
    raw_dir = get_raw_dir()
    path_zones_urb = raw_dir / "i058.csv"
    df_zones_urb = pd.read_csv(path_zones_urb, sep=",")

    # Traitement des données des zones urbaines
    df_zones_urb.drop("Unnamed: 6", axis=1, inplace=True)
    df_zones_urb.drop(
        "* Les donnes proviennent de Corine Land Cover millésime 2018",
        axis=1,
        inplace=True,
    )
    mapping = {
        "SIREN": "siren",
        "Nom de l'EPCI": "nom_epci",
        "Nature de l'EPCI": "nature_epci",
        "Superficie de l'EPCI (km²)": "superficie_epci",
        "Superficie des territoires artificialisés* (km²)": "superficie_artificialisee",
        "Part de la superficie artificialisée": "part_percent_superficie_artificialisee",
    }

    df_zones_urb.rename(columns=mapping, inplace=True)
    df_zones_urb["superficie_epci"] = (
        df_zones_urb["superficie_epci"].replace(",", ".", regex=True).astype(float)
    )
    df_zones_urb["superficie_artificialisee"] = (
        df_zones_urb["superficie_artificialisee"]
        .replace(",", ".", regex=True)
        .astype(float)
    )
    df_zones_urb["part_percent_superficie_artificialisee"] = (
        df_zones_urb["part_percent_superficie_artificialisee"]
        .replace(",", ".", regex=True)
        .replace(" %", "", regex=True)
        .astype(float)
    )

    return df_zones_urb


def load_amenagement_cyclable() -> pd.DataFrame:
    """Charge le fichier des lieux de covoiturage et retourne le DataFrame"""
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/b464775c-8d01-4faf-a46e-342d50369cca"
    )
    df_amenagement_cyclable = pd.read_csv(url)
    return df_amenagement_cyclable


def clean_and_prepare_df(
    df_zones_urb: pd.DataFrame, df_amenagement_cyclable: pd.DataFrame
) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    raw_dir = get_raw_dir()

    # Téléchargement des données epci pour jointure
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    # km_amenagement par epci
    query = """ 
    SELECT 
        df_epci.dept_epci,
        df_epci.siren,
        df_epci.epci_nom,
        ROUND(sum(numerateur), 0) AS km_amenagement_cyclable
    FROM df_epci
    LEFT JOIN df_amenagement_cyclable
    ON df_amenagement_cyclable.geocode_commune = df_epci.code_insee
    GROUP BY df_epci.siren, df_epci.epci_nom, df_epci.dept_epci
    ORDER BY df_epci.dept_epci, df_epci.epci_nom
    """

    df_amenagement_epci = duckdb.sql(query)

    # On merge les aménagements cyclables avec les zones urbanisées
    query_bdd = """ 
    SELECT 
        ape.dept_epci AS dept_id,
        ape.siren AS id_epci,
        ape.epci_nom AS epci_lib,
        'i058' AS id_indicator,
        ROUND(ape.km_amenagement_cyclable / zu.superficie_artificialisee,2) AS valeur_brute,
        '2025' AS annee
    FROM df_amenagement_epci ape
    LEFT JOIN df_zones_urb zu
    ON ape.siren = zu.siren
    ORDER BY ape.dept_epci, ape.siren
    """

    # sauvegarde du dataframe final
    df_final = duckdb.sql(query_bdd)

    return df_final.df()


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
            unit="km_amenagements/km2_urbanise",
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
        df_zone_urb = prepare_zone_urb()
        df_amenagement_cyclable = load_amenagement_cyclable()
        df_processed = clean_and_prepare_df(df_zone_urb, df_amenagement_cyclable)

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
        help="i058",
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
        df_zones_urb = prepare_zone_urb()
        df_amenagement_cyclable = load_amenagement_cyclable()
        df_processed = clean_and_prepare_df(df_zones_urb, df_amenagement_cyclable)
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
