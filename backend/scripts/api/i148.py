#!/usr/bin/env python3
"""
Indicateur i148 : Distance moyenne aux urgences la plus proche par EPCI
Source : Cartosanté
URL : https://cartosante.atlasante.fr/#c=indicator&f=7&i=prox_struct.dist_str&s=2024&t=A01&view=map12

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
URL = "https://cartosante.atlasante.fr/#c=indicator&f=7&i=prox_struct.dist_str&s=2024&t=A01&view=map12"
DEFAULT_INDICATOR_ID = "i148"
DEFAULT_YEAR = 2024
DEFAULT_SOURCE = "i148.csv"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def traitement_urg() -> pd.DataFrame:
    """Charge le fichier des urgences et retourne le DataFrame retraité"""

    raw_dir = get_raw_dir()

    # Lire le CSV
    path_file = raw_dir / DEFAULT_SOURCE
    if not path_file.exists():
        raise FileNotFoundError(
            f"Fichier {path_file} introuvable dans le dossier {raw_dir}"
        )
    logger.info("Téléchargement des données de urgences")

    df_dist_urg = pd.read_csv(path_file, skiprows=2, sep=";")

    # Changement des noms de colonnes
    mapping_urg = {
        "Code": "code_insee",
        "Libellé": "nom_commune",
        "Distance à la structure la plus proche 2024": "dist_urgence_min",
    }

    df_dist_urg = df_dist_urg.rename(columns=mapping_urg)

    # On modifie le code_insee de Paris, Marseille et Lyon
    df_dist_urg.loc[df_dist_urg["code_insee"].str.startswith("75"), "code_insee"] = (
        "75056"
    )
    df_dist_urg.loc[df_dist_urg["code_insee"].str.startswith("693"), "code_insee"] = (
        "69123"
    )
    df_dist_urg.loc[df_dist_urg["code_insee"].str.startswith("132"), "code_insee"] = (
        "13055"
    )

    # on supprime les lignes où dist_urg_min est "'N/A - résultat non disponible'"
    df_dist_urg.loc[
        df_dist_urg["dist_urgence_min"].str.contains("N/A", na=False),
        "dist_urgence_min",
    ] = np.nan

    # on groupe par code_insee en faisant la moyenne des distances
    df_dist_urg["dist_urgence_min"] = (
        df_dist_urg["dist_urgence_min"].str.replace(",", ".").astype(float)
    )
    df_dist_urg = df_dist_urg.groupby("code_insee", as_index=False).agg(
        {"dist_urgence_min": "mean"}
    )

    return df_dist_urg


def clean_and_prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    raw_dir = get_raw_dir()
    # Chargement de df_dist_urg
    df_dist_urg = traitement_urg()

    # Création du dataframe des communes (cf functions.py)
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    query_final = """ 
    SELECT
        df_epci.dept_epci AS dept_id,
        df_epci.siren AS id_epci,
        df_epci.epci_nom AS epci_lib,
        'i148' AS id_indicator,
        ROUND(AVG(TRY_CAST(dist_urgence_min AS DOUBLE)),2) AS valeur_brute,
        '2024' AS annee
    FROM df_epci
    LEFT JOIN df_dist_urg
        ON df_epci.code_insee = df_dist_urg.code_insee
    GROUP BY siren, epci_nom, dept_id
    ORDER BY dept_id, id_epci
    """

    df_dist_urg_moy = duckdb.sql(query_final)

    return df_dist_urg_moy.df()


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
            unit="km",
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
        df_processed = clean_and_prepare_df()

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
        description="Import des données de distance aux urgences -> i148"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="i148",
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
        df_processed = clean_and_prepare_df()
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
