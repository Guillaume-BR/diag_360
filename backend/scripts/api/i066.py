#!/usr/bin/env python3
"""
Indicateur i066 : Densité de pharmacies pour 10 000 habitants par EPCI

Source : data.gouv.fr
URL : https://www.data.gouv.fr/datasets/finess-extraction-du-fichier-des-etablissements
Dernières données disponibles : 2026
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, Iterator  # ✅ Correction
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
from utils.functions import download_file, get_raw_dir

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
DEFAULT_INDICATOR_ID = "i066"
DEFAULT_YEAR = 2026
DEFAULT_SOURCE = (
    "https://www.data.gouv.fr/api/1/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
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


def pharma_traitement():
    # chargement des données des pharmacies
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
    )
    content = download_file(url)
    df_pharma = pd.read_csv(
        BytesIO(content), sep=";", dtype=str, skiprows=1, header=None
    )

    # Traitement des données de pharmacies
    df_pharma = df_pharma.iloc[:, [12, 13, 15, 19]]
    df_pharma.rename(
        columns={19: "type", 15: "code_postal", 12: "code_com", 13: "dept"},
        inplace=True,
    )
    df_pharma["code_insee"] = df_pharma["dept"] + df_pharma["code_com"]
    df_pharma = df_pharma.loc[
        df_pharma["type"].str.startswith("Pharmacie")
    ].reset_index(drop=True)

    # Correction des codes insee de Paris, Lyon, Marseille pour les faire correspondre à ceux de l'INSEE
    df_pharma = df_pharma.dropna(subset=["code_insee"])
    df_pharma.loc[df_pharma["code_insee"].str.startswith("75"), "code_insee"] = "75056"
    df_pharma.loc[df_pharma["code_insee"].str.startswith("693"), "code_insee"] = "69123"
    df_pharma.loc[df_pharma["code_insee"].str.startswith("132"), "code_insee"] = "13055"

    return df_pharma


def clean_and_prepare_df() -> pd.DataFrame:
    raw_dir = get_raw_dir()
    # Chargement de df_pharma
    df_pharma = pharma_traitement()

    # Chargement de la table epci
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    query_final = """
    SELECT
        df_epci.dept_epci AS dept_id,
        df_epci.siren AS id_epci,
        df_epci.epci_nom AS epci_lib,
        'i066' AS id_indicator,
        round(COUNT(df_pharma.code_insee) / cast(df_epci.total_pop_mun AS FLOAT) * 10000,2) AS valeur_brute,
        '2026' AS annee
    FROM df_epci
    LEFT JOIN df_pharma AS df_pharma
        ON df_pharma.code_insee = df_epci.code_insee
    GROUP BY df_epci.siren, df_epci.dept_epci, df_epci.epci_nom, df_epci.total_pop_mun
    ORDER BY df_epci.dept_epci, df_epci.siren
        """

    df_densite_pharma_final = duckdb.sql(query_final)

    return df_densite_pharma_final.df()


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
            unit="nb_pharmacies/10000hab",
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

        # Téléchargement, extraction et nettoyage des données
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
    parser = argparse.ArgumentParser(description="Téléchargement de données -> i066")
    parser.add_argument("--indicator", default=DEFAULT_INDICATOR_ID, help="i066")
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
