#!/usr/bin/env python3
"""
Indicateur i037 : Taux d'évolution annuel du nombre de logements sociaux (RPLS) (%) 2019-2023
Source : Atlas Santé
URL : https://www.observatoire-des-territoires.gouv.fr/taux-devolution-annuel-du-nombre-de-logements-sociaux-rpls
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
from diag_360.backend.scripts.api.i164 import traitement_precaire
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
DEFAULT_INDICATOR_ID = "i037"
DEFAULT_YEAR = 2023
DEFAULT_SOURCE = "i037.csv"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def traitement_rpls() -> pd.DataFrame:
    """Charge le fichier des nombres de logements sociaux et retourne le DataFrame"""

    raw_dir = get_raw_dir()

    # Lire le CSV
    path_file = raw_dir / DEFAULT_SOURCE
    if not path_file.exists():
        raise FileNotFoundError(
            f"Fichier {path_file} introuvable dans le dossier {raw_dir}"
        )
    logger.info("Téléchargement des données du taux de logements sociaux")

    data_rpls = pd.read_csv(path_file, sep=";", low_memory=False)

    # Traitement de la table des rpls
    df_rpls = (
        data_rpls[["DEPCOM_ARM", "nb_ls2023", "nb_ls2019"]]
        .copy()
        .rename(
            columns={
                "DEPCOM_ARM": "code_insee",
                "nb_ls2023": "ls_2023",
                "nb_ls2019": "ls_2019",
            }
        )
    )

    df_rpls["code_insee"] = (
        df_rpls["code_insee"].replace(" ", "").apply(lambda x: str(x).zfill(5))
    )
    df_rpls["ls_2023"] = df_rpls["ls_2023"].str.replace("\xa0", "").astype(int)
    df_rpls["ls_2019"] = df_rpls["ls_2019"].str.replace("\xa0", "").astype(int)

    # modification des codes_inse de paris, marseille et lyon pour les faire correspondre à ceux de l'epci
    df_rpls.loc[df_rpls["code_insee"].str.startswith("75"), "code_insee"] = "75056"
    df_rpls.loc[df_rpls["code_insee"].str.startswith("132"), "code_insee"] = "13055"
    df_rpls.loc[df_rpls["code_insee"].str.startswith("693"), "code_insee"] = "69123"

    return df_rpls


def clean_and_prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    raw_dir = get_raw_dir()

    # Téléchargement des données epci pour jointure
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    # query pour avoir l'indicteur
    query = """
    SELECT 
        df_epci.dept_epci as dept_id,
        df_epci.siren as epci_id,
        df_epci.epci_nom AS epci_lib,
        'i037' as id_indicator,
        case WHEN sum(df_rpls.ls_2019) = 0 THEN null
             ELSE ROUND((sum(df_rpls.ls_2023) - sum(df_rpls.ls_2019)) / sum(df_rpls.ls_2019)  * 100, 2) END as valeur_brute,
        '2023' as annee
    FROM df_epci
    JOIN df_rpls 
        ON df_epci.code_insee = df_rpls.code_insee
    GROUP BY df_epci.dept_epci,df_epci.siren, df_epci.epci_nom
    ORDER BY df_epci.dept_epci,df_epci.siren
    """

    df_final = duckdb.sql(query)
    return df_final.df()


def transform_payload(df: pd.DataFrame) -> Iterator[RawValue]:
    """Transforme le DataFrame en itérable de RawValue"""
    for _, row in df.iterrows():
        if pd.isna(row["valeur_brute"]):
            continue

        yield RawValue(
            epci_id=str(row["epci_id"]),
            indicator_id=str(row["id_indicator"]),
            year=str(row["annee"]),
            value=float(row["valeur_brute"]),
            unit="taux_population_precaire",
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
        df_rpls = traitement_rpls()
        df_processed = clean_and_prepare_df(df_rpls)

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
        description=f"Import des données du taux de population précaire -> {DEFAULT_INDICATOR_ID}"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="i037",
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
        df_rpls = traitement_rpls()
        df_processed = clean_and_prepare_df(df_rpls)
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
