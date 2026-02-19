#!/usr/bin/env python3
"""
Indicateur i119 : Nombre de risques majeurs par EPCI
Source : France Découverte
URL : https://france-decouverte.geoclip.fr/#c=report&chapter=env&report=r03
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
DEFAULT_INDICATOR_ID = "i119"
DEFAULT_YEAR = 2025 
DEFAULT_SOURCE = (
    "https://france-decouverte.geoclip.fr/#c=report&chapter=env&report=r03"
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

def traitement_risques() -> pd.DataFrame:
    """Traitement des données de France Découverte"""

    # Chargement local
    raw_dir = get_raw_dir()
    path_file = raw_dir / "i119.csv"

     if not path_file.exists():
        raise FileNotFoundError(f"Fichier {path_file} introuvable après extraction")

    df_risques = pd.read_csv(path_file, sep=";", header=2, low_memory=False)

    mapping = {
        "Code": "code_insee",
        "Libellé": "nom_commune",
        "risque d'inondations, 2019": "inondations",
        "risque de mouvements de terrain, 2019": "mouvements_terrain",
        "risque de séismes, 2019": "seismes",
        "risque d'avalanches, 2019": "avalanches",
        "risque de feux de forêt, 2019": "feux_foret",
        "risque de lié à des phénomènes atmosphériques, 2019": "phenomenes_atmo",
        "risque d'éruptions volcaniques, 2019": "eruptions",
        "risque industriel, 2019": "industriel",
        "risque nucléaire, 2019": "nucleaire",
        "risque de rupture de barrage, 2019": "barrage",
        "risque lié au transport de marchandises dangereuses, 2019": "transport_matieres",
        "risque lié aux engins de guerre": "engins_guerre",
        "risque d'affaissements miniers, 2019": "affaissements_miniers",
    }

    # Modification des valeurs commençant par "N/A" en Nan
    df_risques = df_risques.replace(r"^N/A.*", pd.NA, regex=True)

    #Supppression des lignes avec code_insee null et traitement des codes insee
    df_risques = df_risques.dropna(subset=["code_insee"])
    df_risques = df_risques.rename(columns=mapping).drop(columns=["nom_commune"])
    df_risques["code_insee"] = df_risques["code_insee"].apply(lambda x: str(x).zfill(5))

    # Modifier Paris, Marseille et Lyon pour les faire correspondre à la table epci
    df_risques.loc[df_risques["code_insee"].str.startswith("75"), "code_insee"] = (
        "75056"
    )
    df_risques.loc[df_risques["code_insee"].str.startswith("693"), "code_insee"] = (
        "69123"
    )
    df_risques.loc[df_risques["code_insee"].str.startswith("132"), "code_insee"] = (
        "13055"
    )
    return df_risques

def clean_and_prepare_df(df_risques: pd.DataFrame) -> pd.DataFrame:
    """Prépare le DataFrame brut pour le traitement."""

    raw_dir = get_raw_dir()

    # Chargement de la table epci
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    # jointure avec les communes pour obtenir les codes epci
    query = """ 
    SELECT 
        df_epci.siren AS id_epci,
        df_risques.*
    FROM df_risques
    LEFT JOIN df_epci
        ON df_risques.code_insee = df_epci.code_insee
    """

    df_risques_epci = duckdb.sql(query)

    # compter le nombre de risques majeurs par epci
    query = """ 
    SELECT
        id_epci,
        SUM(TRY_CAST(inondations AS INTEGER)) AS inondations,
        SUM(TRY_CAST(mouvements_terrain AS INTEGER)) AS mouvements_terrain,
        SUM(TRY_CAST(seismes AS INTEGER)) AS seismes,
        SUM(TRY_CAST(avalanches AS INTEGER)) AS avalanches,
        SUM(TRY_CAST(feux_foret AS INTEGER)) AS feux_foret,
        SUM(TRY_CAST(phenomenes_atmo AS INTEGER)) AS phenomenes_atmo,
        SUM(TRY_CAST(eruptions AS INTEGER)) AS eruptions,
        SUM(TRY_CAST(nucleaire AS INTEGER)) AS nucleaire,
        SUM(TRY_CAST(barrage AS INTEGER)) AS barrage,
        SUM(TRY_CAST(transport_matieres AS INTEGER)) AS transport_matieres,
        SUM(TRY_CAST(engins_guerre AS INTEGER)) AS engins_guerre,
        SUM(TRY_CAST(affaissements_miniers AS INTEGER)) AS affaissements_miniers,
        SUM(TRY_CAST(industriel AS INTEGER)) AS industriel
    FROM df_risques_epci
    GROUP BY id_epci
    """

    df_total_risques = duckdb.sql(query)

    query = """ 
    SELECT 
        df_total_risques.id_epci,
        SUM(CASE WHEN inondations > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN mouvements_terrain > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN seismes > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN avalanches > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN feux_foret > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN phenomenes_atmo > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN eruptions > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN nucleaire > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN barrage > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN transport_matieres > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN engins_guerre > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN affaissements_miniers > 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN industriel > 0 THEN 1 ELSE 0 END)
        AS total_risques
    FROM df_total_risques
    GROUP BY id_epci
        """
    df_total_risques = duckdb.sql(query)

    # query complete with join
    query = """
    WITH s1 AS (
    SELECT DISTINCT siren, dept_epci, epci_nom
    FROM df_epci
        )

        SELECT
            s1.dept_epci AS dept_id,
            CAST(s1.siren AS VARCHAR) AS id_epci,
            s1.epci_nom AS epci_lib,
            'i119' AS id_indicator,
            s2.total_risques
        AS valeur_brute,
            '2019' AS annee
        FROM s1
        LEFT JOIN df_total_risques AS s2
            ON CAST(s1.siren AS VARCHAR) = CAST(s2.id_epci AS VARCHAR)
        GROUP BY s1.dept_epci, s1.siren, s1.epci_nom, s2.total_risques
        ORDER BY s1.dept_epci, s1.siren
    """

    df_complete = duckdb.sql(query)
    return df_complete.df()


def transform_payload(df: pd.DataFrame) -> Iterator[RawValue]:

    for _, row in df.iterrows():
        if pd.isna(row["valeur_brute"]):
            continue

        yield RawValue(
            epci_id=str(row["id_epci"]),
            indicator_id=str(row["id_indicator"]),
            year=str(row["annee"]),
            value=float(row["valeur_brute"]),
            unit="nb_risques_majeurs",
            source=DEFAULT_SOURCE,
            meta={},
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
        df_risques = traitement_risques()
        df_processed = clean_and_prepare_df(df_risques)

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
    parser = argparse.ArgumentParser(description="Téléchargement de données -> i119")
    parser.add_argument("--indicator", default=DEFAULT_INDICATOR_ID, help="i119")
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
        df_risques = traitement_risques()
        df_processed = clean_and_prepare_df(df_risques)
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
