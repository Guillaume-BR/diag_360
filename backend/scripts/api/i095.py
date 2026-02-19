#!/usr/bin/env python3
"""
Indicateur i095 : Nombre de lieux de médiation numérique pour 10000 habs
Source : Data.gouv.fr
URL : "https://www.data.gouv.fr/api/1/datasets/r/398edc71-0d51-4cb6-9cbe-2540a4db573c"

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
from utils.functions import get_raw_dir, create_dataframe_communes

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/398edc71-0d51-4cb6-9cbe-2540a4db573c"
DEFAULT_INDICATOR_ID = "i095"
DEFAULT_YEAR = 2025
DEFAULT_SOURCE = "i095.csv"


@dataclass
class RawValue:
    epci_id: str
    indicator_id: str
    year: int
    value: float
    unit: str | None = None
    source: str | None = None
    meta: dict | None = None


def prepare_med_num():
    # Chargement local des données mais sinon utiliser :
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/398edc71-0d51-4cb6-9cbe-2540a4db573c"
    )

    # Télécharger et extraire les données médiation
    df_mediation_num = pd.read_csv(url, low_memory=False)

    df_mediation_num["code_postal"] = (
        df_mediation_num["code_postal"].astype(str).str.zfill(5)
    )
    df_med_num = (
        df_mediation_num[["commune", "code_postal", "code_insee", "adresse"]]
        .sort_values(by="code_insee")
        .drop_duplicates()
    )

    # Identifier les lignes avec code_insee manquant et extraire le code département
    df_isna = df_med_num[df_med_num["code_insee"].isna()]
    df_isna["dep_code"] = df_isna["code_postal"].astype(str).str.zfill(5).str[:2]

    def find_code_insee(row):
        com = str(row["commune"]).upper()
        dep = row["dep_code"]
        matches = df_com[
            (df_com["dep_code"] == dep)
            & (
                df_com["nom_standard_majuscule"].str.contains(
                    com, case=False, na=False, regex=False
                )
            )
        ]
        return matches["code_insee"].iloc[0] if not matches.empty else None

    df_com = create_dataframe_communes()

    df_isna["code_insee"] = df_isna.apply(find_code_insee, axis=1)

    # Afficher les lignes avec code_insee toujours manquant après tentative de correspondance
    df_isna_null = df_isna[df_isna["code_insee"].isna()]

    # On crée un mapping des communes avec code_insee manquant pour les corriger dans df_med_num
    mapping_com = {
        "AIX-LA-DURANNE": "13001",
        "BLETTRANS": "39056",
        "Bordères-et-Lamensens": "40049",
        "CAPAVENIR-VOSGES": "88465",
        "CEZAIS": "85292",
        "CHERVES-RICHEMONT": "16097",
        "COSNE-SUR-LOIRE": "58086",
        "ETRAT": "42092",
        "Eryaud-Crempse-Maurens": "24259",
        "Etroeugnt": "59218",
        "HELLEMMES---LILLE": "59350",
        "HELLEMMES-LILLE": "59350",
        "LA-TARDIERE": "85289",
        "LE-BÉNY-BOCAGE": "14061",
        "La-Chapelle-aux-Pots": "61275",
        "Le-Merlerault": "60333",
        "MARDYCK": "59183",
        "MONTIGNY-PRES-LOUHANS": "71303",
        "MORET-SUR-LOING": "77316",
        "MOREZ": "39368",
        "NANTEUIL-LE-HAUDOIN": "60446",
        "NUIT-SAINT-GEORGES": "21464",
        "Neussargues-en-Pinatelle": "15141",
        "PONT-A-BUCY": "02559",
        "PONT-DU-LOUP": "06148",
        "Richerbourg": "62706",
        "SAINT-MACOUX": "86247",
        "SAINT-SAVIOL": "86247",
        "SAINT-SULPICE-DE-COGNAC": "16097",
        "SECHELLES": "02004",
        "SENNECEY-SUR-SAÔNE-SAINT-ALBIN": "70482",
        "Saint-Macoux": "86247",
        "Saint-Paul-lez-Durance": "13099",
        "Saint-Saviol": "86247",
        "THOUARSAIS-BOUILDROUX": "85292",
        "TOURETTES-SUR-LOUP": "06148",
        "VALENCIENES": "59606",
        "hondshoote": "59309",
    }
    # On applique le mapping
    df_isna_null["code_insee"] = df_isna_null["commune"].map(mapping_com)

    # On regroupe les datarame
    df_isna_not_null = df_isna[df_isna["code_insee"].notna()]
    df_med_num_not_null = df_med_num[df_med_num["code_insee"].notna()]
    df_med_num_final = pd.concat(
        [df_isna_not_null, df_isna_null, df_med_num_not_null], ignore_index=True
    ).sort_values(by="code_insee")

    return df_med_num_final


def clean_and_prepare_df(df_med_num_final: pd.DataFrame) -> pd.DataFrame:
    """Calcule l'indicateur via DuckDB à partir des données."""

    raw_dir = get_raw_dir()

    # Téléchargement des données epci pour jointure
    df_epci = pd.read_csv(raw_dir / "epci_membres.csv", sep=",")

    # Jointure des données
    query = """ 
    WITH df_mediation_num_grouped AS (
    SELECT 
        count(*) AS nb_mediation,
        code_insee
    FROM df_med_num_final
    GROUP BY code_insee)

    SELECT 
        dept_epci as dept_id,
        siren as epci_id,
        epci_nom as epci_lib,
        'i095' as id_indicator,
        round(10000 * sum(dmn.nb_mediation) / df_epci.total_pop_mun, 2) AS mediation_per_10k_habs ,
        '2025' as annee
    FROM df_epci
    LEFT JOIN df_mediation_num_grouped as dmn
    ON dmn.code_insee = df_epci.code_insee
    GROUP BY dept_epci, siren, epci_nom, total_pop_mun
    ORDER BY dept_epci, siren
    """
    df_epci_mediation = duckdb.sql(query).df()

    return df_epci_mediation


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
            unit="mediations/10k_habitants",
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
        df_med_num_final = prepare_med_num()
        df_processed = clean_and_prepare_df(df_med_num_final)

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
        help="i095",
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
        df_med_num_final = prepare_med_num()
        df_processed = clean_and_prepare_df(df_med_num_final)
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
