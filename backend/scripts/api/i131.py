#!/usr/bin/env python3
"""
Indicateur i131 : Nombre d'associations pour 1000 habitants
Source : data.gouv.fr
URL : https://www.data.gouv.fr/api/1/datasets/r/c2334d19-c752-413f-b64b-38006d9d0513

"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
import os
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
from utils.functions import download_file, create_dataframe_communes, create_dataframe_epci

logger = logging.getLogger(__name__)

# Configuration
URL = "https://www.data.gouv.fr/api/1/datasets/r/c2334d19-c752-413f-b64b-38006d9d0513"
DEFAULT_INDICATOR_ID = "i131"
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


def get_raw_dir() -> Path:
    """Retourne le chemin du répertoire source, le crée si nécessaire."""
    base_dir = Path(__file__).resolve().parent.parent
    raw_dir = base_dir / "source"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def asso_sans_alsace() -> pd.DataFrame:
    """
    Charge les données des associations hors Alsace-Moselle.

    Returns
    -------
    pd.DataFrame
        DataFrame contenant les données des associations hors Alsace-Moselle nettoyées.
    """

    # Chargement des données des associations hors Alsace-Moselle
    url_complete = (
        "https://www.data.gouv.fr/api/1/datasets/r/cc7b8f0c-45ea-4444-8b55-55d30bc34ac5"
    )
    df_asso = duckdb.read_parquet(url_complete)

    # On garde les assos actives et on retire celles que l'on ne peut rattacher à une commune
    query = """ 
    SELECT 
        id,
        adrs_codeinsee, 
        adrs_codepostal
    FROM df_asso 
    WHERE position = 'A' 
            AND (adrs_codeinsee IS NOT NULL OR adrs_codepostal IS NOT NULL)
            AND (adrs_codeinsee != '0' OR adrs_codepostal != '00000')
            AND (adrs_codeinsee !='0' OR adrs_codepostal IS NOT NULL)
    ORDER BY adrs_codeinsee
    """

    df_asso_filtered = duckdb.sql(query).df()

    # Récupération des valeurs code_insee et code_postal manquantes via jointure avec df_com
    query_insee_not_null = """
    SELECT
        *
    FROM df_asso_filtered
    WHERE adrs_codeinsee IS NOT NULL 
        AND adrs_codeinsee!='0'
        """

    df_insee_not_null = duckdb.sql(query_insee_not_null).df()

    query_insee_null = """
    SELECT
        *
    FROM df_asso_filtered
    WHERE adrs_codeinsee IS NULL 
        OR adrs_codeinsee='0'
    """
    df_insee_null = duckdb.sql(query_insee_null).df()

    # Création de la table duckdb des communes
    df_com = create_dataframe_communes()

    for _, row in df_insee_null.iterrows():
        code_postal = row["adrs_codepostal"]
        code_insee = df_com[df_com["code_postal"] == code_postal]["code_insee"].values[
            0
        ]
        print(f"Code postal: {code_postal} -> Code insee: {code_insee}")
        row["adrs_codeinsee"] = code_insee

    query = """ 
    select * from df_insee_null
    union
    select * from df_insee_not_null
    """

    df_asso_als = duckdb.sql(query)

    # On règle les problèmes de code postal
    query_sans_pb_postal = """ 
    SELECT * 
    FROM df_asso_als 
    WHERE adrs_codepostal not like '00%' 
    ORDER BY adrs_codeinsee
    """

    df_sans_pb_postal = duckdb.sql(query_sans_pb_postal).df()

    query_pb_postal = """ 
    SELECT *
    FROM df_asso_als
    WHERE adrs_codepostal like '00%'
    ORDER BY adrs_codeinsee
    """

    df_pb_postal = duckdb.sql(query_pb_postal)

    ##Correction des codes postaux de Paris
    query_paris_corrige = """ 
    SELECT 
        id, 
        '75056' AS adrs_codeinsee,
        '75012' AS adrs_codepostal
    FROM df_pb_postal 
    WHERE adrs_codeinsee = '75112' and adrs_codepostal like '00%'
    ORDER BY adrs_codeinsee
    """

    df_paris_corrige = duckdb.sql(query_paris_corrige).df()

    ##On revient aux codes postaux problématiques en enlevant paris corrigé
    query_pb_postal_sans_paris = """
    SELECT * 
    FROM df_pb_postal
    WHERE adrs_codeinsee != '75112'
    """
    df_pb_postal_sans_paris = duckdb.sql(query_pb_postal_sans_paris).df()

    # Correction des derniers codes postaux problématiques via jointure avec df_com
    for _, row in df_pb_postal_sans_paris.iterrows():
        code_insee = row["adrs_codeinsee"]
        try:
            code_postal = df_com[df_com["code_insee"] == code_insee][
                "code_postal"
            ].values[0]
        except IndexError:
            print(f"Code insee: {code_insee} not found in df_com")
            continue

    # On supprime les derniers problèmes
    df_pb_postal_clean = df_pb_postal_sans_paris[
        df_pb_postal_sans_paris["adrs_codepostal"] != "00000"
    ]

    # On concatène les dataframes pour obtenir le dataframe final
    df_asso_sans_final = pd.concat(
        [df_sans_pb_postal, df_pb_postal_clean, df_paris_corrige]
    )

    return df_asso_sans_final


def asso_alsace_moselle() -> pd.DataFrame:
    # Données des associations pour 57,67,68 (Alsace-Moselle)
    dico_url = {
        "57": "https://www.data.gouv.fr/api/1/datasets/r/f5073265-9689-441c-bd6d-8d9fbd360161",
        "67": "https://www.data.gouv.fr/api/1/datasets/r/b7acf7a2-1480-465e-b02b-22633d0a378d",
        "68": "https://www.data.gouv.fr/api/1/datasets/r/b7d6b412-5da6-4ed2-97cc-c9d8e7b321de",
    }

    df_asso = pd.DataFrame()
    for _, url in dico_url.items():
        content = download_file(url)
        df_dept = pd.read_csv(BytesIO(content), sep=";", dtype=str)
        df_asso = pd.concat([df_asso, df_dept], ignore_index=True)

    # On garde les assos inscrites
    query = """ 
    SELECT 
    NUMERO_AMALIA as id,
    COMMUNE as commune,
    CODE_POSTAL as adrs_codepostal
    FROM df_asso
    WHERE ETAT_ASSOCIATION = 'INSCRITE'
    ORDER BY CODE_POSTAL
    """

    df_asso_filtered = duckdb.sql(query).df()

    # On règle les problèmes de code postal
    # query = """
    # SELECT *
    # FROM df_asso_filtered
    # WHERE LENGTH(adrs_codepostal) != 5
    # """
    # df_asso_filtered_5 = duckdb.sql(query).df()

    mapping_code_postal = {
        "57": "57050",
        "5700": "57000",
        "570000": "57000",
        "573000": "57300",
        "57657660": "57660",
        "67000": "67000",
        "670000": "67000",
        "680000": "68000",
        "681180 ": "68118",
        "684809": "68480",
        "686102": "68610",
    }

    df_asso_corrige = df_asso_filtered.replace({"adrs_codepostal": mapping_code_postal})
    return df_asso_corrige


def clean_and_prepare_df(
    df_asso_sans_als: pd.DataFrame, df_asso_als: pd.DataFrame
) -> pd.DataFrame:
    """Nettoie et prépare le DataFrame des associations pour le calcul de l'indicateur."""

    # Création de la table duckdb des communes
    df_com = create_dataframe_communes()

    # Création de la table duckdb des epci
    df_epci = create_dataframe_epci()

    # jointure avec les communes pour récupérer les codes insee
    query_join = """ 
    SELECT 
        a.id,
        c.nom_standard_majuscule as commune,
        a.adrs_codepostal,
        c.code_insee as adrs_codeinsee  
    FROM df_asso_als a
    LEFT JOIN df_com c
    ON a.commune = c.nom_standard_majuscule 
        AND LEFT(a.adrs_codepostal, 2) = LEFT(c.code_postal, 2)
    ORDER BY a.adrs_codepostal  
    """

    df_joined = duckdb.sql(query_join)

    # Concaténation des deux dataframes des assos
    query_union = """
    SELECT 
        id,
        adrs_codeinsee,
        adrs_codepostal 
    FROM df_joined

    UNION

    SELECT * 
    FROM df_asso_sans_als
    ORDER BY adrs_codeinsee
    """

    df_asso_complete = duckdb.sql(query_union)

    # Jointure avec les epci pour le calcul de l'indicateur
    query = """ 
    SELECT 
        TRY_CAST(siren AS INTEGER) AS siren,
        raison_sociale,
        dept,
        insee,
        TRY_CAST(REPLACE(total_pop_tot, ' ', '') AS INTEGER) AS total_pop_tot,
    from df_epci
    """

    df_epci = duckdb.sql(query)

    # query pour le calcul de l'indicateur i131
    query_bdd = """
    SELECT 
        CAST(e2.siren AS VARCHAR) as id_epci, 
        'i131' AS id_indicator,
        ROUND(count(e1.adrs_codeinsee) / e2.total_pop_tot * 1000,2) AS valeur_brute,
        '2025' AS annee
    FROM df_epci e2
    LEFT JOIN df_asso_complete e1
    ON e1.adrs_codeinsee = e2.insee
    GROUP BY e2.siren, e2.total_pop_tot
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
            unit="nb_asso/1000_habitants",
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
        df_asso_alsace = asso_alsace_moselle()
        df_asso_sans_alsace = asso_sans_alsace()
        df_processed = clean_and_prepare_df(df_asso_alsace, df_asso_sans_alsace)

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
        description=f"Import des données du nombre d'associations -> {DEFAULT_INDICATOR_ID}"
    )
    parser.add_argument(
        "--indicator",
        default=DEFAULT_INDICATOR_ID,
        help="i131",
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
        df_asso_alsace = asso_alsace_moselle()
        df_asso_sans_alsace = asso_sans_alsace()
        df_processed = clean_and_prepare_df(df_asso_alsace, df_asso_sans_alsace)
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
