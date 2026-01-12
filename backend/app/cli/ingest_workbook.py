import argparse
import logging
import sys
import unicodedata
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import select

from app.db import Base, SessionLocal, engine
from app.models import Epci, Indicator, IndicatorScore, IndicatorType, IndicatorValue, Need, Objective

logger = logging.getLogger("diag360.ingest_workbook")


def normalise_str(value) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text


def normalise_code(value) -> Optional[str]:
    text = normalise_str(value)
    if text is None:
        return None
    try:
        if "e" in text.lower():
            number = int(Decimal(text))
        elif text.endswith(".0"):
            number = int(float(text))
        else:
            number = int(text)
        return f"{number}"
    except (ValueError, InvalidOperation):
        return text


def normalise_indicator_id(value) -> Optional[str]:
    text = normalise_str(value)
    if text is None:
        return None
    text = text.lower()
    if text.startswith("i"):
        suffix = text[1:].strip().replace(" ", "")
        if suffix.isdigit():
            return f"i{int(suffix):03d}"
    if text.isdigit():
        return f"i{int(text):03d}"
    return text


def to_int(value) -> Optional[int]:
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_float(value) -> Optional[float]:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def iter_rows(df: pd.DataFrame):
    df = df.rename(columns=lambda c: str(c).strip())
    columns = list(df.columns)
    for values in df.itertuples(index=False, name=None):
        yield dict(zip(columns, values))


def ingest_needs(session, df: pd.DataFrame):
    for row in iter_rows(df):
        need_id = normalise_str(row.get("ID_besoins"))
        if not need_id:
            continue
        need = Need(
            id=need_id,
            label=normalise_str(row.get("Libellé")) or "",
            category=normalise_str(row.get("Type_de_besoins")) or normalise_str(row.get("Catégorie")),
            description=normalise_str(row.get("Description")),
        )
        session.merge(need)


def ingest_objectives(session, df: pd.DataFrame):
    for row in iter_rows(df):
        obj_id = normalise_str(row.get("ID_Objectifs"))
        if not obj_id:
            continue
        obj = Objective(
            id=obj_id,
            label=normalise_str(row.get("Libellé")) or "",
            description=normalise_str(row.get("Description")),
        )
        session.merge(obj)


def ingest_indicator_types(session, df: pd.DataFrame):
    for row in iter_rows(df):
        type_id = normalise_str(row.get("ID_")) or normalise_str(row.get("ID_Type"))
        if not type_id:
            continue
        indicator_type = IndicatorType(
            id=type_id,
            label=normalise_str(row.get("Libellé")) or "",
            description=normalise_str(row.get("Description")),
        )
        session.merge(indicator_type)


def ingest_indicators(session, df: pd.DataFrame):
    for row in iter_rows(df):
        indicator_id = normalise_indicator_id(row.get("ID_indicateurs"))
        if not indicator_id:
            continue
        indicator = Indicator(
            id=indicator_id,
            label=normalise_str(row.get("Libellé_indicateurs")) or "",
            description=normalise_str(row.get("Description")),
            primary_source=normalise_str(row.get("Domaine_Source_principale")),
            primary_url=normalise_str(row.get("URL Source_Principale")),
            api_available=bool(normalise_str(row.get("API disponible"))),
            secondary_source=normalise_str(row.get("Domaine_Source_secondaire")),
            secondary_url=normalise_str(row.get("URL_Source_Secondaire")),
            value_type=normalise_str(row.get("TYPE DE VALEUR")),
            unit=normalise_str(row.get("Unité")),
        )
        session.merge(indicator)


def ingest_indicator_need_links(session, df: pd.DataFrame):
    indicator_to_needs: dict[str, set[str]] = defaultdict(set)
    need_to_indicators: dict[str, set[str]] = defaultdict(set)
    need_categories: dict[str, str] = {}
    need_ids = {need.id for need in session.execute(select(Need)).scalars()}

    for row in iter_rows(df):
        indicator_id = normalise_indicator_id(row.get("ID_Indicateurs"))
        if not indicator_id:
            continue
        base_need = normalise_str(row.get("ID_Besoin")) or normalise_str(row.get("Besoin 1"))
        extra_needs = []
        for col in ["Besoin 1", "Besoin 2", "Besoin 3", "Besoin 4", "Besoin 5"]:
            val = normalise_str(row.get(col))
            if val:
                extra_needs.append(val)
        needs = [base_need] if base_need else []
        needs.extend(extra_needs)
        needs = [need for need in needs if need and need in need_ids]
        if not needs:
            continue
        category = normalise_str(row.get("Type_de_besoins")) or normalise_str(row.get("Type de besoins"))
        for need_id in needs:
            indicator_to_needs[indicator_id].add(need_id)
            need_to_indicators[need_id].add(indicator_id)
            if category:
                need_categories.setdefault(need_id, category)

    for indicator_id, needs in indicator_to_needs.items():
        indicator = session.get(Indicator, indicator_id)
        if indicator:
            indicator.need_ids = sorted(needs)
            session.add(indicator)

    for need_id, indicators in need_to_indicators.items():
        need = session.get(Need, need_id)
        if need:
            need.indicator_ids = sorted(indicators)
            if need_categories.get(need_id):
                need.category = need_categories[need_id]
            session.add(need)


def _flag(value) -> bool:
    text = normalise_str(value)
    if not text:
        return False
    return text.lower() in {"x", "1", "true", "oui"}


def ingest_indicator_objective_links(session, df: pd.DataFrame):
    objective_ids = {obj.id for obj in session.execute(select(Objective)).scalars()}
    indicator_to_objectives: dict[str, set[str]] = defaultdict(set)
    objective_to_indicators: dict[str, set[str]] = defaultdict(set)

    for row in iter_rows(df):
        indicator_id = normalise_indicator_id(row.get("ID_Indicateurs"))
        if not indicator_id:
            continue
        mapping = {
            "o1": row.get("o1_Subsistance"),
            "o2": row.get("o2_Gestion-de-crise"),
            "o3": row.get("o3_Soutenabilité"),
        }
        for objective_code, flag_value in mapping.items():
            if objective_code not in objective_ids:
                continue
            if _flag(flag_value):
                indicator_to_objectives[indicator_id].add(objective_code)
                objective_to_indicators[objective_code].add(indicator_id)

    for indicator_id, objectives in indicator_to_objectives.items():
        indicator = session.get(Indicator, indicator_id)
        if indicator:
            indicator.objective_ids = sorted(objectives)
            session.add(indicator)

    for objective_id, indicators in objective_to_indicators.items():
        objective = session.get(Objective, objective_id)
        if objective:
            objective.indicator_ids = sorted(indicators)
            session.add(objective)


def ingest_indicator_type_links(session, df: pd.DataFrame):
    indicator_to_types: dict[str, set[str]] = defaultdict(set)
    type_to_indicators: dict[str, set[str]] = defaultdict(set)
    type_ids = {t.id for t in session.execute(select(IndicatorType)).scalars()}

    for row in iter_rows(df):
        indicator_id = normalise_indicator_id(row.get("ID_indicateurs"))
        if not indicator_id:
            continue
        entries = [
            ("Typ1", _flag(row.get("Typ1_Etat"))),
            ("Typ2", _flag(row.get("Typ2_Action"))),
        ]
        for type_id, flag in entries:
            if not flag or type_id not in type_ids:
                continue
            indicator_to_types[indicator_id].add(type_id)
            type_to_indicators[type_id].add(indicator_id)

    for indicator_id, type_ids_set in indicator_to_types.items():
        indicator = session.get(Indicator, indicator_id)
        if indicator:
            indicator.type_ids = sorted(type_ids_set)
            session.add(indicator)

    for type_id, indicators in type_to_indicators.items():
        indicator_type = session.get(IndicatorType, type_id)
        if indicator_type:
            indicator_type.indicator_ids = sorted(indicators)
            session.add(indicator_type)


def ingest_epcis(session, df: pd.DataFrame):
    df = df.rename(columns=lambda c: str(c).strip().lower())
    for row in iter_rows(df):
        siren = normalise_code(row.get("siren"))
        if not siren:
            continue
        epci = Epci(
            id=siren,
            department_code=normalise_code(row.get("dept")),
            label=normalise_str(row.get("epci_libellé")) or "",
            legal_form=normalise_str(row.get("nature_juridique")),
            population_communal=to_int(row.get("total_pop_mun")),
            population_total=to_int(row.get("total_pop_tot")),
            area_km2=to_float(row.get("superficie_km2")),
            urbanised_area_km2=to_float(row.get("superficie_urbanisee_km2")),
            density_per_km2=to_float(row.get("densite_par_km2")),
            department_count=to_int(row.get("nb_departements")),
            region_count=to_int(row.get("nb_regions")),
            member_count=to_int(row.get("nb_membres")),
            delegate_count=to_int(row.get("nb_delegues")),
            competence_count=to_int(row.get("nb_competences")),
            fiscal_potential=to_float(row.get("potentiel_fiscal")),
            grant_global=to_float(row.get("dotation_globale")),
            grant_compensation=to_float(row.get("dotation_compensation")),
            grant_intercommunality=to_float(row.get("dotation_intercommunalite")),
            seat_city=normalise_str(row.get("ville_siege")),
            source="Excel/Table EPCI",
        )
        session.merge(epci)


def _melt_indicator_values(
    df: pd.DataFrame,
    value_columns: Iterable[str],
    id_column: str,
    value_column_name: str,
):
    melted = df.melt(id_vars=[id_column], value_vars=value_columns, var_name="indicator_id", value_name=value_column_name)
    melted = melted.dropna(subset=[value_column_name, id_column], how="any")
    return melted


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=lambda c: str(c).strip())


def _normalize_column_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace(" ", "_").replace("-", "_").lower()
    while "__" in text:
        text = text.replace("__", "_")
    return text


def _detect_epci_column(df: pd.DataFrame) -> str:
    candidates = {"id_epci", "code_epci"}
    for col in df.columns:
        normalized = _normalize_column_name(col)
        if normalized in candidates:
            return col
    raise ValueError("Impossible de détecter la colonne ID_EPCI dans l’onglet Table Valeurs.")


def _detect_epci_label_column(df: pd.DataFrame) -> str | None:
    candidates = {"libelle_epci"}
    for col in df.columns:
        normalized = _normalize_column_name(col)
        if normalized in candidates:
            return col
    return None


def _detect_indicator_columns(df: pd.DataFrame) -> list[str]:
    indicator_cols = []
    for col in df.columns:
        name = str(col).strip()
        if name.lower().startswith("i"):
            indicator_cols.append(col)
    return indicator_cols


def _attach_epci_metadata(df: pd.DataFrame, id_column: str, label_column: str | None) -> pd.DataFrame:
    rename_map = {}
    if id_column != "ID_EPCI":
        rename_map[id_column] = "ID_EPCI"
    target_label = None
    if label_column:
        target_label = "LIBELLE_EPCI"
        if label_column != target_label:
            rename_map[label_column] = target_label
    if rename_map:
        df = df.rename(columns=rename_map)
    metadata_columns = ["ID_EPCI"]
    if target_label:
        metadata_columns.append(target_label)
    metadata = df[metadata_columns].drop_duplicates()
    return df, metadata


def ingest_indicator_values(session, df: pd.DataFrame):
    df = _normalise_columns(df)
    id_column = _detect_epci_column(df)
    label_column = _detect_epci_label_column(df)
    indicator_cols = _detect_indicator_columns(df)
    if not indicator_cols:
        logger.warning("Aucune colonne indicateur (prefix i###) détectée dans Table Valeurs.")
        return
    df, metadata = _attach_epci_metadata(df, id_column, label_column)
    long_df = _melt_indicator_values(df, indicator_cols, "ID_EPCI", "value")
    long_df = long_df.merge(metadata, on="ID_EPCI", how="left")
    for row in iter_rows(long_df):
        epci = normalise_code(row.get("ID_EPCI"))
        indicator_id = normalise_indicator_id(row.get("indicator_id"))
        if not epci or not indicator_id:
            continue
        value = to_float(row.get("value"))
        record = IndicatorValue(
            epci_id=epci,
            indicator_id=indicator_id,
            year=0,
            value=value,
            source="Excel/Table Valeurs",
            libelle_epci=normalise_str(row.get("LIBELLE_EPCI")),
        )
        session.merge(record)


def ingest_indicator_scores(session, df: pd.DataFrame):
    df = _normalise_columns(df)
    id_column = _detect_epci_column(df)
    label_column = _detect_epci_label_column(df)
    indicator_cols = _detect_indicator_columns(df)
    if not indicator_cols:
        logger.warning("Aucune colonne indicateur détectée dans Table Scores indicateurs.")
        return
    df, metadata = _attach_epci_metadata(df, id_column, label_column)
    long_df = _melt_indicator_values(df, indicator_cols, "ID_EPCI", "score")
    long_df = long_df.merge(metadata, on="ID_EPCI", how="left")
    for row in iter_rows(long_df):
        epci = normalise_code(row.get("ID_EPCI"))
        indicator_id = normalise_indicator_id(row.get("indicator_id"))
        if not epci or not indicator_id:
            continue
        score = to_float(row.get("score"))
        record = IndicatorScore(
            epci_id=epci,
            indicator_id=indicator_id,
            year=0,
            indicator_score=score,
            libelle_epci=normalise_str(row.get("LIBELLE_EPCI")),
        )
        session.merge(record)


SHEETS_MAPPING = {
    "Table Besoins": ingest_needs,
    "Table Objectifs": ingest_objectives,
    "Table Type indicateurs": ingest_indicator_types,
    "Table Indicateurs-Sources": ingest_indicators,
    "Correspondance Indicateurs-Beso": ingest_indicator_need_links,
    "Correspondance Indicateurs-Obje": ingest_indicator_objective_links,
    "Correspondance Indicateurs-Type": ingest_indicator_type_links,
    "Table EPCI": ingest_epcis,
    "Table Valeurs": ingest_indicator_values,
    "Table Scores indicateurs": ingest_indicator_scores,
}


def ingest_workbook(path: Path):
    logging.info("Lecture du classeur %s", path)
    xl = pd.read_excel(path, sheet_name=list(SHEETS_MAPPING.keys()), dtype=object)
    session = SessionLocal()
    try:
        for sheet_name, func in SHEETS_MAPPING.items():
            df = xl.get(sheet_name)
            if df is None:
                logger.warning("Onglet %s introuvable, ignore.", sheet_name)
                continue
            logger.info("Ingestion de l’onglet %s (%s lignes)", sheet_name, len(df))
            func(session, df)
        session.commit()
        logger.info("Import terminé.")
    except Exception:
        session.rollback()
        logger.exception("Échec de l’import.")
        raise
    finally:
        session.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Ingestion complète Diag360_EvolV2.xlsx")
    parser.add_argument("--file", required=True, help="Chemin vers le fichier XLSX")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(path)

    Base.metadata.create_all(bind=engine)
    ingest_workbook(path)


if __name__ == "__main__":
    main()
