from sqlalchemy import Column, DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB

from app.db import Base


class Epci(Base):
    __tablename__ = "epci"

    id = Column("id_epci", String, primary_key=True)
    label = Column("libelle", Text, nullable=False)
    department_code = Column("departement_code", Text)
    region_code = Column("region_code", Text)
    legal_form = Column("forme_juridique", Text)
    population_communal = Column("population_commune", Numeric)
    population_total = Column("population_totale", Numeric)
    area_km2 = Column("surface_km2", Numeric)
    urbanised_area_km2 = Column("surface_urbanisee_km2", Numeric)
    density_per_km2 = Column("densite_km2", Numeric)
    department_count = Column("nb_departements", Numeric)
    region_count = Column("nb_regions", Numeric)
    member_count = Column("nb_membres", Numeric)
    delegate_count = Column("nb_delegues", Numeric)
    competence_count = Column("nb_competences", Numeric)
    fiscal_potential = Column("potentiel_fiscal", Numeric)
    grant_global = Column("dotation_globale", Numeric)
    grant_compensation = Column("dotation_compensation", Numeric)
    grant_intercommunality = Column("dotation_intercommunalite", Numeric)
    seat_city = Column("ville_siege", Text)
    source = Column(Text)
    date_import = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    meta = Column(JSONB, server_default="{}")


class IndicatorValue(Base):
    __tablename__ = "valeur_indicateur"

    epci_id = Column("id_epci", String, ForeignKey("epci.id_epci", ondelete="CASCADE"), primary_key=True)
    indicator_id = Column("id_indicateur", String, ForeignKey("indicateur.id_indicateur", ondelete="CASCADE"), primary_key=True)
    year = Column("annee", Numeric, primary_key=True, default=0)
    value = Column("valeur_brute", Numeric)
    unit = Column("unite", Text)
    source = Column(Text)
    date_import = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    meta = Column(JSONB, server_default="{}")


class IndicatorScore(Base):
    __tablename__ = "score_indicateur"

    epci_id = Column("id_epci", String, ForeignKey("epci.id_epci", ondelete="CASCADE"), primary_key=True)
    indicator_id = Column("id_indicateur", String, ForeignKey("indicateur.id_indicateur", ondelete="CASCADE"), primary_key=True)
    year = Column("annee", Numeric, primary_key=True, default=0)
    indicator_score = Column("score_indicateur", Numeric(5, 2))
    need_id = Column("id_besoin", String, ForeignKey("besoin.id_besoin"))
    need_score = Column("score_besoin", Numeric(5, 2))
    objective_id = Column("id_objectif", String, ForeignKey("objectif.id_objectif"))
    objective_score = Column("score_objectif", Numeric(5, 2))
    type_id = Column("id_type", String, ForeignKey("type_indicateur.id_type"))
    type_score = Column("score_type", Numeric(5, 2))
    global_score = Column("score_global", Numeric(5, 2))
    report = Column("rapport", JSONB)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
