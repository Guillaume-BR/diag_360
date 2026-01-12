from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from app.db import Base


class Need(Base):
    __tablename__ = "besoin"

    id = Column("id_besoin", String, primary_key=True)
    label = Column("libelle", Text, nullable=False)
    category = Column("categorie", Text)
    description = Column(Text)
    indicator_ids = Column("ids_indicateurs", ARRAY(String), default=list)
    meta = Column(JSONB, server_default="{}")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class IndicatorNeedLink(Base):
    __tablename__ = "indicateur_besoin"

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_id = Column("id_indicateur", String, ForeignKey("indicateur.id_indicateur", ondelete="CASCADE"), nullable=False)
    need_id = Column("id_besoin", String, ForeignKey("besoin.id_besoin", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class IndicatorObjectiveLink(Base):
    __tablename__ = "indicateur_objectif"

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_id = Column("id_indicateur", String, ForeignKey("indicateur.id_indicateur", ondelete="CASCADE"), nullable=False)
    objective_id = Column("id_objectif", String, ForeignKey("objectif.id_objectif", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class IndicatorTypeLink(Base):
    __tablename__ = "indicateur_type"

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_id = Column("id_indicateur", String, ForeignKey("indicateur.id_indicateur", ondelete="CASCADE"), nullable=False)
    type_id = Column("id_type", String, ForeignKey("type_indicateur.id_type", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Objective(Base):
    __tablename__ = "objectif"

    id = Column("id_objectif", String, primary_key=True)
    label = Column("libelle", Text, nullable=False)
    description = Column(Text)
    indicator_ids = Column("ids_indicateurs", ARRAY(String), default=list)
    meta = Column(JSONB, server_default="{}")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class IndicatorType(Base):
    __tablename__ = "type_indicateur"

    id = Column("id_type", String, primary_key=True)
    label = Column("libelle", Text, nullable=False)
    description = Column(Text)
    indicator_ids = Column("ids_indicateurs", ARRAY(String), default=list)
    meta = Column(JSONB, server_default="{}")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class Indicator(Base):
    __tablename__ = "indicateur"

    id = Column("id_indicateur", String, primary_key=True)
    label = Column("libelle", Text, nullable=False)
    description = Column(Text)
    primary_source = Column("source_principale", Text)
    primary_url = Column("url_principale", Text)
    api_available = Column("api_disponible", Boolean, default=False)
    secondary_source = Column("source_secondaire", Text)
    secondary_url = Column("url_secondaire", Text)
    value_type = Column("type_valeur", Text)
    unit = Column("unite", Text)
    need_ids = Column("ids_besoins", ARRAY(String), default=list)
    objective_ids = Column("ids_objectifs", ARRAY(String), default=list)
    type_ids = Column("ids_types", ARRAY(String), default=list)
    meta = Column(JSONB, server_default="{}")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
