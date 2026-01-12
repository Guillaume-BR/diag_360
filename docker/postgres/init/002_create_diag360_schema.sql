-- Nouvelle base Diag360 orientée tables françaises et colonnes listant les relations.
-- On supprime les anciens schémas normalisés pour repartir proprement.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'diag360_ref') THEN
        EXECUTE 'DROP SCHEMA diag360_ref CASCADE';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'diag360_raw') THEN
        EXECUTE 'DROP SCHEMA diag360_raw CASCADE';
    END IF;
END $$;

-----------------------------------------------------------------------
-- Tables de référence (schéma public)
-----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS epci (
    id_epci TEXT PRIMARY KEY,
    libelle TEXT NOT NULL,
    departement_code TEXT,
    region_code TEXT,
    forme_juridique TEXT,
    population_commune INTEGER,
    population_totale INTEGER,
    surface_km2 NUMERIC,
    surface_urbanisee_km2 NUMERIC,
    densite_km2 NUMERIC,
    nb_departements INTEGER,
    nb_regions INTEGER,
    nb_membres INTEGER,
    nb_delegues INTEGER,
    nb_competences INTEGER,
    potentiel_fiscal NUMERIC,
    dotation_globale NUMERIC,
    dotation_compensation NUMERIC,
    dotation_intercommunalite NUMERIC,
    ville_siege TEXT,
    source TEXT,
    date_import TIMESTAMPTZ DEFAULT NOW(),
    meta JSONB DEFAULT '{}'::JSONB
);

CREATE TABLE IF NOT EXISTS indicateur (
    id_indicateur TEXT PRIMARY KEY,
    libelle TEXT NOT NULL,
    description TEXT,
    source_principale TEXT,
    url_principale TEXT,
    api_disponible BOOLEAN DEFAULT FALSE,
    source_secondaire TEXT,
    url_secondaire TEXT,
    type_valeur TEXT,
    unite TEXT,
    ids_besoins TEXT[] DEFAULT '{}',
    ids_objectifs TEXT[] DEFAULT '{}',
    ids_types TEXT[] DEFAULT '{}',
    meta JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS besoin (
    id_besoin TEXT PRIMARY KEY,
    libelle TEXT NOT NULL,
    categorie TEXT,
    description TEXT,
    ids_indicateurs TEXT[] DEFAULT '{}',
    meta JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS objectif (
    id_objectif TEXT PRIMARY KEY,
    libelle TEXT NOT NULL,
    description TEXT,
    ids_indicateurs TEXT[] DEFAULT '{}',
    meta JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS type_indicateur (
    id_type TEXT PRIMARY KEY,
    libelle TEXT NOT NULL,
    description TEXT,
    ids_indicateurs TEXT[] DEFAULT '{}',
    meta JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-----------------------------------------------------------------------
-- Tables métiers
-----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS valeur_indicateur (
    id_epci TEXT NOT NULL REFERENCES epci(id_epci) ON DELETE CASCADE,
    id_indicateur TEXT NOT NULL REFERENCES indicateur(id_indicateur) ON DELETE CASCADE,
    annee INTEGER NOT NULL DEFAULT 0,
    libelle_epci TEXT,
    valeur_brute NUMERIC,
    unite TEXT,
    source TEXT,
    date_import TIMESTAMPTZ DEFAULT NOW(),
    meta JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (id_epci, id_indicateur, annee)
);

CREATE TABLE IF NOT EXISTS score_indicateur (
    id_epci TEXT NOT NULL REFERENCES epci(id_epci) ON DELETE CASCADE,
    id_indicateur TEXT NOT NULL REFERENCES indicateur(id_indicateur) ON DELETE CASCADE,
    annee INTEGER NOT NULL DEFAULT 0,
    libelle_epci TEXT,
    score_indicateur NUMERIC(5,2),
    id_besoin TEXT REFERENCES besoin(id_besoin),
    score_besoin NUMERIC(5,2),
    id_objectif TEXT REFERENCES objectif(id_objectif),
    score_objectif NUMERIC(5,2),
    id_type TEXT REFERENCES type_indicateur(id_type),
    score_type NUMERIC(5,2),
    score_global NUMERIC(5,2),
    rapport JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id_epci, id_indicateur, annee)
);

-----------------------------------------------------------------------
-- Vues utilitaires pour lecture rapide
-----------------------------------------------------------------------

CREATE OR REPLACE VIEW vue_indicateur_details AS
SELECT
    i.*,
    (
        SELECT ARRAY_AGG(DISTINCT b.libelle ORDER BY b.libelle)
        FROM indicateur_besoin ib
        JOIN besoin b ON b.id_besoin = ib.id_besoin
        WHERE ib.id_indicateur = i.id_indicateur
    ) AS besoins_libelles,
    (
        SELECT ARRAY_AGG(DISTINCT o.libelle ORDER BY o.libelle)
        FROM indicateur_objectif io
        JOIN objectif o ON o.id_objectif = io.id_objectif
        WHERE io.id_indicateur = i.id_indicateur
    ) AS objectifs_libelles,
    (
        SELECT ARRAY_AGG(DISTINCT t.libelle ORDER BY t.libelle)
        FROM indicateur_type it
        JOIN type_indicateur t ON t.id_type = it.id_type
        WHERE it.id_indicateur = i.id_indicateur
    ) AS types_libelles
FROM indicateur i;

CREATE OR REPLACE VIEW vue_scores_epci AS
SELECT
    s.*,
    e.libelle AS epci_libelle,
    e.departement_code,
    e.region_code
FROM score_indicateur s
JOIN epci e ON e.id_epci = s.id_epci;
