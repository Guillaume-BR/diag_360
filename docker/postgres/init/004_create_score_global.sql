-----------------------------------------------------------------------
-- Table des scores globaux (lecture front)
-----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS score_global (
    id_epci TEXT NOT NULL REFERENCES epci(id_epci) ON DELETE CASCADE,
    annee INTEGER NOT NULL DEFAULT 0,
    score_global NUMERIC(5,2),
    score_besoin NUMERIC(5,2),
    score_objectif NUMERIC(5,2),
    score_type NUMERIC(5,2),
    rapport JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id_epci, annee)
);

-----------------------------------------------------------------------
-- Vue synthétique combinant scores indicateurs & globaux
-----------------------------------------------------------------------

DROP VIEW IF EXISTS vue_scores_epci CASCADE;

CREATE VIEW vue_scores_epci AS
SELECT
    s.id_epci,
    e.libelle AS epci_libelle,
    e.departement_code,
    e.region_code,
    s.annee,
    sg.score_global,
    sg.score_besoin,
    sg.score_objectif,
    sg.score_type,
    sg.updated_at AS global_updated_at,
    COUNT(DISTINCT s.id_indicateur) AS indicator_count,
    AVG(s.score_indicateur) AS avg_indicator_score
FROM score_indicateur s
JOIN epci e ON e.id_epci = s.id_epci
LEFT JOIN score_global sg ON sg.id_epci = s.id_epci AND sg.annee = s.annee
GROUP BY
    s.id_epci,
    e.libelle,
    e.departement_code,
    e.region_code,
    s.annee,
    sg.score_global,
    sg.score_besoin,
    sg.score_objectif,
    sg.score_type,
    sg.updated_at;
