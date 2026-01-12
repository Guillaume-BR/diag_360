-----------------------------------------------------------------------
-- Tables de relation indicateur ↔ besoins / objectifs / types
-----------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'indicateur_besoin'
    ) THEN
        CREATE TABLE indicateur_besoin (
            id SERIAL PRIMARY KEY,
            id_indicateur TEXT NOT NULL REFERENCES indicateur(id_indicateur) ON DELETE CASCADE,
            id_besoin TEXT NOT NULL REFERENCES besoin(id_besoin) ON DELETE CASCADE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (id_indicateur, id_besoin)
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'indicateur_objectif'
    ) THEN
        CREATE TABLE indicateur_objectif (
            id SERIAL PRIMARY KEY,
            id_indicateur TEXT NOT NULL REFERENCES indicateur(id_indicateur) ON DELETE CASCADE,
            id_objectif TEXT NOT NULL REFERENCES objectif(id_objectif) ON DELETE CASCADE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (id_indicateur, id_objectif)
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'indicateur_type'
    ) THEN
        CREATE TABLE indicateur_type (
            id SERIAL PRIMARY KEY,
            id_indicateur TEXT NOT NULL REFERENCES indicateur(id_indicateur) ON DELETE CASCADE,
            id_type TEXT NOT NULL REFERENCES type_indicateur(id_type) ON DELETE CASCADE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (id_indicateur, id_type)
        );
    END IF;
END $$;

-----------------------------------------------------------------------
-- Rétro-remplissage depuis les colonnes ARRAY existantes
-----------------------------------------------------------------------

INSERT INTO indicateur_besoin (id_indicateur, id_besoin)
SELECT i.id_indicateur, need_id
FROM indicateur i
CROSS JOIN LATERAL unnest(i.ids_besoins) AS need_id
ON CONFLICT (id_indicateur, id_besoin) DO NOTHING;

INSERT INTO indicateur_objectif (id_indicateur, id_objectif)
SELECT i.id_indicateur, obj_id
FROM indicateur i
CROSS JOIN LATERAL unnest(i.ids_objectifs) AS obj_id
ON CONFLICT (id_indicateur, id_objectif) DO NOTHING;

INSERT INTO indicateur_type (id_indicateur, id_type)
SELECT i.id_indicateur, type_id
FROM indicateur i
CROSS JOIN LATERAL unnest(i.ids_types) AS type_id
ON CONFLICT (id_indicateur, id_type) DO NOTHING;
