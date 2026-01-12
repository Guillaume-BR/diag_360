-- Create territories table for resilience diagnostic data
CREATE TABLE public.territories (
  id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  code_siren TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  type TEXT DEFAULT 'EPCI',
  population INTEGER,
  department TEXT,
  region TEXT,
  
  -- Overall resilience score (0-100)
  score NUMERIC(5,2) NOT NULL DEFAULT 0,
  
  -- 11 key functions scores (0-100 each)
  score_food NUMERIC(5,2) DEFAULT 0,           -- Se nourrir
  score_housing NUMERIC(5,2) DEFAULT 0,        -- Se loger
  score_mobility NUMERIC(5,2) DEFAULT 0,       -- Se déplacer
  score_water NUMERIC(5,2) DEFAULT 0,          -- Accéder à l'eau
  score_energy NUMERIC(5,2) DEFAULT 0,         -- Accéder à l'énergie
  score_waste NUMERIC(5,2) DEFAULT 0,          -- Gérer ses déchets
  score_work NUMERIC(5,2) DEFAULT 0,           -- Se former / travailler
  score_healthcare NUMERIC(5,2) DEFAULT 0,     -- Accéder aux soins
  score_culture NUMERIC(5,2) DEFAULT 0,        -- S'épanouir / se cultiver
  score_ecosystems NUMERIC(5,2) DEFAULT 0,     -- Préserver les écosystèmes
  score_social_cohesion NUMERIC(5,2) DEFAULT 0,-- Maintenir la cohésion sociale
  
  -- Metadata
  data_year INTEGER DEFAULT 2024,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Enable Row Level Security
ALTER TABLE public.territories ENABLE ROW LEVEL SECURITY;

-- Public read access (data is meant to be consultable by all)
CREATE POLICY "Territories are publicly readable"
ON public.territories
FOR SELECT
USING (true);

-- Create index for faster lookups
CREATE INDEX idx_territories_code_siren ON public.territories(code_siren);
CREATE INDEX idx_territories_department ON public.territories(department);
CREATE INDEX idx_territories_score ON public.territories(score);

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = public;

CREATE TRIGGER update_territories_updated_at
BEFORE UPDATE ON public.territories
FOR EACH ROW
EXECUTE FUNCTION public.update_updated_at_column();