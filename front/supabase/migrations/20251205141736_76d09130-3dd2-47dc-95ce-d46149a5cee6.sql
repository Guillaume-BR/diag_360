-- Add new columns for official Diag360 needs
ALTER TABLE public.territories 
ADD COLUMN IF NOT EXISTS score_security numeric DEFAULT 0,
ADD COLUMN IF NOT EXISTS score_education numeric DEFAULT 0,
ADD COLUMN IF NOT EXISTS score_nature numeric DEFAULT 0,
ADD COLUMN IF NOT EXISTS score_local_economy numeric DEFAULT 0;

-- Copy data from score_ecosystems to score_nature (they represent the same concept)
UPDATE public.territories SET score_nature = score_ecosystems WHERE score_ecosystems IS NOT NULL;

-- Drop obsolete columns that are not part of the official Diag360
ALTER TABLE public.territories 
DROP COLUMN IF EXISTS score_waste,
DROP COLUMN IF EXISTS score_work,
DROP COLUMN IF EXISTS score_culture,
DROP COLUMN IF EXISTS score_ecosystems;