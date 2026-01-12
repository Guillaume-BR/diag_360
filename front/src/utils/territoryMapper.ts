import { TerritoryData } from "@/types/territory";

/**
 * Maps a database territory record to the TerritoryData type.
 */
export const mapDbToTerritoryData = (dbRecord: any): TerritoryData => {
  return {
    id: dbRecord.id,
    code_siren: dbRecord.code_siren,
    name: dbRecord.name,
    type: dbRecord.type,
    population: dbRecord.population,
    department: dbRecord.department,
    region: dbRecord.region,
    score: Number(dbRecord.score),
    score_water: dbRecord.score_water != null ? Number(dbRecord.score_water) : null,
    score_food: dbRecord.score_food != null ? Number(dbRecord.score_food) : null,
    score_housing: dbRecord.score_housing != null ? Number(dbRecord.score_housing) : null,
    score_healthcare: dbRecord.score_healthcare != null ? Number(dbRecord.score_healthcare) : null,
    score_security: dbRecord.score_security != null ? Number(dbRecord.score_security) : null,
    score_education: dbRecord.score_education != null ? Number(dbRecord.score_education) : null,
    score_social_cohesion: dbRecord.score_social_cohesion != null ? Number(dbRecord.score_social_cohesion) : null,
    score_nature: dbRecord.score_nature != null ? Number(dbRecord.score_nature) : null,
    score_local_economy: dbRecord.score_local_economy != null ? Number(dbRecord.score_local_economy) : null,
    score_energy: dbRecord.score_energy != null ? Number(dbRecord.score_energy) : null,
    score_mobility: dbRecord.score_mobility != null ? Number(dbRecord.score_mobility) : null,
    data_year: dbRecord.data_year,
  };
};

/**
 * Creates mock TerritoryData for territories not in the database.
 */
export const createMockTerritoryData = (
  code: string,
  name: string,
  type: string,
  baseScore: number
): TerritoryData => {
  const variance = () => Math.random() * 10 - 5;
  return {
    id: code,
    code_siren: code,
    name,
    type,
    population: null,
    department: null,
    region: null,
    score: baseScore,
    score_water: baseScore + variance(),
    score_food: baseScore + variance(),
    score_housing: baseScore + variance(),
    score_healthcare: baseScore + variance(),
    score_security: baseScore + variance(),
    score_education: baseScore + variance(),
    score_social_cohesion: baseScore + variance(),
    score_nature: baseScore + variance(),
    score_local_economy: baseScore + variance(),
    score_energy: baseScore + variance(),
    score_mobility: baseScore + variance(),
    data_year: 2024,
  };
};
