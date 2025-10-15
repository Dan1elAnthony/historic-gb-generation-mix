/* 
  db/ddl.sql

  Historic GB Generation Mix schema.

  Conventions:
    - Primary key is UTC timestamp at the dataset’s native granularity.
    - *_mw columns are absolute outputs in megawatts (MW).
    - *_pct columns are share of total generation at that timestamp (percent).
    - carbon_intensity_gco2_kwh is grams CO2 per kWh (gCO2/kWh).
    - Nulls are allowed where the upstream source omits or later backfills values.
    - ingested_at records when this row was written/updated by the pipeline.
*/

CREATE TABLE IF NOT EXISTS generation_mix (
  datetime_utc              TIMESTAMPTZ PRIMARY KEY,          -- Natural key (UTC); aligns with NESO DATETIME
  gas_mw                    NUMERIC,                           -- MW from gas-fired generation
  coal_mw                   NUMERIC,                           -- MW from coal
  nuclear_mw                NUMERIC,                           -- MW from nuclear
  wind_mw                   NUMERIC,                           -- MW from onshore/offshore wind (excl. embedded below)
  wind_emb_mw               NUMERIC,                           -- MW from embedded/small-scale wind
  hydro_mw                  NUMERIC,                           -- MW from hydro
  imports_mw                NUMERIC,                           -- MW net imports (interconnectors)
  biomass_mw                NUMERIC,                           -- MW from biomass
  other_mw                  NUMERIC,                           -- MW from other/uncategorised sources
  solar_mw                  NUMERIC,                           -- MW from solar PV
  storage_mw                NUMERIC,                           -- MW from storage (positive when discharging)
  generation_mw             NUMERIC,                           -- Total generation MW at timestamp (as provided upstream)

  carbon_intensity_gco2_kwh NUMERIC,                           -- gCO2 per kWh at timestamp
  low_carbon_mw             NUMERIC,                           -- Aggregate low-carbon MW (as defined upstream)
  zero_carbon_mw            NUMERIC,                           -- Aggregate zero-carbon MW (as defined upstream)
  renewable_mw              NUMERIC,                           -- Aggregate renewable MW (as defined upstream)
  fossil_mw                 NUMERIC,                           -- Aggregate fossil MW (as defined upstream)

  gas_pct                   NUMERIC,                           -- % of total generation from gas
  coal_pct                  NUMERIC,                           -- % from coal
  nuclear_pct               NUMERIC,                           -- % from nuclear
  wind_pct                  NUMERIC,                           -- % from wind (excl. embedded below)
  wind_emb_pct              NUMERIC,                           -- % from embedded wind
  hydro_pct                 NUMERIC,                           -- % from hydro
  imports_pct               NUMERIC,                           -- % net imports share
  biomass_pct               NUMERIC,                           -- % from biomass
  other_pct                 NUMERIC,                           -- % from other
  solar_pct                 NUMERIC,                           -- % from solar
  storage_pct               NUMERIC,                           -- % from storage
  generation_pct            NUMERIC,                           -- % of total generation (as provided; may be 100 or NA depending on source)

  ingested_at               TIMESTAMPTZ DEFAULT now()          -- Row ingestion/update time (DB server clock)
);
