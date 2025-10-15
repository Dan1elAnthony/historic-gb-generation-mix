"""
db/models.py

SQLAlchemy table definitions mirroring the warehouse schema.

Responsibilities
----------------
- Provide a programmatic (SQLAlchemy Core) representation of the
  `generation_mix` table so tests, migrations, or ad-hoc scripts can
  reference the schema without raw SQL.
- Keep column names/types aligned with `db/ddl.sql`.

Conventions
-----------
- `datetime_utc` is the primary key and is timezone-aware (UTC).
- *_mw columns store absolute generation/output in megawatts (MW).
- *_pct columns store share of total generation at the timestamp (percent).
- `ingested_at` defaults to the current timestamp on the database server.

Notes
-----
- All numeric metrics use `Numeric` to avoid precision loss; downstream code
  may cast to `float` when materialising results if appropriate.
"""

from sqlalchemy import TIMESTAMP, Column, MetaData, Numeric, Table, text

metadata = MetaData()

# The Historic GB Generation Mix at half-hourly/hourly resolution keyed by UTC timestamp.
generation_mix = Table(
    "generation_mix",
    metadata,
    # Natural key for the dataset; aligns with NESO's DATETIME field.
    Column("datetime_utc", TIMESTAMP(timezone=True), primary_key=True),
    # Absolute outputs (MW)
    Column("gas_mw", Numeric),
    Column("coal_mw", Numeric),
    Column("nuclear_mw", Numeric),
    Column("wind_mw", Numeric),
    Column("wind_emb_mw", Numeric),
    Column("hydro_mw", Numeric),
    Column("imports_mw", Numeric),
    Column("biomass_mw", Numeric),
    Column("other_mw", Numeric),
    Column("solar_mw", Numeric),
    Column("storage_mw", Numeric),
    Column("generation_mw", Numeric),
    # Additional metrics / rollups
    Column("carbon_intensity_gco2_kwh", Numeric),
    Column("low_carbon_mw", Numeric),
    Column("zero_carbon_mw", Numeric),
    Column("renewable_mw", Numeric),
    Column("fossil_mw", Numeric),
    # Mix shares (% of total generation at the timestamp)
    Column("gas_pct", Numeric),
    Column("coal_pct", Numeric),
    Column("nuclear_pct", Numeric),
    Column("wind_pct", Numeric),
    Column("wind_emb_pct", Numeric),
    Column("hydro_pct", Numeric),
    Column("imports_pct", Numeric),
    Column("biomass_pct", Numeric),
    Column("other_pct", Numeric),
    Column("solar_pct", Numeric),
    Column("storage_pct", Numeric),
    Column("generation_pct", Numeric),
    # Ingestion metadata
    Column("ingested_at", TIMESTAMP(timezone=True), server_default=text("now()")),
)
