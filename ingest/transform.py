"""
ingest/transform.py

Column mapping layer from NESO CKAN field names to the warehouse schema.

Responsibilities
----------------
- Provide a deterministic mapping from upstream NESO keys (e.g., "GAS",
  "WIND_perc") to our snake_case columns (e.g., "gas_mw", "wind_pct").
- Offer a small helper, `to_row`, that remaps a validated payload into a
  dict ready for insertion/upsert into the database.

Conventions
-----------
- Upstream NESO fields are UPPERCASE; percentage fields use the suffix
  "_perc" in the source dataset.
- MW keys represent absolute generation/output (megawatts) at the timestamp.
- Percentage keys represent the share (%) of total generation at the timestamp.
- Some aggregate/rollup keys (e.g., LOW_CARBON, ZERO_CARBON) are provided by
  NESO and mapped directly to *_mw columns in the warehouse.

Notes
-----
- `to_row` performs a pure key remap; it does not compute or coerce values.
  Missing upstream fields will result in `None` values in the output dict,
  which the loader will write as NULLs. Although the type annotation uses
  `dict[str, float]`, callers may pass values that are `float | None` after
  validation; this function forwards them unchanged.
"""

from __future__ import annotations

# Mapping from NESO source keys to warehouse column names.
MAP_KEYS = {
    # Absolute outputs (MW)
    "GAS": "gas_mw",
    "COAL": "coal_mw",
    "NUCLEAR": "nuclear_mw",
    "WIND": "wind_mw",
    "WIND_EMB": "wind_emb_mw",
    "HYDRO": "hydro_mw",
    "IMPORTS": "imports_mw",
    "BIOMASS": "biomass_mw",
    "OTHER": "other_mw",
    "SOLAR": "solar_mw",
    "STORAGE": "storage_mw",
    "GENERATION": "generation_mw",
    # Aggregate rollups (as provided upstream)
    "CARBON_INTENSITY": "carbon_intensity_gco2_kwh",
    "LOW_CARBON": "low_carbon_mw",
    "ZERO_CARBON": "zero_carbon_mw",
    "RENEWABLE": "renewable_mw",
    "FOSSIL": "fossil_mw",
    # Mix shares (% of total generation at the timestamp)
    "GAS_perc": "gas_pct",
    "COAL_perc": "coal_pct",
    "NUCLEAR_perc": "nuclear_pct",
    "WIND_perc": "wind_pct",
    "WIND_EMB_perc": "wind_emb_pct",
    "HYDRO_perc": "hydro_pct",
    "IMPORTS_perc": "imports_pct",
    "BIOMASS_perc": "biomass_pct",
    "OTHER_perc": "other_pct",
    "SOLAR_perc": "solar_pct",
    "STORAGE_perc": "storage_pct",
    "GENERATION_perc": "generation_pct",
}


def to_row(valid_payload: dict[str, float]) -> dict[str, float]:
    """Map a validated NESO payload to warehouse column names.

    The function is a simple projection/remap: for each known NESO key,
    it looks up the value in `valid_payload` and stores it under the target
    warehouse column name. Keys not present in `valid_payload` are written as
    `None` (i.e., NULL in the database).

    Args:
        valid_payload: Dictionary keyed by NESO field names (e.g., "GAS",
            "WIND_perc") containing numeric values (floats). Values may be
            `None` if the upstream record omits a field after validation.

    Returns:
        dict[str, float]: A new dict keyed by warehouse column names ready
        for insertion/upsert. Values are forwarded unchanged (float or None).

    Notes:
        - This function does not enforce units; upstream MW and % values are
          already distinguished by their keys and mapped to *_mw or *_pct
          columns accordingly.
        - No side effects; purely functional transformation.
    """
    out = {}
    for src, dst in MAP_KEYS.items():
        # Use .get to allow missing fields to come through as None.
        out[dst] = valid_payload.get(src)
    return out
