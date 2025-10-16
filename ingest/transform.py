"""
ingest/transform.py

Column mapping layer from NESO CKAN field names to the warehouse schema.

Responsibilities
----------------
- Define `MAP_KEYS`, translating NESO CKAN fields to warehouse columns.
- Provide `to_row` for turning validated payloads into warehouse-ready rows.
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
    """Return a warehouse-keyed copy of the NESO payload.

    Args:
        valid_payload: NESO field/value pairs (floats or None after validation).

    Returns:
        dict[str, float]: New dict keyed by warehouse column names.
    """
    out = {}
    for src, dst in MAP_KEYS.items():
        # Use .get to allow missing fields to come through as None.
        out[dst] = valid_payload.get(src)
    return out
