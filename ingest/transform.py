from __future__ import annotations

MAP_KEYS = {
    # MW
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
    # rollups
    "CARBON_INTENSITY": "carbon_intensity_gco2_kwh",
    "LOW_CARBON": "low_carbon_mw",
    "ZERO_CARBON": "zero_carbon_mw",
    "RENEWABLE": "renewable_mw",
    "FOSSIL": "fossil_mw",
    # percentages
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
    out = {}
    for src, dst in MAP_KEYS.items():
        out[dst] = valid_payload.get(src)
    return out
