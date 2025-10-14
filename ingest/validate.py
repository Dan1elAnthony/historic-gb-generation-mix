from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class Record(BaseModel):
    datetime_utc: datetime
    payload: dict[str, float | None]

    @field_validator("datetime_utc", mode="before")
    @classmethod
    def parse_dt(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


NUMERIC_KEYS = {
    # MWs
    "GAS",
    "COAL",
    "NUCLEAR",
    "WIND",
    "WIND_EMB",
    "HYDRO",
    "IMPORTS",
    "BIOMASS",
    "OTHER",
    "SOLAR",
    "STORAGE",
    "GENERATION",
    # rollups
    "CARBON_INTENSITY",
    "LOW_CARBON",
    "ZERO_CARBON",
    "RENEWABLE",
    "FOSSIL",
    # PERCs
    "GAS_perc",
    "COAL_perc",
    "NUCLEAR_perc",
    "WIND_perc",
    "WIND_EMB_perc",
    "HYDRO_perc",
    "IMPORTS_perc",
    "BIOMASS_perc",
    "OTHER_perc",
    "SOLAR_perc",
    "STORAGE_perc",
    "GENERATION_perc",
}


def validate_raw(rec: dict[str, Any]) -> Record:
    dt = rec["DATETIME"]
    payload = {}
    for k, v in rec.items():
        if k == "DATETIME":
            continue
        if k in NUMERIC_KEYS:
            try:
                payload[k] = None if v in (None, "") else float(v)
            except (TypeError, ValueError):
                payload[k] = None
    return Record(datetime_utc=dt, payload=payload)
