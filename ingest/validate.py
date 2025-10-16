"""
ingest/validate.py

Validation and typing layer for raw NESO CKAN records.

Responsibilities
----------------
- Define a `Record` model that captures:
  * `datetime_utc`: the timestamp of the observation (timezone-aware UTC).
  * `payload`: a mapping of NESO numeric fields to `float | None`.
- Provide `validate_raw` to:
  * Extract the timestamp from the `"DATETIME"` field.
  * Coerce known numeric fields to `float`, writing invalid/missing values as None.
  * Discard unexpected keys to keep the pipeline schema-tight.

Conventions
-----------
- Upstream timestamp is the `"DATETIME"` field. It may be ISO-8601 with "Z".
- Only fields listed in `NUMERIC_KEYS` are retained in the payload.
- Empty strings and `None` are normalized to `None` (NULL in the DB).

Notes
-----
- This module intentionally avoids computing derived values; it only performs
  type coercion and filtering. Mapping to warehouse column names happens in
  `ingest/transform.py`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class Record(BaseModel):
    """Validated record passed to the transform/load stages.

    Attributes:
        datetime_utc: Observation timestamp as a timezone-aware UTC datetime.
        payload: Dictionary of NESO numeric fields (by upstream name) to
            coerced floats or None (for blanks/invalids).
    """

    datetime_utc: datetime
    payload: dict[str, float | None]

    @field_validator("datetime_utc", mode="before")
    @classmethod
    def parse_dt(cls, v):
        """Normalize ISO-8601 strings (including 'Z') into aware UTC datetimes.

        CKAN typically returns timestamps like "2024-01-01T00:00:00Z". Pydantic
        will call this validator in "before" mode, allowing us to convert strings
        to timezone-aware datetimes prior to model construction.

        Args:
            v: Incoming value for `datetime_utc`, either a str or datetime.

        Returns:
            datetime: A timezone-aware UTC datetime when given a string; the
            original value if already a datetime.
        """
        if isinstance(v, str):
            # Replace trailing 'Z' with explicit +00:00 to satisfy fromisoformat.
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


# Upstream NESO keys that are expected to be numeric. These include absolute
# outputs (MW), rollup categories, and share-of-mix percentages.
NUMERIC_KEYS = {
    # MWs (absolute output at the timestamp)
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
    # Rollups / additional metrics
    "CARBON_INTENSITY",
    "LOW_CARBON",
    "ZERO_CARBON",
    "RENEWABLE",
    "FOSSIL",
    # Percent shares of total generation at the timestamp
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
    """Validate and coerce a raw CKAN record into a `Record`.

    Behavior:
      - Reads the observation timestamp from `rec["DATETIME"]`.
      - For each key listed in `NUMERIC_KEYS`, attempts to coerce to `float`.
        * `None` or empty string "" → `None`.
        * Non-coercible values (TypeError/ValueError) → `None`.
      - Ignores keys that are not in `NUMERIC_KEYS`.

    Args:
        rec: Raw record dictionary from CKAN (as returned by `requests` JSON).

    Returns:
        Record: A validated record with normalized timestamp and numeric payload.

    Raises:
        KeyError: If the mandatory `"DATETIME"` key is missing.
    """
    dt = rec["DATETIME"]  # Required upstream field; will be validated in Record.
    payload = {}

    # Iterate over all raw fields; only keep numeric keys we recognize.
    for k, v in rec.items():
        if k == "DATETIME":
            continue
        if k in NUMERIC_KEYS:
            try:
                # Normalize blanks and None to None; otherwise cast to float.
                payload[k] = None if v in (None, "") else float(v)
            except (TypeError, ValueError):
                # Any non-numeric garbage is treated as missing.
                payload[k] = None

    return Record(datetime_utc=dt, payload=payload)
