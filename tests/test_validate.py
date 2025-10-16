"""Tests for validation logic of raw CKAN records."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest import validate


def test_validate_raw_filters_and_coerces():
    """Raw records should be cleaned, coerced, and filtered into the model."""

    rec = {
        "DATETIME": "2024-01-01T00:00:00Z",
        "GAS": "123.4",
        "COAL": "",
        "NUCLEAR": None,
        "UNKNOWN": 1,
        "GAS_perc": "45.6",
        "BAD": "n/a",
    }

    model = validate.validate_raw(rec)

    assert model.datetime_utc == datetime(2024, 1, 1, tzinfo=timezone.utc)
    # Known numeric keys should be coerced, blanks to None, and unknown keys dropped.
    assert model.payload == {
        "GAS": 123.4,
        "COAL": None,
        "NUCLEAR": None,
        "GAS_perc": 45.6,
    }


def test_validate_raw_missing_datetime():
    """Records lacking DATETIME should raise a KeyError."""

    with pytest.raises(KeyError):
        validate.validate_raw({"GAS": 1})


def test_record_accepts_datetime_instances():
    """The Pydantic model should accept datetime instances without coercion."""

    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    payload = {"GAS": 1.0}

    model = validate.Record(datetime_utc=dt, payload=payload)

    assert model.datetime_utc is dt
    assert model.payload == payload
