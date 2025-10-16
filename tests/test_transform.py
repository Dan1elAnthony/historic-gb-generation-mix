"""Tests for the transform layer that remaps validated payloads."""

from __future__ import annotations

from ingest import transform


def test_to_row_maps_known_keys():
    """Known generation keys should be mapped to their database column names."""

    payload = {"GAS": 10.5, "WIND": 5.2, "GAS_perc": 40.0}

    row = transform.to_row(payload)

    assert row["gas_mw"] == 10.5
    assert row["wind_mw"] == 5.2
    assert row["gas_pct"] == 40.0


def test_to_row_defaults_missing_keys_to_none():
    """Missing keys should still be present in the result defaulted to `None`."""

    payload = {"GAS": 1.0}

    row = transform.to_row(payload)

    # Every mapped key should be present even if missing upstream.
    assert set(row) == set(transform.MAP_KEYS.values())
    assert row["coal_mw"] is None
