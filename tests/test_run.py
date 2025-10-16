"""Tests covering the ingest run orchestrator utilities."""

from __future__ import annotations

from datetime import datetime, timezone

from ingest import run


class FixedDateTime(datetime):
    """Helper to patch `datetime.now` deterministically in tests."""

    @classmethod
    def now(cls, tz=None):  # pragma: no cover - trivial
        return cls(2024, 1, 10, 12, tzinfo=timezone.utc)


def test_iso_and_parse_roundtrip():
    """`iso` and `parse_utc` should be inverse helpers for UTC datetimes."""

    dt = datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
    assert run.parse_utc("2024-01-01T06:00:00Z") == dt
    assert run.iso(dt) == "2024-01-01T06:00:00+00:00"


def test_run_happy_path(monkeypatch):
    """The orchestrated run should fetch, validate, transform, and upsert rows."""

    monkeypatch.setattr(run, "datetime", FixedDateTime)
    monkeypatch.setattr(run, "get_engine", lambda: "engine")
    monkeypatch.setattr(
        run, "get_max_dt", lambda engine: datetime(2024, 1, 9, 12, tzinfo=timezone.utc)
    )

    captured_windows = []

    def fake_iter_window(start, end, batch_size):
        captured_windows.append((start, end, batch_size))
        yield [
            {"DATETIME": "2024-01-09T10:00:00Z", "GAS": 1},
            {"DATETIME": "2024-01-09T11:00:00Z", "GAS": 2},
        ]

    monkeypatch.setattr(run, "iter_window", fake_iter_window)

    class DummyRecord:
        def __init__(self, dt, payload):
            self.datetime_utc = dt
            self.payload = payload

    def fake_validate_raw(rec):
        dt = datetime.fromisoformat(rec["DATETIME"].replace("Z", "+00:00"))
        return DummyRecord(dt, {"GAS": float(rec["GAS"])})

    monkeypatch.setattr(run, "validate_raw", fake_validate_raw)
    monkeypatch.setattr(run, "to_row", lambda payload: {"gas_mw": payload["GAS"]})

    written = []

    def fake_upsert_rows(engine, rows):
        written.extend(rows)
        return len(rows)

    monkeypatch.setattr(run, "upsert_rows", fake_upsert_rows)

    stats = run.run(days=3, overlap_hours=48, batch_size=100)

    assert stats == {"fetched": 2, "upserted": 2}
    assert captured_windows[0][2] == 100
    # Batch should be flushed once at the end with two transformed rows.
    assert written == [
        {
            "datetime_utc": datetime(2024, 1, 9, 10, tzinfo=timezone.utc),
            "gas_mw": 1.0,
        },
        {
            "datetime_utc": datetime(2024, 1, 9, 11, tzinfo=timezone.utc),
            "gas_mw": 2.0,
        },
    ]


def test_main_invokes_run(monkeypatch, capsys):
    """The CLI wrapper should invoke `run` and surface summary stats."""

    monkeypatch.setattr(run, "run", lambda **kwargs: {"fetched": 1})

    code = run.main(["--days", "1"])

    assert code == 0
    assert "Done. Stats" in capsys.readouterr().out
