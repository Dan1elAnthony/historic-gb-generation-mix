"""Tests for the database helper utilities."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest import load


def test_get_engine_uses_db_url(monkeypatch):
    """`get_engine` should build an engine using the configured DB_URL."""

    fake_engine = object()

    def fake_create_engine(url, pool_pre_ping):
        assert url == "postgresql://example"
        assert pool_pre_ping is True
        return fake_engine

    monkeypatch.setenv("DB_URL", "postgresql://example")
    monkeypatch.setattr(load, "create_engine", fake_create_engine)

    engine = load.get_engine()

    assert engine is fake_engine


def test_get_engine_missing_env(monkeypatch, capsys):
    """Missing DB_URL should result in a user-facing error and exit."""

    monkeypatch.delenv("DB_URL", raising=False)

    with pytest.raises(SystemExit) as exc:
        load.get_engine()

    assert exc.value.code == 2
    assert "ERROR: DB_URL is not set" in capsys.readouterr().err


class DummyContext:
    def __init__(self, log, result=None):
        self.log = log
        self.result = result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, stmt, params=None):
        self.log.append((stmt, params))
        if self.result is not None:
            return self.result


class DummyResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class DummyEngine:
    def __init__(self, result=None):
        self.log = []
        self._result = result

    def begin(self):
        return DummyContext(self.log, self._result)


def test_init_db_executes_sql(monkeypatch):
    """Initialising the DB should emit the DDL to the engine."""

    engine = DummyEngine()

    load.init_db(engine)

    assert engine.log, "DDL should be executed"
    stmt, params = engine.log[0]
    assert params is None
    assert "CREATE TABLE" in stmt.text


def test_upsert_rows_noop():
    """`upsert_rows` should short-circuit on an empty payload."""

    engine = DummyEngine()

    assert load.upsert_rows(engine, []) == 0
    assert engine.log == []


def test_upsert_rows_executes_statement():
    """Rows passed to `upsert_rows` should be written via an INSERT/UPSERT."""

    engine = DummyEngine()
    rows = [
        {"datetime_utc": datetime(2024, 1, 1, tzinfo=timezone.utc), "gas_mw": 1.0, "coal_mw": 2.0}
    ]

    count = load.upsert_rows(engine, rows)

    assert count == 1
    stmt, params = engine.log[0]
    sql = " ".join(stmt.text.split())
    assert "INSERT INTO generation_mix" in sql
    assert "ON CONFLICT (datetime_utc) DO UPDATE SET" in sql
    assert params == rows


def test_get_max_dt_returns_value():
    """`get_max_dt` should return the scalar datetime value from the DB."""

    value = datetime(2024, 1, 2, tzinfo=timezone.utc)
    engine = DummyEngine(result=DummyResult(value))

    result = load.get_max_dt(engine)

    assert result == value


def test_main_init_db(monkeypatch, capsys):
    """CLI `--init-db` flag should trigger database initialisation."""

    calls = []

    monkeypatch.setattr(load, "get_engine", lambda: "engine")

    def fake_init(engine):
        calls.append(engine)

    monkeypatch.setattr(load, "init_db", fake_init)

    code = load.main(["--init-db"])

    assert code == 0
    assert calls == ["engine"]
    assert "DB initialised." in capsys.readouterr().out


def test_main_no_args(monkeypatch, capsys):
    """Without CLI flags the command should report that nothing was done."""

    monkeypatch.setattr(load, "get_engine", lambda: "engine")
    monkeypatch.setattr(load, "init_db", lambda engine: None)

    code = load.main([])

    assert code == 0
    assert "Nothing to do" in capsys.readouterr().out
