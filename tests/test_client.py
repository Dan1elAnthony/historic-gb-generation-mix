"""Unit tests for the NESO CKAN client helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from ingest import client


def test_build_sql_defaults(monkeypatch):
    """When no columns are provided the query should select * by default."""

    monkeypatch.setattr(client, "RESOURCE_ID", "resource")

    sql = client.build_sql("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")

    expected = (
        'SELECT * FROM "resource" '
        "WHERE \"DATETIME\" >= '2024-01-01T00:00:00Z' "
        'AND "DATETIME" < \'2024-01-02T00:00:00Z\' ORDER BY "DATETIME" ASC'
    )
    assert sql == expected


def test_build_sql_quotes_columns(monkeypatch):
    """Explicit column lists should be quoted unless already quoted."""

    monkeypatch.setattr(client, "RESOURCE_ID", "RID")

    sql = client.build_sql(
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        columns=["GAS", '"WEIRD"'],
    )

    assert (
        sql == 'SELECT "GAS","WEIRD" FROM "RID" '
        "WHERE \"DATETIME\" >= '2024-01-01T00:00:00Z' "
        'AND "DATETIME" < \'2024-01-02T00:00:00Z\' ORDER BY "DATETIME" ASC'
    )


def test_fetch_sql_success(monkeypatch):
    """`fetch_sql` should call the CKAN endpoint with paging parameters."""

    responses = []

    class DummyResponse:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            responses.append("json")
            return self.payload

    def fake_get(url, params, headers, timeout):
        assert url.endswith("/datastore_search_sql")
        assert params == {"sql": "SELECT 1 LIMIT 10 OFFSET 5"}
        assert headers["User-Agent"] == client.USER_AGENT
        assert timeout == client.HTTP_TIMEOUT
        return DummyResponse({"result": {"records": []}})

    monkeypatch.setattr(client.requests, "get", fake_get)

    result = client.fetch_sql("SELECT 1", limit=10, offset=5)

    assert result == {"result": {"records": []}}
    assert responses == ["json"]


def test_fetch_sql_retries(monkeypatch):
    """Transient `RequestException`s should be retried before succeeding."""

    attempts = 0

    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True}

    def fake_get(url, params, headers, timeout):
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise requests.RequestException("boom")
        return DummyResponse()

    monkeypatch.setattr(client.requests, "get", fake_get)
    monkeypatch.setattr(client.time, "sleep", lambda _: None)

    result = client.fetch_sql("SELECT 1")

    assert attempts == 2
    assert result == {"ok": True}


def test_fetch_sql_raises_after_max_retries(monkeypatch):
    """When all retry attempts fail the final exception should bubble up."""

    attempts = 0

    def fake_get(url, params, headers, timeout):
        nonlocal attempts
        attempts += 1
        raise requests.RequestException("nope")

    monkeypatch.setattr(client.requests, "get", fake_get)
    monkeypatch.setattr(client.time, "sleep", lambda _: None)

    with pytest.raises(requests.RequestException):
        client.fetch_sql("SELECT 1")

    assert attempts == client.MAX_RETRIES


def test_iter_window_pages(monkeypatch):
    """`iter_window` should advance the offset and stop on a short page."""

    calls: list[SimpleNamespace] = []

    def fake_fetch(sql, limit, offset):
        calls.append(SimpleNamespace(sql=sql, limit=limit, offset=offset))
        records_by_offset = {
            0: [{"DATETIME": "a"}, {"DATETIME": "b"}],
            2: [{"DATETIME": "c"}],
        }
        return {"result": {"records": records_by_offset.get(offset, [])}}

    monkeypatch.setattr(client, "fetch_sql", fake_fetch)

    pages = list(client.iter_window("start", "end", batch_size=2))

    assert pages == [[{"DATETIME": "a"}, {"DATETIME": "b"}], [{"DATETIME": "c"}]]
    # Should stop after the short page (size < batch_size) without extra calls.
    assert [c.offset for c in calls] == [0, 2]
