from __future__ import annotations

import os
import time
from collections.abc import Iterator

import requests

BASE_API = os.getenv("NESO_BASE_API", "https://api.neso.energy/api/3/action")
RESOURCE_ID = os.getenv("NESO_RESOURCE_ID", "f93d1835-75bc-43e5-84ad-12472b180a98")

HTTP_TIMEOUT = 30
USER_AGENT = "historic-gb-generation-mix/0.1 (+https://github.com/)"
MAX_RETRIES = 5


def build_sql(start_iso: str, end_iso: str, columns=None) -> str:
    if not columns:
        select_cols = "*"
    else:
        cols = [f'"{c}"' if not c.startswith('"') else c for c in columns]
        select_cols = ",".join(cols)
    return (
        f'SELECT {select_cols} FROM "{RESOURCE_ID}" '
        f"WHERE \"DATETIME\" >= '{start_iso}' AND \"DATETIME\" < '{end_iso}' "
        f'ORDER BY "DATETIME" ASC'
    )


def fetch_sql(sql: str, limit: int = 5000, offset: int = 0) -> dict:
    url = f"{BASE_API}/datastore_search_sql"
    params = {"sql": f"{sql} LIMIT {limit} OFFSET {offset}"}
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("Unreachable")


def iter_window(
    start_iso: str, end_iso: str, batch_size: int = 5000, columns: list[str] | None = None
) -> Iterator[list[dict]]:
    sql = build_sql(start_iso, end_iso, columns=columns)
    offset = 0
    while True:
        data = fetch_sql(sql, limit=batch_size, offset=offset)
        records = data.get("result", {}).get("records", [])
        if not records:
            break
        yield records
        offset += len(records)
        if len(records) < batch_size:
            break
