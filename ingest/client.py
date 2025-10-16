"""
ingest/client.py

A minimal NESO (National Energy System Operator) CKAN client used by the
ingestion pipeline to fetch Historic GB Generation Mix rows within a given
UTC time window.

Responsibilities
---------------
- Construct a CKAN `datastore_search_sql` statement filtering by the NESO
  dataset's `"DATETIME"` column.
- Page through results deterministically (ascending by DATETIME).
- Perform HTTP GET requests with a bounded timeout, a custom User-Agent,
  and simple exponential backoff retries for transient failures.

Environment Variables
---------------------
NESO_BASE_API
    Base CKAN API endpoint. Defaults to
    "https://api.neso.energy/api/3/action".
NESO_RESOURCE_ID
    Resource ID of the Historic GB Generation Mix datastore table.

Notes
-----
- All time parameters are expected to be ISO-8601 strings in UTC
  (e.g., "2024-01-01T00:00:00Z").
- Pagination is offset-based. The iterator yields lists of raw record dicts
  exactly as returned by CKAN for each page.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

import requests

# Base CKAN endpoint and resource id (can be overridden via environment).
BASE_API = os.getenv("NESO_BASE_API", "https://api.neso.energy/api/3/action")
RESOURCE_ID = os.getenv("NESO_RESOURCE_ID", "f93d1835-75bc-43e5-84ad-12472b180a98")

# HTTP client settings.
HTTP_TIMEOUT = 30  # seconds
USER_AGENT = "historic-gb-generation-mix/0.1 (+https://github.com/)"
MAX_RETRIES = 5  # total attempts including the first try


def build_sql(start_iso: str, end_iso: str, columns=None) -> str:
    """Build a CKAN SQL query for `datastore_search_sql`.

    The query:
      - Selects the requested columns (or "*" if not provided).
      - Filters rows where "DATETIME" is in the half-open interval
        [start_iso, end_iso).
      - Orders ascending by "DATETIME" for stable pagination.

    Args:
        start_iso: Inclusive lower bound as an ISO-8601 UTC string,
            e.g. "2024-01-01T00:00:00Z".
        end_iso: Exclusive upper bound as an ISO-8601 UTC string.
        columns: Optional iterable of column names to select. Items will be
            quoted with double-quotes unless already quoted. If falsy, "*"
            is used.

    Returns:
        A SQL string suitable for the `sql` parameter to
        `/datastore_search_sql`.

    Example:
        >>> build_sql("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z",
        ...           columns=["DATETIME", "GAS"])
        'SELECT "DATETIME","GAS" FROM "<RESOURCE_ID>" WHERE "DATETIME" >= ...
         ORDER BY "DATETIME" ASC'
    """
    if not columns:
        select_cols = "*"
    else:
        # Ensure identifiers are quoted to handle uppercase names (e.g. GAS)
        # and any special characters. If already quoted, keep as-is.
        cols = [f'"{c}"' if not c.startswith('"') else c for c in columns]
        select_cols = ",".join(cols)

    # Date predicate is half-open to avoid duplicating the end bound in
    # adjacent windows.
    return (
        f'SELECT {select_cols} FROM "{RESOURCE_ID}" '
        f"WHERE \"DATETIME\" >= '{start_iso}' AND \"DATETIME\" < '{end_iso}' "
        f'ORDER BY "DATETIME" ASC'
    )


def fetch_sql(sql: str, limit: int = 5000, offset: int = 0) -> dict:
    """Execute a CKAN SQL query with paging and retry.

    This function wraps `GET /datastore_search_sql` and applies:
      - `LIMIT` and `OFFSET` to the provided SQL for page control.
      - A bounded timeout.
      - Simple exponential backoff on `requests`-level exceptions.

    Args:
        sql: The base SQL string returned by :func:`build_sql`.
        limit: Maximum rows to request for this page.
        offset: Offset into the result set.

    Returns:
        The parsed JSON response (`dict`) from CKAN.

    Raises:
        requests.RequestException: If all retry attempts fail due to HTTP
            or connection errors (the last exception is re-raised).
        ValueError: If CKAN returns a non-JSON body that cannot be parsed.
            (Implicit via `Response.json()`.)
    """
    url = f"{BASE_API}/datastore_search_sql"
    # Inject LIMIT/OFFSET at the end of the provided SQL for pagination.
    params = {"sql": f"{sql} LIMIT {limit} OFFSET {offset}"}
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            # On the final attempt, propagate the error to the caller.
            if attempt == MAX_RETRIES - 1:
                raise
            # Exponential backoff: 1, 2, 4, 8... seconds between retries.
            time.sleep(2**attempt)

    # Defensive: the loop above always either returns or raises.
    raise RuntimeError("Unreachable")


def iter_window(
    start_iso: str,
    end_iso: str,
    batch_size: int = 5000,
    columns: list[str] | None = None,
) -> Iterator[list[dict]]:
    """Iterate over all rows in [start_iso, end_iso) in fixed-size pages.

    This generator requests pages in ascending `"DATETIME"` order and yields
    each page (list of dict records) as returned by CKAN. It stops when a
    page is empty or the page size is less than `batch_size`, indicating the
    end of the window.

    Args:
        start_iso: Inclusive lower bound as an ISO-8601 UTC string.
        end_iso: Exclusive upper bound as an ISO-8601 UTC string.
        batch_size: Number of records to request per page (LIMIT).
        columns: Optional list of column names to select. If None, selects "*".

    Yields:
        Lists of raw record dictionaries for each page fetched.

    Notes:
        - If the upstream dataset mutates during iteration, offset pagination
          can theoretically skip or duplicate rows; in practice, ingestion
          is windowed and frequently rerun with overlap to reconcile updates.
    """
    sql = build_sql(start_iso, end_iso, columns=columns)
    offset = 0

    while True:
        data = fetch_sql(sql, limit=batch_size, offset=offset)
        # CKAN wraps records under result -> records.
        records = data.get("result", {}).get("records", [])
        if not records:
            break

        yield records

        # Advance the offset by the number of records we just yielded to
        # request the next page.
        offset += len(records)

        # If the current page is not full, we've reached the end of the window.
        if len(records) < batch_size:
            break
