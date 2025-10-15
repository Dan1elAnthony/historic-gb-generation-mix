"""
ingest/run.py

End-to-end ETL orchestrator for the Historic GB Generation Mix dataset.

Responsibilities
----------------
- Compute a UTC time window (either relative to "now" or explicitly provided).
- Apply incremental ingestion with a configurable overlap to reconcile NESO
  backfills/corrections.
- Stream records from NESO CKAN in pages, validate and transform them into the
  warehouse schema, and bulk upsert them into Postgres in buffered batches.
- Expose a CLI for ad-hoc runs and backfills.

Conventions
-----------
- All timestamps are handled in UTC.
- Window is half-open: [start, end).
- Overlap is applied only when incremental mode is active (default).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd  # noqa: F401  # Imported for potential future analysis/debug use.
from dateutil import parser as dtp

from .client import iter_window
from .load import get_engine, get_max_dt, upsert_rows
from .transform import to_row
from .validate import validate_raw

# Number of rows to buffer before writing a batch to the database.
BATCH_WRITE_SIZE = 5000


def iso(dt: datetime) -> str:
    """Return an ISO-8601 string in UTC for a given datetime.

    Args:
        dt: A timezone-aware or naive datetime. Naive datetimes are treated
            according to the system default when converted by `astimezone`.

    Returns:
        str: ISO-8601 string (e.g., "2024-01-01T00:00:00+00:00").
    """
    return dt.astimezone(timezone.utc).isoformat()


def parse_utc(s: str) -> datetime:
    """Parse an ISO-8601 string as a timezone-aware UTC datetime.

    Args:
        s: ISO-8601 string (e.g., "2024-01-01T00:00:00Z").

    Returns:
        datetime: The parsed datetime normalized to UTC.
    """
    return dtp.isoparse(s).astimezone(timezone.utc)


def run(
    days: int = 3,
    overlap_hours: int = 48,
    batch_size: int = 5000,
    start_date: str | None = None,
    end_date: str | None = None,
    no_incremental: bool = False,
) -> dict[str, int]:
    """Execute a single ETL pass for the requested time window.

    The time window can be provided explicitly via `start_date`/`end_date`
    (ISO strings) or computed relative to the current UTC hour using `days`.
    When incremental mode is enabled (default), the effective start is clamped
    forward to `(max(datetime_utc) - overlap_hours)` to safely re-ingest a
    small overlap for late data corrections.

    Records are fetched in pages, validated (type coercion / field filtering),
    transformed to the warehouse schema, and then upserted in buffered batches.

    Args:
        days: If `start_date` is not provided, fetch this many days back from
            the current UTC hour (inclusive lower bound).
        overlap_hours: Number of hours to re-fetch before the table's current
            maximum timestamp to accommodate upstream corrections.
        batch_size: Page size for CKAN fetches (also influences memory usage).
        start_date: Optional ISO-8601 UTC string for the inclusive lower bound.
        end_date: Optional ISO-8601 UTC string for the exclusive upper bound.
        no_incremental: If True, do not clamp `start` using the table's max;
            useful for full backfills.

    Returns:
        dict[str, int]: A small stats dictionary:
            {
              "fetched": <total raw rows pulled from CKAN>,
              "upserted": <total rows written (via upsert) to Postgres>
            }
    """
    # Align "now" to the current UTC hour to avoid partial-hour edges and
    # ensure stable window boundaries on repeated runs.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # Compute initial [start, end) based on provided overrides or relative days.
    # Add a small 30-minute cushion to `end` to capture very recent rows that
    # may land slightly ahead of the aligned hour boundary.
    start = parse_utc(start_date) if start_date else now - timedelta(days=days)
    end = parse_utc(end_date) if end_date else now + timedelta(minutes=30)

    engine = get_engine()

    # Incremental clamp: re-fetch a small overlap so that any NESO corrections
    # within the last `overlap_hours` are reconciled idempotently on upsert.
    if not no_incremental:
        last = get_max_dt(engine)
        if last:
            start = max(start, last - timedelta(hours=overlap_hours))

    total_in = total_ok = 0
    to_insert: list[dict] = []

    # Stream the window in pages. The client enforces ORDER BY DATETIME ASC,
    # so accumulation is deterministic.
    for chunk in iter_window(iso(start), iso(end), batch_size=batch_size):
        total_in += len(chunk)

        # Validate and map each raw record to the warehouse row shape.
        for rec in chunk:
            model = validate_raw(rec)
            row = {"datetime_utc": model.datetime_utc}
            row.update(to_row(model.payload))
            to_insert.append(row)

        # Flush buffered rows in batches to reduce round-trips and control
        # memory usage for large backfills.
        if len(to_insert) >= BATCH_WRITE_SIZE:
            total_ok += upsert_rows(engine, to_insert)
            to_insert.clear()

    # Flush any trailing buffered rows.
    if to_insert:
        total_ok += upsert_rows(engine, to_insert)

    return {"fetched": total_in, "upserted": total_ok}


def main(argv=None):
    """CLI entry point for running the ETL.

    Args:
        argv: Optional list of CLI arguments (useful for testing).

    Returns:
        int: Exit code (0 on success).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3, help="How many days back to fetch")
    parser.add_argument("--overlap-hours", type=int, default=48, help="Overlap for corrections")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--no-incremental", action="store_true")
    args = parser.parse_args(argv)

    stats = run(
        days=args.days,
        overlap_hours=args.overlap_hours,
        start_date=args.start_date,
        end_date=args.end_date,
        no_incremental=args.no_incremental,
    )
    print(f"Done. Stats: {stats}")
    return 0


if __name__ == "__main__":
    # Convert the `main()` return value into a process exit status.
    raise SystemExit(main())
