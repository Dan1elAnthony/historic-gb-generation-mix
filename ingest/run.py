from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd  # noqa: F401

from .client import iter_window
from .load import get_engine, get_max_dt, upsert_rows
from .transform import to_row
from .validate import validate_raw

BATCH_WRITE_SIZE = 5000


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def run(
    days: int = 3, overlap_hours: int = 48, batch_size: int = BATCH_WRITE_SIZE
) -> dict[str, int]:
    engine = get_engine()

    # Determine window
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days)
    last = get_max_dt(engine)
    if last:
        # incremental with overlap
        start = max(start, last - timedelta(hours=overlap_hours))

    total_in = total_ok = 0
    to_insert: list[dict] = []

    for chunk in iter_window(iso(start), iso(now + timedelta(minutes=30)), batch_size=batch_size):
        total_in += len(chunk)
        for rec in chunk:
            model = validate_raw(rec)
            row = {"datetime_utc": model.datetime_utc}
            row.update(to_row(model.payload))
            to_insert.append(row)

        # buffer writes ~5000 rows
        if len(to_insert) >= BATCH_WRITE_SIZE:
            total_ok += upsert_rows(engine, to_insert)
            to_insert.clear()

    if to_insert:
        total_ok += upsert_rows(engine, to_insert)

    return {"fetched": total_in, "upserted": total_ok}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3, help="How many days back to fetch")
    parser.add_argument("--overlap-hours", type=int, default=48, help="Overlap for corrections")
    args = parser.parse_args(argv)

    stats = run(days=args.days, overlap_hours=args.overlap_hours)
    print(f"Done. Stats: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
