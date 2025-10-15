"""
ingest/load.py

Database layer for the ingestion pipeline.

Responsibilities
----------------
- Create a SQLAlchemy Engine from the `DB_URL` environment variable.
- Initialise the warehouse schema by executing `db/ddl.sql`.
- Perform idempotent bulk upserts into the `generation_mix` table using
  `INSERT ... ON CONFLICT (datetime_utc) DO UPDATE`.
- Retrieve the most recent `datetime_utc` to support incremental ingestion.
- Provide a small CLI for one-off DB initialisation (`--init-db`).

Environment Variables
---------------------
DB_URL
    PostgreSQL SQLAlchemy connection string. Example (Neon):
    postgresql+psycopg2://user:password@host/dbname?sslmode=require

Notes
-----
- All write operations are executed inside `engine.begin()` transactional
  contexts for atomicity.
- Upserts consider `datetime_utc` the natural key of the dataset.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load `.env` for local development so shells on Windows/macOS/Linux
# do not need to export environment variables manually.
load_dotenv()


def get_engine() -> Engine:
    """Create and return a SQLAlchemy engine from ``DB_URL``.

    Returns:
        Engine: A SQLAlchemy engine with `pool_pre_ping=True` to guard against
        stale connections.

    Side Effects:
        Exits the process with status 2 if ``DB_URL`` is not set.

    """
    db_url = os.environ.get("DB_URL")
    if not db_url:
        # Fail-fast with a clear error to help local setup and CI.
        print("ERROR: DB_URL is not set", file=sys.stderr)
        sys.exit(2)
    return create_engine(db_url, pool_pre_ping=True)


def init_db(engine: Engine):
    """Initialise the database schema by executing ``db/ddl.sql``.

    This is safe to run multiple times: the DDL should create objects only
    if they do not already exist.

    Args:
        engine: A live SQLAlchemy engine connected to the target database.
    """
    import os  # Local import to keep the module's top-level minimal.

    # Resolve the path to the DDL file relative to this module.
    ddl_path = os.path.join(os.path.dirname(__file__), "..", "db", "ddl.sql")
    with open(ddl_path, encoding="utf-8") as f:
        ddl = f.read()

    # Execute the full DDL in a single transactional context.
    with engine.begin() as cx:
        cx.execute(text(ddl))


def upsert_rows(engine: Engine, rows: Iterable[dict]):
    """Bulk upsert rows into the ``generation_mix`` table.

    The upsert uses ``ON CONFLICT (datetime_utc) DO UPDATE`` so re-ingesting
    the same timestamp (e.g., in overlap windows) is idempotent. All non-key
    columns are updated from ``EXCLUDED``.

    Args:
        engine: SQLAlchemy engine.
        rows: Iterable of dicts where keys match table column names. Each dict
            must include ``datetime_utc`` and any other relevant columns.

    Returns:
        int: Number of rows passed to the statement (i.e., attempted upserts).

    Notes:
        - Uses a single ``INSERT ... VALUES (:col, ...)`` with an executemany
          parameter set to send all rows in one round-trip.
        - Column order is derived from the first row's keys; ensure all rows
          share the same keys to avoid per-row shape mismatches.
    """
    if not rows:
        return 0

    # Prepare column lists and placeholders from the first row's keys.
    cols = list(rows[0].keys())
    placeholders = ", ".join(f":{c}" for c in cols)
    columns_sql = ", ".join(cols)

    # Build the SET clause for all non-PK columns (PK: datetime_utc).
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "datetime_utc")

    # Parameterized SQL; values are supplied via the `rows` iterable.
    sql = f"""
        INSERT INTO generation_mix ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (datetime_utc) DO UPDATE SET
            {updates}
    """

    # Execute as a single transaction for atomicity and performance.
    with engine.begin() as cx:
        cx.execute(text(sql), rows)

    return len(rows)


def get_max_dt(engine: Engine):
    """Return the latest ``datetime_utc`` present in ``generation_mix``.

    Args:
        engine: SQLAlchemy engine.

    Returns:
        datetime | None: The maximum timestamp in UTC if the table has data,
        otherwise ``None``.
    """
    sql = "SELECT max(datetime_utc) FROM generation_mix"
    with engine.begin() as cx:
        res = cx.execute(text(sql)).scalar()
    return res


def main(argv=None):
    """CLI entry point for DB utilities.

    Supported actions:
        --init-db  Create tables and indexes by executing ``db/ddl.sql``.

    Args:
        argv: Optional list of CLI arguments for testing.

    Returns:
        int: Process exit code (0 for success).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-db", action="store_true", help="Create tables and indexes")
    args, _ = parser.parse_known_args(argv)

    engine = get_engine()

    if args.init_db:
        init_db(engine)
        print("DB initialised.")
        return 0

    print("Nothing to do. Use --init-db from this module or run ingest.run.")
    return 0


if __name__ == "__main__":
    # Delegate to `main()` and convert its return value to a process exit code.
    raise SystemExit(main())
