from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load .env for local development (Windows/macOS/Linux)
load_dotenv()


def get_engine() -> Engine:
    db_url = os.environ.get("DB_URL")
    if not db_url:
        print("ERROR: DB_URL is not set", file=sys.stderr)
        sys.exit(2)
    return create_engine(db_url, pool_pre_ping=True)


def init_db(engine: Engine):
    import os

    ddl_path = os.path.join(os.path.dirname(__file__), "..", "db", "ddl.sql")
    with open(ddl_path, encoding="utf-8") as f:
        ddl = f.read()
    with engine.begin() as cx:
        cx.execute(text(ddl))


def upsert_rows(engine: Engine, rows: Iterable[dict]):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    placeholders = ", ".join(f":{c}" for c in cols)
    columns_sql = ", ".join(cols)
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "datetime_utc")
    sql = f"""
        INSERT INTO generation_mix ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (datetime_utc) DO UPDATE SET
            {updates}
    """
    with engine.begin() as cx:
        cx.execute(text(sql), rows)
    return len(rows)


def get_max_dt(engine: Engine):
    sql = "SELECT max(datetime_utc) FROM generation_mix"
    with engine.begin() as cx:
        res = cx.execute(text(sql)).scalar()
    return res


def main(argv=None):
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
    raise SystemExit(main())
