# Historic GB Generation Mix

Ingest the **Historic GB Generation Mix** (NESO CKAN), store it in Postgres, and expose a simple Streamlit UI for exploration. **No additional API service is deployed** — the Streamlit app reads the database directly.

## Architecture

- **ingest/**: Python client that fetches from CKAN (`datastore_search_sql`), validates, and **upserts** to Postgres.
- **db/**: SQLAlchemy model + `ddl.sql` defining the table and indexes.
- **app/**: Streamlit app reading from Postgres (read-only).
- **.github/workflows/**: GitHub Actions workflow to schedule ingestion (cron) and a basic CI lint.

**Source of truth (dataset & API):**
- NESO dataset page shows the resource id and endpoints (`datastore_search`, `datastore_search_sql`).  
  Resource: `f93d1835-75bc-43e5-84ad-12472b180a98`  
  Base: `https://api.neso.energy/api/3/action`

> Note: The dataset is periodically cleansed (historical corrections). The pipeline re-reads an overlap window on each run to merge corrections idempotently.

## Quickstart (local)

1. **Create a virtualenv** (Python 3.10+):
   - **PowerShell (Windows 11)**  
     ```pwsh
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     pip install -r requirements.txt
     ```
   - **bash (macOS/Linux)**  
     ```bash
     python -m venv .venv && source .venv/bin/activate
     pip install -r requirements.txt
     ```

2. **Copy and edit** `.env.example` → `.env` and set `DB_URL`.  
   (Windows: keeping `sslmode=require` is typically the least fussy.)

3. **Create the table** (one-off):
   ```bash
   python -m ingest.load --init-db
   ```

4. **Backfill a small window** (e.g., last 3 days):
   ```bash
   python -m ingest.run --days 3
   ```

### Backfill options

- **Full backfill (one-off, non-incremental):**
  ```bash
  python -m ingest.run --start-date 2009-01-01T00:00:00Z --no-incremental

5. **Run the Streamlit app**:
   ```bash
   streamlit run app/streamlit_app.py
   ```

## Deploy

- **DB**: Use a managed Postgres (e.g., Neon) with SSL in the connection string (e.g., `sslmode=require`).
- **Scheduler**: GitHub Actions cron triggers `python -m ingest.run` on the default branch.
- **App**: Deploy Streamlit Community Cloud; store `DB_URL` in **Secrets** (`st.secrets`).

## Repo scripts

- `ingest.run`: Orchestrates incremental fetch (with overlap) and upserts.
- `ingest.client`: CKAN SQL queries with paging.
- `ingest.transform`: Type coercion & column standardisation.
- `ingest.validate`: Pydantic validators.
- `ingest.load`: DB engine, table DDL, upsert helpers.

## Notes

- Timestamps are UTC `TIMESTAMPTZ` (PK). Upserts ensure idempotency.
- For long historical backfill, run in **monthly** windows.
- The UI uses server-side aggregation (GROUP BY day) for speed.
