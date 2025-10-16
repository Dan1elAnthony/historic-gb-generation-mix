# Historic GB Generation Mix

Ingest the **Historic GB Generation Mix** (NESO CKAN), store it in Postgres, and expose a simple Streamlit UI for exploration. **No additional API service is deployed** — the Streamlit app reads the database directly.

## Architecture

- **ingest/**: Python package that wraps the CKAN API (`datastore_search_sql`), validates payloads, performs type coercion, and **upserts** rows to Postgres.
- **db/**: Contains the canonical schema definition (`ddl.sql`) plus a SQLAlchemy Core table (`models.py`) covering both the primary fuel columns and derived metrics (e.g., carbon intensity, low/zero/renewable/fossil aggregates).
- **app/**: Streamlit UI (`streamlit_app.py`) that runs entirely read-only against the Postgres database.
- **.github/workflows/**: Automation for CI (`ci.yml`) and the scheduled ingestion job (`ingest.yml`).

## Repository structure

| Path | Purpose & key details |
| --- | --- |
| `app/streamlit_app.py` | Streamlit dashboard offering time window controls, MW/% toggles, resampling, KPIs, and recent snapshots sourced directly from Postgres. |
| `ingest/run.py` | CLI entrypoint orchestrating incremental and historical backfills (`python -m ingest.run`). |
| `ingest/load.py` | Database helpers to initialise the schema (`--init-db`), manage SQLAlchemy engines, and execute upserts with overlap windows. |
| `ingest/client.py` | CKAN API client handling SQL queries, pagination, and retryable HTTP behaviour. |
| `ingest/transform.py` | Normalises raw CKAN payloads into the warehouse schema, ensuring columns such as `_mw`, `_pct`, and carbon intensity fields are populated. |
| `ingest/validate.py` | Pydantic validators that enforce required fields and coerce optional metrics before they reach the database. |
| `db/ddl.sql` | Canonical Postgres schema for `generation_mix`, including aggregate columns (`low_carbon_mw`, `renewable_mw`, etc.) and ingestion metadata. |
| `db/models.py` | SQLAlchemy Core table mirroring `ddl.sql` so tests and ad-hoc scripts can reason about the schema programmatically. |
| `tests/` | Pytest suite covering the CKAN client (`test_client.py`), loaders (`test_load.py`), orchestration helpers (`test_run.py`), transforms (`test_transform.py`), and validators (`test_validate.py`). |
| `.env.example` | Sample environment file exposing `DB_URL`, `NESO_RESOURCE_ID`, and `NESO_BASE_API`; copy to `.env` for local development. |
| `requirements.txt` | Pinned runtime (pandas, SQLAlchemy, Streamlit, etc.) and developer dependencies (pytest, black, ruff, isort). |
| `pyproject.toml` | Tooling configuration for Black, Ruff, and isort (line length, target Python version). |
| `.pre-commit-config.yaml` | Pre-commit hooks wiring Black, Ruff (with autofix), and isort; run `pre-commit install` to enable locally. |
| `.github/workflows/ci.yml` | Validates linting and tests on pushes/PRs. |
| `.github/workflows/ingest.yml` | Schedules the ingestion CLI on a cron to keep Postgres up to date. |
| `CONTRIBUTING.md` | Contribution guidelines, development workflow expectations, and code style notes. |
| `LICENSE` | MIT licence covering reuse of the project. |

## Assumptions

- **Dataset stability**: The NESO CKAN resource id and column layout remain consistent. Field mappings in `ingest/transform.py` and the warehouse schema in `db/ddl.sql` assume the current structure; upstream changes will require a code/schema update.
- **48-hour overlap is sufficient**: Incremental runs re-read the last 48 hours to pick up NESO corrections. If the operator observes corrections beyond that window, bump `--overlap-hours` in the scheduler.
- **Postgres is network-accessible**: Both GitHub Actions (scheduler) and Streamlit Cloud connect directly to the same Postgres instance via `DB_URL`. Provisioning should ensure SSL support and public ingress where required.

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

## Test suite overview

- `tests/test_client.py`: Exercises SQL construction, retry behaviour, and CKAN interactions for the ingestion client.
- `tests/test_load.py`: Covers engine configuration, schema initialisation, upsert logic, and CLI flows for the loading layer.
- `tests/test_run.py`: Validates helper datetime utilities and the end-to-end orchestration pipeline including the CLI wrapper.
- `tests/test_transform.py`: Confirms generation records map to the expected column schema with sensible defaults for missing fields.
- `tests/test_validate.py`: Checks raw payload cleaning and mandatory field enforcement.

## Notes

- Timestamps are UTC `TIMESTAMPTZ` (PK). Upserts ensure idempotency.
- For long historical backfill, run in **monthly** windows.
- The UI uses server-side aggregation (GROUP BY day) for speed.
