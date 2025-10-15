"""
app/streamlit_app.py

Read-only Streamlit dashboard for the Historic GB Generation Mix.

Responsibilities
----------------
- Read data from the `generation_mix` table in Postgres.
- Allow users to choose a time window, metric mode (MW vs %), and resampling
  frequency, then visualise the selected series.
- Display simple KPIs and a recent snapshot table for quick inspection.

Conventions
-----------
- All timestamps are handled in UTC end-to-end.
- When "Show percentages" is enabled, *_pct columns are selected; otherwise
  the *_mw (absolute MW) columns are selected.
- Resampling is applied with a mean aggregation to smooth higher-frequency
  data to the chosen interval (e.g., hourly/daily averages).

Notes
-----
- DB_URL can be supplied via environment variable for local dev or via
  Streamlit Secrets in hosted deployments.
- This app is intentionally read-only: it never mutates the database.
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load .env locally so Windows shells don't need to export env vars.
# Safe to call in hosted environments as well (no-op if not present).
load_dotenv()

st.set_page_config(page_title="GB Generation Mix", layout="wide")

st.title("GB Generation Mix")
st.caption(
    "Historic GB generation mix (MW and %), stored in Postgres and visualised via Streamlit."
)

# Prefer environment variable for local dev; fall back to Streamlit Secrets
# for hosted environments (e.g., Streamlit Cloud).
db_url = os.environ.get("DB_URL") or st.secrets.get("DB_URL")
if not db_url:
    st.warning(
        "DB_URL is not set. Add it via environment variable or Streamlit Secrets to enable queries."
    )
    st.stop()

# Pre-ping helps drop stale connections in ephemeral environments.
engine = create_engine(db_url, pool_pre_ping=True)

# ---------------------------
# Controls (left-to-right UI)
# ---------------------------
col1, col2, col3 = st.columns(3)
with col1:
    # Default to 30 days for a balanced view.
    days = st.selectbox("Window", [7, 30, 90, 180, 365], index=1)
with col2:
    # Toggle between absolute MW and percentage mix views.
    pct_mode = st.toggle("Show percentages", value=False)
with col3:
    # Pandas resample rules: "30min" (native cadence), "H" (hour), "D" (day).
    resample = st.selectbox("Resample", ["30min", "H", "D"], index=1)

# ---------------------------
# Query window (UTC)
# ---------------------------
end = datetime.now(timezone.utc)
start = end - timedelta(days=days)

# Columns to project depending on the selected mode.
cols_mw = [
    "gas_mw",
    "coal_mw",
    "nuclear_mw",
    "wind_mw",
    "wind_emb_mw",
    "solar_mw",
    "hydro_mw",
    "biomass_mw",
    "imports_mw",
    "storage_mw",
    "other_mw",
    "generation_mw",
]
cols_pct = [
    "gas_pct",
    "coal_pct",
    "nuclear_pct",
    "wind_pct",
    "wind_emb_pct",
    "solar_pct",
    "hydro_pct",
    "biomass_pct",
    "imports_pct",
    "storage_pct",
    "other_pct",
    "generation_pct",
]
sel = cols_pct if pct_mode else cols_mw

# Parameterised SQL to avoid string interpolation of temporal bounds and
# to allow the driver to handle typing/timezones.
sql = f"""
SELECT datetime_utc, {", ".join(sel)}
FROM generation_mix
WHERE datetime_utc >= :start AND datetime_utc <= :end
ORDER BY datetime_utc
"""

# Execute read-only query inside a transaction context for consistent reads.
with engine.begin() as cx:
    df = pd.read_sql(text(sql), cx, params={"start": start, "end": end})

if df.empty:
    st.info("No data in the selected window. Run the ingestion job first.")
    st.stop()

# Ensure numeric dtype for all projected series; coerce any non-numerics to NaN.
num_cols = [c for c in sel if c in df.columns]
df[num_cols] = df[num_cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))

# ---------------------------
# Resample to chosen cadence
# ---------------------------
# Use mean to summarise higher-frequency readings within each bin, which is a
# reasonable default for continuous power data (MW) and for percentage mix.
df = df.set_index("datetime_utc").resample(resample).mean().reset_index()

# ---------------------------
# KPIs
# ---------------------------
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Rows", len(df))
with k2:
    st.metric("Start", df["datetime_utc"].min().strftime("%Y-%m-%d %H:%M"))
with k3:
    st.metric("End", df["datetime_utc"].max().strftime("%Y-%m-%d %H:%M"))
with k4:
    # Only show this KPI when MW mode is active and wind is present.
    if "wind_mw" in df.columns:
        st.metric("Peak Wind (MW)", f"{df['wind_mw'].max():,.0f}")

# ---------------------------
# Chart
# ---------------------------
# Simple multi-series line chart keyed by UTC timestamp. Streamlit will handle
# legend and interactivity. For stacked views, consider Altair in a future pass.
st.line_chart(df.set_index("datetime_utc")[sel])

# ---------------------------
# Snapshot table
# ---------------------------
st.subheader("Latest snapshot")
st.dataframe(df.tail(10))
