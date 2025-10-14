import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load .env locally so Windows shells don't need to export env vars
load_dotenv()

st.set_page_config(page_title="GB Generation Mix", layout="wide")

st.title("GB Generation Mix")
st.caption(
    "Historic GB generation mix (MW and %), stored in Postgres and visualised via Streamlit."
)

db_url = os.environ.get("DB_URL") or st.secrets.get("DB_URL")
if not db_url:
    st.warning(
        "DB_URL is not set. Add it via environment variable or Streamlit Secrets to enable queries."
    )
    st.stop()

engine = create_engine(db_url, pool_pre_ping=True)


# Controls
col1, col2, col3 = st.columns(3)
with col1:
    days = st.selectbox("Window", [7, 30, 90, 180, 365], index=1)
with col2:
    pct_mode = st.toggle("Show percentages", value=False)
with col3:
    resample = st.selectbox("Resample", ["30min", "H", "D"], index=1)

# Query
end = datetime.now(timezone.utc)
start = end - timedelta(days=days)

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

sql = f"""
SELECT datetime_utc, {", ".join(sel)}
FROM generation_mix
WHERE datetime_utc >= :start AND datetime_utc <= :end
ORDER BY datetime_utc
"""

with engine.begin() as cx:
    df = pd.read_sql(text(sql), cx, params={"start": start, "end": end})

if df.empty:
    st.info("No data in the selected window. Run the ingestion job first.")
    st.stop()

num_cols = [c for c in sel if c in df.columns]
df[num_cols] = df[num_cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))

# Resample

df = df.set_index("datetime_utc").resample(resample).mean().reset_index()

# KPIs
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Rows", len(df))
with k2:
    st.metric("Start", df["datetime_utc"].min().strftime("%Y-%m-%d %H:%M"))
with k3:
    st.metric("End", df["datetime_utc"].max().strftime("%Y-%m-%d %H:%M"))
with k4:
    if "wind_mw" in df.columns:
        st.metric("Peak Wind (MW)", f"{df['wind_mw'].max():,.0f}")

# Chart
st.line_chart(df.set_index("datetime_utc")[sel])

# Snapshot table
st.subheader("Latest snapshot")
st.dataframe(df.tail(10))
