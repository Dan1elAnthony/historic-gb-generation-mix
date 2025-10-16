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

import altair as alt
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
today_utc = datetime.now(timezone.utc).date()
default_start_date = today_utc - timedelta(days=30)

col1, col2, col3, col4 = st.columns(4)
with col1:
    start_date = st.date_input(
        "Start date",
        value=default_start_date,
        max_value=today_utc,
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=today_utc,
        min_value=start_date,
        max_value=today_utc,
    )
with col3:
    # Toggle between absolute MW and percentage mix views.
    pct_mode = st.toggle("Show percentages", value=False)
with col4:
    resample_options = {
        "30 minutes": "30min",
        "Hour": "H",
        "Day": "D",
        "Week": "W",
        "Month": "M",
    }
    resample_label = st.selectbox(
        "Resample",
        options=list(resample_options.keys()),
        index=1,
        help="Aggregate readings to the chosen interval before charting and summarising.",
    )
    resample = resample_options[resample_label]

if start_date > end_date:
    st.error("Start date must be on or before the end date.")
    st.stop()

# ---------------------------
# Query window (UTC)
# ---------------------------
start = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
end = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)

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

COLUMN_LABELS = {
    "datetime_utc": "Timestamp (UTC)",
    "gas_mw": "Gas (MW)",
    "coal_mw": "Coal (MW)",
    "nuclear_mw": "Nuclear (MW)",
    "wind_mw": "Wind (MW)",
    "wind_emb_mw": "Embedded Wind (MW)",
    "solar_mw": "Solar (MW)",
    "hydro_mw": "Hydro (MW)",
    "biomass_mw": "Biomass (MW)",
    "imports_mw": "Imports (MW)",
    "storage_mw": "Storage (MW)",
    "other_mw": "Other (MW)",
    "generation_mw": "Total Generation (MW)",
    "gas_pct": "Gas (%)",
    "coal_pct": "Coal (%)",
    "nuclear_pct": "Nuclear (%)",
    "wind_pct": "Wind (%)",
    "wind_emb_pct": "Embedded Wind (%)",
    "solar_pct": "Solar (%)",
    "hydro_pct": "Hydro (%)",
    "biomass_pct": "Biomass (%)",
    "imports_pct": "Imports (%)",
    "storage_pct": "Storage (%)",
    "other_pct": "Other (%)",
    "generation_pct": "Total Generation (%)",
}


def friendly_label(column: str) -> str:
    """Return a human-readable label for a generation mix column name."""

    return COLUMN_LABELS.get(column, column.replace("_", " ").title())


cols_query = cols_mw + [c for c in cols_pct if c not in cols_mw]
sel = cols_pct if pct_mode else cols_mw

# Parameterised SQL to avoid string interpolation of temporal bounds and
# to allow the driver to handle typing/timezones.
sql = f"""
SELECT datetime_utc, {", ".join(cols_query)}
FROM generation_mix
WHERE datetime_utc >= :start AND datetime_utc <= :end
ORDER BY datetime_utc
"""

# Execute read-only query inside a transaction context for consistent reads.
with engine.begin() as cx:
    df_raw = pd.read_sql(text(sql), cx, params={"start": start, "end": end})

has_rows = not df_raw.empty
if not has_rows:
    st.info("No data in the selected window. Run the ingestion job first.")
    st.stop()

# Ensure numeric dtype for all projected series; coerce any non-numerics to NaN.
numeric_cols = [c for c in cols_query if c in df_raw.columns]
df_raw[numeric_cols] = df_raw[numeric_cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))

# ---------------------------
# Series selection for charting
# ---------------------------
series_options = [c for c in sel if c in df_raw.columns]
select_all_series = st.checkbox("Show all series", value=True)
series_default = series_options if series_options else []
series_selection = st.multiselect(
    "Series to display",
    options=series_options,
    default=series_default,
    disabled=select_all_series,
    format_func=friendly_label,
)

if select_all_series or not series_selection:
    selected_series = series_options
else:
    selected_series = series_selection

# ---------------------------
# Resample to chosen cadence
# ---------------------------
# Use mean to summarise higher-frequency readings within each bin, which is a
# reasonable default for continuous power data (MW) and for percentage mix.
df = df_raw.set_index("datetime_utc")[cols_query].resample(resample).mean().reset_index()

# ---------------------------
# KPIs
# ---------------------------
generation_col = "generation_pct" if pct_mode else "generation_mw"
wind_col = "wind_pct" if pct_mode else "wind_mw"

avg_generation = None
if generation_col in df.columns:
    avg_generation = df[generation_col].mean()

renewable_cols = [
    ("wind_pct" if pct_mode else "wind_mw"),
    ("wind_emb_pct" if pct_mode else "wind_emb_mw"),
    ("solar_pct" if pct_mode else "solar_mw"),
    ("hydro_pct" if pct_mode else "hydro_mw"),
    ("biomass_pct" if pct_mode else "biomass_mw"),
]
available_renewables = [c for c in renewable_cols if c in df.columns]
avg_renewables = None
if available_renewables:
    avg_renewables = df[available_renewables].sum(axis=1).mean()

kpis = st.columns(6)
with kpis[0]:
    st.metric("Rows", len(df))
with kpis[1]:
    st.metric("Start", df["datetime_utc"].min().strftime("%Y-%m-%d %H:%M"))
with kpis[2]:
    st.metric("End", df["datetime_utc"].max().strftime("%Y-%m-%d %H:%M"))
with kpis[3]:
    if avg_generation is not None:
        suffix = "%" if pct_mode else "MW"
        st.metric(f"Average Generation ({suffix})", f"{avg_generation:,.1f}")
        st.caption("Mean output across the selected period.")
with kpis[4]:
    if wind_col in df.columns:
        suffix = "%" if pct_mode else "MW"
        st.metric(f"Peak Wind ({suffix})", f"{df[wind_col].max():,.1f}")
        st.caption("Highest wind reading observed in the window.")
with kpis[5]:
    if avg_renewables is not None:
        suffix = "%" if pct_mode else "MW"
        st.metric(f"Average Renewables ({suffix})", f"{avg_renewables:,.1f}")
        st.caption("Mean combined wind, solar, hydro, and biomass output.")

# ---------------------------
# Metric descriptions
# ---------------------------
metric_descriptions = [
    ("Gas", "Electricity produced from natural gas-fired power stations."),
    ("Coal", "Electricity generated by coal-fuelled units."),
    ("Nuclear", "Output from the UK civil nuclear fleet."),
    ("Wind", "Onshore and offshore wind generation that feeds the transmission system."),
    (
        "Embedded Wind",
        "Smaller wind sites connected to distribution networks and embedded generation.",
    ),
    ("Solar", "Utility-scale and embedded solar photovoltaic output."),
    ("Hydro", "Run-of-river and reservoir hydroelectric generation."),
    ("Biomass", "Generation from biomass and energy-from-waste plants."),
    ("Imports", "Interconnector flows importing power from neighbouring markets."),
    ("Storage", "Net output from storage technologies such as pumped storage and batteries."),
    (
        "Other",
        "Other generation sources not captured elsewhere, including oil and miscellaneous fuels.",
    ),
    (
        "Total Generation",
        "Aggregate supply across all categories, representing national demand met.",
    ),
]
st.subheader("Metric definitions")
st.table(pd.DataFrame(metric_descriptions, columns=["Metric", "Description"]))

# ---------------------------
# Chart
# ---------------------------
# Simple multi-series line chart keyed by UTC timestamp. Streamlit will handle
# legend and interactivity. For stacked views, consider Altair in a future pass.
st.subheader("Generation mix over time")
chart_cols = [c for c in selected_series if c in df.columns]
if chart_cols:
    chart_df = df.set_index("datetime_utc")[chart_cols].rename(
        columns={col: friendly_label(col) for col in chart_cols}
    )
    st.line_chart(chart_df)
else:
    st.info("Select at least one series to display a chart.")

# ---------------------------
# Percentage share pie chart
# ---------------------------
# Work from the resampled frame so the pie chart reflects any aggregation
# choice in the controls while still relying on percentage columns.
percentage_cols = [c for c in cols_pct if c in df.columns and c != "generation_pct"]
percentage_means = (
    df[percentage_cols].mean(skipna=True) if percentage_cols else pd.Series(dtype=float)
)
percentage_means = percentage_means.dropna()

total_percentage = percentage_means.sum()
if total_percentage > 0:
    pie_df = percentage_means.rename(
        index=lambda c: friendly_label(c).replace(" (%)", "")
    ).reset_index()
    pie_df.columns = ["Category", "Percentage"]
    pie_df["Label"] = pie_df.apply(
        lambda row: f"{row['Category']} ({row['Percentage']:.1f}%)", axis=1
    )

    category_order = pie_df["Category"].tolist()
    # Provide a fixed, high-contrast palette so adjacent slices remain distinguishable.
    colour_palette = [
        "#4c78a8",
        "#f58518",
        "#54a24b",
        "#e45756",
        "#72b7b2",
        "#f2cf5b",
        "#b279a2",
        "#ff9da6",
        "#9c755f",
        "#bab0ab",
        "#66c2a5",
        "#a05195",
    ]
    colour_scale = alt.Scale(domain=category_order, range=colour_palette)

    pie_base = alt.Chart(pie_df).encode(
        theta=alt.Theta("Percentage:Q", stack=True),
        color=alt.Color("Category:N", legend=alt.Legend(title="Category"), scale=colour_scale),
        tooltip=[
            alt.Tooltip("Category:N"),
            alt.Tooltip("Percentage:Q", format=".2f"),
        ],
    )

    pie_chart = pie_base.mark_arc().properties(title="Generation mix share (%)")
    pie_labels = (
        alt.Chart(pie_df)
        .mark_text(radius=110, size=11, color="black")
        .encode(
            theta=alt.Theta("Percentage:Q", stack=True),
            text="Label:N",
        )
    )

    st.subheader("Generation share for selected period")
    st.caption("Average percentage contribution by source between the chosen start and end dates.")
    st.altair_chart(pie_chart + pie_labels, use_container_width=True)
else:
    st.info("Percentage data unavailable for the selected period.")

# ---------------------------
# Snapshot table
# ---------------------------
st.subheader("Latest snapshot")
st.caption("Most recent records after resampling, limited to the last 10 intervals.")
snapshot_df = df.tail(10).rename(columns={col: friendly_label(col) for col in df.columns})
st.dataframe(snapshot_df)
