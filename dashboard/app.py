import os
import pandas as pd
import duckdb
import streamlit as st
import altair as alt

# --- MinIO (S3) settings ---
# Use host:port (no http://). We tell DuckDB to use http via s3_use_ssl=0.
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
S3_KEY = os.getenv("MINIO_KEY", "minioadmin")
S3_SECRET = os.getenv("MINIO_SECRET", "minioadmin")
BUCKET = os.getenv("LP_BUCKET", "llm-pulse")
GOLD_PATH = f"s3://{BUCKET}/gold/model_daily/day=*/**/*.parquet"

def connect_duck():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-east-1'")
    con.execute("SET s3_endpoint=?", [S3_ENDPOINT])          # e.g., localhost:9000
    con.execute("SET s3_access_key_id=?", [S3_KEY])
    con.execute("SET s3_secret_access_key=?", [S3_SECRET])
    con.execute("SET s3_use_ssl=0")                           # 0 = http
    con.execute("SET s3_url_style='path'")
    return con

@st.cache_data(ttl=60)
def load_gold() -> pd.DataFrame:
    con = connect_duck()
    try:
        df = con.sql(f"""
            SELECT
              CAST(day AS DATE) AS day,
              model,
              requests,
              p95_latency_ms,
              total_cost_usd,
              error_rate
            FROM read_parquet('{GOLD_PATH}')
        """).df()
    finally:
        con.close()
    if df.empty:
        return df
    df["day"] = pd.to_datetime(df["day"]).dt.date
    return df

st.set_page_config(page_title="LLM Pulse Dashboard", layout="wide")
st.title("LLM Pulse — Cost & Latency Dashboard")

df = load_gold()
if df.empty:
    st.info("No Gold data found yet. Run generator → ingestor → transform, then refresh.")
    st.stop()

# ---- Filters ----
min_day, max_day = df["day"].min(), df["day"].max()

# If only one day exists, avoid a range slider
if min_day == max_day:
    st.caption(f"Only one day of data available: **{min_day}**")
    day_range = (min_day, max_day)
else:
    day_range = st.slider(
        "Date range",
        min_value=min_day,
        max_value=max_day,
        value=(min_day, max_day),
    )

models = sorted(df["model"].unique().tolist())
selected_models = st.multiselect("Models", options=models, default=models)

mask = (
    (df["day"] >= day_range[0]) &
    (df["day"] <= day_range[1]) &
    (df["model"].isin(selected_models))
)
df_f = df.loc[mask].copy()

# ---- Top KPIs ----
left, mid, right, right2 = st.columns(4)
left.metric("Total Requests", int(df_f["requests"].sum()))
mid.metric("Total Cost (USD)", f"{df_f['total_cost_usd'].sum():.4f}")
right.metric("Global p95 Latency (ms)", int(df_f["p95_latency_ms"].quantile(0.95)) if not df_f.empty else 0)
right2.metric("Avg Error Rate", f"{(df_f['error_rate'].mean() if not df_f.empty else 0):.2%}")

# ---- Charts ----
st.subheader("p95 Latency by Day & Model")
lat_chart = alt.Chart(df_f).mark_line(point=True).encode(
    x="day:T",
    y="p95_latency_ms:Q",
    color="model:N",
    tooltip=["day","model","p95_latency_ms"]
).properties(height=300)
st.altair_chart(lat_chart, use_container_width=True)

st.subheader("Total Cost by Day & Model")
cost_chart = alt.Chart(df_f).mark_bar().encode(
    x="day:T",
    y="sum(total_cost_usd):Q",
    color="model:N",
    tooltip=["day","model","total_cost_usd"]
).properties(height=300)
st.altair_chart(cost_chart, use_container_width=True)

st.subheader("Details")
st.dataframe(df_f.sort_values(["day","model"]).reset_index(drop=True))

