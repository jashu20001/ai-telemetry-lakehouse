import os
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="LLM Pulse — Cost & Latency", layout="wide")

DEMO_FILE = "demo_data/gold_sample.parquet"

def read_gold():
    con = duckdb.connect()
    try:
        if os.path.exists(DEMO_FILE):
            df = con.execute(f"SELECT * FROM read_parquet('{DEMO_FILE}')").df()
        else:
            # Fall back to MinIO (S3 compatible)
            endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
            bucket   = os.getenv("LP_BUCKET", "llm-pulse")
            key      = os.getenv("MINIO_KEY", "minioadmin")
            secret   = os.getenv("MINIO_SECRET", "minioadmin")
            s3_path  = f"s3://{bucket}/gold/model_daily/**/*.parquet"

            con.execute("""
              SET s3_region='us-east-1';
              SET s3_endpoint=$endpoint;
              SET s3_access_key_id=$key;
              SET s3_secret_access_key=$secret;
              SET s3_url_style='path';
              SET s3_use_ssl=false;
            """, {'endpoint': endpoint, 'key': key, 'secret': secret})

            df = con.execute(
                f"SELECT * FROM read_parquet('{s3_path}', hive_partitioning=1)"
            ).df()
        return df
    finally:
        con.close()

df = read_gold()
df["day"] = pd.to_datetime(df["day"])

st.title("LLM Pulse — Cost & Latency Dashboard")

# Filters
models = sorted(df["model"].unique().tolist())
selected = st.multiselect("Models", models, default=models)

fdf = df[df["model"].isin(selected)]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Requests", int(fdf["requests"].sum()))
col2.metric("Total Cost (USD)", f"{fdf['total_cost_usd'].sum():.4f}")
col3.metric("Global p95 Latency (ms)", int(fdf["p95_latency_ms"].mean()))
col4.metric("Avg Error Rate", f"{fdf['error_rate'].mean()*100:.2f}%")

# p95 latency by day & model
st.subheader("p95 Latency by Day & Model")
lat = (fdf.groupby(["day","model"])["p95_latency_ms"]
          .mean().reset_index().sort_values("day"))
st.line_chart(lat.pivot(index="day", columns="model", values="p95_latency_ms"))

# Cost by day & model
st.subheader("Total Cost by Day & Model")
cost = (fdf.groupby(["day","model"])["total_cost_usd"]
          .sum().reset_index().sort_values("day"))
st.bar_chart(cost.pivot(index="day", columns="model", values="total_cost_usd"))

st.subheader("Details")
st.dataframe(
    fdf[["day","model","requests","p95_latency_ms","total_cost_usd","error_rate"]]
      .sort_values(["day","model"]).reset_index(drop=True)
)
