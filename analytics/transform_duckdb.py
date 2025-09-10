import os
import duckdb

# ---- MinIO (S3) config ----
# Use host:port (NO "http://"), and set ssl=0 so DuckDB builds "http://".
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
S3_KEY = os.getenv("MINIO_KEY", "minioadmin")
S3_SECRET = os.getenv("MINIO_SECRET", "minioadmin")
BUCKET = os.getenv("LP_BUCKET", "llm-pulse")

PROMPTS = f"s3://{BUCKET}/bronze/prompts/date=*/hour=*/*.parquet"
RESPONSES = f"s3://{BUCKET}/bronze/responses/date=*/hour=*/*.parquet"

def main():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-east-1'")
    con.execute("SET s3_endpoint=?", [S3_ENDPOINT])         # e.g. localhost:9000
    con.execute("SET s3_access_key_id=?", [S3_KEY])
    con.execute("SET s3_secret_access_key=?", [S3_SECRET])
    con.execute("SET s3_use_ssl=0")                          # 0 = http
    con.execute("SET s3_url_style='path'")                   # path-style URLs

    # ---- Silver: join prompts & responses ----
    con.execute(f"""
    CREATE OR REPLACE TABLE silver_sessions AS
    SELECT
      p.prompt_id,
      p.user_id,
      p.team,
      p.model,
      CAST(p.ts AS TIMESTAMP)     AS prompt_ts,
      CAST(r.ts AS TIMESTAMP)     AS response_ts,
      r.latency_ms,
      r.tokens_in,
      r.tokens_out,
      r.total_cost_usd,
      r.http_status
    FROM read_parquet('{PROMPTS}') p
    LEFT JOIN read_parquet('{RESPONSES}') r USING (prompt_id);
    """)

    # Write Silver to MinIO partitioned by day
    con.execute(f"""
    COPY (
      SELECT *,
             DATE_TRUNC('day', COALESCE(response_ts, prompt_ts)) AS day
      FROM silver_sessions
    )
    TO 's3://{BUCKET}/silver/sessions'
    (FORMAT PARQUET, PARTITION_BY (day), OVERWRITE_OR_IGNORE 1);
    """)

    # ---- Gold: daily metrics by model ----
    con.execute("""
    CREATE OR REPLACE TABLE gold_model_daily AS
    SELECT
      DATE_TRUNC('day', COALESCE(response_ts, prompt_ts)) AS day,
      model,
      COUNT(*)                                           AS requests,
      QUANTILE(latency_ms, 0.95)                         AS p95_latency_ms,
      SUM(COALESCE(total_cost_usd, 0))                   AS total_cost_usd,
      AVG(CASE WHEN http_status = 200 THEN 0 ELSE 1 END) AS error_rate
    FROM silver_sessions
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """)

    con.execute(f"""
    COPY gold_model_daily
    TO 's3://{BUCKET}/gold/model_daily'
    (FORMAT PARQUET, PARTITION_BY (day), OVERWRITE_OR_IGNORE 1);
    """)

    print("[transform] wrote Silver and Gold to MinIO ✅")

if __name__ == "__main__":
    main()

