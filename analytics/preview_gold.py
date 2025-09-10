import duckdb

ENDPOINT = "localhost:9000"   # MinIO
KEY = "minioadmin"
SECRET = "minioadmin"
BUCKET = "llm-pulse"

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region='us-east-1'")
con.execute("SET s3_endpoint=?", [ENDPOINT])   # host:port (no http://)
con.execute("SET s3_access_key_id=?", [KEY])
con.execute("SET s3_secret_access_key=?", [SECRET])
con.execute("SET s3_use_ssl=0")
con.execute("SET s3_url_style='path'")

df = con.sql(f"""
  SELECT *
  FROM read_parquet('s3://{BUCKET}/gold/model_daily/day=*/**/*.parquet')
  ORDER BY day DESC, model
  LIMIT 20
""").df()

print(df.to_string(index=False))

