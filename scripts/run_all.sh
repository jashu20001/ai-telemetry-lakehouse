#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Load .env if present
if [ -f .env ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
fi

VENV_PY="./.venv/bin/python"
STREAMLIT="./.venv/bin/streamlit"

if [ ! -x "$VENV_PY" ]; then
  echo "❌ .venv not found. Create it first:  python3 -m venv .venv && source .venv/bin/activate && pip install -U pip -r requirements.txt"
  exit 1
fi

# Defaults if env vars missing
: "${KAFKA_BROKERS:=localhost:9092}"
: "${MINIO_ENDPOINT:=localhost:9000}"
: "${LP_BUCKET:=llm-pulse}"
: "${EPS:=3}"
: "${SECONDS_RUN:=30}"
: "${PORT:=8502}"

echo "▶️  Generating events (${EPS} eps for ${SECONDS_RUN}s) -> Kafka @ ${KAFKA_BROKERS}"
$VENV_PY services/event_gen/event_gen.py --eps "${EPS}" --seconds "${SECONDS_RUN}" --brokers "${KAFKA_BROKERS}"

echo "▶️  Ingesting to Bronze -> s3://${LP_BUCKET} (endpoint http://${MINIO_ENDPOINT})"
$VENV_PY services/ingest/ingest.py --brokers "${KAFKA_BROKERS}" --endpoint "http://${MINIO_ENDPOINT}" --bucket "${LP_BUCKET}"

echo "▶️  Transforming Bronze -> Silver/Gold with DuckDB"
$VENV_PY analytics/transform_duckdb.py

echo "▶️  Starting Streamlit dashboard on port ${PORT}"
exec $VENV_PY -m streamlit run dashboard/app.py --server.port "${PORT}"
