# Quick commands for local workflows

.PHONY: up down logs ps gen ingest transform dash all clean

up:
\tdocker compose up -d

down:
\tdocker compose down

ps:
\tdocker compose ps

logs:
\tdocker compose logs -f

gen:
\t. .env && ./.venv/bin/python services/event_gen/event_gen.py --eps 3 --seconds 30 --brokers $${KAFKA_BROKERS}

ingest:
\t. .env && ./.venv/bin/python services/ingest/ingest.py --brokers $${KAFKA_BROKERS} --endpoint http://$${MINIO_ENDPOINT} --bucket $${LP_BUCKET}

transform:
\t. .env && ./.venv/bin/python analytics/transform_duckdb.py

dash:
\t. .env && ./.venv/bin/python -m streamlit run dashboard/app.py --server.port 8502

all:
\tbash scripts/run_all.sh

clean:
\trm -rf .venv
\trm -rf analytics/__pycache__ services/**/__pycache__ dashboard/__pycache__
