# Contributing

Thanks for your interest in improving **AI Telemetry Lakehouse**! 🧪

## Dev setup
1) Clone and enter:
   ```bash
   git clone https://github.com/jashu20001/ai-telemetry-lakehouse.git
   cd ai-telemetry-lakehouse
Create a venv and install deps:
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip -r requirements.txt
Copy env template and adjust if needed:
cp .env.example .env
Useful commands
Bring up infra (Redpanda + MinIO):
make up
Generate events, ingest, transform, launch dashboard:
make gen
make ingest
make transform
make dash
One-shot end-to-end demo:
make all
Branch & PR
Create feature branches from main.
Keep commits focused and descriptive.
Open a PR with a clear summary and screenshots if UI changes.
Code style
Python: ruff / black style (optional).
Keep scripts small and documented.
Reporting issues
Open an issue with:
What you did
What you expected
What happened (logs/screenshots)
Happy hacking! ✨
