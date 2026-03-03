# Roberto

Roberto is a deterministic CLI pipeline that:
1. Ingests selected X user timelines with official API.
2. Caches tweets/state in SQLite.
3. Safely updates Markdown notes using marker-only auto sections.
4. Uses Gemini (`google-genai`) JSON mode for per-user summaries and cross-user digests.

## Requirements
- Python 3.11+
- X API bearer token (app-only)
- Gemini API key

## Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configure
1. Copy `.env.example` to `.env` and fill values.
2. Fill `config/following.txt` (one username per line).
3. Optional: tune `config/settings.yaml`.

## Run
```bash
python -m roberto_app.cli v1
python -m roberto_app.cli v2
python -m roberto_app.cli v2 --from-db-only
python -m roberto_app.cli sync
python -m roberto_app.cli sync --full
python -m roberto_app.cli build
python -m roberto_app.cli eval
python -m roberto_app.cli eval --json
python -m roberto_app.cli import-json --file ./provider_dump.json
python -m roberto_app.cli status
python -m roberto_app.cli status --json
python -m roberto_app.cli stories status
python -m roberto_app.cli export --format json
python -m roberto_app.cli export --format md
```

## Outputs
- User notes: `notes/users/*.md`
- Digest notes: `notes/digests/*.md`
- Story notes: `notes/stories/*.md`
- SQLite cache: `data/roberto.db`
- Run exports: `data/exports/run_<run_id>.json`

## Dependency Pins
Versions are pinned in `pyproject.toml` for reproducible behavior across runs and CI.
