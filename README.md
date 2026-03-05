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
python -m roberto_app.cli v1 --resume
python -m roberto_app.cli v2
python -m roberto_app.cli v2 --from-db-only
python -m roberto_app.cli v2 --resume
python -m roberto_app.cli sync
python -m roberto_app.cli sync --full
python -m roberto_app.cli book ./path/to/book.pdf
python -m roberto_app.cli book ./path/to/book.pdf --max-pages 30
python -m roberto_app.cli book ./path/to/book.pdf --title "Mastery"
python -m roberto_app.cli book ./path/to/book.pdf --json
python -m roberto_app.cli sources stats
python -m roberto_app.cli sources backfill
python -m roberto_app.cli sources validate
python -m roberto_app.cli build
python -m roberto_app.cli eval
python -m roberto_app.cli eval --json
python -m roberto_app.cli eval --fixtures-dir roberto_app/pipeline/fixtures/golden
python -m roberto_app.cli eval --baseline ./baseline_fixture.json
python -m roberto_app.cli doctor
python -m roberto_app.cli doctor --online
python -m roberto_app.cli gemini-probe
python -m roberto_app.cli gemini-probe --scope both --prompt "Reply in one sentence."
python -m roberto_app.cli import-json --file ./provider_dump.json
python -m roberto_app.cli status
python -m roberto_app.cli status --json
python -m roberto_app.cli stories status
python -m roberto_app.cli stories show <story-slug>
python -m roberto_app.cli stories show <story-slug> --since-run-id <run_id>
python -m roberto_app.cli stories merge story-a story-b --into merged-story
python -m roberto_app.cli stories split merged-story --plan ./split_plan.json
python -m roberto_app.cli stories pin <story-slug>
python -m roberto_app.cli stories mute <story-slug>
python -m roberto_app.cli stories snooze <story-slug> --until 2026-03-10T09:00:00Z
python -m roberto_app.cli conflicts list --status open
python -m roberto_app.cli conflicts resolve <conflict-id>
python -m roberto_app.cli entity list
python -m roberto_app.cli entity show NVIDIA --days 90
python -m roberto_app.cli entity show NVIDIA --days 90 --since-run-id <run_id>
python -m roberto_app.cli entity pin NVIDIA
python -m roberto_app.cli entity mute NVIDIA
python -m roberto_app.cli entity snooze NVIDIA --until 2026-03-10T09:00:00Z
python -m roberto_app.cli search "nvidia inference stack" --type story --days 30
python -m roberto_app.cli lens list
python -m roberto_app.cli lens run ai
python -m roberto_app.cli brief --mode fast
python -m roberto_app.cli brief --mode deep --date 2026-03-03
python -m roberto_app.cli greene sync
python -m roberto_app.cli greene cards --state keeper --week-key 2026-W10
python -m roberto_app.cli chapters propose --topic strategy
python -m roberto_app.cli argument --topic strategy
python -m roberto_app.cli gaps --topic strategy
python -m roberto_app.cli profile init
python -m roberto_app.cli profile show
python -m roberto_app.cli feedback mark --card <card_id> --type good
python -m roberto_app.cli draft generate --mode memo --topic strategy
python -m roberto_app.cli actions run --name one-issue
python -m roberto_app.cli editor review --run-id <run_id>
python -m roberto_app.cli editor promote --run-id <run_id>
python -m roberto_app.cli editor snapshots --note notes/users/karpathy.md
python -m roberto_app.cli editor rollback --note notes/users/karpathy.md --snapshot-id <id>
python -m roberto_app.cli export --format json
python -m roberto_app.cli export --format md
```

## Outputs
- User notes: `notes/users/*.md`
- Digest notes: `notes/digests/*.md`
- Story notes: `notes/stories/*.md`
- Idea notes: `notes/ideas/*.md`
- Conflict note: `notes/conflicts/latest.md`
- Weekly shuffle notes: `notes/shuffles/*.md`
- Entity timeline notes: `notes/entities/*.md`
- Daily briefing notes: `notes/briefings/*.md`
- Greene cards/chapters/arguments/gaps/drafts: `notes/greene/**`
- Book notes (v26 mode): `Books/*.md`
- Prompt packs: `prompts/v*/`
- Schema packs: `schemas/v*/`
- FTS/lens config: `config/lenses.yaml`
- Taxonomy config: `config/taxonomy.yaml`
- Entity alias overrides: `config/entity_alias_overrides.yaml`
- Doctrine/taste profile: `profile/doctrine.md`, `profile/tags.yaml`
- Staged notes: `notes/_staging/<run_id>/*` (when `v13.enabled=true`)
- SQLite cache: `data/roberto.db`
- Run exports: `data/exports/run_<run_id>.json`
- Book exports + per-query token usage: `data/exports/book_<run_id>.json`

## v26 Book Mode
- Supports `.pdf`, `.txt`, `.md` input files.
- Default output note path is `Books/<book-slug>.md`.
- Each LLM query is persisted in SQLite (`llm_query_usage`) with prompt/output/total token counts when available from the model provider.
- CLI output prints token usage per query and aggregated totals for the run.
- Multi-model fallback is configurable in `config/settings.yaml`:
  - `llm.model` and `llm.model_fallbacks`
  - retry behavior: `llm.retry_max_attempts`, `llm.retry_min_backoff_s`, `llm.retry_max_backoff_s`

## Dependency Pins
Versions are pinned in `pyproject.toml` for reproducible behavior across runs and CI.
