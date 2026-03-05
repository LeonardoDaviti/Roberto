# Roberto Agent Skills Guide

This file is for AI agents operating this repository.

## Purpose

Roberto is a deterministic CLI for:
- ingesting X timelines into SQLite,
- generating citation-backed notes/digests/cards,
- running editorial/reliability tooling,
- running book-reading mode (`v26`) into `Books/`.

## Ground Rules

- Use only official X API access for timeline ingestion.
- Do not overwrite manual note content; update only Roberto auto blocks.
- Prefer `sync` + `build` split when debugging ingest vs generation.
- Use `--json` modes for machine-readable automation.

## Core Commands (Top-Level)

| Command | What it does | Writes |
|---|---|---|
| `v1` | Initial pipeline build from configured following list | DB + user notes + digest + run export |
| `v2` | Incremental pipeline using `last_seen_tweet_id` | DB + changed user notes + digest + run export |
| `status` | Show cached per-user state | read-only (unless internal migrations) |
| `export` | Export latest digest/story set (`json`/`md`) | `data/exports/*` |
| `import-json` | Import local JSON posts into DB cache | DB |
| `sync` | Ingest from X only (no note build) | DB + run state |
| `book` | Read PDF/TXT/MD, generate book cards/notes | `Books/*.md`, `Books/themes/*.md`, `data/books/*`, export JSON |
| `sources` | SourceRef contract ops (`stats/backfill/validate`) | DB and validation artifacts |
| `build` | Build notes/digests/stories from cached DB only | notes + exports |
| `eval` | Run deterministic quality eval | console/JSON output |
| `doctor` | Environment and reliability diagnostics | console/JSON output |
| `gemini-probe` | One-sentence model availability probe | console/JSON output |
| `stories` | Story memory ops (status/show/merge/split/attention controls) | DB + story notes |
| `conflicts` | Conflict ledger ops | DB + conflict notes |
| `entity` | Entity index ops and timelines | DB + entity notes |
| `search` | SQLite FTS5 search across Roberto memory | optional index rebuild |
| `lens` | Saved-query lens list/run | lens outputs (notes/console) |
| `brief` | Daily briefing in `fast`/`deep` mode | briefing notes |
| `greene` | Greene distillation cycle and card listing | Greene notes/tables |
| `chapters` | Chapter emergence proposals | Greene chapter notes |
| `argument` | Claim/support/counter/synthesis output | Greene argument notes |
| `gaps` | Research-gap detection from keeper cards | Greene gap notes |
| `profile` | Doctrine/taxonomy initialization and display | `profile/*` |
| `feedback` | Mark card quality feedback | DB/profile feedback state |
| `draft` | Generate traceable memo/brief/essay outputs | Greene draft notes |
| `actions` | Run preset AI actions | action output notes |
| `editor` | Staging review/promote/snapshots/rollback control plane | `notes/_staging/*`, snapshots, promoted notes |

## High-Value Subcommands

### `sources`
- `sources stats`: coverage of SourceRefs/snapshots.
- `sources backfill`: migrate legacy tweet/json rows into SourceRef shape.
- `sources validate`: verify references resolve to local cached sources.

### `stories`
- `stories status`: list story memory state.
- `stories show <slug>`: full story detail with evidence.
- `stories merge <a> <b> --into <slug>`: deterministic story merge.
- `stories split <slug> --plan <json>`: split via explicit plan.
- `stories pin|unpin|mute|unmute|snooze`: attention controls.

### `editor`
- `editor review --run-id <id>`: show staged diffs first.
- `editor promote --run-id <id>`: publish staged results.
- `editor snapshots --note <path>`: list rollback points.
- `editor rollback --note <path> --snapshot-id <id>`: restore last-good note.

### `book` (v26)
- Supports `.pdf`, `.txt`, `.md`.
- Chunk controls: `--chunk-offset`, `--chunk-limit`, `--chunk-chars`.
- Card density: `--cards-per-chunk`.
- Use `--json` for token accounting and run artifacts.

## Recommended Agent Workflows

### Fresh setup smoke test
1. `python -m roberto_app.cli doctor`
2. `python -m roberto_app.cli gemini-probe --scope configured`
3. `python -m roberto_app.cli status --json`

### Ingest/build split (debuggable)
1. `python -m roberto_app.cli sync`
2. `python -m roberto_app.cli build`
3. `python -m roberto_app.cli export --format json`

### Incremental production run
1. `python -m roberto_app.cli v2 --resume`
2. `python -m roberto_app.cli brief --mode fast`
3. `python -m roberto_app.cli stories status`

### Book run with machine output
1. `python -m roberto_app.cli book /path/to/book.txt --json`
2. Read `data/exports/book_<run_id>.json`
3. Inspect `Books/<slug>.md` and related `Books/themes/*.md`

## Important Output Paths

- DB: `data/roberto.db`
- Run exports: `data/exports/run_<run_id>.json`
- Book exports: `data/exports/book_<run_id>.json`
- User notes: `notes/users/*.md`
- Digest notes: `notes/digests/*.md`
- Story notes: `notes/stories/*.md`
- Greene outputs: `notes/greene/**`
- Book notes: `Books/*.md`
- Cross-book themes: `Books/themes/*.md`

## Operational Notes

- For long jobs, prefer resumable flags (`v1 --resume`, `v2 --resume`).
- If X API returns `402`, account tier/credits are insufficient for timeline fetch.
- Token metrics may be `null`/`0` on cache hits or when provider usage metadata is unavailable.
