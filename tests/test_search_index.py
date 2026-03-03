from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.cli import _filter_since_run, _load_lenses
from roberto_app.pipeline.search_index import rebuild_search_index, search
from roberto_app.settings import load_settings
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo, StoryUpsert


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "users").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "digests").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)
    (root / "config" / "following.txt").write_text("alice\n", encoding="utf-8")
    settings = {
        "x": {
            "exclude": ["replies", "retweets"],
            "max_results": 100,
            "tweet_fields": ["id", "text", "created_at"],
            "request_timeout_s": 20,
            "retry": {"max_attempts": 5, "backoff_s": [1, 2, 4, 8, 16]},
        },
        "llm": {
            "provider": "gemini",
            "model": "gemini-flash-latest",
            "temperature": 0.2,
            "max_output_tokens": 4096,
            "thinking_level": "low",
            "json_mode": True,
        },
        "notes": {
            "per_user_note_enabled": True,
            "digest_note_enabled": True,
            "note_timezone": "Asia/Tbilisi",
            "overwrite_mode": "markers_only",
        },
        "pipeline": {
            "v1": {"backfill_count": 100},
            "v2": {"max_new_tweets_per_user": 200, "create_digest_each_run": True},
        },
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")


def test_rebuild_search_index_and_query(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    repo.upsert_user("alice", "id_alice", "Alice")
    repo.insert_tweets(
        "alice",
        [
            {
                "id": "7001",
                "text": "NVIDIA inference stack and OpenAI model rollout",
                "created_at": "2026-03-03T10:00:00Z",
            }
        ],
    )

    repo.upsert_story(
        StoryUpsert(
            story_id="story:nvidia-openai",
            slug="nvidia-openai",
            title="NVIDIA and OpenAI stack",
            run_id="2026-03-03T100000000000Z",
            confidence="high",
            tags=["nvidia", "openai"],
            summary_json={
                "title": "NVIDIA and OpenAI stack",
                "what_happened": "OpenAI discussed NVIDIA inference clusters",
                "why_it_matters": "Core infra signal",
            },
            now_iso="2026-03-03T10:00:00Z",
        )
    )
    repo.upsert_story_claims(
        [
            {
                "claim_id": "claim:nvidia-openai:1",
                "story_id": "story:nvidia-openai",
                "run_id": "2026-03-03T100000000000Z",
                "claim_text": "NVIDIA cluster saturation is the bottleneck.",
                "evidence_refs": [{"username": "alice", "tweet_id": "7001"}],
                "confidence": "high",
                "status": "active",
                "created_at": "2026-03-03T10:00:00Z",
                "updated_at": "2026-03-03T10:00:00Z",
            }
        ]
    )
    repo.upsert_conflicts(
        [
            {
                "conflict_id": "conflict:7001",
                "run_id": "2026-03-03T100000000000Z",
                "topic": "nvidia supply",
                "claim_a": {"username": "alice", "text": "Demand is accelerating."},
                "claim_b": {"username": "bob", "text": "Demand is flat."},
                "source_refs": [
                    {"username": "alice", "tweet_id": "7001"},
                    {"username": "bob", "tweet_id": "8001"},
                ],
                "status": "open",
                "created_at": "2026-03-03T10:00:00Z",
                "updated_at": "2026-03-03T10:00:00Z",
            }
        ]
    )

    note_path = settings.resolve("notes", "users", "alice.md")
    note_path.write_text(
        "# @alice - Roberto Notes\n\n"
        "<!-- ROBERTO:AUTO:BEGIN -->\n"
        "- mention [alice:7001](https://x.com/alice/status/7001)\n"
        "<!-- ROBERTO:AUTO:END -->\n",
        encoding="utf-8",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(note_path),
            note_type="user",
            username="alice",
            created_at="2026-03-03T10:00:00Z",
            updated_at="2026-03-03T10:00:00Z",
            last_run_id="2026-03-03T100000000000Z",
        )
    )

    inserted = rebuild_search_index(settings, repo)
    assert inserted > 0

    tweet_hits = search(settings, repo, "nvidia", kind="tweet", limit=10)
    assert any(row["item_id"] == "7001" for row in tweet_hits)

    story_hits = search(settings, repo, "inference clusters", kind="story", limit=10)
    assert any("NVIDIA and OpenAI stack" in str(row["title"]) for row in story_hits)
    claim_hits = search(settings, repo, "cluster saturation bottleneck", kind="story", limit=10)
    assert any(str(row.get("subtype") or "") == "claim" for row in claim_hits)

    note_hits = search(settings, repo, "alice 7001", kind="note", limit=10)
    assert any("alice.md" in str(row["ref_path"]) for row in note_hits)

    conflict_hits = search(settings, repo, "demand is flat", kind="conflict", limit=10)
    assert any(row["item_id"] == "conflict:7001" for row in conflict_hits)

    repo.close()


def test_load_lenses_and_since_filter(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    (tmp_path / "config" / "lenses.yaml").write_text(
        yaml.safe_dump(
            {
                "lenses": [
                    {"name": "ai", "query": "nvidia openai", "type": "story", "days": 30},
                    {"name": "founders", "query": "startup founder"},
                ]
            }
        ),
        encoding="utf-8",
    )
    settings = load_settings(tmp_path)
    lenses = _load_lenses(settings)
    assert len(lenses) == 2
    assert lenses[0]["name"] == "ai"

    rows = [
        {"created_at": "2026-03-03T12:00:00Z", "id": "new"},
        {"created_at": "2026-03-01T12:00:00Z", "id": "old"},
    ]
    filtered = _filter_since_run(rows, "2026-03-02T00:00:00Z")
    assert [r["id"] for r in filtered] == ["new"]
