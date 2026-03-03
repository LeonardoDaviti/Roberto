from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.llm.schemas import DailyDigestAutoBlock, Story, StorySource
from roberto_app.pipeline.report import RunReport
from roberto_app.pipeline.story_memory import persist_stories
from roberto_app.pipeline.uncertainty import to_conflict_nodes
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "stories").mkdir(parents=True, exist_ok=True)
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


def test_conflict_nodes_lifecycle(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    conflict_cards = [
        {
            "conflict_id": "conflict:alpha",
            "title": "Conflict on chips",
            "claim_a": {"username": "alice", "title": "GPU demand rising", "hypothesis": "Demand is rising"},
            "claim_b": {"username": "bob", "title": "GPU demand flat", "hypothesis": "Demand is flat"},
            "tags": ["chips", "supply"],
            "source_refs": [
                {"username": "alice", "tweet_id": "101"},
                {"username": "bob", "tweet_id": "202"},
            ],
            "created_at": "2026-03-03T12:00:00Z",
        }
    ]
    nodes = to_conflict_nodes(
        run_id="2026-03-03T120000000000Z",
        now_iso="2026-03-03T12:00:00Z",
        conflict_cards=conflict_cards,
    )
    repo.upsert_conflicts(nodes)
    open_rows = repo.list_conflicts(status="open", limit=20)
    assert len(open_rows) == 1
    assert open_rows[0]["conflict_id"] == "conflict:alpha"
    assert open_rows[0]["topic"] == "chips, supply"

    changed = repo.set_conflict_status("conflict:alpha", "resolved", "2026-03-03T13:00:00Z")
    assert changed is True
    resolved_rows = repo.list_conflicts(status="resolved", limit=20)
    assert len(resolved_rows) == 1
    assert resolved_rows[0]["conflict_id"] == "conflict:alpha"
    repo.close()


def test_story_confidence_evolution_and_claim_ledger_rendered(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    digest_high = DailyDigestAutoBlock(
        stories=[
            Story(
                title="Alpha Launch",
                what_happened="Alpha shipped a major new model.",
                why_it_matters="It resets baseline capabilities.",
                sources=[StorySource(username="alice", tweet_id="501")],
                tags=["ai"],
                confidence="high",
            )
        ]
    )
    report_1 = RunReport(
        run_id="2026-03-03T120000000000Z",
        mode="v2",
        started_at="2026-03-03T12:00:00Z",
    )
    persist_stories(
        settings,
        repo,
        digest_high,
        run_id=report_1.run_id,
        now_iso="2026-03-03T12:00:00Z",
        report=report_1,
        staging_enabled=False,
        mode="v2",
    )

    story_id = "story:alpha-launch"
    events_1 = repo.list_confidence_events(story_id, limit=20)
    claims_1 = repo.list_story_claims(story_id, limit=20)
    assert len(events_1) == 1
    assert events_1[0]["new_confidence"] == "high"
    assert len(claims_1) >= 2

    digest_low = DailyDigestAutoBlock(
        stories=[
            Story(
                title="Alpha Launch",
                what_happened="Alpha shipped a major new model.",
                why_it_matters="External evaluations are mixed.",
                sources=[StorySource(username="alice", tweet_id="601")],
                tags=["ai"],
                confidence="low",
            )
        ]
    )
    report_2 = RunReport(
        run_id="2026-03-04T120000000000Z",
        mode="v2",
        started_at="2026-03-04T12:00:00Z",
    )
    persist_stories(
        settings,
        repo,
        digest_low,
        run_id=report_2.run_id,
        now_iso="2026-03-04T12:00:00Z",
        report=report_2,
        staging_enabled=False,
        mode="v2",
    )

    events_2 = repo.list_confidence_events(story_id, limit=20)
    assert len(events_2) == 2
    assert any(
        row.get("previous_confidence") == "high" and row.get("new_confidence") == "low"
        for row in events_2
    )
    note_path = settings.resolve("notes", "stories", "alpha-launch.md")
    text = note_path.read_text(encoding="utf-8")
    assert "### Confidence Evolution" in text
    assert "### Claim Ledger" in text
    assert "high -> low" in text
    repo.close()
