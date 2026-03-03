from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.llm.schemas import Connection, ConnectionSupport, DailyDigestAutoBlock, Story, StorySource
from roberto_app.pipeline.briefing import build_daily_briefing, render_briefing
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo, StoryUpsert


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "stories").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)
    (root / "config" / "following.txt").write_text("alice\nbob\n", encoding="utf-8")
    payload = {
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
        "v18": {
            "enabled": True,
            "top_story_deltas": 5,
            "top_connections": 3,
            "top_ideas": 3,
            "default_mode": "fast",
        },
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(payload), encoding="utf-8")


def _seed_story(repo: StorageRepo, *, story_id: str, slug: str, title: str, run_id: str, confidence: str) -> None:
    repo.upsert_story(
        StoryUpsert(
            story_id=story_id,
            slug=slug,
            title=title,
            run_id=run_id,
            confidence=confidence,
            tags=["ai"],
            summary_json={
                "title": title,
                "what_happened": f"{title} updated",
                "why_it_matters": f"{title} matters",
            },
            now_iso="2026-03-03T10:00:00Z",
        )
    )


def _seed_briefing_state(repo: StorageRepo, run_id: str) -> None:
    _seed_story(repo, story_id="story:alpha", slug="alpha", title="Alpha Story", run_id=run_id, confidence="high")
    _seed_story(repo, story_id="story:beta", slug="beta", title="Beta Story", run_id=run_id, confidence="medium")

    repo.add_story_sources(
        story_id="story:alpha",
        run_id=run_id,
        created_at="2026-03-03T11:00:00Z",
        sources=[("alice", "100"), ("alice", "101")],
    )
    repo.add_story_sources(
        story_id="story:beta",
        run_id=run_id,
        created_at="2026-03-03T11:00:00Z",
        sources=[("bob", "200")],
    )

    repo.add_confidence_event(
        story_id="story:alpha",
        run_id="run-prev",
        previous_confidence=None,
        new_confidence="medium",
        reason="start",
        created_at="2026-03-02T10:00:00Z",
    )
    repo.add_confidence_event(
        story_id="story:alpha",
        run_id=run_id,
        previous_confidence="medium",
        new_confidence="high",
        reason="improved evidence",
        created_at="2026-03-03T11:00:00Z",
    )

    repo.upsert_conflicts(
        [
            {
                "conflict_id": "conflict:alpha",
                "run_id": run_id,
                "topic": "alpha disagreement",
                "claim_a": {"username": "alice", "text": "A"},
                "claim_b": {"username": "bob", "text": "B"},
                "source_refs": [{"username": "alice", "tweet_id": "100"}],
                "status": "open",
                "created_at": "2026-03-03T11:00:00Z",
                "updated_at": "2026-03-03T11:00:00Z",
            }
        ]
    )

    repo.insert_idea_cards(
        [
            {
                "card_id": "idea:1",
                "run_id": run_id,
                "username": "alice",
                "idea_type": "essay",
                "title": "Alpha writeup",
                "hypothesis": "Explain alpha",
                "why_now": "Signal is fresh",
                "tags": ["ai"],
                "source_refs": [{"username": "alice", "tweet_id": "100"}],
                "created_at": "2026-03-03T11:00:00Z",
            }
        ]
    )


def test_briefing_ranking_is_deterministic(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    run_id = "run-current"
    _seed_briefing_state(repo, run_id)

    digest = DailyDigestAutoBlock(
        stories=[
            Story(
                title="Alpha Story",
                what_happened="Alpha changed",
                why_it_matters="Alpha matters",
                sources=[StorySource(username="alice", tweet_id="100")],
                tags=["ai"],
                confidence="high",
            )
        ],
        connections=[
            Connection(
                insight="Alpha and beta signals converge",
                supports=[
                    ConnectionSupport(username="alice", tweet_id="100"),
                    ConnectionSupport(username="bob", tweet_id="200"),
                ],
            )
        ],
    )

    a = build_daily_briefing(repo, digest, run_id=run_id, now_iso="2026-03-03T12:00:00+04:00")
    b = build_daily_briefing(repo, digest, run_id=run_id, now_iso="2026-03-03T12:00:00+04:00")
    assert a.summary["story_deltas"] == b.summary["story_deltas"]
    assert a.summary["connections"] == b.summary["connections"]
    assert a.summary["ideas"] == b.summary["ideas"]
    repo.close()


def test_briefing_items_are_citation_backed(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    run_id = "run-current"
    _seed_briefing_state(repo, run_id)
    digest = DailyDigestAutoBlock()
    brief = build_daily_briefing(repo, digest, run_id=run_id, now_iso="2026-03-03T12:00:00+04:00")
    assert brief.refs
    assert all(item["refs"] for item in brief.item_rows)
    assert all(ref.get("username") and ref.get("tweet_id") for item in brief.item_rows for ref in item["refs"])
    repo.close()


def test_briefing_fast_vs_deep_rendering(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    run_id = "run-current"
    _seed_briefing_state(repo, run_id)
    digest = DailyDigestAutoBlock()
    brief = build_daily_briefing(repo, digest, run_id=run_id, now_iso="2026-03-03T12:00:00+04:00")
    fast = render_briefing(brief.summary, mode="fast")
    deep = render_briefing(brief.summary, mode="deep")
    assert "Ranking:" not in fast
    assert "Ranking:" in deep
    assert len(deep) > len(fast)
    repo.close()
