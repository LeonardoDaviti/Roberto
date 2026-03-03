from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.pipeline.greene import (
    build_argumentation,
    detect_gaps,
    generate_draft,
    mark_card_feedback,
    propose_chapters,
    run_ai_action,
    run_greene_cycle,
)
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo, StoryUpsert


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "stories").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "briefings").mkdir(parents=True, exist_ok=True)
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
        "v19": {"enabled": True, "keeper_cap_per_story_week": 2, "auto_reject_overflow": True},
        "v21": {"enabled": True, "chapter_count": 2, "cards_per_chapter": 3},
        "v22": {"enabled": True, "doctrine_path": "profile/doctrine.md", "tags_path": "profile/tags.yaml"},
        "v23": {"enabled": True, "default_mode": "memo"},
        "v24": {"enabled": True, "one_issue_enabled": True},
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(payload), encoding="utf-8")


def _seed_story(repo: StorageRepo, *, story_id: str, slug: str, title: str, confidence: str) -> None:
    repo.upsert_story(
        StoryUpsert(
            story_id=story_id,
            slug=slug,
            title=title,
            run_id="run-seed",
            confidence=confidence,
            tags=["strategy"],
            summary_json={
                "title": title,
                "what_happened": f"{title} moved to new deployment posture.",
                "why_it_matters": f"{title} changes execution leverage.",
                "tags": ["strategy"],
            },
            now_iso="2026-03-03T10:00:00+04:00",
        )
    )
    repo.add_story_sources(
        story_id=story_id,
        run_id="run-seed",
        created_at="2026-03-03T10:00:00+04:00",
        sources=[("alice", "1001"), ("bob", "2001")],
    )
    repo.add_confidence_event(
        story_id=story_id,
        run_id="run-seed",
        previous_confidence=None,
        new_confidence=confidence,
        reason="seed",
        created_at="2026-03-03T10:00:00+04:00",
    )
    repo.upsert_story_claims(
        [
            {
                "claim_id": f"claim:{story_id}:1",
                "story_id": story_id,
                "run_id": "run-seed",
                "claim_text": f"{title} has primary evidence support.",
                "evidence_refs": [{"username": "alice", "tweet_id": "1001"}],
                "confidence": confidence,
                "status": "active",
                "created_at": "2026-03-03T10:00:00+04:00",
                "updated_at": "2026-03-03T10:00:00+04:00",
            }
        ]
    )


def test_run_greene_cycle_creates_keeper_cards_and_feedback(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    _seed_story(repo, story_id="story:alpha", slug="alpha", title="Alpha Story", confidence="high")
    _seed_story(repo, story_id="story:beta", slug="beta", title="Beta Story", confidence="medium")

    stats = run_greene_cycle(settings, repo, run_id="run-a", now_iso="2026-03-03T12:00:00+04:00")
    assert stats["captured"] > 0
    keepers = repo.list_greene_cards(state="keeper", week_key=stats["week_key"], limit=100)
    assert keepers
    assert Path(stats["note_path"]).exists()

    card_id = str(keepers[0]["card_id"])
    mark_card_feedback(repo, card_id=card_id, feedback="good", note="high utility", now_iso="2026-03-03T12:05:00+04:00")
    score = repo.feedback_score_for_card(card_id)
    assert score > 0
    repo.close()


def test_chapters_argument_gaps_and_draft(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    _seed_story(repo, story_id="story:alpha", slug="alpha", title="Alpha Story", confidence="high")
    run_greene_cycle(settings, repo, run_id="run-b", now_iso="2026-03-03T12:00:00+04:00")

    chapters = propose_chapters(settings, repo, run_id="run-b", now_iso="2026-03-03T12:10:00+04:00", topic=None)
    assert chapters["thematic"] or chapters["chronological"] or chapters["strategy"]

    argument = build_argumentation(repo, topic=None)
    assert argument["strongest_argument"] is not None

    cards = repo.list_greene_cards(state="keeper", limit=100)
    gaps = detect_gaps(cards)
    assert isinstance(gaps, list)

    memo = generate_draft(
        settings,
        repo,
        run_id="run-b",
        now_iso="2026-03-03T12:20:00+04:00",
        mode="memo",
        topic=None,
    )
    compiled = generate_draft(
        settings,
        repo,
        run_id="run-b",
        now_iso="2026-03-03T12:21:00+04:00",
        mode="compile",
        topic=None,
    )
    assert "Sources:" in memo["text"]
    assert "Sources:" not in compiled["text"]

    repo.upsert_briefing(
        brief_id="brief:2026-03-03",
        run_id="run-b",
        brief_date="2026-03-03",
        note_path=str(settings.resolve("notes", "briefings", "2026-03-03.md")),
        summary={"story_deltas": [{"title": "Alpha Story", "what_changed": "Changed", "why_it_matters": "Matters", "refs": [{"username": "alice", "tweet_id": "1001"}]}]},
        created_at="2026-03-03T12:30:00+04:00",
        updated_at="2026-03-03T12:30:00+04:00",
    )
    action = run_ai_action(settings, repo, action="one-issue")
    assert "One issue today" in action["text"]
    repo.close()
