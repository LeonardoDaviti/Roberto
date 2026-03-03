from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.llm.schemas import DailyDigestAutoBlock, Highlight, NoteCard, Story, StorySource, UserNoteAutoBlock
from roberto_app.pipeline.common import utc_now_iso
from roberto_app.pipeline.editorial import (
    build_diff_preview,
    normalize_trigger_refs,
    promote_staged_run,
    rollback_note,
    staging_target_path,
)
from roberto_app.pipeline.v2 import run_v2
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo


def test_diff_preview_and_trigger_normalization(tmp_path: Path) -> None:
    live = tmp_path / "live.md"
    staged = tmp_path / "staged.md"
    live.write_text("a\nb\n", encoding="utf-8")
    staged.write_text("a\nc\n", encoding="utf-8")

    diff = build_diff_preview(live, staged, max_lines=100)
    assert diff["changed"] is True
    assert diff["added_lines"] >= 1
    assert diff["removed_lines"] >= 1
    assert "-b" in diff["diff"]
    assert "+c" in diff["diff"]

    refs = normalize_trigger_refs(
        [
            {"username": "alice", "tweet_id": "1"},
            {"username": "alice", "tweet_id": "1"},
            {"username": "bob", "tweet_id": "2"},
            {"username": "", "tweet_id": "3"},
        ]
    )
    assert len(refs) == 3
    assert {(ref.get("username"), ref.get("tweet_id")) for ref in refs} == {
        ("alice", "1"),
        ("bob", "2"),
        (None, "3"),
    }
    assert all(
        ref.get("provider") == "x"
        and ref.get("source_id")
        and ref.get("anchor_type") == "id"
        and ref.get("anchor")
        for ref in refs
    )


def test_promote_and_rollback_roundtrip(tmp_path: Path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    notes_root = tmp_path / "notes"
    live = notes_root / "users" / "alice.md"
    live.parent.mkdir(parents=True, exist_ok=True)
    live.write_text("old content\n", encoding="utf-8")

    run_id = "2026-03-03T120000000000Z"
    staged = staging_target_path(notes_root, run_id, live)
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_text("new content\n", encoding="utf-8")

    now_iso = utc_now_iso()
    repo.upsert_staged_note(
        run_id=run_id,
        live_path=str(live),
        staged_path=str(staged),
        mode="v2",
        note_type="user",
        trigger_refs=[{"username": "alice", "tweet_id": "123"}],
        created_at=now_iso,
    )

    promoted = promote_staged_run(repo, run_id, now_iso=now_iso)
    assert promoted.promoted == [str(live)]
    assert live.read_text(encoding="utf-8") == "new content\n"

    snapshots = repo.list_note_snapshots(str(live), limit=10)
    assert snapshots
    assert snapshots[0]["reason"] == "pre_promote"

    rolled = rollback_note(repo, note_path=str(live), now_iso=utc_now_iso())
    assert rolled.restored_snapshot_id == snapshots[0]["snapshot_id"]
    assert live.read_text(encoding="utf-8") == "old content\n"

    snapshots_after = repo.list_note_snapshots(str(live), limit=10)
    reasons = {row["reason"] for row in snapshots_after}
    assert "pre_rollback" in reasons
    repo.close()


class _FakeLLM:
    def summarize_user(self, username, tweets, *, retrieval_context=None):
        tweet_id = str(tweets[0]["tweet_id"]) if tweets else "1"
        return UserNoteAutoBlock(
            themes=["t"],
            notecards=[
                NoteCard(
                    type="claim",
                    title="title",
                    payload="payload",
                    why_it_matters="why",
                    tags=["tag"],
                    source_tweet_ids=[tweet_id],
                )
            ],
            highlights=[Highlight(title="h", summary="s", source_tweet_ids=[tweet_id])],
        )

    def summarize_digest(self, highlights_by_user, new_tweets_by_user, *, retrieval_context=None):
        for username, tweets in new_tweets_by_user.items():
            if tweets:
                tweet_id = str(tweets[0]["tweet_id"])
                return DailyDigestAutoBlock(
                    stories=[
                        Story(
                            title="Story",
                            what_happened="What",
                            why_it_matters="Why",
                            sources=[StorySource(username=username, tweet_id=tweet_id)],
                            tags=["Tag"],
                            confidence="high",
                        )
                    ],
                    connections=[],
                )
        return DailyDigestAutoBlock()


def test_pipeline_staging_enabled_writes_only_staging(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "notes" / "users").mkdir(parents=True, exist_ok=True)
    (tmp_path / "notes" / "digests").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "exports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "following.txt").write_text("alice\n", encoding="utf-8")
    settings_payload = {
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
        "v13": {"enabled": True, "max_diff_lines": 200},
    }
    (tmp_path / "config" / "settings.yaml").write_text(yaml.safe_dump(settings_payload), encoding="utf-8")

    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    repo.upsert_user("alice", "local:alice", "alice")
    repo.insert_tweets(
        "alice",
        [{"id": "900", "text": "Alice update", "created_at": "2026-03-03T10:00:00Z"}],
    )
    report = run_v2(settings, repo, x_client=None, llm=_FakeLLM(), from_db_only=True)

    live_note = settings.resolve("notes", "users", "alice.md")
    assert not live_note.exists()
    staged_note = settings.resolve("notes", "_staging", report.run_id, "users", "alice.md")
    assert staged_note.exists()

    staged_rows = repo.list_staged_notes(report.run_id, status="staged")
    assert any(row["live_path"] == str(live_note) for row in staged_rows)
    assert str(live_note) in report.staged_notes
    repo.close()
