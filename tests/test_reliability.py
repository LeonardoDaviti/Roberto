from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from roberto_app.llm.schemas import DailyDigestAutoBlock, UserNoteAutoBlock
from roberto_app.pipeline.v2 import run_v2
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo
from roberto_app.x_api.models import XUser


@dataclass
class FakeTweet:
    id: str
    text: str
    created_at: str

    def created_at_iso(self) -> str:
        return self.created_at


class FakeXClient:
    def __init__(self) -> None:
        self.data = {
            "alice": [FakeTweet("200", "alice one", "2026-03-01T10:00:00Z")],
            "bob": [FakeTweet("300", "bob one", "2026-03-01T11:00:00Z")],
        }

    def lookup_user(self, username: str) -> XUser:
        return XUser(id=f"id_{username}", username=username, name=username.title())

    def fetch_user_tweets(
        self,
        user_id: str,
        *,
        since_id: str | None,
        max_results: int,
        exclude: list[str],
        tweet_fields: list[str],
        max_pages: int = 1,
    ) -> list[FakeTweet]:
        username = user_id.replace("id_", "")
        rows = self.data.get(username, [])
        if since_id:
            rows = [r for r in rows if int(r.id) > int(since_id)]
        return rows[: max_results * max_pages]


class FlakyLLM:
    def __init__(self) -> None:
        self.fail_once_for = "bob"

    def summarize_user(
        self,
        username: str,
        tweets: list[dict[str, Any]],
        *,
        retrieval_context: list[dict[str, Any]] | None = None,
    ) -> UserNoteAutoBlock:
        if self.fail_once_for == username:
            self.fail_once_for = ""
            raise RuntimeError("synthetic llm failure")
        return UserNoteAutoBlock(themes=["theme"], notecards=[], highlights=[])

    def summarize_digest(
        self,
        highlights_by_user: list[dict[str, Any]],
        new_tweets_by_user: dict[str, list[dict[str, Any]]],
        *,
        retrieval_context: list[dict[str, Any]] | None = None,
    ) -> DailyDigestAutoBlock:
        return DailyDigestAutoBlock(stories=[], connections=[])


class StableLLM:
    def summarize_user(
        self,
        username: str,
        tweets: list[dict[str, Any]],
        *,
        retrieval_context: list[dict[str, Any]] | None = None,
    ) -> UserNoteAutoBlock:
        return UserNoteAutoBlock(themes=["theme"], notecards=[], highlights=[])

    def summarize_digest(
        self,
        highlights_by_user: list[dict[str, Any]],
        new_tweets_by_user: dict[str, list[dict[str, Any]]],
        *,
        retrieval_context: list[dict[str, Any]] | None = None,
    ) -> DailyDigestAutoBlock:
        return DailyDigestAutoBlock(stories=[], connections=[])


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "users").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "digests").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "stories").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)
    (root / "data" / "logs").mkdir(parents=True, exist_ok=True)

    (root / "config" / "following.txt").write_text("alice\nbob\n", encoding="utf-8")

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
        "v4": {
            "retrieval": {
                "enabled": True,
                "top_k_user_context": 5,
                "top_k_story_context": 5,
                "max_context_chars": 320,
            },
            "eval": {
                "enabled": True,
                "thresholds": {
                    "citation_coverage_min": 0.7,
                    "invalid_citation_rate_max": 0.3,
                    "duplicate_notecard_rate_max": 0.5,
                    "note_churn_max": 0.6,
                    "story_continuity_score_min": 0.5,
                },
            },
        },
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")


def test_v2_resume_from_checkpoint(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    x = FakeXClient()

    with pytest.raises(RuntimeError):
        run_v2(settings, repo, x, FlakyLLM(), resume=False)

    checkpoint_path = settings.resolve("data", "logs", "checkpoint_v2.json")
    assert checkpoint_path.exists()
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert "alice" in checkpoint["completed_users"]
    assert "bob" in checkpoint["failed_users"]

    resumed = run_v2(settings, repo, x, StableLLM(), resume=True)
    assert resumed.run_id == checkpoint["run_id"]
    assert resumed.per_user_new_tweets["alice"] == 0
    assert checkpoint_path.exists() is False

    repo.close()
