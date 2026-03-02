from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from roberto_app.llm.schemas import (
    Connection,
    ConnectionSupport,
    DailyDigestAutoBlock,
    Highlight,
    Story,
    StorySource,
    UserNoteAutoBlock,
)
from roberto_app.pipeline.v1 import run_v1
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
        self.data: dict[str, list[FakeTweet]] = {
            "alice": [
                FakeTweet("200", "alice post 2", "2026-03-01T10:00:00Z"),
                FakeTweet("100", "alice post 1", "2026-03-01T09:00:00Z"),
            ],
            "bob": [
                FakeTweet("150", "bob post 1", "2026-03-01T11:00:00Z"),
            ],
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


class FakeLLM:
    def summarize_user(self, username: str, tweets: list[dict[str, Any]]) -> UserNoteAutoBlock:
        tweet_ids = [str(t["tweet_id"]) for t in tweets]
        highlights: list[Highlight] = []
        if tweet_ids:
            highlights = [Highlight(title=f"{username} highlight", summary="summary", source_tweet_ids=tweet_ids[:1])]
        return UserNoteAutoBlock(themes=[f"theme-{username}"], highlights=highlights, notecards=[])

    def summarize_digest(
        self,
        highlights_by_user: list[dict[str, Any]],
        new_tweets_by_user: dict[str, list[dict[str, Any]]],
    ) -> DailyDigestAutoBlock:
        if not highlights_by_user and not new_tweets_by_user:
            return DailyDigestAutoBlock()
        sources: list[StorySource] = []
        supports: list[ConnectionSupport] = []
        for username, tweets in new_tweets_by_user.items():
            for tweet in tweets[:1]:
                sources.append(StorySource(username=username, tweet_id=tweet["tweet_id"]))
                supports.append(ConnectionSupport(username=username, tweet_id=tweet["tweet_id"]))
        if not sources:
            return DailyDigestAutoBlock()
        return DailyDigestAutoBlock(
            stories=[
                Story(
                    title="Story",
                    what_happened="what",
                    why_it_matters="why",
                    sources=sources,
                    tags=["tag"],
                    confidence="high",
                )
            ],
            connections=[Connection(insight="Connection", supports=supports)],
        )


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "users").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "digests").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)

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
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")


def test_pipeline_v1_v2_smoke(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    x = FakeXClient()
    llm = FakeLLM()

    report_v1 = run_v1(settings, repo, x, llm)
    assert report_v1.mode == "v1"
    assert len(report_v1.created_notes) == 3  # 2 user notes + 1 digest

    bob_path = settings.resolve("notes", "users", "bob.md")
    bob_before = bob_path.read_text(encoding="utf-8")

    # Keep only one new tweet for alice and no updates for bob.
    x.data["alice"] = [
        FakeTweet("250", "alice post 3", "2026-03-02T10:00:00Z"),
        FakeTweet("200", "alice post 2", "2026-03-01T10:00:00Z"),
        FakeTweet("100", "alice post 1", "2026-03-01T09:00:00Z"),
    ]
    x.data["bob"] = [FakeTweet("150", "bob post 1", "2026-03-01T11:00:00Z")]

    report_v2 = run_v2(settings, repo, x, llm)

    alice_note = str(settings.resolve("notes", "users", "alice.md"))
    bob_note = str(settings.resolve("notes", "users", "bob.md"))

    assert report_v2.mode == "v2"
    assert report_v2.per_user_new_tweets["alice"] == 1
    assert report_v2.per_user_new_tweets["bob"] == 0
    assert alice_note in report_v2.updated_notes
    assert bob_note not in report_v2.updated_notes

    bob_after = bob_path.read_text(encoding="utf-8")
    assert bob_before == bob_after

    export_v1 = settings.resolve("data", "exports", f"run_{report_v1.run_id}.json")
    export_v2 = settings.resolve("data", "exports", f"run_{report_v2.run_id}.json")
    assert export_v1.exists()
    assert export_v2.exists()

    repo.close()
