from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from roberto_app.pipeline.common import newest_tweet_id, read_following, run_id_now, utc_now_iso
from roberto_app.pipeline.search_index import rebuild_search_index
from roberto_app.storage.repo import StorageRepo
from roberto_app.x_api.client import XClient


@dataclass
class SyncReport:
    run_id: str
    mode: str
    started_at: str
    finished_at: str | None = None
    per_user_new_tweets: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "per_user_new_tweets": self.per_user_new_tweets,
        }

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def run_sync(settings, repo: StorageRepo, x_client: XClient, *, full: bool = False) -> SyncReport:
    usernames = read_following(settings.resolve("config", "following.txt"))
    run_id = run_id_now()
    started_at = utc_now_iso()

    mode = "sync-full" if full else "sync-incremental"
    report = SyncReport(run_id=run_id, mode=mode, started_at=started_at)

    page_cap = max(1, math.ceil(settings.pipeline.v2.max_new_tweets_per_user / settings.x.max_results))

    for username in usernames:
        user_row = repo.get_user(username)
        if not user_row or not user_row.get("user_id"):
            looked_up = x_client.lookup_user(username)
            repo.upsert_user(username, looked_up.id, looked_up.name)
            user_id = looked_up.id
            last_seen = None
        else:
            user_id = str(user_row["user_id"])
            last_seen = user_row.get("last_seen_tweet_id")

        since_id = None if full else last_seen
        tweets = x_client.fetch_user_tweets(
            user_id,
            since_id=since_id,
            max_results=settings.x.max_results,
            exclude=settings.x.exclude,
            tweet_fields=settings.x.tweet_fields,
            max_pages=(1 if full else page_cap),
        )
        inserted_count = repo.insert_tweets(username, tweets)
        report.per_user_new_tweets[username] = inserted_count

        newest = newest_tweet_id([t.id for t in tweets])
        repo.update_user_state(username, newest or last_seen, utc_now_iso())

    report.finished_at = utc_now_iso()
    rebuild_search_index(settings, repo)
    export_path = settings.resolve("data", "exports", f"sync_{run_id}.json")
    report.write_json(export_path)
    return report
