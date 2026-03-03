from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from roberto_app.storage.repo import StorageRepo


@dataclass
class ImportJsonReport:
    source_file: str
    records_read: int
    records_inserted: int
    users_seen: list[str]
    inserted_per_user: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_file": self.source_file,
            "records_read": self.records_read,
            "records_inserted": self.records_inserted,
            "users_seen": self.users_seen,
            "inserted_per_user": self.inserted_per_user,
        }


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("tweets", "data", "items", "posts"):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
    raise ValueError("Unsupported JSON shape. Expected a list or an object with tweets/data/items/posts list")


def _normalize_row(row: dict[str, Any], idx: int, default_username: str | None) -> tuple[str, str | None, str | None, dict[str, Any]]:
    user_obj = row.get("user") if isinstance(row.get("user"), dict) else {}

    username = (
        row.get("username")
        or row.get("screen_name")
        or row.get("author_username")
        or user_obj.get("username")
        or default_username
    )
    if not username:
        raise ValueError(f"Row {idx} missing username and no --default-username provided")

    tweet_id = row.get("tweet_id") or row.get("id") or row.get("post_id")
    if tweet_id is None:
        raise ValueError(f"Row {idx} missing tweet id (expected tweet_id/id/post_id)")

    text = row.get("text")
    if text is None:
        text = row.get("full_text")
    if text is None:
        text = ""

    created_at = row.get("created_at") or row.get("timestamp") or row.get("published_at")
    user_id = row.get("user_id") or row.get("author_id") or user_obj.get("id")
    display_name = row.get("display_name") or row.get("name") or user_obj.get("name")

    normalized = dict(row)
    normalized["id"] = str(tweet_id)
    normalized["text"] = str(text)
    if created_at is not None:
        normalized["created_at"] = str(created_at)

    return str(username), str(user_id) if user_id is not None else None, str(display_name) if display_name else None, normalized


def import_json_file(
    repo: StorageRepo,
    json_path: Path,
    *,
    default_username: str | None = None,
) -> ImportJsonReport:
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    rows = _extract_records(payload)

    by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
    users_meta: dict[str, tuple[str | None, str | None]] = {}

    for idx, row in enumerate(rows, start=1):
        username, user_id, display_name, normalized = _normalize_row(row, idx, default_username)
        by_user[username].append(normalized)
        users_meta[username] = (user_id, display_name)

    inserted_per_user: dict[str, int] = {}
    total_inserted = 0

    for username, tweets in by_user.items():
        user_id, display_name = users_meta.get(username, (None, None))
        repo.upsert_user(username, user_id, display_name)
        inserted = repo.insert_tweets(username, tweets)
        inserted_per_user[username] = inserted
        total_inserted += inserted

    return ImportJsonReport(
        source_file=str(json_path),
        records_read=len(rows),
        records_inserted=total_inserted,
        users_seen=sorted(by_user.keys()),
        inserted_per_user=inserted_per_user,
    )
