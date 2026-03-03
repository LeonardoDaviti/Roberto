from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import connect_db, init_db


@dataclass
class NoteIndexUpsert:
    note_path: str
    note_type: str
    username: str | None
    created_at: str
    updated_at: str
    last_run_id: str


class StorageRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    @classmethod
    def from_path(cls, db_path: Path) -> "StorageRepo":
        conn = connect_db(db_path)
        init_db(conn)
        return cls(conn)

    def close(self) -> None:
        self.conn.close()

    def upsert_user(self, username: str, user_id: str | None, display_name: str | None) -> None:
        self.conn.execute(
            """
            INSERT INTO users(username, user_id, display_name)
            VALUES (?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
              user_id = COALESCE(excluded.user_id, users.user_id),
              display_name = COALESCE(excluded.display_name, users.display_name)
            """,
            (username, user_id, display_name),
        )
        self.conn.commit()

    def get_user(self, username: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return dict(row) if row else None

    def list_users(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM users ORDER BY username ASC").fetchall()
        return [dict(r) for r in rows]

    def update_user_state(self, username: str, last_seen_tweet_id: str | None, last_polled_at: str) -> None:
        self.conn.execute(
            """
            UPDATE users
            SET last_seen_tweet_id = ?, last_polled_at = ?
            WHERE username = ?
            """,
            (last_seen_tweet_id, last_polled_at, username),
        )
        self.conn.commit()

    def insert_tweets(self, username: str, tweets: list[Any]) -> int:
        inserted = 0
        for tweet in tweets:
            created_at = None
            if hasattr(tweet, "created_at_iso"):
                created_at = tweet.created_at_iso()
            elif isinstance(tweet, dict):
                created_at = tweet.get("created_at")

            if isinstance(tweet, dict):
                raw_json = tweet
                tweet_id = tweet["id"]
                text = tweet["text"]
            elif hasattr(tweet, "id") and hasattr(tweet, "text"):
                raw_json = getattr(
                    tweet,
                    "raw",
                    {"id": str(tweet.id), "text": str(tweet.text), "created_at": created_at},
                )
                tweet_id = str(tweet.id)
                text = str(tweet.text)
            else:
                raise TypeError(f"Unsupported tweet object type: {type(tweet)}")

            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO tweets(tweet_id, username, created_at, text, json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tweet_id, username, created_at, text, json.dumps(raw_json, sort_keys=True)),
            )
            inserted += int(cur.rowcount > 0)
        self.conn.commit()
        return inserted

    def get_recent_tweets(self, username: str, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT tweet_id, username, created_at, text, json
            FROM tweets
            WHERE username = ?
            ORDER BY datetime(created_at) DESC, tweet_id DESC
            LIMIT ?
            """,
            (username, limit),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["json"] = json.loads(item["json"])
            out.append(item)
        return out

    def get_tweets_since_id(self, username: str, since_id: str | None, limit: int = 200) -> list[dict[str, Any]]:
        if since_id is None:
            rows = self.conn.execute(
                """
                SELECT tweet_id, username, created_at, text, json
                FROM tweets
                WHERE username = ?
                ORDER BY CAST(tweet_id AS INTEGER) DESC
                LIMIT ?
                """,
                (username, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT tweet_id, username, created_at, text, json
                FROM tweets
                WHERE username = ?
                  AND CAST(tweet_id AS INTEGER) > CAST(? AS INTEGER)
                ORDER BY CAST(tweet_id AS INTEGER) DESC
                LIMIT ?
                """,
                (username, since_id, limit),
            ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["json"] = json.loads(item["json"])
            out.append(item)
        return out

    def get_newest_tweet_id(self, username: str) -> str | None:
        row = self.conn.execute(
            """
            SELECT tweet_id
            FROM tweets
            WHERE username = ?
            ORDER BY CAST(tweet_id AS INTEGER) DESC
            LIMIT 1
            """,
            (username,),
        ).fetchone()
        if not row:
            return None
        return str(row["tweet_id"])

    def count_tweets(self, username: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tweets WHERE username = ?",
            (username,),
        ).fetchone()
        return int(row["c"] if row else 0)

    def create_run(self, run_id: str, mode: str, started_at: str) -> None:
        self.conn.execute(
            "INSERT INTO runs(run_id, mode, started_at) VALUES (?, ?, ?)",
            (run_id, mode, started_at),
        )
        self.conn.commit()

    def finish_run(self, run_id: str, finished_at: str, stats_json: dict[str, Any]) -> None:
        self.conn.execute(
            "UPDATE runs SET finished_at = ?, stats_json = ? WHERE run_id = ?",
            (finished_at, json.dumps(stats_json, sort_keys=True), run_id),
        )
        self.conn.commit()

    def upsert_note_index(self, row: NoteIndexUpsert) -> None:
        self.conn.execute(
            """
            INSERT INTO note_index(note_path, note_type, username, created_at, updated_at, last_run_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(note_path) DO UPDATE SET
              note_type = excluded.note_type,
              username = excluded.username,
              updated_at = excluded.updated_at,
              last_run_id = excluded.last_run_id
            """,
            (
                row.note_path,
                row.note_type,
                row.username,
                row.created_at,
                row.updated_at,
                row.last_run_id,
            ),
        )
        self.conn.commit()

    def get_latest_digest_note(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT * FROM note_index
            WHERE note_type = 'digest'
            ORDER BY datetime(updated_at) DESC
            LIMIT 1
            """
        ).fetchone()
        return dict(row) if row else None

    def get_last_run(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM runs ORDER BY datetime(started_at) DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        out = dict(row)
        if out.get("stats_json"):
            out["stats_json"] = json.loads(out["stats_json"])
        else:
            out["stats_json"] = {}
        return out

    def get_llm_cache(self, cache_key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT response_json FROM llm_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        return json.loads(row["response_json"])

    def set_llm_cache(self, cache_key: str, response_json: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.conn.execute(
            """
            INSERT OR REPLACE INTO llm_cache(cache_key, response_json, created_at)
            VALUES (?, ?, ?)
            """,
            (cache_key, json.dumps(response_json, sort_keys=True), now),
        )
        self.conn.commit()
