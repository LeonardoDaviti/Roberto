from __future__ import annotations

import json
import sqlite3
import hashlib
from contextlib import contextmanager
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


@dataclass
class StoryUpsert:
    story_id: str
    slug: str
    title: str
    run_id: str
    confidence: str
    tags: list[str]
    summary_json: dict[str, Any]
    now_iso: str


class StorageRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._tx_depth = 0

    @classmethod
    def from_path(cls, db_path: Path) -> "StorageRepo":
        conn = connect_db(db_path)
        init_db(conn)
        return cls(conn)

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self, label: str = "tx"):
        savepoint = f"sp_{label}_{self._tx_depth}"
        outermost = self._tx_depth == 0
        self._tx_depth += 1
        try:
            if outermost:
                self.conn.execute("BEGIN")
            else:
                self.conn.execute(f"SAVEPOINT {savepoint}")
            yield
            if outermost:
                self.conn.execute("COMMIT")
            else:
                self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception:
            if outermost:
                self.conn.execute("ROLLBACK")
            else:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        finally:
            self._tx_depth -= 1

    def _auto_commit(self) -> None:
        if self._tx_depth == 0:
            self.conn.commit()

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
        self._auto_commit()

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
        self._auto_commit()

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
        self._auto_commit()
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

    def tweet_exists(self, username: str, tweet_id: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM tweets
            WHERE username = ? AND tweet_id = ?
            LIMIT 1
            """,
            (username, tweet_id),
        ).fetchone()
        return bool(row)

    def count_tweets(self, username: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tweets WHERE username = ?",
            (username,),
        ).fetchone()
        return int(row["c"] if row else 0)

    def create_run(self, run_id: str, mode: str, started_at: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO runs(run_id, mode, started_at) VALUES (?, ?, ?)",
            (run_id, mode, started_at),
        )
        self._auto_commit()

    def finish_run(self, run_id: str, finished_at: str, stats_json: dict[str, Any]) -> None:
        self.conn.execute(
            "UPDATE runs SET finished_at = ?, stats_json = ? WHERE run_id = ?",
            (finished_at, json.dumps(stats_json, sort_keys=True), run_id),
        )
        self._auto_commit()

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
        self._auto_commit()

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

    def upsert_story(self, story: StoryUpsert) -> bool:
        existing = self.conn.execute(
            "SELECT story_id FROM stories WHERE story_id = ?",
            (story.story_id,),
        ).fetchone()

        self.conn.execute(
            """
            INSERT INTO stories(
              story_id, slug, title, first_seen_run_id, last_seen_run_id,
              mention_count, confidence, tags_json, summary_json, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(story_id) DO UPDATE SET
              title = excluded.title,
              last_seen_run_id = excluded.last_seen_run_id,
              mention_count = stories.mention_count + 1,
              confidence = excluded.confidence,
              tags_json = excluded.tags_json,
              summary_json = excluded.summary_json,
              updated_at = excluded.updated_at
            """,
            (
                story.story_id,
                story.slug,
                story.title,
                story.run_id,
                story.run_id,
                story.confidence,
                json.dumps(story.tags, sort_keys=True),
                json.dumps(story.summary_json, sort_keys=True),
                story.now_iso,
                story.now_iso,
            ),
        )
        self._auto_commit()
        return existing is None

    def add_story_sources(
        self,
        story_id: str,
        run_id: str,
        created_at: str,
        sources: list[tuple[str, str]],
    ) -> int:
        inserted = 0
        for username, tweet_id in sources:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO story_sources(story_id, username, tweet_id, run_id, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (story_id, username, tweet_id, run_id, created_at),
            )
            inserted += int(cur.rowcount > 0)
        self._auto_commit()
        return inserted

    def list_stories(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT story_id, slug, title, first_seen_run_id, last_seen_run_id,
                   mention_count, confidence, tags_json, summary_json, created_at, updated_at
            FROM stories
            ORDER BY datetime(updated_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["tags_json"] = json.loads(item["tags_json"])
            item["summary_json"] = json.loads(item["summary_json"])
            out.append(item)
        return out

    def get_story_by_id(self, story_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT story_id, slug, title, first_seen_run_id, last_seen_run_id,
                   mention_count, confidence, tags_json, summary_json, created_at, updated_at
            FROM stories
            WHERE story_id = ?
            LIMIT 1
            """,
            (story_id,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["tags_json"] = json.loads(item["tags_json"])
        item["summary_json"] = json.loads(item["summary_json"])
        return item

    def list_story_sources(self, story_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT story_id, username, tweet_id, run_id, created_at
            FROM story_sources
            WHERE story_id = ?
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """,
            (story_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

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
        self._auto_commit()

    def upsert_embedding(self, kind: str, item_id: str, text: str, vector: list[float]) -> None:
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        key = f"{kind}:{item_id}"
        self.conn.execute(
            """
            INSERT INTO llm_embeddings(embedding_key, kind, item_id, text_hash, vector_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(embedding_key) DO UPDATE SET
              text_hash = excluded.text_hash,
              vector_json = excluded.vector_json,
              updated_at = excluded.updated_at
            """,
            (key, kind, item_id, text_hash, json.dumps(vector), now),
        )
        self._auto_commit()

    def get_embedding(self, kind: str, item_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT embedding_key, kind, item_id, text_hash, vector_json, updated_at
            FROM llm_embeddings
            WHERE embedding_key = ?
            LIMIT 1
            """,
            (f"{kind}:{item_id}",),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["vector"] = json.loads(item.pop("vector_json"))
        return item
