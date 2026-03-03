from __future__ import annotations

import json
import sqlite3
import hashlib
import re
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

    def list_note_index(self, note_type: str | None = None, limit: int = 5000) -> list[dict[str, Any]]:
        if note_type:
            rows = self.conn.execute(
                """
                SELECT note_path, note_type, username, created_at, updated_at, last_run_id
                FROM note_index
                WHERE note_type = ?
                ORDER BY datetime(updated_at) DESC
                LIMIT ?
                """,
                (note_type, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT note_path, note_type, username, created_at, updated_at, last_run_id
                FROM note_index
                ORDER BY datetime(updated_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
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

    def list_tweets_for_search(self, limit: int = 50000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT tweet_id, username, created_at, text
            FROM tweets
            ORDER BY datetime(created_at) DESC, tweet_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

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

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM runs WHERE run_id = ? LIMIT 1",
            (run_id,),
        ).fetchone()
        if not row:
            return None
        out = dict(row)
        if out.get("stats_json"):
            out["stats_json"] = json.loads(out["stats_json"])
        else:
            out["stats_json"] = {}
        return out

    def patch_run_stats(self, run_id: str, updates: dict[str, Any]) -> None:
        run = self.get_run(run_id)
        if not run:
            return
        stats = dict(run.get("stats_json") or {})
        stats.update(updates)
        self.conn.execute(
            "UPDATE runs SET stats_json = ? WHERE run_id = ?",
            (json.dumps(stats, sort_keys=True), run_id),
        )
        self._auto_commit()

    def upsert_staged_note(
        self,
        *,
        run_id: str,
        live_path: str,
        staged_path: str,
        mode: str,
        note_type: str,
        trigger_refs: list[dict[str, str]],
        created_at: str,
        status: str = "staged",
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO staged_notes(
              run_id, live_path, staged_path, mode, note_type, trigger_refs_json, status, created_at, promoted_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(run_id, live_path) DO UPDATE SET
              staged_path = excluded.staged_path,
              mode = excluded.mode,
              note_type = excluded.note_type,
              trigger_refs_json = excluded.trigger_refs_json,
              status = excluded.status,
              created_at = excluded.created_at,
              promoted_at = NULL
            """,
            (
                run_id,
                live_path,
                staged_path,
                mode,
                note_type,
                json.dumps(trigger_refs, sort_keys=True),
                status,
                created_at,
            ),
        )
        self._auto_commit()

    def list_staged_notes(self, run_id: str, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            rows = self.conn.execute(
                """
                SELECT run_id, live_path, staged_path, mode, note_type, trigger_refs_json, status, created_at, promoted_at
                FROM staged_notes
                WHERE run_id = ? AND status = ?
                ORDER BY note_type ASC, live_path ASC
                """,
                (run_id, status),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT run_id, live_path, staged_path, mode, note_type, trigger_refs_json, status, created_at, promoted_at
                FROM staged_notes
                WHERE run_id = ?
                ORDER BY note_type ASC, live_path ASC
                """,
                (run_id,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["trigger_refs"] = json.loads(item.pop("trigger_refs_json") or "[]")
            out.append(item)
        return out

    def mark_staged_note_status(
        self,
        run_id: str,
        live_path: str,
        status: str,
        promoted_at: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE staged_notes
            SET status = ?, promoted_at = ?
            WHERE run_id = ? AND live_path = ?
            """,
            (status, promoted_at, run_id, live_path),
        )
        self._auto_commit()

    def insert_note_snapshot(
        self,
        *,
        note_path: str,
        run_id: str | None,
        captured_at: str,
        reason: str,
        content: str,
    ) -> int:
        sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
        cur = self.conn.execute(
            """
            INSERT INTO note_snapshots(note_path, run_id, captured_at, reason, sha256, content)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (note_path, run_id, captured_at, reason, sha, content),
        )
        self._auto_commit()
        return int(cur.lastrowid)

    def list_note_snapshots(self, note_path: str, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT snapshot_id, note_path, run_id, captured_at, reason, sha256
            FROM note_snapshots
            WHERE note_path = ?
            ORDER BY snapshot_id DESC
            LIMIT ?
            """,
            (note_path, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_note_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT snapshot_id, note_path, run_id, captured_at, reason, sha256, content
            FROM note_snapshots
            WHERE snapshot_id = ?
            LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def get_latest_note_snapshot(self, note_path: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT snapshot_id, note_path, run_id, captured_at, reason, sha256, content
            FROM note_snapshots
            WHERE note_path = ?
            ORDER BY snapshot_id DESC
            LIMIT 1
            """,
            (note_path,),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def reset_search_index(self) -> None:
        self.conn.execute("DELETE FROM search_fts")
        self._auto_commit()

    def insert_search_docs(self, docs: list[dict[str, str]]) -> int:
        if not docs:
            return 0
        self.conn.executemany(
            """
            INSERT INTO search_fts(
              kind, subtype, item_id, ref_path, source_ids, title, body, tags, username, entity, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    d.get("kind", ""),
                    d.get("subtype", ""),
                    d.get("item_id", ""),
                    d.get("ref_path", ""),
                    d.get("source_ids", ""),
                    d.get("title", ""),
                    d.get("body", ""),
                    d.get("tags", ""),
                    d.get("username", ""),
                    d.get("entity", ""),
                    d.get("created_at", ""),
                )
                for d in docs
            ],
        )
        self._auto_commit()
        return len(docs)

    def count_search_docs(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM search_fts").fetchone()
        return int(row["c"] if row else 0)

    def set_attention_state(
        self,
        *,
        target_type: str,
        target_id: str,
        state: str,
        updated_at: str,
        snoozed_until: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO attention_state(target_type, target_id, state, snoozed_until, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(target_type, target_id) DO UPDATE SET
              state = excluded.state,
              snoozed_until = excluded.snoozed_until,
              updated_at = excluded.updated_at
            """,
            (target_type, target_id, state, snoozed_until, updated_at),
        )
        self._auto_commit()

    def get_attention_state(self, target_type: str, target_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT target_type, target_id, state, snoozed_until, updated_at
            FROM attention_state
            WHERE target_type = ? AND target_id = ?
            LIMIT 1
            """,
            (target_type, target_id),
        ).fetchone()
        return dict(row) if row else None

    def is_attention_blocked(self, target_type: str, target_id: str, now_iso: str) -> bool:
        row = self.get_attention_state(target_type, target_id)
        if not row:
            return False
        state = str(row.get("state") or "active")
        if state == "muted":
            return True
        if state == "snoozed":
            until = row.get("snoozed_until")
            if until and str(until) > now_iso:
                return True
        return False

    def add_story_alias(self, alias_slug: str, story_id: str, created_at: str) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO story_aliases(alias_slug, story_id, created_at)
            VALUES (?, ?, ?)
            """,
            (alias_slug, story_id, created_at),
        )
        self._auto_commit()

    def resolve_story_alias(self, alias_slug: str) -> str | None:
        row = self.conn.execute(
            """
            SELECT story_id
            FROM story_aliases
            WHERE alias_slug = ?
            LIMIT 1
            """,
            (alias_slug,),
        ).fetchone()
        if not row:
            return None
        return str(row["story_id"])

    def list_story_aliases(self, story_id: str) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT alias_slug
            FROM story_aliases
            WHERE story_id = ?
            ORDER BY alias_slug ASC
            """,
            (story_id,),
        ).fetchall()
        return [str(row["alias_slug"]) for row in rows]

    def add_story_lineage(self, parent_story_id: str, child_story_id: str, relation: str, created_at: str) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO story_lineage(parent_story_id, child_story_id, relation, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (parent_story_id, child_story_id, relation, created_at),
        )
        self._auto_commit()

    def list_story_lineage(self, story_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT parent_story_id, child_story_id, relation, created_at
            FROM story_lineage
            WHERE parent_story_id = ? OR child_story_id = ?
            ORDER BY datetime(created_at) DESC
            """,
            (story_id, story_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def search_docs(
        self,
        query: str,
        *,
        kind: str | None = None,
        limit: int = 20,
        days: int | None = None,
        include_muted: bool = False,
        now_iso: str | None = None,
    ) -> list[dict[str, Any]]:
        args: list[Any] = [query]
        where_parts = ["search_fts MATCH ?"]
        if kind:
            where_parts.append("kind = ?")
            args.append(kind)
        if days and days > 0:
            where_parts.append("datetime(created_at) >= datetime('now', ?)")
            args.append(f"-{days} days")
        args.append(max(1, limit))
        sql = f"""
            SELECT
              kind, subtype, item_id, ref_path, source_ids, title, tags, username, entity, created_at,
              snippet(search_fts, 6, '[', ']', '...', 18) AS snippet,
              bm25(search_fts) AS rank
            FROM search_fts
            WHERE {' AND '.join(where_parts)}
            ORDER BY rank
            LIMIT ?
        """
        rows = self.conn.execute(sql, tuple(args)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["rank"] = float(item.get("rank") or 0.0)
            if not include_muted and now_iso:
                if item.get("kind") == "story" and self.is_attention_blocked("story", str(item.get("item_id") or ""), now_iso):
                    continue
                if item.get("kind") == "entity" and self.is_attention_blocked("entity", str(item.get("item_id") or ""), now_iso):
                    continue
            out.append(item)
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

    def list_stories(self, limit: int = 100, include_aliased: bool = False) -> list[dict[str, Any]]:
        where = ""
        if not include_aliased:
            where = "WHERE s.slug NOT IN (SELECT alias_slug FROM story_aliases)"
        rows = self.conn.execute(
            f"""
            SELECT s.story_id, s.slug, s.title, s.first_seen_run_id, s.last_seen_run_id,
                   s.mention_count, s.confidence, s.tags_json, s.summary_json, s.created_at, s.updated_at,
                   a.state AS attention_state, a.snoozed_until
            FROM stories s
            LEFT JOIN attention_state a ON a.target_type = 'story' AND a.target_id = s.story_id
            {where}
            ORDER BY datetime(s.updated_at) DESC
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
            SELECT s.story_id, s.slug, s.title, s.first_seen_run_id, s.last_seen_run_id,
                   s.mention_count, s.confidence, s.tags_json, s.summary_json, s.created_at, s.updated_at,
                   a.state AS attention_state, a.snoozed_until
            FROM stories s
            LEFT JOIN attention_state a ON a.target_type = 'story' AND a.target_id = s.story_id
            WHERE s.story_id = ?
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

    def get_story_by_slug(self, slug: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT s.story_id, s.slug, s.title, s.first_seen_run_id, s.last_seen_run_id,
                   s.mention_count, s.confidence, s.tags_json, s.summary_json, s.created_at, s.updated_at,
                   a.state AS attention_state, a.snoozed_until
            FROM stories s
            LEFT JOIN attention_state a ON a.target_type = 'story' AND a.target_id = s.story_id
            WHERE s.slug = ?
            LIMIT 1
            """,
            (slug,),
        ).fetchone()
        if row:
            item = dict(row)
            item["tags_json"] = json.loads(item["tags_json"])
            item["summary_json"] = json.loads(item["summary_json"])
            return item
        alias_story_id = self.resolve_story_alias(slug)
        if not alias_story_id:
            return None
        return self.get_story_by_id(alias_story_id)

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

    def set_story_slug(self, story_id: str, slug: str, updated_at: str) -> None:
        self.conn.execute(
            """
            UPDATE stories
            SET slug = ?, updated_at = ?
            WHERE story_id = ?
            """,
            (slug, updated_at, story_id),
        )
        self._auto_commit()

    def update_story_summary(
        self,
        story_id: str,
        *,
        title: str,
        confidence: str,
        tags: list[str],
        summary_json: dict[str, Any],
        mention_count: int | None,
        last_seen_run_id: str,
        updated_at: str,
    ) -> None:
        if mention_count is None:
            self.conn.execute(
                """
                UPDATE stories
                SET title = ?, confidence = ?, tags_json = ?, summary_json = ?,
                    last_seen_run_id = ?, updated_at = ?
                WHERE story_id = ?
                """,
                (
                    title,
                    confidence,
                    json.dumps(tags, sort_keys=True),
                    json.dumps(summary_json, sort_keys=True),
                    last_seen_run_id,
                    updated_at,
                    story_id,
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE stories
                SET title = ?, confidence = ?, tags_json = ?, summary_json = ?,
                    mention_count = ?, last_seen_run_id = ?, updated_at = ?
                WHERE story_id = ?
                """,
                (
                    title,
                    confidence,
                    json.dumps(tags, sort_keys=True),
                    json.dumps(summary_json, sort_keys=True),
                    mention_count,
                    last_seen_run_id,
                    updated_at,
                    story_id,
                ),
            )
        self._auto_commit()

    def list_story_entities(self, story_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT se.story_id, se.entity_id, se.created_at, e.canonical_name
            FROM story_entities se
            JOIN entities e ON e.entity_id = se.entity_id
            WHERE se.story_id = ?
            ORDER BY e.canonical_name ASC
            """,
            (story_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def insert_idea_cards(self, cards: list[dict[str, Any]]) -> int:
        inserted = 0
        for card in cards:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO idea_cards(
                  card_id, run_id, username, idea_type, title, hypothesis, why_now,
                  tags_json, source_refs_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card["card_id"],
                    card["run_id"],
                    card["username"],
                    card["idea_type"],
                    card["title"],
                    card["hypothesis"],
                    card["why_now"],
                    json.dumps(card.get("tags", []), sort_keys=True),
                    json.dumps(card.get("source_refs", []), sort_keys=True),
                    card["created_at"],
                ),
            )
            inserted += int(cur.rowcount > 0)
        self._auto_commit()
        return inserted

    def list_recent_idea_cards(self, days: int = 7, limit: int = 500, username: str | None = None) -> list[dict[str, Any]]:
        if username:
            rows = self.conn.execute(
                """
                SELECT card_id, run_id, username, idea_type, title, hypothesis, why_now,
                       tags_json, source_refs_json, created_at
                FROM idea_cards
                WHERE datetime(created_at) >= datetime('now', ?)
                  AND username = ?
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """,
                (f"-{days} days", username, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT card_id, run_id, username, idea_type, title, hypothesis, why_now,
                       tags_json, source_refs_json, created_at
                FROM idea_cards
                WHERE datetime(created_at) >= datetime('now', ?)
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """,
                (f"-{days} days", limit),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["tags"] = json.loads(item.pop("tags_json"))
            item["source_refs"] = json.loads(item.pop("source_refs_json"))
            out.append(item)
        return out

    def insert_conflict_cards(self, cards: list[dict[str, Any]]) -> int:
        inserted = 0
        for card in cards:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO conflict_cards(
                  conflict_id, run_id, title, claim_a_json, claim_b_json,
                  tags_json, source_refs_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card["conflict_id"],
                    card["run_id"],
                    card["title"],
                    json.dumps(card["claim_a"], sort_keys=True),
                    json.dumps(card["claim_b"], sort_keys=True),
                    json.dumps(card.get("tags", []), sort_keys=True),
                    json.dumps(card.get("source_refs", []), sort_keys=True),
                    card["created_at"],
                ),
            )
            inserted += int(cur.rowcount > 0)
        self._auto_commit()
        return inserted

    def list_recent_conflict_cards(self, days: int = 30, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT conflict_id, run_id, title, claim_a_json, claim_b_json, tags_json, source_refs_json, created_at
            FROM conflict_cards
            WHERE datetime(created_at) >= datetime('now', ?)
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """,
            (f"-{days} days", limit),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["claim_a"] = json.loads(item.pop("claim_a_json"))
            item["claim_b"] = json.loads(item.pop("claim_b_json"))
            item["tags"] = json.loads(item.pop("tags_json"))
            item["source_refs"] = json.loads(item.pop("source_refs_json"))
            out.append(item)
        return out

    def upsert_entity(self, canonical_name: str, aliases: list[str], now_iso: str) -> str:
        base_slug = re.sub(r"[^a-z0-9]+", "-", canonical_name.lower()).strip("-") or "entity"
        entity_id = base_slug
        existing = self.conn.execute(
            """
            SELECT canonical_name FROM entities WHERE entity_id = ?
            """,
            (entity_id,),
        ).fetchone()
        if existing and str(existing["canonical_name"]).lower() != canonical_name.lower():
            suffix = hashlib.sha256(canonical_name.lower().encode("utf-8")).hexdigest()[:6]
            entity_id = f"{base_slug}-{suffix}"
        self.conn.execute(
            """
            INSERT INTO entities(entity_id, canonical_name, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
              canonical_name = excluded.canonical_name,
              last_seen_at = excluded.last_seen_at
            """,
            (entity_id, canonical_name, now_iso, now_iso),
        )
        all_aliases = {canonical_name.lower(), canonical_name, entity_id, *aliases}
        for alias in sorted(a for a in all_aliases if a):
            self.conn.execute(
                """
                INSERT OR IGNORE INTO entity_aliases(alias, entity_id)
                VALUES (?, ?)
                """,
                (alias.lower(), entity_id),
            )
        self._auto_commit()
        return entity_id

    def link_entity_ref(self, entity_id: str, ref_type: str, ref_id: str, username: str | None, created_at: str) -> bool:
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO entity_links(entity_id, ref_type, ref_id, username, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (entity_id, ref_type, ref_id, username, created_at),
        )
        self._auto_commit()
        return bool(cur.rowcount > 0)

    def link_story_entity(self, story_id: str, entity_id: str, created_at: str) -> bool:
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO story_entities(story_id, entity_id, created_at)
            VALUES (?, ?, ?)
            """,
            (story_id, entity_id, created_at),
        )
        self._auto_commit()
        return bool(cur.rowcount > 0)

    def resolve_entity(self, query: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT e.entity_id, e.canonical_name, e.first_seen_at, e.last_seen_at,
                   a2.state AS attention_state, a2.snoozed_until
            FROM entity_aliases a
            JOIN entities e ON e.entity_id = a.entity_id
            LEFT JOIN attention_state a2 ON a2.target_type = 'entity' AND a2.target_id = e.entity_id
            WHERE a.alias = ?
            LIMIT 1
            """,
            (query.strip().lower(),),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def list_entities(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT e.entity_id, e.canonical_name, e.first_seen_at, e.last_seen_at,
                   a.state AS attention_state, a.snoozed_until
            FROM entities e
            LEFT JOIN attention_state a ON a.target_type = 'entity' AND a.target_id = e.entity_id
            ORDER BY datetime(e.last_seen_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT e.entity_id, e.canonical_name, e.first_seen_at, e.last_seen_at,
                   a.state AS attention_state, a.snoozed_until
            FROM entities e
            LEFT JOIN attention_state a ON a.target_type = 'entity' AND a.target_id = e.entity_id
            WHERE e.entity_id = ?
            LIMIT 1
            """,
            (entity_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def get_entity_aliases(self, entity_id: str) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT alias
            FROM entity_aliases
            WHERE entity_id = ?
            ORDER BY alias ASC
            """,
            (entity_id,),
        ).fetchall()
        return [str(r["alias"]) for r in rows]

    def get_entity_timeline(self, entity_id: str, days: int = 90, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT entity_id, ref_type, ref_id, username, created_at
            FROM entity_links
            WHERE entity_id = ?
              AND datetime(created_at) >= datetime('now', ?)
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """,
            (entity_id, f"-{days} days", limit),
        ).fetchall()
        out = [dict(r) for r in rows]
        for item in out:
            if item["ref_type"] == "story":
                story = self.get_story_by_id(str(item["ref_id"]))
                if story:
                    item["story_title"] = story.get("title")
            elif item["ref_type"] == "tweet":
                tweet = self.get_tweet_by_id(str(item["ref_id"]))
                if tweet:
                    item["tweet_text"] = tweet.get("text")
                    item["tweet_created_at"] = tweet.get("created_at")
                    if not item.get("username"):
                        item["username"] = tweet.get("username")
        return out

    def get_tweet_by_id(self, tweet_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT tweet_id, username, created_at, text, json
            FROM tweets
            WHERE tweet_id = ?
            LIMIT 1
            """,
            (tweet_id,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["json"] = json.loads(item["json"])
        return item

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
