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

from roberto_app.sources.models import SourceRef, SourceSnapshot, build_x_source_artifacts

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

    def _upsert_source_snapshot(self, snapshot: SourceSnapshot) -> None:
        self.conn.execute(
            """
            INSERT INTO source_snapshots(
              snapshot_hash, provider, source_id, url, text, metadata_json, captured_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_hash) DO UPDATE SET
              url = COALESCE(excluded.url, source_snapshots.url),
              text = excluded.text,
              metadata_json = excluded.metadata_json,
              captured_at = excluded.captured_at
            """,
            (
                snapshot.snapshot_hash,
                snapshot.provider,
                snapshot.source_id,
                snapshot.url,
                snapshot.text,
                json.dumps(snapshot.metadata, sort_keys=True, default=str),
                snapshot.captured_at,
            ),
        )

    def _upsert_source_ref(self, source_ref: SourceRef, *, username: str | None, tweet_id: str | None) -> None:
        record = source_ref.to_record(username=username, tweet_id=tweet_id)
        self.conn.execute(
            """
            INSERT INTO source_refs(
              ref_id, provider, source_id, url, anchor_type, anchor, excerpt_hash, snapshot_hash,
              captured_at, username, tweet_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, source_id, anchor_type, anchor) DO UPDATE SET
              url = COALESCE(excluded.url, source_refs.url),
              excerpt_hash = COALESCE(excluded.excerpt_hash, source_refs.excerpt_hash),
              snapshot_hash = COALESCE(excluded.snapshot_hash, source_refs.snapshot_hash),
              captured_at = excluded.captured_at,
              username = COALESCE(excluded.username, source_refs.username),
              tweet_id = COALESCE(excluded.tweet_id, source_refs.tweet_id)
            """,
            (
                record["ref_id"],
                record["provider"],
                record["source_id"],
                record["url"],
                record["anchor_type"],
                record["anchor"],
                record["excerpt_hash"],
                record["snapshot_hash"],
                record["captured_at"],
                record["username"],
                record["tweet_id"],
            ),
        )

    def _write_x_source_ref(
        self,
        *,
        username: str,
        tweet_id: str,
        text: str,
        created_at: str | None,
        raw_json: dict[str, Any],
    ) -> None:
        source_ref, snapshot = build_x_source_artifacts(
            username=username,
            tweet_id=tweet_id,
            text=text,
            created_at=created_at,
            raw=raw_json,
        )
        self._upsert_source_snapshot(snapshot)
        self._upsert_source_ref(source_ref, username=username, tweet_id=tweet_id)

    def insert_tweets(self, username: str, tweets: list[Any]) -> int:
        inserted = 0
        for tweet in tweets:
            created_at = None
            if hasattr(tweet, "created_at_iso"):
                created_at = tweet.created_at_iso()
            elif isinstance(tweet, dict):
                created_at = tweet.get("created_at")

            if isinstance(tweet, dict):
                raw_json = dict(tweet)
                tweet_id = str(tweet["id"])
                text = str(tweet["text"])
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
                (tweet_id, username, created_at, text, json.dumps(raw_json, sort_keys=True, default=str)),
            )
            inserted += int(cur.rowcount > 0)
            self._write_x_source_ref(
                username=username,
                tweet_id=tweet_id,
                text=text,
                created_at=created_at,
                raw_json=raw_json,
            )
        self._auto_commit()
        return inserted

    def get_source_ref(
        self,
        *,
        provider: str,
        source_id: str,
        anchor_type: str = "id",
        anchor: str | None = None,
    ) -> dict[str, Any] | None:
        if anchor is None:
            anchor = source_id
        row = self.conn.execute(
            """
            SELECT ref_id, provider, source_id, url, anchor_type, anchor, excerpt_hash, snapshot_hash,
                   captured_at, username, tweet_id
            FROM source_refs
            WHERE provider = ? AND source_id = ? AND anchor_type = ? AND anchor = ?
            LIMIT 1
            """,
            (provider, source_id, anchor_type, anchor),
        ).fetchone()
        return dict(row) if row else None

    def get_source_snapshot(self, snapshot_hash: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT snapshot_hash, provider, source_id, url, text, metadata_json, captured_at
            FROM source_snapshots
            WHERE snapshot_hash = ?
            LIMIT 1
            """,
            (snapshot_hash,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
        return item

    def backfill_x_source_refs(self, limit: int = 100000) -> int:
        rows = self.conn.execute(
            """
            SELECT t.tweet_id, t.username, t.created_at, t.text, t.json
            FROM tweets t
            LEFT JOIN source_refs sr
              ON sr.provider = 'x'
             AND sr.source_id = t.tweet_id
             AND sr.anchor_type = 'id'
             AND sr.anchor = t.tweet_id
            WHERE sr.ref_id IS NULL
            ORDER BY CAST(t.tweet_id AS INTEGER) DESC
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()

        written = 0
        for row in rows:
            item = dict(row)
            raw_json = json.loads(item.get("json") or "{}")
            username = str(item.get("username") or "")
            tweet_id = str(item.get("tweet_id") or "")
            if not username or not tweet_id:
                continue
            self._write_x_source_ref(
                username=username,
                tweet_id=tweet_id,
                text=str(item.get("text") or ""),
                created_at=str(item.get("created_at") or "") or None,
                raw_json=raw_json if isinstance(raw_json, dict) else {},
            )
            written += 1
        self._auto_commit()
        return written

    def source_ref_stats(self) -> dict[str, Any]:
        provider_rows = self.conn.execute(
            """
            SELECT provider, COUNT(*) AS refs
            FROM source_refs
            GROUP BY provider
            ORDER BY refs DESC, provider ASC
            """
        ).fetchall()
        providers = [{"provider": str(r["provider"]), "refs": int(r["refs"])} for r in provider_rows]

        total_refs_row = self.conn.execute("SELECT COUNT(*) AS c FROM source_refs").fetchone()
        total_snapshots_row = self.conn.execute("SELECT COUNT(*) AS c FROM source_snapshots").fetchone()
        unresolved_row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM source_refs sr
            LEFT JOIN source_snapshots ss ON ss.snapshot_hash = sr.snapshot_hash
            WHERE sr.snapshot_hash IS NOT NULL
              AND ss.snapshot_hash IS NULL
            """
        ).fetchone()

        return {
            "providers": providers,
            "total_refs": int(total_refs_row["c"] if total_refs_row else 0),
            "total_snapshots": int(total_snapshots_row["c"] if total_snapshots_row else 0),
            "unresolved_snapshot_refs": int(unresolved_row["c"] if unresolved_row else 0),
        }

    def validate_source_refs(self, limit: int = 10000) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT sr.ref_id, sr.provider, sr.source_id, sr.url, sr.anchor_type, sr.anchor, sr.snapshot_hash
            FROM source_refs sr
            ORDER BY sr.captured_at DESC, sr.ref_id ASC
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()

        invalid: list[dict[str, str]] = []
        checked = 0
        for row in rows:
            checked += 1
            item = dict(row)
            provider = str(item.get("provider") or "")
            source_id = str(item.get("source_id") or "")
            anchor_type = str(item.get("anchor_type") or "")
            anchor = str(item.get("anchor") or "")
            snapshot_hash = item.get("snapshot_hash")

            if provider == "x":
                if anchor_type != "id":
                    invalid.append(
                        {
                            "ref_id": str(item.get("ref_id") or ""),
                            "reason": "x_provider_requires_id_anchor",
                        }
                    )
                    continue
                if anchor != source_id:
                    invalid.append(
                        {
                            "ref_id": str(item.get("ref_id") or ""),
                            "reason": "x_anchor_mismatch_source_id",
                        }
                    )
                    continue

            if snapshot_hash:
                snap = self.conn.execute(
                    "SELECT 1 FROM source_snapshots WHERE snapshot_hash = ? LIMIT 1",
                    (snapshot_hash,),
                ).fetchone()
                if not snap:
                    invalid.append(
                        {
                            "ref_id": str(item.get("ref_id") or ""),
                            "reason": "missing_snapshot",
                        }
                    )

        return {
            "checked": checked,
            "invalid_count": len(invalid),
            "invalid_refs": invalid,
        }

    def _canonicalize_source_ref_row(
        self,
        ref: dict[str, Any],
        *,
        fallback_username: str | None = None,
        fallback_created_at: str | None = None,
    ) -> dict[str, Any] | None:
        allowed_anchor_types = {"id", "hash", "dom", "timecode", "chunk"}
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        provider = str(ref.get("provider") or "").strip().lower()
        source_id = str(ref.get("source_id") or "").strip()
        anchor_type = str(ref.get("anchor_type") or "").strip().lower()
        anchor = str(ref.get("anchor") or "").strip()
        username = str(ref.get("username") or fallback_username or "").strip()
        tweet_id = str(ref.get("tweet_id") or "").strip()

        if not provider and (tweet_id or source_id or anchor):
            provider = "x"

        if provider == "x":
            if not source_id:
                source_id = tweet_id or anchor
            if not source_id:
                return None
            if not anchor_type or anchor_type not in allowed_anchor_types or anchor_type != "id":
                anchor_type = "id"
            if not anchor:
                anchor = source_id
            if not tweet_id:
                tweet_id = source_id

            canonical = self.get_source_ref(
                provider="x",
                source_id=source_id,
                anchor_type="id",
                anchor=source_id,
            )
            if canonical:
                url = str(canonical.get("url") or "")
                excerpt_hash = canonical.get("excerpt_hash")
                snapshot_hash = canonical.get("snapshot_hash")
                captured_at = str(canonical.get("captured_at") or now_iso)
                if not username:
                    username = str(canonical.get("username") or "")
                if not tweet_id:
                    tweet_id = str(canonical.get("tweet_id") or source_id)
            else:
                url = str(ref.get("url") or "")
                if not url:
                    if username:
                        url = f"https://x.com/{username}/status/{source_id}"
                    else:
                        url = f"https://x.com/i/web/status/{source_id}"
                excerpt_hash = ref.get("excerpt_hash")
                snapshot_hash = ref.get("snapshot_hash")
                captured_at = str(ref.get("captured_at") or fallback_created_at or now_iso)

            out: dict[str, Any] = {
                "provider": "x",
                "source_id": source_id,
                "url": url,
                "anchor_type": "id",
                "anchor": source_id,
                "excerpt_hash": excerpt_hash,
                "snapshot_hash": snapshot_hash,
                "captured_at": captured_at,
                "tweet_id": tweet_id or source_id,
            }
            if username:
                out["username"] = username
            return out

        if not provider:
            return None
        if not source_id:
            source_id = anchor
        if not source_id:
            return None
        if not anchor_type or anchor_type not in allowed_anchor_types:
            anchor_type = "id"
        if not anchor:
            anchor = source_id

        canonical = self.get_source_ref(
            provider=provider,
            source_id=source_id,
            anchor_type=anchor_type,
            anchor=anchor,
        )
        out = {
            "provider": provider,
            "source_id": source_id,
            "url": str((canonical or {}).get("url") or ref.get("url") or ""),
            "anchor_type": anchor_type,
            "anchor": anchor,
            "excerpt_hash": (canonical or {}).get("excerpt_hash") or ref.get("excerpt_hash"),
            "snapshot_hash": (canonical or {}).get("snapshot_hash") or ref.get("snapshot_hash"),
            "captured_at": str((canonical or {}).get("captured_at") or ref.get("captured_at") or fallback_created_at or now_iso),
        }
        return out

    def _looks_like_ref_object(self, payload: dict[str, Any]) -> bool:
        if "source_id" in payload:
            return True
        if "tweet_id" in payload and ("username" in payload or "provider" in payload):
            return True
        if "provider" in payload and ("anchor" in payload or "anchor_type" in payload):
            return True
        return False

    def _normalize_source_ref_list(
        self,
        refs: list[Any],
        *,
        fallback_username: str | None = None,
        fallback_created_at: str | None = None,
    ) -> tuple[list[dict[str, Any]], bool, int]:
        seen: set[tuple[str, str, str, str]] = set()
        out: list[dict[str, Any]] = []
        changed = False
        normalized_count = 0

        for item in refs:
            if not isinstance(item, dict):
                changed = True
                continue
            normalized = self._canonicalize_source_ref_row(
                item,
                fallback_username=fallback_username,
                fallback_created_at=fallback_created_at,
            )
            if not normalized:
                changed = True
                continue
            key = (
                str(normalized.get("provider") or ""),
                str(normalized.get("source_id") or ""),
                str(normalized.get("anchor_type") or ""),
                str(normalized.get("anchor") or ""),
            )
            if key in seen:
                changed = True
                continue
            seen.add(key)
            if normalized != item:
                changed = True
                normalized_count += 1
            out.append(normalized)

        if len(out) != len(refs):
            changed = True
        return out, changed, normalized_count

    def _normalize_refs_in_payload(
        self,
        payload: Any,
        *,
        fallback_username: str | None = None,
        fallback_created_at: str | None = None,
    ) -> tuple[Any, bool, int]:
        ref_keys = {"refs", "source_refs", "sources", "supports", "evidence_refs", "trigger_refs"}

        if isinstance(payload, list):
            if payload and all(isinstance(item, dict) for item in payload):
                dict_items = [item for item in payload if isinstance(item, dict)]
                if dict_items and any(self._looks_like_ref_object(item) for item in dict_items):
                    refs, refs_changed, refs_count = self._normalize_source_ref_list(
                        dict_items,
                        fallback_username=fallback_username,
                        fallback_created_at=fallback_created_at,
                    )
                    return refs, refs_changed, refs_count
            changed = False
            normalized_count = 0
            out_items: list[Any] = []
            for item in payload:
                new_item, item_changed, item_count = self._normalize_refs_in_payload(
                    item,
                    fallback_username=fallback_username,
                    fallback_created_at=fallback_created_at,
                )
                if item_changed:
                    changed = True
                normalized_count += item_count
                out_items.append(new_item)
            return out_items, changed, normalized_count

        if isinstance(payload, dict):
            if self._looks_like_ref_object(payload):
                normalized = self._canonicalize_source_ref_row(
                    payload,
                    fallback_username=fallback_username,
                    fallback_created_at=fallback_created_at,
                )
                if not normalized:
                    return payload, False, 0
                return normalized, normalized != payload, int(normalized != payload)
            changed = False
            normalized_count = 0
            out_obj = dict(payload)
            for key, value in payload.items():
                if key in ref_keys and isinstance(value, list):
                    refs, refs_changed, refs_count = self._normalize_source_ref_list(
                        value,
                        fallback_username=fallback_username,
                        fallback_created_at=fallback_created_at,
                    )
                    normalized_count += refs_count
                    if refs_changed:
                        out_obj[key] = refs
                        changed = True
                    continue

                new_value, value_changed, value_count = self._normalize_refs_in_payload(
                    value,
                    fallback_username=fallback_username,
                    fallback_created_at=fallback_created_at,
                )
                normalized_count += value_count
                if value_changed:
                    out_obj[key] = new_value
                    changed = True
            return out_obj, changed, normalized_count

        return payload, False, 0

    def _backfill_json_ref_column(
        self,
        *,
        table: str,
        json_col: str,
        limit_per_table: int,
        fallback_username_col: str | None = None,
        fallback_created_at_col: str | None = None,
    ) -> dict[str, int]:
        select_cols = ["rowid AS _rowid", json_col]
        if fallback_username_col:
            select_cols.append(fallback_username_col)
        if fallback_created_at_col:
            select_cols.append(fallback_created_at_col)
        rows = self.conn.execute(
            f"SELECT {', '.join(select_cols)} FROM {table} ORDER BY rowid ASC LIMIT ?",
            (max(1, limit_per_table),),
        ).fetchall()

        scanned = 0
        updated = 0
        refs_normalized = 0
        for row in rows:
            scanned += 1
            item = dict(row)
            rowid = int(item["_rowid"])
            raw = item.get(json_col)
            try:
                payload = json.loads(raw or "null")
            except json.JSONDecodeError:
                continue

            fallback_username = str(item.get(fallback_username_col) or "").strip() if fallback_username_col else None
            fallback_created_at = str(item.get(fallback_created_at_col) or "").strip() if fallback_created_at_col else None
            normalized_payload, changed, normalized_count = self._normalize_refs_in_payload(
                payload,
                fallback_username=fallback_username,
                fallback_created_at=fallback_created_at,
            )
            refs_normalized += normalized_count
            if not changed:
                continue
            self.conn.execute(
                f"UPDATE {table} SET {json_col} = ? WHERE rowid = ?",
                (json.dumps(normalized_payload, sort_keys=True), rowid),
            )
            updated += 1

        return {
            "rows_scanned": scanned,
            "rows_updated": updated,
            "refs_normalized": refs_normalized,
        }

    def backfill_legacy_source_ref_payloads(self, limit_per_table: int = 10000) -> dict[str, Any]:
        table_configs = [
            ("greene_cards", "source_refs_json", "username", "created_at"),
            ("idea_cards", "source_refs_json", "username", "created_at"),
            ("conflict_cards", "source_refs_json", None, "created_at"),
            ("conflicts", "source_refs_json", None, "created_at"),
            ("story_claims", "evidence_refs_json", None, "created_at"),
            ("briefing_items", "refs_json", None, "created_at"),
            ("briefing_items", "payload_json", None, "created_at"),
            ("briefings", "summary_json", None, "created_at"),
            ("stories", "summary_json", None, "created_at"),
            ("chapter_candidates", "payload_json", None, "created_at"),
            ("studio_outputs", "payload_json", None, "created_at"),
            ("staged_notes", "trigger_refs_json", None, "created_at"),
        ]

        per_table: dict[str, dict[str, int]] = {}
        totals = {"rows_scanned": 0, "rows_updated": 0, "refs_normalized": 0}
        for table, json_col, fallback_username_col, fallback_created_at_col in table_configs:
            key = f"{table}.{json_col}"
            stats = self._backfill_json_ref_column(
                table=table,
                json_col=json_col,
                limit_per_table=max(1, limit_per_table),
                fallback_username_col=fallback_username_col,
                fallback_created_at_col=fallback_created_at_col,
            )
            per_table[key] = stats
            totals["rows_scanned"] += stats["rows_scanned"]
            totals["rows_updated"] += stats["rows_updated"]
            totals["refs_normalized"] += stats["refs_normalized"]

        self._auto_commit()
        return {"tables": per_table, **totals}

    def _collect_ref_issues(
        self,
        payload: Any,
        *,
        table: str,
        row_pointer: str,
        issues: list[dict[str, str]],
        path: str,
        checked_refs: list[int],
        max_issues: int,
    ) -> None:
        ref_keys = {"refs", "source_refs", "sources", "supports", "evidence_refs", "trigger_refs"}
        if len(issues) >= max_issues:
            return

        if isinstance(payload, dict):
            if self._looks_like_ref_object(payload):
                checked_refs[0] += 1
                provider = str(payload.get("provider") or "").strip().lower()
                source_id = str(payload.get("source_id") or "").strip()
                anchor_type = str(payload.get("anchor_type") or "").strip().lower()
                anchor = str(payload.get("anchor") or "").strip()
                tweet_id = str(payload.get("tweet_id") or "").strip()
                if not provider and tweet_id:
                    provider = "x"
                if provider == "x" and not source_id:
                    source_id = tweet_id or anchor
                if not source_id:
                    issues.append(
                        {
                            "table": table,
                            "row": row_pointer,
                            "path": path,
                            "reason": "missing_source_id",
                        }
                    )
                    return
                if not anchor_type:
                    issues.append(
                        {
                            "table": table,
                            "row": row_pointer,
                            "path": path,
                            "reason": "missing_anchor_type",
                        }
                    )
                    return
                if not anchor:
                    issues.append(
                        {
                            "table": table,
                            "row": row_pointer,
                            "path": path,
                            "reason": "missing_anchor",
                        }
                    )
                    return
                existing = self.get_source_ref(
                    provider=provider or "x",
                    source_id=source_id,
                    anchor_type=anchor_type,
                    anchor=anchor,
                )
                if not existing:
                    issues.append(
                        {
                            "table": table,
                            "row": row_pointer,
                            "path": path,
                            "reason": "unresolved_source_ref",
                        }
                    )
                return

            for key, value in payload.items():
                child_path = f"{path}.{key}" if path else key
                if key in ref_keys and isinstance(value, list):
                    for idx, ref in enumerate(value):
                        checked_refs[0] += 1
                        if not isinstance(ref, dict):
                            issues.append(
                                {
                                    "table": table,
                                    "row": row_pointer,
                                    "path": f"{child_path}[{idx}]",
                                    "reason": "ref_not_object",
                                }
                            )
                            if len(issues) >= max_issues:
                                return
                            continue
                        provider = str(ref.get("provider") or "").strip().lower()
                        source_id = str(ref.get("source_id") or "").strip()
                        anchor_type = str(ref.get("anchor_type") or "").strip().lower()
                        anchor = str(ref.get("anchor") or "").strip()
                        tweet_id = str(ref.get("tweet_id") or "").strip()
                        if not provider and tweet_id:
                            provider = "x"
                        if provider == "x" and not source_id:
                            source_id = tweet_id or anchor
                        if not source_id:
                            issues.append(
                                {
                                    "table": table,
                                    "row": row_pointer,
                                    "path": f"{child_path}[{idx}]",
                                    "reason": "missing_source_id",
                                }
                            )
                            if len(issues) >= max_issues:
                                return
                            continue
                        if not anchor_type:
                            issues.append(
                                {
                                    "table": table,
                                    "row": row_pointer,
                                    "path": f"{child_path}[{idx}]",
                                    "reason": "missing_anchor_type",
                                }
                            )
                            if len(issues) >= max_issues:
                                return
                            continue
                        if not anchor:
                            issues.append(
                                {
                                    "table": table,
                                    "row": row_pointer,
                                    "path": f"{child_path}[{idx}]",
                                    "reason": "missing_anchor",
                                }
                            )
                            if len(issues) >= max_issues:
                                return
                            continue
                        existing = self.get_source_ref(
                            provider=provider or "x",
                            source_id=source_id,
                            anchor_type=anchor_type,
                            anchor=anchor,
                        )
                        if not existing:
                            issues.append(
                                {
                                    "table": table,
                                    "row": row_pointer,
                                    "path": f"{child_path}[{idx}]",
                                    "reason": "unresolved_source_ref",
                                }
                            )
                            if len(issues) >= max_issues:
                                return
                    continue
                self._collect_ref_issues(
                    value,
                    table=table,
                    row_pointer=row_pointer,
                    issues=issues,
                    path=child_path,
                    checked_refs=checked_refs,
                    max_issues=max_issues,
                )
                if len(issues) >= max_issues:
                    return
            return

        if isinstance(payload, list):
            for idx, item in enumerate(payload):
                child_path = f"{path}[{idx}]"
                self._collect_ref_issues(
                    item,
                    table=table,
                    row_pointer=row_pointer,
                    issues=issues,
                    path=child_path,
                    checked_refs=checked_refs,
                    max_issues=max_issues,
                )
                if len(issues) >= max_issues:
                    return

    def validate_source_ref_payloads(self, limit_per_table: int = 10000, max_issues: int = 200) -> dict[str, Any]:
        table_configs = [
            ("greene_cards", "source_refs_json"),
            ("idea_cards", "source_refs_json"),
            ("conflict_cards", "source_refs_json"),
            ("conflicts", "source_refs_json"),
            ("story_claims", "evidence_refs_json"),
            ("briefing_items", "refs_json"),
            ("briefing_items", "payload_json"),
            ("briefings", "summary_json"),
            ("stories", "summary_json"),
            ("chapter_candidates", "payload_json"),
            ("studio_outputs", "payload_json"),
            ("staged_notes", "trigger_refs_json"),
        ]

        issues: list[dict[str, str]] = []
        checked_refs = [0]
        rows_scanned = 0
        for table, json_col in table_configs:
            rows = self.conn.execute(
                f"SELECT rowid AS _rowid, {json_col} FROM {table} ORDER BY rowid ASC LIMIT ?",
                (max(1, limit_per_table),),
            ).fetchall()
            for row in rows:
                rows_scanned += 1
                item = dict(row)
                row_pointer = f"{table}:rowid={item['_rowid']}"
                raw = item.get(json_col)
                try:
                    payload = json.loads(raw or "null")
                except json.JSONDecodeError:
                    issues.append(
                        {
                            "table": table,
                            "row": row_pointer,
                            "path": json_col,
                            "reason": "invalid_json",
                        }
                    )
                    if len(issues) >= max_issues:
                        break
                    continue

                self._collect_ref_issues(
                    payload,
                    table=table,
                    row_pointer=row_pointer,
                    issues=issues,
                    path=json_col,
                    checked_refs=checked_refs,
                    max_issues=max_issues,
                )
                if len(issues) >= max_issues:
                    break
            if len(issues) >= max_issues:
                break

        return {
            "rows_scanned": rows_scanned,
            "checked_refs": checked_refs[0],
            "invalid_count": len(issues),
            "invalid_refs": issues,
        }

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

    def upsert_briefing(
        self,
        *,
        brief_id: str,
        run_id: str,
        brief_date: str,
        note_path: str,
        summary: dict[str, Any],
        created_at: str,
        updated_at: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO briefings(brief_id, run_id, brief_date, note_path, summary_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(brief_id) DO UPDATE SET
              run_id = excluded.run_id,
              note_path = excluded.note_path,
              summary_json = excluded.summary_json,
              updated_at = excluded.updated_at
            """,
            (
                brief_id,
                run_id,
                brief_date,
                note_path,
                json.dumps(summary, sort_keys=True),
                created_at,
                updated_at,
            ),
        )
        self._auto_commit()

    def get_latest_briefing(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT brief_id, run_id, brief_date, note_path, summary_json, created_at, updated_at
            FROM briefings
            ORDER BY datetime(updated_at) DESC, brief_id DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["summary"] = json.loads(item.pop("summary_json") or "{}")
        return item

    def get_briefing_by_date(self, brief_date: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT brief_id, run_id, brief_date, note_path, summary_json, created_at, updated_at
            FROM briefings
            WHERE brief_date = ?
            ORDER BY datetime(updated_at) DESC
            LIMIT 1
            """,
            (brief_date,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["summary"] = json.loads(item.pop("summary_json") or "{}")
        return item

    def replace_briefing_items(
        self,
        *,
        brief_id: str,
        run_id: str,
        items: list[dict[str, Any]],
        created_at: str,
    ) -> None:
        self.conn.execute("DELETE FROM briefing_items WHERE brief_id = ?", (brief_id,))
        for item in items:
            self.conn.execute(
                """
                INSERT INTO briefing_items(
                  item_id, brief_id, run_id, item_type, rank, score, refs_json, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(item["item_id"]),
                    brief_id,
                    run_id,
                    str(item["item_type"]),
                    int(item["rank"]),
                    float(item.get("score") or 0.0),
                    json.dumps(item.get("refs", []), sort_keys=True),
                    json.dumps(item.get("payload", {}), sort_keys=True),
                    created_at,
                ),
            )
        self._auto_commit()

    def list_briefing_items(self, brief_id: str, item_type: str | None = None) -> list[dict[str, Any]]:
        if item_type:
            rows = self.conn.execute(
                """
                SELECT item_id, brief_id, run_id, item_type, rank, score, refs_json, payload_json, created_at
                FROM briefing_items
                WHERE brief_id = ? AND item_type = ?
                ORDER BY rank ASC, item_id ASC
                """,
                (brief_id, item_type),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT item_id, brief_id, run_id, item_type, rank, score, refs_json, payload_json, created_at
                FROM briefing_items
                WHERE brief_id = ?
                ORDER BY item_type ASC, rank ASC, item_id ASC
                """,
                (brief_id,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["refs"] = json.loads(item.pop("refs_json") or "[]")
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            out.append(item)
        return out

    def upsert_greene_cards(self, cards: list[dict[str, Any]]) -> int:
        upserted = 0
        for card in cards:
            self.conn.execute(
                """
                INSERT INTO greene_cards(
                  card_id, run_id, story_id, username, week_key, card_type, title, payload, why_it_matters,
                  source_refs_json, theme, principle, strategic_use_case, reusable_quote,
                  confidence, state, score, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(card_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  story_id = excluded.story_id,
                  username = excluded.username,
                  week_key = excluded.week_key,
                  card_type = excluded.card_type,
                  title = excluded.title,
                  payload = excluded.payload,
                  why_it_matters = excluded.why_it_matters,
                  source_refs_json = excluded.source_refs_json,
                  theme = excluded.theme,
                  principle = excluded.principle,
                  strategic_use_case = excluded.strategic_use_case,
                  reusable_quote = excluded.reusable_quote,
                  confidence = excluded.confidence,
                  state = excluded.state,
                  score = excluded.score,
                  updated_at = excluded.updated_at
                """,
                (
                    str(card["card_id"]),
                    str(card["run_id"]),
                    card.get("story_id"),
                    card.get("username"),
                    str(card["week_key"]),
                    str(card["card_type"]),
                    str(card["title"]),
                    str(card["payload"]),
                    str(card.get("why_it_matters") or ""),
                    json.dumps(card.get("source_refs", []), sort_keys=True),
                    card.get("theme"),
                    card.get("principle"),
                    card.get("strategic_use_case"),
                    card.get("reusable_quote"),
                    str(card.get("confidence") or "medium"),
                    str(card.get("state") or "captured"),
                    float(card.get("score") or 0.0),
                    str(card["created_at"]),
                    str(card["updated_at"]),
                ),
            )
            upserted += 1
        self._auto_commit()
        return upserted

    def list_greene_cards(
        self,
        *,
        state: str | None = None,
        week_key: str | None = None,
        story_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        args: list[Any] = []
        if state:
            where.append("state = ?")
            args.append(state)
        if week_key:
            where.append("week_key = ?")
            args.append(week_key)
        if story_id:
            where.append("story_id = ?")
            args.append(story_id)
        args.append(max(1, limit))
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = self.conn.execute(
            f"""
            SELECT card_id, run_id, story_id, username, week_key, card_type, title, payload, why_it_matters,
                   source_refs_json, theme, principle, strategic_use_case, reusable_quote,
                   confidence, state, score, created_at, updated_at
            FROM greene_cards
            {where_sql}
            ORDER BY score DESC, datetime(updated_at) DESC, card_id ASC
            LIMIT ?
            """,
            tuple(args),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["source_refs"] = json.loads(item.pop("source_refs_json") or "[]")
            out.append(item)
        return out

    def get_greene_card(self, card_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT card_id, run_id, story_id, username, week_key, card_type, title, payload, why_it_matters,
                   source_refs_json, theme, principle, strategic_use_case, reusable_quote,
                   confidence, state, score, created_at, updated_at
            FROM greene_cards
            WHERE card_id = ?
            LIMIT 1
            """,
            (card_id,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["source_refs"] = json.loads(item.pop("source_refs_json") or "[]")
        return item

    def set_greene_card_state(self, card_id: str, *, state: str, score: float, updated_at: str) -> bool:
        cur = self.conn.execute(
            """
            UPDATE greene_cards
            SET state = ?, score = ?, updated_at = ?
            WHERE card_id = ?
            """,
            (state, float(score), updated_at, card_id),
        )
        self._auto_commit()
        return bool(cur.rowcount > 0)

    def add_card_feedback(self, *, card_id: str, feedback: str, note: str | None, created_at: str) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO card_feedback(card_id, feedback, note, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (card_id, feedback, note, created_at),
        )
        self._auto_commit()
        return int(cur.lastrowid)

    def list_card_feedback(self, card_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT feedback_id, card_id, feedback, note, created_at
            FROM card_feedback
            WHERE card_id = ?
            ORDER BY feedback_id DESC
            LIMIT ?
            """,
            (card_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def feedback_score_for_card(self, card_id: str) -> float:
        rows = self.list_card_feedback(card_id, limit=200)
        weights = {"good": 2.0, "bad": -2.0, "wrong_pile": -1.0, "wrong_story": -1.5}
        score = 0.0
        for row in rows:
            score += float(weights.get(str(row.get("feedback") or ""), 0.0))
        return score

    def replace_chapter_candidates(
        self,
        *,
        run_id: str,
        toc_style: str,
        rows: list[dict[str, Any]],
        created_at: str,
    ) -> int:
        self.conn.execute(
            "DELETE FROM chapter_candidates WHERE run_id = ? AND toc_style = ?",
            (run_id, toc_style),
        )
        inserted = 0
        for row in rows:
            chapter_id = str(row.get("chapter_id") or "")
            if not chapter_id:
                continue
            self.conn.execute(
                """
                INSERT OR REPLACE INTO chapter_candidates(chapter_id, run_id, toc_style, thesis, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chapter_id,
                    run_id,
                    toc_style,
                    str(row.get("thesis") or ""),
                    json.dumps(row, sort_keys=True),
                    created_at,
                ),
            )
            inserted += 1
        self._auto_commit()
        return inserted

    def list_chapter_candidates(
        self,
        *,
        run_id: str | None = None,
        toc_style: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        args: list[Any] = []
        if run_id:
            where.append("run_id = ?")
            args.append(run_id)
        if toc_style:
            where.append("toc_style = ?")
            args.append(toc_style)
        args.append(max(1, limit))
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = self.conn.execute(
            f"""
            SELECT chapter_id, run_id, toc_style, thesis, payload_json, created_at
            FROM chapter_candidates
            {where_sql}
            ORDER BY datetime(created_at) DESC, chapter_id ASC
            LIMIT ?
            """,
            tuple(args),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload = json.loads(item.pop("payload_json") or "{}")
            if isinstance(payload, dict):
                payload.setdefault("chapter_id", item["chapter_id"])
                payload.setdefault("run_id", item["run_id"])
                payload.setdefault("toc_style", item["toc_style"])
                payload.setdefault("thesis", item["thesis"])
                payload.setdefault("created_at", item["created_at"])
                out.append(payload)
            else:
                out.append(item)
        return out

    def upsert_studio_output(
        self,
        *,
        output_id: str,
        run_id: str,
        mode: str,
        topic: str | None,
        output_path: str,
        payload: dict[str, Any],
        created_at: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO studio_outputs(output_id, run_id, mode, topic, output_path, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                output_id,
                run_id,
                mode,
                topic,
                output_path,
                json.dumps(payload, sort_keys=True),
                created_at,
            ),
        )
        self._auto_commit()

    def get_latest_studio_output(self, *, mode: str | None = None) -> dict[str, Any] | None:
        if mode:
            row = self.conn.execute(
                """
                SELECT output_id, run_id, mode, topic, output_path, payload_json, created_at
                FROM studio_outputs
                WHERE mode = ?
                ORDER BY datetime(created_at) DESC, output_id DESC
                LIMIT 1
                """,
                (mode,),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT output_id, run_id, mode, topic, output_path, payload_json, created_at
                FROM studio_outputs
                ORDER BY datetime(created_at) DESC, output_id DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        return item

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

    def list_runs(self, limit: int = 100, exclude_run_id: str | None = None) -> list[dict[str, Any]]:
        if exclude_run_id:
            rows = self.conn.execute(
                """
                SELECT run_id, mode, started_at, finished_at, stats_json
                FROM runs
                WHERE run_id != ?
                ORDER BY datetime(started_at) DESC
                LIMIT ?
                """,
                (exclude_run_id, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT run_id, mode, started_at, finished_at, stats_json
                FROM runs
                ORDER BY datetime(started_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["stats_json"] = json.loads(item["stats_json"]) if item.get("stats_json") else {}
            out.append(item)
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

    def upsert_conflicts(self, conflicts: list[dict[str, Any]]) -> int:
        upserted = 0
        for row in conflicts:
            self.conn.execute(
                """
                INSERT INTO conflicts(
                  conflict_id, run_id, topic, claim_a_json, claim_b_json, source_refs_json, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(conflict_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  topic = excluded.topic,
                  claim_a_json = excluded.claim_a_json,
                  claim_b_json = excluded.claim_b_json,
                  source_refs_json = excluded.source_refs_json,
                  updated_at = excluded.updated_at
                """,
                (
                    row["conflict_id"],
                    row["run_id"],
                    row["topic"],
                    json.dumps(row["claim_a"], sort_keys=True),
                    json.dumps(row["claim_b"], sort_keys=True),
                    json.dumps(row.get("source_refs", []), sort_keys=True),
                    row.get("status", "open"),
                    row["created_at"],
                    row["updated_at"],
                ),
            )
            upserted += 1
        self._auto_commit()
        return upserted

    def list_conflicts(self, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if status:
            rows = self.conn.execute(
                """
                SELECT conflict_id, run_id, topic, claim_a_json, claim_b_json, source_refs_json, status, created_at, updated_at
                FROM conflicts
                WHERE status = ?
                ORDER BY datetime(updated_at) DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT conflict_id, run_id, topic, claim_a_json, claim_b_json, source_refs_json, status, created_at, updated_at
                FROM conflicts
                ORDER BY datetime(updated_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["claim_a"] = json.loads(item.pop("claim_a_json"))
            item["claim_b"] = json.loads(item.pop("claim_b_json"))
            item["source_refs"] = json.loads(item.pop("source_refs_json"))
            out.append(item)
        return out

    def set_conflict_status(self, conflict_id: str, status: str, updated_at: str) -> bool:
        cur = self.conn.execute(
            """
            UPDATE conflicts
            SET status = ?, updated_at = ?
            WHERE conflict_id = ?
            """,
            (status, updated_at, conflict_id),
        )
        self._auto_commit()
        return bool(cur.rowcount > 0)

    def add_confidence_event(
        self,
        *,
        story_id: str,
        run_id: str,
        previous_confidence: str | None,
        new_confidence: str,
        reason: str,
        created_at: str,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO confidence_events(story_id, run_id, previous_confidence, new_confidence, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (story_id, run_id, previous_confidence, new_confidence, reason, created_at),
        )
        self._auto_commit()
        return int(cur.lastrowid)

    def list_confidence_events(self, story_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT event_id, story_id, run_id, previous_confidence, new_confidence, reason, created_at
            FROM confidence_events
            WHERE story_id = ?
            ORDER BY datetime(created_at) DESC, event_id DESC
            LIMIT ?
            """,
            (story_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def upsert_story_claims(self, claims: list[dict[str, Any]]) -> int:
        upserted = 0
        for claim in claims:
            self.conn.execute(
                """
                INSERT INTO story_claims(
                  claim_id, story_id, run_id, claim_text, evidence_refs_json,
                  confidence, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(claim_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  claim_text = excluded.claim_text,
                  evidence_refs_json = excluded.evidence_refs_json,
                  confidence = excluded.confidence,
                  status = excluded.status,
                  updated_at = excluded.updated_at
                """,
                (
                    claim["claim_id"],
                    claim["story_id"],
                    claim["run_id"],
                    claim["claim_text"],
                    json.dumps(claim.get("evidence_refs", []), sort_keys=True),
                    claim["confidence"],
                    claim.get("status", "active"),
                    claim["created_at"],
                    claim["updated_at"],
                ),
            )
            upserted += 1
        self._auto_commit()
        return upserted

    def list_story_claims(self, story_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT claim_id, story_id, run_id, claim_text, evidence_refs_json, confidence, status, created_at, updated_at
            FROM story_claims
            WHERE story_id = ?
            ORDER BY datetime(updated_at) DESC
            LIMIT ?
            """,
            (story_id, limit),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["evidence_refs"] = json.loads(item.pop("evidence_refs_json"))
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
