from __future__ import annotations

import sqlite3
from pathlib import Path


def _migrate_note_index_if_needed(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'note_index'"
    ).fetchone()
    if not row:
        return
    sql = row["sql"] if isinstance(row, sqlite3.Row) else row[0]
    required = {"'story'", "'idea'", "'shuffle'", "'conflict'", "'entity'", "'briefing'", "'greene'"}
    if sql and all(token in sql for token in required):
        return

    conn.execute("DROP INDEX IF EXISTS idx_note_index_type_updated")
    conn.execute("ALTER TABLE note_index RENAME TO note_index_old")
    conn.executescript(
        """
        CREATE TABLE note_index (
          note_path TEXT PRIMARY KEY,
          note_type TEXT NOT NULL CHECK (
            note_type IN ('user', 'digest', 'story', 'idea', 'shuffle', 'conflict', 'entity', 'briefing', 'greene')
          ),
          username TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          last_run_id TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_note_index_type_updated
          ON note_index(note_type, updated_at DESC);
        """
    )
    conn.execute(
        """
        INSERT INTO note_index(note_path, note_type, username, created_at, updated_at, last_run_id)
        SELECT note_path, note_type, username, created_at, updated_at, last_run_id
        FROM note_index_old
        """
    )
    conn.execute("DROP TABLE note_index_old")


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    schema_path = Path(__file__).with_name("schema.sql")
    conn.executescript(schema_path.read_text(encoding="utf-8"))
    _migrate_note_index_if_needed(conn)
    conn.commit()
