PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  user_id TEXT,
  display_name TEXT,
  last_seen_tweet_id TEXT,
  last_polled_at TEXT
);

CREATE TABLE IF NOT EXISTS tweets (
  tweet_id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  created_at TEXT,
  text TEXT NOT NULL,
  json TEXT NOT NULL,
  FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tweets_username_created
  ON tweets(username, created_at DESC);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL CHECK (mode IN ('v1', 'v2')),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  stats_json TEXT
);

CREATE TABLE IF NOT EXISTS note_index (
  note_path TEXT PRIMARY KEY,
  note_type TEXT NOT NULL CHECK (note_type IN ('user', 'digest')),
  username TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_run_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_note_index_type_updated
  ON note_index(note_type, updated_at DESC);

CREATE TABLE IF NOT EXISTS llm_cache (
  cache_key TEXT PRIMARY KEY,
  response_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
