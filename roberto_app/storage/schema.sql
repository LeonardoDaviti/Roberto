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
  note_type TEXT NOT NULL CHECK (note_type IN ('user', 'digest', 'story', 'idea', 'shuffle', 'conflict', 'entity')),
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

CREATE TABLE IF NOT EXISTS llm_embeddings (
  embedding_key TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  item_id TEXT NOT NULL,
  text_hash TEXT NOT NULL,
  vector_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_embeddings_kind_item
  ON llm_embeddings(kind, item_id);

CREATE TABLE IF NOT EXISTS stories (
  story_id TEXT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  first_seen_run_id TEXT NOT NULL,
  last_seen_run_id TEXT NOT NULL,
  mention_count INTEGER NOT NULL DEFAULT 0,
  confidence TEXT NOT NULL,
  tags_json TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stories_updated
  ON stories(updated_at DESC);

CREATE TABLE IF NOT EXISTS story_sources (
  story_id TEXT NOT NULL,
  username TEXT NOT NULL,
  tweet_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (story_id, username, tweet_id, run_id),
  FOREIGN KEY (story_id) REFERENCES stories(story_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_story_sources_story
  ON story_sources(story_id, created_at DESC);

CREATE TABLE IF NOT EXISTS idea_cards (
  card_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  username TEXT NOT NULL,
  idea_type TEXT NOT NULL CHECK (idea_type IN ('essay', 'product', 'experiment')),
  title TEXT NOT NULL,
  hypothesis TEXT NOT NULL,
  why_now TEXT NOT NULL,
  tags_json TEXT NOT NULL,
  source_refs_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idea_cards_created
  ON idea_cards(created_at DESC);

CREATE TABLE IF NOT EXISTS conflict_cards (
  conflict_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  title TEXT NOT NULL,
  claim_a_json TEXT NOT NULL,
  claim_b_json TEXT NOT NULL,
  tags_json TEXT NOT NULL,
  source_refs_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_conflict_cards_created
  ON conflict_cards(created_at DESC);

CREATE TABLE IF NOT EXISTS entities (
  entity_id TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL UNIQUE,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_aliases (
  alias TEXT PRIMARY KEY,
  entity_id TEXT NOT NULL,
  FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS entity_links (
  entity_id TEXT NOT NULL,
  ref_type TEXT NOT NULL CHECK (ref_type IN ('tweet', 'story')),
  ref_id TEXT NOT NULL,
  username TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(entity_id, ref_type, ref_id),
  FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entity_links_created
  ON entity_links(created_at DESC);

CREATE TABLE IF NOT EXISTS story_entities (
  story_id TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(story_id, entity_id),
  FOREIGN KEY(story_id) REFERENCES stories(story_id) ON DELETE CASCADE,
  FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE
);
