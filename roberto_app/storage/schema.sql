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
  note_type TEXT NOT NULL CHECK (note_type IN ('user', 'digest', 'story', 'idea', 'shuffle', 'conflict', 'entity', 'briefing', 'greene')),
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

CREATE TABLE IF NOT EXISTS briefings (
  brief_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  brief_date TEXT NOT NULL,
  note_path TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_briefings_date_updated
  ON briefings(brief_date DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS briefing_items (
  item_id TEXT PRIMARY KEY,
  brief_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  item_type TEXT NOT NULL CHECK (item_type IN ('story_delta', 'connection', 'idea')),
  rank INTEGER NOT NULL,
  score REAL NOT NULL,
  refs_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (brief_id) REFERENCES briefings(brief_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_briefing_items_brief
  ON briefing_items(brief_id, item_type, rank ASC);

CREATE TABLE IF NOT EXISTS greene_cards (
  card_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  story_id TEXT,
  username TEXT,
  week_key TEXT NOT NULL,
  card_type TEXT NOT NULL CHECK (card_type IN ('claim', 'evidence', 'angle')),
  title TEXT NOT NULL,
  payload TEXT NOT NULL,
  why_it_matters TEXT NOT NULL,
  source_refs_json TEXT NOT NULL,
  theme TEXT,
  principle TEXT,
  strategic_use_case TEXT,
  reusable_quote TEXT,
  confidence TEXT NOT NULL CHECK (confidence IN ('high', 'medium', 'low')),
  state TEXT NOT NULL CHECK (state IN ('captured', 'distilled', 'keeper', 'rejected')),
  score REAL NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_greene_cards_week_state
  ON greene_cards(week_key, state, score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_greene_cards_story
  ON greene_cards(story_id, week_key, score DESC);

CREATE TABLE IF NOT EXISTS card_feedback (
  feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
  card_id TEXT NOT NULL,
  feedback TEXT NOT NULL CHECK (feedback IN ('good', 'bad', 'wrong_pile', 'wrong_story')),
  note TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(card_id) REFERENCES greene_cards(card_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_card_feedback_card_created
  ON card_feedback(card_id, created_at DESC);

CREATE TABLE IF NOT EXISTS chapter_candidates (
  chapter_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  toc_style TEXT NOT NULL CHECK (toc_style IN ('chronological', 'thematic', 'strategy')),
  thesis TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chapter_candidates_run_style
  ON chapter_candidates(run_id, toc_style, created_at DESC);

CREATE TABLE IF NOT EXISTS studio_outputs (
  output_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('memo', 'brief', 'essay-skeleton', 'chapter-draft', 'compile')),
  topic TEXT,
  output_path TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_studio_outputs_run_mode
  ON studio_outputs(run_id, mode, created_at DESC);

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

CREATE TABLE IF NOT EXISTS staged_notes (
  run_id TEXT NOT NULL,
  live_path TEXT NOT NULL,
  staged_path TEXT NOT NULL,
  mode TEXT NOT NULL,
  note_type TEXT NOT NULL,
  trigger_refs_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('staged', 'promoted', 'discarded')),
  created_at TEXT NOT NULL,
  promoted_at TEXT,
  PRIMARY KEY (run_id, live_path)
);

CREATE INDEX IF NOT EXISTS idx_staged_notes_run_status
  ON staged_notes(run_id, status);

CREATE TABLE IF NOT EXISTS note_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  note_path TEXT NOT NULL,
  run_id TEXT,
  captured_at TEXT NOT NULL,
  reason TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  content TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_note_snapshots_note
  ON note_snapshots(note_path, snapshot_id DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5(
  kind UNINDEXED,
  subtype UNINDEXED,
  item_id UNINDEXED,
  ref_path UNINDEXED,
  source_ids,
  title,
  body,
  tags,
  username,
  entity,
  created_at UNINDEXED
);

CREATE TABLE IF NOT EXISTS story_aliases (
  alias_slug TEXT PRIMARY KEY,
  story_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(story_id) REFERENCES stories(story_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_story_aliases_story_id
  ON story_aliases(story_id);

CREATE TABLE IF NOT EXISTS story_lineage (
  parent_story_id TEXT NOT NULL,
  child_story_id TEXT NOT NULL,
  relation TEXT NOT NULL CHECK (relation IN ('merge_into', 'split_into')),
  created_at TEXT NOT NULL,
  PRIMARY KEY(parent_story_id, child_story_id, relation),
  FOREIGN KEY(parent_story_id) REFERENCES stories(story_id) ON DELETE CASCADE,
  FOREIGN KEY(child_story_id) REFERENCES stories(story_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attention_state (
  target_type TEXT NOT NULL CHECK (target_type IN ('story', 'entity')),
  target_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active', 'pinned', 'muted', 'snoozed')),
  snoozed_until TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(target_type, target_id)
);

CREATE INDEX IF NOT EXISTS idx_attention_state_state
  ON attention_state(target_type, state, snoozed_until);

CREATE TABLE IF NOT EXISTS conflicts (
  conflict_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  claim_a_json TEXT NOT NULL,
  claim_b_json TEXT NOT NULL,
  source_refs_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_conflicts_status_updated
  ON conflicts(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS confidence_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  story_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  previous_confidence TEXT,
  new_confidence TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(story_id) REFERENCES stories(story_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_confidence_events_story
  ON confidence_events(story_id, created_at DESC);

CREATE TABLE IF NOT EXISTS story_claims (
  claim_id TEXT PRIMARY KEY,
  story_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  claim_text TEXT NOT NULL,
  evidence_refs_json TEXT NOT NULL,
  confidence TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'retracted', 'contested')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(story_id) REFERENCES stories(story_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_story_claims_story
  ON story_claims(story_id, updated_at DESC);
