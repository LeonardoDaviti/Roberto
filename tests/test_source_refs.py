from __future__ import annotations

import json

from roberto_app.storage.repo import StorageRepo, StoryUpsert


def test_insert_tweets_dual_writes_source_ref_and_snapshot(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    repo.upsert_user("alice", "id_alice", "Alice")

    inserted = repo.insert_tweets(
        "alice",
        [
            {
                "id": "123",
                "text": "Kernel launch latency remains the bottleneck",
                "created_at": "2026-03-03T10:00:00Z",
                "public_metrics": {"like_count": 7},
            }
        ],
    )

    assert inserted == 1

    source_ref = repo.get_source_ref(provider="x", source_id="123")
    assert source_ref is not None
    assert source_ref["provider"] == "x"
    assert source_ref["anchor_type"] == "id"
    assert source_ref["anchor"] == "123"
    assert source_ref["username"] == "alice"
    assert source_ref["tweet_id"] == "123"

    snapshot_hash = str(source_ref["snapshot_hash"])
    snapshot = repo.get_source_snapshot(snapshot_hash)
    assert snapshot is not None
    assert snapshot["provider"] == "x"
    assert snapshot["source_id"] == "123"
    assert snapshot["metadata"]["username"] == "alice"
    assert snapshot["metadata"]["raw"]["id"] == "123"

    repo.close()


def test_backfill_x_source_refs_for_legacy_rows(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    repo.upsert_user("alice", "id_alice", "Alice")

    legacy_raw = {
        "id": "9001",
        "text": "Legacy row inserted before SourceRef dual-write",
        "created_at": "2026-03-03T11:00:00Z",
    }
    repo.conn.execute(
        """
        INSERT INTO tweets(tweet_id, username, created_at, text, json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "9001",
            "alice",
            "2026-03-03T11:00:00Z",
            legacy_raw["text"],
            json.dumps(legacy_raw, sort_keys=True),
        ),
    )
    repo.conn.commit()

    assert repo.get_source_ref(provider="x", source_id="9001") is None

    written = repo.backfill_x_source_refs(limit=10)
    assert written == 1
    assert repo.get_source_ref(provider="x", source_id="9001") is not None

    written_again = repo.backfill_x_source_refs(limit=10)
    assert written_again == 0

    repo.close()


def test_validate_source_refs_flags_invalid_x_anchor_contract(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    repo.conn.execute(
        """
        INSERT INTO source_refs(
          ref_id, provider, source_id, url, anchor_type, anchor, excerpt_hash, snapshot_hash,
          captured_at, username, tweet_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "bad-ref",
            "x",
            "111",
            "https://x.com/alice/status/111",
            "hash",
            "sha256:bad",
            None,
            None,
            "2026-03-03T12:00:00Z",
            "alice",
            "111",
        ),
    )
    repo.conn.commit()

    payload = repo.validate_source_refs(limit=10)
    assert payload["checked"] >= 1
    assert payload["invalid_count"] >= 1
    reasons = {row["reason"] for row in payload["invalid_refs"]}
    assert "x_provider_requires_id_anchor" in reasons

    repo.close()


def test_backfill_legacy_payload_refs_updates_idea_cards_and_story_summary(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    repo.upsert_user("alice", "id_alice", "Alice")
    repo.insert_tweets(
        "alice",
        [
            {
                "id": "321",
                "text": "Source row for payload ref migration",
                "created_at": "2026-03-03T13:00:00Z",
            }
        ],
    )

    repo.insert_idea_cards(
        [
            {
                "card_id": "idea:1",
                "run_id": "run:1",
                "username": "alice",
                "idea_type": "essay",
                "title": "Idea title",
                "hypothesis": "Hypothesis",
                "why_now": "Why now",
                "tags": ["ai"],
                "source_refs": [{"username": "alice", "tweet_id": "321"}],
                "created_at": "2026-03-03T13:05:00Z",
            }
        ]
    )

    repo.upsert_story(
        StoryUpsert(
            story_id="story:alpha",
            slug="alpha",
            title="Alpha",
            run_id="run:1",
            confidence="high",
            tags=["ai"],
            summary_json={
                "title": "Alpha",
                "what_happened": "Something changed",
                "why_it_matters": "It matters",
                "sources": [{"username": "alice", "tweet_id": "321"}],
                "tags": ["ai"],
                "confidence": "high",
            },
            now_iso="2026-03-03T13:05:00Z",
        )
    )

    migrated = repo.backfill_legacy_source_ref_payloads(limit_per_table=1000)
    assert migrated["rows_updated"] >= 2
    assert migrated["refs_normalized"] >= 2

    idea_rows = repo.list_recent_idea_cards(days=365, limit=10, username="alice")
    assert idea_rows
    idea_ref = idea_rows[0]["source_refs"][0]
    assert idea_ref["provider"] == "x"
    assert idea_ref["source_id"] == "321"
    assert idea_ref["anchor_type"] == "id"
    assert idea_ref["anchor"] == "321"
    assert idea_ref["username"] == "alice"
    assert idea_ref["tweet_id"] == "321"

    story = repo.get_story_by_id("story:alpha")
    assert story is not None
    story_ref = story["summary_json"]["sources"][0]
    assert story_ref["provider"] == "x"
    assert story_ref["source_id"] == "321"
    assert story_ref["anchor_type"] == "id"
    assert story_ref["anchor"] == "321"
    assert story_ref["tweet_id"] == "321"

    repo.close()


def test_validate_source_ref_payloads_detects_and_then_clears_legacy_shape(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    repo.upsert_user("alice", "id_alice", "Alice")
    repo.insert_tweets(
        "alice",
        [
            {
                "id": "654",
                "text": "Payload validation source",
                "created_at": "2026-03-03T14:00:00Z",
            }
        ],
    )
    repo.insert_idea_cards(
        [
            {
                "card_id": "idea:legacy",
                "run_id": "run:legacy",
                "username": "alice",
                "idea_type": "product",
                "title": "Legacy idea",
                "hypothesis": "Legacy hypothesis",
                "why_now": "Legacy why now",
                "tags": ["ai"],
                "source_refs": [{"username": "alice", "tweet_id": "654"}],
                "created_at": "2026-03-03T14:05:00Z",
            }
        ]
    )

    before = repo.validate_source_ref_payloads(limit_per_table=1000)
    assert before["invalid_count"] >= 1

    repo.backfill_legacy_source_ref_payloads(limit_per_table=1000)
    after = repo.validate_source_ref_payloads(limit_per_table=1000)
    assert after["invalid_count"] == 0

    repo.close()
