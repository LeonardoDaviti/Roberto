from __future__ import annotations

import json

from roberto_app.storage.repo import StorageRepo


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
