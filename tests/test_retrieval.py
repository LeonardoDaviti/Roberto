from __future__ import annotations

from types import SimpleNamespace

from roberto_app.llm.retrieval import RetrievalContextBuilder
from roberto_app.storage.repo import StorageRepo, StoryUpsert


def test_retrieval_user_and_digest_context(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    repo.upsert_user("alice", "id_alice", "Alice")
    repo.insert_tweets(
        "alice",
        [
            {"id": "100", "text": "GPU kernels and memory bandwidth", "created_at": "2026-03-01T10:00:00Z"},
            {"id": "101", "text": "Compiler optimizations for kernels", "created_at": "2026-03-01T11:00:00Z"},
            {"id": "102", "text": "Weekend travel notes", "created_at": "2026-03-01T12:00:00Z"},
        ],
    )

    cfg = SimpleNamespace(enabled=True, top_k_user_context=2, top_k_story_context=2, max_context_chars=120)
    retriever = RetrievalContextBuilder(repo, cfg)

    recent = repo.get_recent_tweets("alice", limit=10)
    user_ctx = retriever.user_context("alice", recent, focus_tweet_ids={"101"})
    assert len(user_ctx) <= 2
    assert all("tweet_id" in row for row in user_ctx)

    repo.upsert_story(
        StoryUpsert(
            story_id="story:gpu-kernels",
            slug="gpu-kernels",
            title="GPU Kernels",
            run_id="2026-03-03T000000Z",
            confidence="high",
            tags=["gpu"],
            summary_json={"title": "GPU Kernels", "what_happened": "kernel tuning"},
            now_iso="2026-03-03T10:00:00Z",
        )
    )

    digest_ctx = retriever.digest_context(
        highlights_by_user=[{"username": "alice", "highlights": [{"title": "GPU", "summary": "kernel tuning"}]}],
        new_tweets_by_user={"alice": [{"tweet_id": "101", "text": "Compiler optimizations for kernels"}]},
    )
    assert len(digest_ctx) == 1
    assert digest_ctx[0]["story_id"] == "story:gpu-kernels"

    repo.close()
