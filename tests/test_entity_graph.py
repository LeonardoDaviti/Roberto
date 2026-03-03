from __future__ import annotations

from roberto_app.llm.schemas import DailyDigestAutoBlock, Story, StorySource
from roberto_app.pipeline.common import utc_now_iso
from roberto_app.pipeline.entity_graph import (
    index_entities_from_digest,
    index_entities_from_tweets,
    render_entity_auto_block,
)
from roberto_app.pipeline.story_memory import slugify_story_title
from roberto_app.storage.repo import StorageRepo, StoryUpsert


def test_index_entities_from_tweets_and_timeline(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    now_iso = utc_now_iso()

    repo.upsert_user("alice", "id_alice", "Alice")
    repo.insert_tweets(
        "alice",
        [
            {
                "id": "1001",
                "text": "NVIDIA chips and OpenAI inference stack",
                "created_at": now_iso,
                "entities": {"hashtags": [{"tag": "AIInfra"}]},
            }
        ],
    )
    rows = repo.get_recent_tweets("alice", limit=10)
    entity_ids = index_entities_from_tweets(
        repo,
        username="alice",
        tweets=rows,
        now_iso=now_iso,
        min_token_len=3,
    )
    assert entity_ids

    nvidia = repo.resolve_entity("nvidia")
    assert nvidia is not None
    timeline = repo.get_entity_timeline(str(nvidia["entity_id"]), days=90, limit=100)
    assert any(item["ref_type"] == "tweet" and item["ref_id"] == "1001" for item in timeline)

    auto = render_entity_auto_block(
        canonical_name=str(nvidia["canonical_name"]),
        aliases=repo.get_entity_aliases(str(nvidia["entity_id"])),
        timeline_rows=timeline,
        days=90,
    )
    assert "Entity Timeline (90 days)" in auto
    assert "x.com/alice/status/1001" in auto
    repo.close()


def test_index_entities_from_digest_links_story(tmp_path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    now_iso = utc_now_iso()
    title = "NVIDIA and OpenAI roadmap"
    slug = slugify_story_title(title)
    story_id = f"story:{slug}"
    repo.upsert_story(
        StoryUpsert(
            story_id=story_id,
            slug=slug,
            title=title,
            run_id="2026-03-03T000000000000Z",
            confidence="high",
            tags=["nvidia", "openai"],
            summary_json={"title": title},
            now_iso=now_iso,
        )
    )
    repo.add_story_sources(
        story_id=story_id,
        run_id="2026-03-03T000000000000Z",
        created_at=now_iso,
        sources=[("alice", "1001")],
    )

    digest = DailyDigestAutoBlock(
        stories=[
            Story(
                title=title,
                what_happened="OpenAI discussed NVIDIA stack choices",
                why_it_matters="Signals model infra direction",
                sources=[StorySource(username="alice", tweet_id="1001")],
                tags=["nvidia", "openai"],
                confidence="high",
            )
        ],
        connections=[],
    )

    entity_ids = index_entities_from_digest(repo, digest, now_iso=now_iso, min_token_len=3)
    assert entity_ids
    story_entities = repo.list_story_entities(story_id)
    names = {row["canonical_name"].lower() for row in story_entities}
    assert "nvidia" in names or "openai" in names
    repo.close()
