from __future__ import annotations

import json
from pathlib import Path

import yaml

from roberto_app.pipeline.search_index import rebuild_search_index, search
from roberto_app.pipeline.story_surgery import merge_stories, split_story
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo, StoryUpsert


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "stories").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)
    (root / "config" / "following.txt").write_text("alice\n", encoding="utf-8")
    payload = {
        "x": {
            "exclude": ["replies", "retweets"],
            "max_results": 100,
            "tweet_fields": ["id", "text", "created_at"],
            "request_timeout_s": 20,
            "retry": {"max_attempts": 5, "backoff_s": [1, 2, 4, 8, 16]},
        },
        "llm": {
            "provider": "gemini",
            "model": "gemini-flash-latest",
            "temperature": 0.2,
            "max_output_tokens": 4096,
            "thinking_level": "low",
            "json_mode": True,
        },
        "notes": {
            "per_user_note_enabled": True,
            "digest_note_enabled": True,
            "note_timezone": "Asia/Tbilisi",
            "overwrite_mode": "markers_only",
        },
        "pipeline": {
            "v1": {"backfill_count": 100},
            "v2": {"max_new_tweets_per_user": 200, "create_digest_each_run": True},
        },
        "v13": {"enabled": False, "max_diff_lines": 200},
        "v15": {"enabled": True, "apply_muted_filters": True},
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(payload), encoding="utf-8")


def _seed_story(repo: StorageRepo, *, story_id: str, slug: str, title: str, source_ref: tuple[str, str]) -> None:
    now = "2026-03-03T10:00:00Z"
    repo.upsert_story(
        StoryUpsert(
            story_id=story_id,
            slug=slug,
            title=title,
            run_id="2026-03-03T100000000000Z",
            confidence="medium",
            tags=["ai"],
            summary_json={
                "title": title,
                "what_happened": f"{title} happened",
                "why_it_matters": "matters",
                "sources": [{"username": source_ref[0], "tweet_id": source_ref[1]}],
                "tags": ["ai"],
                "confidence": "medium",
            },
            now_iso=now,
        )
    )
    repo.add_story_sources(
        story_id=story_id,
        run_id="2026-03-03T100000000000Z",
        created_at=now,
        sources=[source_ref],
    )


def test_merge_stories_creates_aliases_and_mutes_sources(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    _seed_story(
        repo,
        story_id="story:alpha",
        slug="alpha",
        title="Alpha Story",
        source_ref=("alice", "100"),
    )
    _seed_story(
        repo,
        story_id="story:beta",
        slug="beta",
        title="Beta Story",
        source_ref=("bob", "200"),
    )

    result = merge_stories(
        settings,
        repo,
        source_slug_a="alpha",
        source_slug_b="beta",
        target_slug="alpha-beta",
        title="Unified Story",
        run_id="manual:2026-03-03T11:00:00Z",
        now_iso="2026-03-03T11:00:00Z",
    )
    assert result.target_story_id == "story:alpha-beta"
    merged = repo.get_story_by_slug("alpha-beta")
    assert merged is not None
    assert merged["title"] == "Unified Story"
    assert repo.resolve_story_alias("alpha") == "story:alpha-beta"
    assert repo.resolve_story_alias("beta") == "story:alpha-beta"
    assert repo.get_attention_state("story", "story:alpha")["state"] == "muted"
    assert repo.get_attention_state("story", "story:beta")["state"] == "muted"
    sources = repo.list_story_sources("story:alpha-beta", limit=20)
    refs = {(s["username"], s["tweet_id"]) for s in sources}
    assert ("alice", "100") in refs and ("bob", "200") in refs
    assert Path(result.note_path).exists()
    repo.close()


def test_split_story_creates_children_and_lineage(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    _seed_story(
        repo,
        story_id="story:parent",
        slug="parent",
        title="Parent Story",
        source_ref=("alice", "100"),
    )
    repo.add_story_sources(
        story_id="story:parent",
        run_id="2026-03-03T100000000000Z",
        created_at="2026-03-03T10:00:00Z",
        sources=[("bob", "200")],
    )
    plan = {
        "children": [
            {
                "slug": "child-one",
                "title": "Child One",
                "source_refs": [{"username": "alice", "tweet_id": "100"}],
                "tags": ["ai"],
                "confidence": "high",
            },
            {
                "slug": "child-two",
                "title": "Child Two",
                "source_refs": [{"username": "bob", "tweet_id": "200"}],
                "tags": ["infra"],
                "confidence": "medium",
            },
        ]
    }
    plan_path = tmp_path / "split_plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    result = split_story(
        settings,
        repo,
        source_slug="parent",
        plan_path=plan_path,
        run_id="manual:2026-03-03T11:00:00Z",
        now_iso="2026-03-03T11:00:00Z",
    )
    assert result.parent_story_id == "story:parent"
    assert len(result.children) == 2
    lineage = repo.list_story_lineage("story:parent")
    assert any(row["relation"] == "split_into" for row in lineage)
    for child in result.children:
        assert repo.get_story_by_id(child["story_id"]) is not None
        assert Path(child["note_path"]).exists()
    assert repo.get_attention_state("story", "story:parent")["state"] == "muted"
    repo.close()


def test_search_hides_muted_story_by_default(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    _seed_story(
        repo,
        story_id="story:alpha",
        slug="alpha",
        title="Alpha Story",
        source_ref=("alice", "100"),
    )
    repo.set_attention_state(
        target_type="story",
        target_id="story:alpha",
        state="muted",
        snoozed_until=None,
        updated_at="2026-03-03T11:00:00Z",
    )
    rebuild_search_index(settings, repo)
    hidden = search(
        settings,
        repo,
        "alpha story",
        kind="story",
        limit=20,
        include_muted=False,
        now_iso="2026-03-03T11:00:00Z",
    )
    shown = search(
        settings,
        repo,
        "alpha story",
        kind="story",
        limit=20,
        include_muted=True,
        now_iso="2026-03-03T11:00:00Z",
    )
    assert hidden == []
    assert shown and shown[0]["item_id"] == "story:alpha"
    repo.close()
