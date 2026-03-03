from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from roberto_app.llm.schemas import Story, StorySource
from roberto_app.notesys.renderer import render_story_auto_block
from roberto_app.notesys.updater import update_note_file
from roberto_app.pipeline.story_memory import slugify_story_title
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo, StoryUpsert

CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}


def _confidence_max(values: list[str]) -> str:
    if not values:
        return "medium"
    return max(values, key=lambda x: CONFIDENCE_RANK.get(x, 1))


def _dedupe_sources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        username = str(row.get("username") or "")
        tweet_id = str(row.get("tweet_id") or "")
        if not username or not tweet_id:
            continue
        key = (username, tweet_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "username": username,
                "tweet_id": tweet_id,
                "run_id": str(row.get("run_id") or ""),
                "created_at": str(row.get("created_at") or ""),
            }
        )
    return out


def _story_from_row(row: dict[str, Any], sources: list[dict[str, Any]]) -> Story:
    summary = row.get("summary_json") or {}
    what = str(summary.get("what_happened") or "Manual story maintenance update.")
    why = str(summary.get("why_it_matters") or "Story structure was updated.")
    tags = list(summary.get("tags") or row.get("tags_json") or [])
    confidence = str(summary.get("confidence") or row.get("confidence") or "medium")
    source_models = [StorySource(username=s["username"], tweet_id=s["tweet_id"]) for s in sources]
    return Story(
        title=str(summary.get("title") or row.get("title") or row.get("slug") or "Story"),
        what_happened=what,
        why_it_matters=why,
        sources=source_models,
        tags=tags,
        confidence=confidence if confidence in {"high", "medium", "low"} else "medium",
    )


def rebuild_story_note(settings, repo: StorageRepo, story_id: str, *, run_id: str, now_iso: str) -> Path:
    row = repo.get_story_by_id(story_id)
    if not row:
        raise ValueError(f"Story not found: {story_id}")
    slug = str(row["slug"])
    note_path = settings.resolve("notes", "stories", f"{slug}.md")
    history_sources = repo.list_story_sources(story_id, limit=80)
    story = _story_from_row(row, _dedupe_sources(history_sources[:30]))
    auto_body = render_story_auto_block(story, history_sources=history_sources, mention_count=int(row["mention_count"]))
    note_res = update_note_file(
        note_path,
        note_type="story",
        run_id=run_id,
        now_iso=now_iso,
        auto_body=auto_body,
        story_id=story_id,
        story_slug=slug,
        story_title=str(row["title"]),
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(note_path),
            note_type="story",
            username=None,
            created_at=note_res.created_at,
            updated_at=note_res.updated_at,
            last_run_id=run_id,
        )
    )
    return note_path


@dataclass
class MergeResult:
    target_story_id: str
    target_slug: str
    merged_from: list[str]
    note_path: str


def merge_stories(
    settings,
    repo: StorageRepo,
    *,
    source_slug_a: str,
    source_slug_b: str,
    target_slug: str | None,
    title: str | None,
    run_id: str,
    now_iso: str,
) -> MergeResult:
    a = repo.get_story_by_slug(source_slug_a)
    b = repo.get_story_by_slug(source_slug_b)
    if not a or not b:
        missing = source_slug_a if not a else source_slug_b
        raise ValueError(f"Story not found: {missing}")
    if str(a["story_id"]) == str(b["story_id"]):
        raise ValueError("Both source slugs resolve to the same story")

    target_slug_value = slugify_story_title(target_slug or str(a["title"]))
    target_story_id = f"story:{target_slug_value}"
    target_existing = repo.get_story_by_id(target_story_id)

    involved_story_ids = [str(a["story_id"]), str(b["story_id"])]
    if target_existing:
        involved_story_ids.append(str(target_existing["story_id"]))
    involved_story_ids = list(dict.fromkeys(involved_story_ids))

    all_sources: list[dict[str, Any]] = []
    all_entities: set[str] = set()
    tags: set[str] = set()
    confidences: list[str] = []
    mention_total = 0
    for sid in involved_story_ids:
        row = repo.get_story_by_id(sid)
        if not row:
            continue
        mention_total += int(row.get("mention_count") or 0)
        tags.update(str(t) for t in row.get("tags_json", []))
        confidences.append(str(row.get("confidence") or "medium"))
        all_sources.extend(repo.list_story_sources(sid, limit=50000))
        all_entities.update(str(e["entity_id"]) for e in repo.list_story_entities(sid))

    deduped_sources = _dedupe_sources(all_sources)
    merged_title = title or (str(target_existing["title"]) if target_existing else str(a["title"]))
    confidence = _confidence_max(confidences)

    summary_json: dict[str, Any]
    if target_existing:
        summary_json = dict(target_existing.get("summary_json", {}))
    else:
        summary_json = dict(a.get("summary_json", {}))
    summary_json["title"] = merged_title
    summary_json["tags"] = sorted(tags)
    summary_json["confidence"] = confidence

    if not target_existing:
        repo.upsert_story(
            StoryUpsert(
                story_id=target_story_id,
                slug=target_slug_value,
                title=merged_title,
                run_id=run_id,
                confidence=confidence,
                tags=sorted(tags),
                summary_json=summary_json,
                now_iso=now_iso,
            )
        )
    else:
        if str(target_existing["slug"]) != target_slug_value:
            repo.add_story_alias(str(target_existing["slug"]), target_story_id, created_at=now_iso)
            repo.set_story_slug(target_story_id, target_slug_value, updated_at=now_iso)

    repo.update_story_summary(
        target_story_id,
        title=merged_title,
        confidence=confidence,
        tags=sorted(tags),
        summary_json=summary_json,
        mention_count=max(1, mention_total),
        last_seen_run_id=run_id,
        updated_at=now_iso,
    )

    for source in deduped_sources:
        repo.add_story_sources(
            story_id=target_story_id,
            run_id=source["run_id"] or run_id,
            created_at=source["created_at"] or now_iso,
            sources=[(source["username"], source["tweet_id"])],
        )
    for entity_id in all_entities:
        repo.link_story_entity(target_story_id, entity_id, created_at=now_iso)

    merged_from = [str(a["slug"]), str(b["slug"])]
    for src in (a, b):
        src_story_id = str(src["story_id"])
        src_slug = str(src["slug"])
        if src_story_id == target_story_id:
            continue
        repo.add_story_alias(src_slug, target_story_id, created_at=now_iso)
        repo.add_story_lineage(src_story_id, target_story_id, relation="merge_into", created_at=now_iso)
        repo.set_attention_state(
            target_type="story",
            target_id=src_story_id,
            state="muted",
            snoozed_until=None,
            updated_at=now_iso,
        )

    note_path = rebuild_story_note(settings, repo, target_story_id, run_id=run_id, now_iso=now_iso)
    return MergeResult(
        target_story_id=target_story_id,
        target_slug=target_slug_value,
        merged_from=merged_from,
        note_path=str(note_path),
    )


@dataclass
class SplitResult:
    parent_story_id: str
    children: list[dict[str, str]]


def _load_split_plan(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    children = payload.get("children", [])
    if not isinstance(children, list) or not children:
        raise ValueError("Split plan must include non-empty 'children' list")
    out: list[dict[str, Any]] = []
    for child in children:
        if not isinstance(child, dict):
            continue
        slug = slugify_story_title(str(child.get("slug") or child.get("title") or ""))
        title = str(child.get("title") or slug.replace("-", " ").title()).strip()
        refs = child.get("source_refs", [])
        if not isinstance(refs, list) or not refs:
            raise ValueError("Each child in split plan must include source_refs")
        out.append(
            {
                "slug": slug,
                "title": title,
                "source_refs": refs,
                "confidence": str(child.get("confidence") or "medium"),
                "tags": child.get("tags") if isinstance(child.get("tags"), list) else [],
            }
        )
    if not out:
        raise ValueError("Split plan has no valid children")
    return out


def split_story(
    settings,
    repo: StorageRepo,
    *,
    source_slug: str,
    plan_path: Path,
    run_id: str,
    now_iso: str,
) -> SplitResult:
    parent = repo.get_story_by_slug(source_slug)
    if not parent:
        raise ValueError(f"Story not found: {source_slug}")
    parent_story_id = str(parent["story_id"])
    parent_sources = _dedupe_sources(repo.list_story_sources(parent_story_id, limit=50000))
    parent_source_index = {(row["username"], row["tweet_id"]): row for row in parent_sources}
    parent_entities = [row["entity_id"] for row in repo.list_story_entities(parent_story_id)]
    plan = _load_split_plan(plan_path)

    created_children: list[dict[str, str]] = []
    for child in plan:
        child_slug = str(child["slug"])
        child_story_id = f"story:{child_slug}"
        refs = child["source_refs"]
        child_tags = [str(t) for t in child["tags"]]
        confidence = str(child["confidence"])
        summary_json = {
            "title": child["title"],
            "what_happened": f"Split from {parent['title']}.",
            "why_it_matters": "Story was decomposed for better tracking.",
            "sources": refs,
            "tags": child_tags,
            "confidence": confidence,
        }
        repo.upsert_story(
            StoryUpsert(
                story_id=child_story_id,
                slug=child_slug,
                title=str(child["title"]),
                run_id=run_id,
                confidence=confidence,
                tags=child_tags,
                summary_json=summary_json,
                now_iso=now_iso,
            )
        )
        mention_count = 0
        for ref in refs:
            username = str(ref.get("username") or "")
            tweet_id = str(ref.get("tweet_id") or "")
            if not username or not tweet_id:
                continue
            source_row = parent_source_index.get((username, tweet_id))
            created_at = source_row["created_at"] if source_row else now_iso
            source_run = source_row["run_id"] if source_row else run_id
            repo.add_story_sources(
                story_id=child_story_id,
                run_id=source_run or run_id,
                created_at=created_at or now_iso,
                sources=[(username, tweet_id)],
            )
            mention_count += 1

        for entity_id in parent_entities:
            repo.link_story_entity(child_story_id, str(entity_id), created_at=now_iso)

        repo.update_story_summary(
            child_story_id,
            title=str(child["title"]),
            confidence=confidence if confidence in {"high", "medium", "low"} else "medium",
            tags=child_tags,
            summary_json=summary_json,
            mention_count=max(1, mention_count),
            last_seen_run_id=run_id,
            updated_at=now_iso,
        )
        repo.add_story_lineage(parent_story_id, child_story_id, relation="split_into", created_at=now_iso)
        note_path = rebuild_story_note(settings, repo, child_story_id, run_id=run_id, now_iso=now_iso)
        created_children.append({"story_id": child_story_id, "slug": child_slug, "note_path": str(note_path)})

    repo.set_attention_state(
        target_type="story",
        target_id=parent_story_id,
        state="muted",
        snoozed_until=None,
        updated_at=now_iso,
    )
    return SplitResult(parent_story_id=parent_story_id, children=created_children)
