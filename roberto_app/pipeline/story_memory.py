from __future__ import annotations

import re
from typing import TYPE_CHECKING

from roberto_app.notesys.renderer import render_story_auto_block
from roberto_app.notesys.updater import update_note_file
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo, StoryUpsert

if TYPE_CHECKING:
    from roberto_app.llm.schemas import DailyDigestAutoBlock
    from roberto_app.pipeline.report import RunReport


def slugify_story_title(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return slug[:80] or "untitled-story"


def persist_stories(
    settings,
    repo: StorageRepo,
    digest_block: "DailyDigestAutoBlock",
    *,
    run_id: str,
    now_iso: str,
    report: "RunReport",
) -> None:
    if not digest_block.stories:
        return

    stories_dir = settings.resolve("notes", "stories")
    stories_dir.mkdir(parents=True, exist_ok=True)

    for story in digest_block.stories:
        slug = slugify_story_title(story.title)
        story_id = f"story:{slug}"

        repo.upsert_story(
            StoryUpsert(
                story_id=story_id,
                slug=slug,
                title=story.title,
                run_id=run_id,
                confidence=story.confidence,
                tags=story.tags,
                summary_json=story.model_dump(),
                now_iso=now_iso,
            )
        )

        repo.add_story_sources(
            story_id=story_id,
            run_id=run_id,
            created_at=now_iso,
            sources=[(source.username, source.tweet_id) for source in story.sources],
        )

        story_row = repo.get_story_by_id(story_id) or {}
        mention_count = int(story_row.get("mention_count") or 1)
        history_sources = repo.list_story_sources(story_id, limit=30)

        story_path = stories_dir / f"{slug}.md"
        auto_body = render_story_auto_block(story, history_sources=history_sources, mention_count=mention_count)
        note_res = update_note_file(
            story_path,
            note_type="story",
            run_id=run_id,
            now_iso=now_iso,
            auto_body=auto_body,
            story_id=story_id,
            story_slug=slug,
            story_title=story.title,
        )

        if note_res.created:
            report.created_notes.append(str(story_path))
        elif note_res.updated:
            report.updated_notes.append(str(story_path))

        repo.upsert_note_index(
            NoteIndexUpsert(
                note_path=str(story_path),
                note_type="story",
                username=None,
                created_at=note_res.created_at,
                updated_at=note_res.updated_at,
                last_run_id=run_id,
            )
        )
