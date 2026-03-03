from __future__ import annotations

import yaml

AUTO_BEGIN = "<!-- ROBERTO:AUTO:BEGIN -->"
AUTO_END = "<!-- ROBERTO:AUTO:END -->"


def _frontmatter_block(meta: dict[str, str]) -> str:
    dumped = yaml.safe_dump(meta, sort_keys=False, allow_unicode=False).strip()
    return f"---\n{dumped}\n---\n"


def user_note_template(
    username: str,
    *,
    created_at: str,
    updated_at: str,
    last_run_id: str,
    auto_body: str,
) -> str:
    meta = {
        "type": "user",
        "username": username,
        "created_at": created_at,
        "updated_at": updated_at,
        "last_run_id": last_run_id,
    }
    body = (
        f"\n# @{username} - Roberto Notes\n\n"
        "## Manual (you write here)\n"
        "- My own hypotheses, links, TODOs...\n\n"
        f"{AUTO_BEGIN}\n"
        f"{auto_body.rstrip()}\n"
        f"{AUTO_END}\n"
    )
    return _frontmatter_block(meta) + body


def digest_note_template(*, run_id: str, created_at: str, updated_at: str, auto_body: str) -> str:
    meta = {
        "type": "digest",
        "created_at": created_at,
        "updated_at": updated_at,
        "last_run_id": run_id,
    }
    body = (
        "\n# Roberto Daily Digest\n\n"
        "## Manual (you write here)\n"
        "- Add your own synthesis, links, decisions.\n\n"
        f"{AUTO_BEGIN}\n"
        f"{auto_body.rstrip()}\n"
        f"{AUTO_END}\n"
    )
    return _frontmatter_block(meta) + body


def story_note_template(
    *,
    story_id: str,
    story_slug: str,
    title: str,
    run_id: str,
    created_at: str,
    updated_at: str,
    auto_body: str,
) -> str:
    meta = {
        "type": "story",
        "story_id": story_id,
        "story_slug": story_slug,
        "title": title,
        "created_at": created_at,
        "updated_at": updated_at,
        "last_run_id": run_id,
    }
    body = (
        f"\n# {title}\n\n"
        "## Manual (you write here)\n"
        "- Add your own evolving thesis and counterpoints.\n\n"
        f"{AUTO_BEGIN}\n"
        f"{auto_body.rstrip()}\n"
        f"{AUTO_END}\n"
    )
    return _frontmatter_block(meta) + body


def memory_note_template(
    *,
    note_type: str,
    title: str,
    run_id: str,
    created_at: str,
    updated_at: str,
    auto_body: str,
    extra_meta: dict[str, str] | None = None,
) -> str:
    meta = {
        "type": note_type,
        "title": title,
        "created_at": created_at,
        "updated_at": updated_at,
        "last_run_id": run_id,
    }
    if extra_meta:
        meta.update(extra_meta)
    body = (
        f"\n# {title}\n\n"
        "## Manual (you write here)\n"
        "- Add your own decisions, hypotheses, and next actions.\n\n"
        f"{AUTO_BEGIN}\n"
        f"{auto_body.rstrip()}\n"
        f"{AUTO_END}\n"
    )
    return _frontmatter_block(meta) + body
