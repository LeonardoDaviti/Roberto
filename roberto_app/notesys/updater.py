from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .templates import AUTO_BEGIN, AUTO_END, digest_note_template, story_note_template, user_note_template

FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n?", re.DOTALL)


@dataclass
class NoteWriteResult:
    path: Path
    created: bool
    updated: bool
    created_at: str
    updated_at: str


def split_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    match = FRONTMATTER_RE.match(content)
    if not match:
        return {}, content
    meta = yaml.safe_load(match.group(1)) or {}
    body = content[match.end() :]
    return meta, body


def render_frontmatter(meta: dict[str, Any]) -> str:
    data = yaml.safe_dump(meta, sort_keys=False, allow_unicode=False).strip()
    return f"---\n{data}\n---\n"


def replace_auto_block(content: str, auto_body: str) -> str:
    marker = re.compile(re.escape(AUTO_BEGIN) + r".*?" + re.escape(AUTO_END), re.DOTALL)
    replacement = f"{AUTO_BEGIN}\n{auto_body.rstrip()}\n{AUTO_END}"

    if AUTO_BEGIN in content and AUTO_END in content:
        return marker.sub(replacement, content, count=1)

    suffix = f"\n\n{replacement}\n"
    return content.rstrip() + suffix


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as fh:
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
        tmp_name = fh.name
    os.replace(tmp_name, path)


def update_note_file(
    path: Path,
    *,
    note_type: str,
    run_id: str,
    now_iso: str,
    auto_body: str,
    username: str | None = None,
    story_id: str | None = None,
    story_slug: str | None = None,
    story_title: str | None = None,
) -> NoteWriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    created = not path.exists()

    if created:
        if note_type == "user":
            if not username:
                raise ValueError("username is required for user notes")
            content = user_note_template(
                username,
                created_at=now_iso,
                updated_at=now_iso,
                last_run_id=run_id,
                auto_body=auto_body,
            )
        elif note_type == "digest":
            content = digest_note_template(
                run_id=run_id,
                created_at=now_iso,
                updated_at=now_iso,
                auto_body=auto_body,
            )
        elif note_type == "story":
            if not story_id or not story_slug or not story_title:
                raise ValueError("story_id, story_slug and story_title are required for story notes")
            content = story_note_template(
                story_id=story_id,
                story_slug=story_slug,
                title=story_title,
                run_id=run_id,
                created_at=now_iso,
                updated_at=now_iso,
                auto_body=auto_body,
            )
        else:
            raise ValueError(f"Unknown note_type: {note_type}")

        _atomic_write_text(path, content)
        return NoteWriteResult(path=path, created=True, updated=True, created_at=now_iso, updated_at=now_iso)

    original = path.read_text(encoding="utf-8")
    meta, body = split_frontmatter(original)

    created_at = meta.get("created_at", now_iso)
    meta["type"] = note_type
    if note_type == "user" and username:
        meta["username"] = username
    if note_type == "story":
        if not story_id or not story_slug:
            raise ValueError("story_id and story_slug are required for story notes")
        meta["story_id"] = story_id
        meta["story_slug"] = story_slug
        if story_title:
            meta["title"] = story_title
    meta["created_at"] = created_at
    meta["updated_at"] = now_iso
    meta["last_run_id"] = run_id

    body = replace_auto_block(body, auto_body)
    updated_content = render_frontmatter(meta) + body
    changed = updated_content != original

    if changed:
        _atomic_write_text(path, updated_content)

    return NoteWriteResult(
        path=path,
        created=False,
        updated=changed,
        created_at=str(created_at),
        updated_at=now_iso,
    )
