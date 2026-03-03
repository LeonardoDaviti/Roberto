from __future__ import annotations

import difflib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from roberto_app.storage.repo import StorageRepo


def staging_target_path(notes_root: Path, run_id: str, live_path: Path) -> Path:
    live_abs = live_path.resolve()
    notes_abs = notes_root.resolve()
    rel = live_abs.relative_to(notes_abs)
    return notes_abs / "_staging" / run_id / rel


def normalize_trigger_refs(refs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for row in refs:
        username = str(row.get("username") or "").strip()
        tweet_id = str(row.get("tweet_id") or "").strip()
        if not username or not tweet_id:
            continue
        key = (username, tweet_id)
        if key in seen:
            continue
        seen.add(key)
        out.append({"username": username, "tweet_id": tweet_id})
    return out


def _read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def build_diff_preview(live_path: Path, staged_path: Path, max_lines: int = 300) -> dict[str, Any]:
    before = _read_text_or_empty(live_path).splitlines()
    after = _read_text_or_empty(staged_path).splitlines()
    diff_lines = list(
        difflib.unified_diff(
            before,
            after,
            fromfile=str(live_path),
            tofile=str(staged_path),
            lineterm="",
        )
    )
    added = sum(1 for line in diff_lines if line.startswith("+") and not line.startswith("+++"))
    removed = sum(1 for line in diff_lines if line.startswith("-") and not line.startswith("---"))
    truncated = False
    if len(diff_lines) > max_lines:
        diff_lines = diff_lines[:max_lines]
        truncated = True
    return {
        "changed": before != after,
        "added_lines": added,
        "removed_lines": removed,
        "diff": "\n".join(diff_lines),
        "truncated": truncated,
    }


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


@dataclass
class PromoteResult:
    run_id: str
    promoted: list[str]
    missing_staged_files: list[str]
    snapshot_ids: list[int]


def promote_staged_run(repo: StorageRepo, run_id: str, now_iso: str) -> PromoteResult:
    rows = repo.list_staged_notes(run_id, status="staged")
    promoted: list[str] = []
    missing: list[str] = []
    snapshot_ids: list[int] = []

    for row in rows:
        live_path = Path(str(row["live_path"]))
        staged_path = Path(str(row["staged_path"]))
        if not staged_path.exists():
            missing.append(str(staged_path))
            continue

        if live_path.exists():
            snapshot_id = repo.insert_note_snapshot(
                note_path=str(live_path),
                run_id=run_id,
                captured_at=now_iso,
                reason="pre_promote",
                content=live_path.read_text(encoding="utf-8"),
            )
            snapshot_ids.append(snapshot_id)

        _atomic_write_text(live_path, staged_path.read_text(encoding="utf-8"))
        repo.mark_staged_note_status(run_id, str(live_path), status="promoted", promoted_at=now_iso)
        promoted.append(str(live_path))

    return PromoteResult(run_id=run_id, promoted=promoted, missing_staged_files=missing, snapshot_ids=snapshot_ids)


@dataclass
class RollbackResult:
    note_path: str
    restored_snapshot_id: int
    created_snapshot_id: int | None


def rollback_note(
    repo: StorageRepo,
    *,
    note_path: str,
    now_iso: str,
    snapshot_id: int | None = None,
) -> RollbackResult:
    target = Path(note_path).resolve()
    snapshot: dict[str, Any] | None
    if snapshot_id is None:
        snapshot = repo.get_latest_note_snapshot(str(target))
    else:
        snapshot = repo.get_note_snapshot(snapshot_id)

    if not snapshot:
        raise ValueError(f"No snapshot found for note: {target}")

    created_snapshot_id: int | None = None
    if target.exists():
        created_snapshot_id = repo.insert_note_snapshot(
            note_path=str(target),
            run_id=None,
            captured_at=now_iso,
            reason="pre_rollback",
            content=target.read_text(encoding="utf-8"),
        )

    _atomic_write_text(target, str(snapshot["content"]))
    return RollbackResult(
        note_path=str(target),
        restored_snapshot_id=int(snapshot["snapshot_id"]),
        created_snapshot_id=created_snapshot_id,
    )
