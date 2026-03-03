from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from roberto_app.llm.schemas import DailyDigestAutoBlock
from roberto_app.storage.repo import StorageRepo

CONF_RANK = {"low": 0, "medium": 1, "high": 2}


@dataclass
class BriefingBuild:
    brief_id: str
    brief_date: str
    summary: dict[str, Any]
    item_rows: list[dict[str, Any]]
    refs: list[dict[str, str]]


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _is_snoozed(state: str, snoozed_until: str | None, now_iso: str) -> bool:
    if state != "snoozed" or not snoozed_until:
        return False
    now_dt = _parse_iso(now_iso)
    until_dt = _parse_iso(snoozed_until)
    if not now_dt or not until_dt:
        return False
    return until_dt > now_dt


def _ref_links(refs: list[dict[str, str]]) -> str:
    return ", ".join(
        f"[{ref['username']}:{ref['tweet_id']}](https://x.com/{ref['username']}/status/{ref['tweet_id']})"
        for ref in refs
        if ref.get("username") and ref.get("tweet_id")
    )


def _dedupe_refs(refs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for ref in refs:
        username = str(ref.get("username") or "").strip()
        tweet_id = str(ref.get("tweet_id") or "").strip()
        if not username or not tweet_id:
            continue
        key = (username, tweet_id)
        if key in seen:
            continue
        seen.add(key)
        out.append({"username": username, "tweet_id": tweet_id})
    return out


def _changed_conflicts(repo: StorageRepo, *, run_id: str, since_iso: str | None) -> list[dict[str, Any]]:
    rows = repo.list_conflicts(limit=2000)
    if not since_iso:
        return [row for row in rows if str(row.get("run_id") or "") == run_id]
    since_dt = _parse_iso(since_iso)
    if not since_dt:
        return [row for row in rows if str(row.get("run_id") or "") == run_id]
    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("run_id") or "") == run_id:
            out.append(row)
            continue
        updated_dt = _parse_iso(str(row.get("updated_at") or ""))
        if updated_dt and updated_dt >= since_dt:
            out.append(row)
    return out


def _latest_runs(repo: StorageRepo, run_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    rows = repo.list_runs(limit=3, exclude_run_id=run_id)
    prev = rows[0] if rows else None
    prev_prev = rows[1] if len(rows) > 1 else None
    return prev, prev_prev


def build_daily_briefing(
    repo: StorageRepo,
    digest_block: DailyDigestAutoBlock,
    *,
    run_id: str,
    now_iso: str,
    top_story_deltas: int = 5,
    top_connections: int = 3,
    top_ideas: int = 3,
) -> BriefingBuild:
    brief_date = now_iso[:10]
    brief_id = f"brief:{brief_date}"
    previous_run, _ = _latest_runs(repo, run_id)
    since_iso = str(previous_run.get("started_at") or "") if previous_run else None
    changed_conflicts = _changed_conflicts(repo, run_id=run_id, since_iso=since_iso)

    story_entries: list[dict[str, Any]] = []
    for story in repo.list_stories(limit=500):
        story_id = str(story["story_id"])
        state = str(story.get("attention_state") or "active")
        if state == "muted":
            continue
        if _is_snoozed(state, story.get("snoozed_until"), now_iso):
            continue

        history_sources = repo.list_story_sources(story_id, limit=400)
        run_sources = [row for row in history_sources if str(row.get("run_id") or "") == run_id]
        refs = _dedupe_refs(
            [{"username": str(row.get("username") or ""), "tweet_id": str(row.get("tweet_id") or "")} for row in run_sources]
        )
        if not refs:
            refs = _dedupe_refs(
                [{"username": str(row.get("username") or ""), "tweet_id": str(row.get("tweet_id") or "")} for row in history_sources[:3]]
            )
        if not refs:
            continue

        confidence_events = repo.list_confidence_events(story_id, limit=2)
        confidence_delta = 0
        if len(confidence_events) >= 2:
            new_conf = str(confidence_events[0].get("new_confidence") or "medium")
            prev_conf = str(confidence_events[1].get("new_confidence") or "medium")
            confidence_delta = abs(CONF_RANK.get(new_conf, 1) - CONF_RANK.get(prev_conf, 1))

        source_keys = {(r["username"], r["tweet_id"]) for r in refs}
        conflict_changes = 0
        for conflict in changed_conflicts:
            conflict_refs = _dedupe_refs(list(conflict.get("source_refs") or []))
            if any((r["username"], r["tweet_id"]) in source_keys for r in conflict_refs):
                conflict_changes += 1

        evidence_new = len(run_sources)
        attention_bonus = 2 if state == "pinned" else 0
        score = (3.0 * evidence_new) + (2.0 * confidence_delta) + (2.0 * conflict_changes) + attention_bonus
        if score <= 0 and evidence_new <= 0 and conflict_changes <= 0:
            continue

        summary = story.get("summary_json", {}) or {}
        story_entries.append(
            {
                "story_id": story_id,
                "slug": str(story.get("slug") or ""),
                "title": str(story.get("title") or ""),
                "confidence": str(story.get("confidence") or "medium"),
                "what_changed": str(summary.get("what_happened") or "Updated with new evidence."),
                "why_it_matters": str(summary.get("why_it_matters") or "Change may impact future decisions."),
                "refs": refs,
                "score": round(score, 4),
                "score_breakdown": {
                    "evidence_new": evidence_new,
                    "confidence_delta": confidence_delta,
                    "conflict_changes": conflict_changes,
                    "attention_bonus": attention_bonus,
                },
                "attention_state": state,
            }
        )

    story_entries.sort(
        key=lambda row: (
            -float(row["score"]),
            -int(row["score_breakdown"]["evidence_new"]),
            str(row["title"]).lower(),
        )
    )
    story_entries = story_entries[: max(1, top_story_deltas)]

    conn_entries: list[dict[str, Any]] = []
    for conn in digest_block.connections:
        refs = _dedupe_refs([{"username": s.username, "tweet_id": s.tweet_id} for s in conn.supports])
        if not refs:
            continue
        uniq_users = len({r["username"] for r in refs})
        score = float(len(refs)) + (1.0 if uniq_users > 1 else 0.0)
        conn_entries.append(
            {
                "insight": conn.insight,
                "refs": refs,
                "score": round(score, 4),
                "score_breakdown": {"support_count": len(refs), "cross_user_bonus": 1 if uniq_users > 1 else 0},
            }
        )
    conn_entries.sort(key=lambda row: (-float(row["score"]), str(row["insight"]).lower()))
    conn_entries = conn_entries[: max(1, top_connections)]

    idea_rows = repo.list_recent_idea_cards(days=30, limit=300)
    run_idea_rows = [row for row in idea_rows if str(row.get("run_id") or "") == run_id]
    if run_idea_rows:
        idea_rows = run_idea_rows
    idea_entries: list[dict[str, Any]] = []
    for row in idea_rows:
        refs = _dedupe_refs(list(row.get("source_refs") or []))
        if not refs:
            continue
        score = float(len(refs)) + (0.5 if str(row.get("run_id") or "") == run_id else 0.0)
        idea_entries.append(
            {
                "card_id": str(row.get("card_id") or ""),
                "username": str(row.get("username") or ""),
                "idea_type": str(row.get("idea_type") or ""),
                "title": str(row.get("title") or ""),
                "hypothesis": str(row.get("hypothesis") or ""),
                "why_now": str(row.get("why_now") or ""),
                "tags": [str(tag) for tag in row.get("tags", [])],
                "refs": refs,
                "score": round(score, 4),
                "score_breakdown": {"ref_count": len(refs), "current_run_bonus": 0.5 if str(row.get("run_id") or "") == run_id else 0.0},
            }
        )
    idea_entries.sort(key=lambda row: (-float(row["score"]), str(row["title"]).lower()))
    idea_entries = idea_entries[: max(1, top_ideas)]

    all_refs = _dedupe_refs(
        [ref for row in story_entries for ref in row["refs"]]
        + [ref for row in conn_entries for ref in row["refs"]]
        + [ref for row in idea_entries for ref in row["refs"]]
    )

    summary = {
        "brief_id": brief_id,
        "brief_date": brief_date,
        "run_id": run_id,
        "generated_at": now_iso,
        "story_deltas": story_entries,
        "connections": conn_entries,
        "ideas": idea_entries,
    }

    item_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(story_entries, start=1):
        item_rows.append(
            {
                "item_id": f"{brief_id}:story:{idx}",
                "item_type": "story_delta",
                "rank": idx,
                "score": float(row["score"]),
                "refs": row["refs"],
                "payload": row,
            }
        )
    for idx, row in enumerate(conn_entries, start=1):
        item_rows.append(
            {
                "item_id": f"{brief_id}:connection:{idx}",
                "item_type": "connection",
                "rank": idx,
                "score": float(row["score"]),
                "refs": row["refs"],
                "payload": row,
            }
        )
    for idx, row in enumerate(idea_entries, start=1):
        item_rows.append(
            {
                "item_id": f"{brief_id}:idea:{idx}",
                "item_type": "idea",
                "rank": idx,
                "score": float(row["score"]),
                "refs": row["refs"],
                "payload": row,
            }
        )

    return BriefingBuild(
        brief_id=brief_id,
        brief_date=brief_date,
        summary=summary,
        item_rows=item_rows,
        refs=all_refs,
    )


def render_briefing(summary: dict[str, Any], *, mode: str = "fast") -> str:
    story_rows = list(summary.get("story_deltas", []))
    conn_rows = list(summary.get("connections", []))
    idea_rows = list(summary.get("ideas", []))
    run_id = str(summary.get("run_id") or "")
    brief_date = str(summary.get("brief_date") or "")

    lines: list[str] = []
    lines.append(f"## Daily Briefing - {brief_date}")
    lines.append(f"- Run: {run_id}")
    lines.append("")

    lines.append("### Top Story Deltas")
    if not story_rows:
        lines.append("- No high-signal story deltas with citations.")
    else:
        for idx, row in enumerate(story_rows, start=1):
            lines.append(f"- {idx}. **{row['title']}** ({row['confidence']})")
            lines.append(f"  - Change: {row['what_changed']}")
            lines.append(f"  - Sources: {_ref_links(row['refs']) or 'none'}")
            if mode == "deep":
                lines.append(f"  - Why it matters: {row['why_it_matters']}")
                lines.append(
                    "  - Ranking: "
                    f"evidence={row['score_breakdown']['evidence_new']}, "
                    f"conf_delta={row['score_breakdown']['confidence_delta']}, "
                    f"conflicts={row['score_breakdown']['conflict_changes']}, "
                    f"attention={row['attention_state']}, score={row['score']}"
                )

    lines.append("")
    lines.append("### Cross-Story Connections")
    if not conn_rows:
        lines.append("- No cited cross-story connections in this run.")
    else:
        for idx, row in enumerate(conn_rows, start=1):
            lines.append(f"- {idx}. {row['insight']}")
            lines.append(f"  - Supports: {_ref_links(row['refs']) or 'none'}")
            if mode == "deep":
                lines.append(
                    "  - Ranking: "
                    f"supports={row['score_breakdown']['support_count']}, "
                    f"cross_user_bonus={row['score_breakdown']['cross_user_bonus']}, "
                    f"score={row['score']}"
                )

    lines.append("")
    lines.append("### Idea Cards")
    if not idea_rows:
        lines.append("- No cited idea cards selected.")
    else:
        for idx, row in enumerate(idea_rows, start=1):
            lines.append(f"- {idx}. **[{row['idea_type'].upper()}] {row['title']}** (@{row['username']})")
            lines.append(f"  - Hypothesis: {row['hypothesis']}")
            lines.append(f"  - Sources: {_ref_links(row['refs']) or 'none'}")
            if mode == "deep":
                lines.append(f"  - Why now: {row['why_now']}")
                lines.append(f"  - Tags: {', '.join(row.get('tags', [])) if row.get('tags') else 'none'}")
                lines.append(
                    "  - Ranking: "
                    f"refs={row['score_breakdown']['ref_count']}, "
                    f"current_run_bonus={row['score_breakdown']['current_run_bonus']}, "
                    f"score={row['score']}"
                )

    return "\n".join(lines).rstrip()

