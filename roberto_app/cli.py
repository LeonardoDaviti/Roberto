from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console
from rich.table import Table

from roberto_app.llm.gemini import GeminiSummarizer
from roberto_app.notesys.updater import update_note_file
from roberto_app.logging_setup import setup_logging
from roberto_app.pipeline.build import run_build
from roberto_app.pipeline.doctor import run_doctor
from roberto_app.pipeline.editorial import build_diff_preview, promote_staged_run, rollback_note
from roberto_app.pipeline.eval import run_eval
from roberto_app.pipeline.greene import (
    build_argumentation,
    detect_gaps,
    ensure_profile_assets,
    generate_draft,
    list_cards,
    mark_card_feedback,
    propose_chapters,
    render_argumentation,
    render_chapter_note,
    render_gap_note,
    run_ai_action,
    run_chapter_argument_gap_cycle,
    run_greene_cycle,
)
from roberto_app.pipeline.import_json import import_json_file
from roberto_app.pipeline.lock import run_lock
from roberto_app.pipeline.search_index import rebuild_search_index, search
from roberto_app.pipeline.briefing import render_briefing
from roberto_app.pipeline.sync import run_sync
from roberto_app.pipeline.taxonomy import apply_entity_alias_override, load_entity_alias_overrides
from roberto_app.pipeline.v1 import run_v1
from roberto_app.pipeline.v2 import run_v2
from roberto_app.pipeline.story_surgery import merge_stories, split_story
from roberto_app.pipeline.common import utc_now_iso
from roberto_app.settings import (
    load_settings,
    require_gemini_api_key,
    require_x_bearer_token,
)
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo
from roberto_app.x_api.client import XClient
from roberto_app.x_api.errors import XAPIError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Roberto CLI")
    parser.add_argument("--base-dir", default=".", help="Project root directory")

    sub = parser.add_subparsers(dest="command", required=True)
    v1_cmd = sub.add_parser("v1", help="Initial build pipeline")
    v1_cmd.add_argument("--resume", action="store_true", help="Resume from last v1 checkpoint if present")
    v2_cmd = sub.add_parser("v2", help="Incremental update pipeline")
    v2_cmd.add_argument(
        "--from-db-only",
        action="store_true",
        help="Skip X API fetch and update notes using only cached/imported tweets in SQLite",
    )
    v2_cmd.add_argument("--resume", action="store_true", help="Resume from last v2 checkpoint if present")
    status_cmd = sub.add_parser("status", help="Show cached status by followed user")
    status_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    export_cmd = sub.add_parser("export", help="Export last digest/story set")
    export_cmd.add_argument("--format", choices=["json", "md"], default="json")

    import_cmd = sub.add_parser("import-json", help="Import tweets from a local JSON file into SQLite")
    import_cmd.add_argument("--file", required=True, help="Path to JSON file")
    import_cmd.add_argument(
        "--default-username",
        default=None,
        help="Fallback username when records do not include username",
    )

    sync_cmd = sub.add_parser("sync", help="Ingest posts from X into SQLite cache only")
    sync_cmd.add_argument("--full", action="store_true", help="Fetch latest window (like v1 ingest)")

    sub.add_parser("build", help="Build notes/digest from cached DB only")
    eval_cmd = sub.add_parser("eval", help="Run deterministic quality evaluation")
    eval_cmd.add_argument("--fixture", default=None, help="Path to eval fixture JSON")
    eval_cmd.add_argument("--fixtures-dir", default=None, help="Path to directory with eval fixture JSON files")
    eval_cmd.add_argument("--baseline", default=None, help="Path to baseline eval fixture JSON")
    eval_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    doctor_cmd = sub.add_parser("doctor", help="Run environment and reliability diagnostics")
    doctor_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    doctor_cmd.add_argument("--online", action="store_true", help="Include online X API diagnostics")

    stories_cmd = sub.add_parser("stories", help="Story memory operations")
    stories_sub = stories_cmd.add_subparsers(dest="stories_command", required=True)
    stories_status = stories_sub.add_parser("status", help="Show persisted story memory")
    stories_status.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_show = stories_sub.add_parser("show", help="Show one story with sources/entities")
    stories_show.add_argument("slug", help="Story slug")
    stories_show.add_argument(
        "--since-run-id",
        default=None,
        help="Only show story events since this run_id",
    )
    stories_show.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_merge = stories_sub.add_parser("merge", help="Merge two stories into a target slug")
    stories_merge.add_argument("source_a", help="Source story slug A")
    stories_merge.add_argument("source_b", help="Source story slug B")
    stories_merge.add_argument("--into", required=True, help="Target merged slug")
    stories_merge.add_argument("--title", default=None, help="Optional title override")
    stories_merge.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_split = stories_sub.add_parser("split", help="Split one story using a JSON plan")
    stories_split.add_argument("source", help="Source story slug")
    stories_split.add_argument("--plan", required=True, help="Path to split plan JSON")
    stories_split.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_pin = stories_sub.add_parser("pin", help="Pin a story")
    stories_pin.add_argument("slug", help="Story slug")
    stories_pin.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_unpin = stories_sub.add_parser("unpin", help="Unpin a story")
    stories_unpin.add_argument("slug", help="Story slug")
    stories_unpin.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_mute = stories_sub.add_parser("mute", help="Mute a story")
    stories_mute.add_argument("slug", help="Story slug")
    stories_mute.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_unmute = stories_sub.add_parser("unmute", help="Unmute a story")
    stories_unmute.add_argument("slug", help="Story slug")
    stories_unmute.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    stories_snooze = stories_sub.add_parser("snooze", help="Snooze a story until an ISO datetime")
    stories_snooze.add_argument("slug", help="Story slug")
    stories_snooze.add_argument("--until", required=True, help="ISO datetime, e.g. 2026-03-10T09:00:00Z")
    stories_snooze.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    conflicts_cmd = sub.add_parser("conflicts", help="Conflict ledger operations")
    conflicts_sub = conflicts_cmd.add_subparsers(dest="conflicts_command", required=True)
    conflicts_list = conflicts_sub.add_parser("list", help="List conflict records")
    conflicts_list.add_argument("--status", choices=["open", "resolved"], default=None)
    conflicts_list.add_argument("--limit", type=int, default=50)
    conflicts_list.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    conflicts_resolve = conflicts_sub.add_parser("resolve", help="Mark a conflict as resolved")
    conflicts_resolve.add_argument("conflict_id", help="Conflict ID")
    conflicts_resolve.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    entity_cmd = sub.add_parser("entity", help="Entity index operations")
    entity_sub = entity_cmd.add_subparsers(dest="entity_command", required=True)
    entity_list = entity_sub.add_parser("list", help="List indexed entities")
    entity_list.add_argument("--limit", type=int, default=50)
    entity_list.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_show = entity_sub.add_parser("show", help="Show one entity timeline")
    entity_show.add_argument("query", help="Entity alias, canonical name, or entity_id")
    entity_show.add_argument("--days", type=int, default=None, help="Window size in days")
    entity_show.add_argument(
        "--since-run-id",
        default=None,
        help="Only show timeline events since this run_id",
    )
    entity_show.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_pin = entity_sub.add_parser("pin", help="Pin an entity")
    entity_pin.add_argument("query", help="Entity alias/canonical/entity_id")
    entity_pin.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_unpin = entity_sub.add_parser("unpin", help="Unpin an entity")
    entity_unpin.add_argument("query", help="Entity alias/canonical/entity_id")
    entity_unpin.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_mute = entity_sub.add_parser("mute", help="Mute an entity")
    entity_mute.add_argument("query", help="Entity alias/canonical/entity_id")
    entity_mute.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_unmute = entity_sub.add_parser("unmute", help="Unmute an entity")
    entity_unmute.add_argument("query", help="Entity alias/canonical/entity_id")
    entity_unmute.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_snooze = entity_sub.add_parser("snooze", help="Snooze an entity until an ISO datetime")
    entity_snooze.add_argument("query", help="Entity alias/canonical/entity_id")
    entity_snooze.add_argument("--until", required=True, help="ISO datetime, e.g. 2026-03-10T09:00:00Z")
    entity_snooze.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    search_cmd = sub.add_parser("search", help="Run full-text search across Roberto memory")
    search_cmd.add_argument("query", help="Search query")
    search_cmd.add_argument(
        "--type",
        choices=["tweet", "story", "note", "entity", "idea", "conflict", "card"],
        default=None,
        help="Filter by result type",
    )
    search_cmd.add_argument("--days", type=int, default=None, help="Limit to last N days")
    search_cmd.add_argument("--limit", type=int, default=20, help="Max results")
    search_cmd.add_argument("--include-muted", action="store_true", help="Include muted/snoozed records")
    search_cmd.add_argument("--reindex", action="store_true", help="Force reindex before query")
    search_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    lens_cmd = sub.add_parser("lens", help="Saved query lenses")
    lens_sub = lens_cmd.add_subparsers(dest="lens_command", required=True)
    lens_list = lens_sub.add_parser("list", help="List configured lenses")
    lens_list.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    lens_run = lens_sub.add_parser("run", help="Run one lens")
    lens_run.add_argument("name", help="Lens name")
    lens_run.add_argument("--limit", type=int, default=20, help="Max results")
    lens_run.add_argument("--include-muted", action="store_true", help="Include muted/snoozed records")
    lens_run.add_argument("--reindex", action="store_true", help="Force reindex before query")
    lens_run.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    brief_cmd = sub.add_parser("brief", help="Show daily briefing in fast/deep mode")
    brief_cmd.add_argument("--mode", choices=["fast", "deep"], default="fast")
    brief_cmd.add_argument("--date", default=None, help="Date in YYYY-MM-DD; defaults to latest briefing")
    brief_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    greene_cmd = sub.add_parser("greene", help="Greene mode card operations")
    greene_sub = greene_cmd.add_subparsers(dest="greene_command", required=True)
    greene_sync = greene_sub.add_parser("sync", help="Run capture/distill/winnow cycle")
    greene_sync.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    greene_cards = greene_sub.add_parser("cards", help="List Greene cards")
    greene_cards.add_argument("--state", choices=["captured", "distilled", "keeper", "rejected"], default=None)
    greene_cards.add_argument("--week-key", default=None, help="ISO week key, e.g. 2026-W10")
    greene_cards.add_argument("--limit", type=int, default=50)
    greene_cards.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    chapters_cmd = sub.add_parser("chapters", help="Chapter emergence operations")
    chapters_sub = chapters_cmd.add_subparsers(dest="chapters_command", required=True)
    chapters_prop = chapters_sub.add_parser("propose", help="Propose chapter candidates from keeper cards")
    chapters_prop.add_argument("--topic", default=None, help="Optional topic/theme filter")
    chapters_prop.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    argument_cmd = sub.add_parser("argument", help="Build argument/counter/synthesis")
    argument_cmd.add_argument("--topic", default=None, help="Optional topic/theme filter")
    argument_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    gaps_cmd = sub.add_parser("gaps", help="List research gaps from keeper cards")
    gaps_cmd.add_argument("--topic", default=None, help="Optional topic/theme filter")
    gaps_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    profile_cmd = sub.add_parser("profile", help="Doctrine and taste calibration assets")
    profile_sub = profile_cmd.add_subparsers(dest="profile_command", required=True)
    profile_init = profile_sub.add_parser("init", help="Create doctrine and tags files if missing")
    profile_init.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    profile_show = profile_sub.add_parser("show", help="Show doctrine and tags contents")
    profile_show.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    feedback_cmd = sub.add_parser("feedback", help="Mark card feedback for taste calibration")
    feedback_sub = feedback_cmd.add_subparsers(dest="feedback_command", required=True)
    feedback_mark = feedback_sub.add_parser("mark", help="Mark feedback on a Greene card")
    feedback_mark.add_argument("--card", required=True, help="Greene card ID")
    feedback_mark.add_argument("--type", required=True, choices=["good", "bad", "wrong_pile", "wrong_story"])
    feedback_mark.add_argument("--note", default=None, help="Optional note")
    feedback_mark.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    draft_cmd = sub.add_parser("draft", help="Output studio draft generation")
    draft_sub = draft_cmd.add_subparsers(dest="draft_command", required=True)
    draft_generate = draft_sub.add_parser("generate", help="Generate citation-backed draft output")
    draft_generate.add_argument(
        "--mode",
        choices=["memo", "brief", "essay-skeleton", "chapter-draft", "compile"],
        default="memo",
    )
    draft_generate.add_argument("--topic", default=None, help="Optional topic/theme filter")
    draft_generate.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    actions_cmd = sub.add_parser("actions", help="AI action presets")
    actions_sub = actions_cmd.add_subparsers(dest="actions_command", required=True)
    actions_run = actions_sub.add_parser("run", help="Run one AI action")
    actions_run.add_argument("--name", required=True, choices=["one-issue", "challenge-thesis", "build-counter", "impact-top"])
    actions_run.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    editor_cmd = sub.add_parser("editor", help="Editorial control plane operations")
    editor_sub = editor_cmd.add_subparsers(dest="editor_command", required=True)
    editor_review = editor_sub.add_parser("review", help="Review staged diffs for a run")
    editor_review.add_argument("--run-id", required=True)
    editor_review.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    editor_promote = editor_sub.add_parser("promote", help="Promote staged notes for a run")
    editor_promote.add_argument("--run-id", required=True)
    editor_promote.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    editor_snapshots = editor_sub.add_parser("snapshots", help="List snapshots for a note path")
    editor_snapshots.add_argument("--note", required=True, help="Note path (relative or absolute)")
    editor_snapshots.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    editor_rollback = editor_sub.add_parser("rollback", help="Rollback a note from snapshots")
    editor_rollback.add_argument("--note", required=True, help="Note path (relative or absolute)")
    editor_rollback.add_argument("--snapshot-id", type=int, default=None)
    editor_rollback.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser


def _open_repo(settings) -> StorageRepo:
    db_path = settings.resolve("data", "roberto.db")
    return StorageRepo.from_path(db_path)


def _lock_path(settings) -> Path:
    return settings.resolve("data", "roberto.lock")


def _print_report(console: Console, report) -> None:
    console.print(f"Run [bold]{report.run_id}[/bold] ({report.mode})")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Username")
    table.add_column("New Tweets", justify="right")
    for username, count in sorted(report.per_user_new_tweets.items()):
        table.add_row(username, str(count))
    console.print(table)
    console.print(f"Created notes: {len(report.created_notes)}")
    console.print(f"Updated notes: {len(report.updated_notes)}")
    if getattr(report, "prompt_pack_version", None) or getattr(report, "schema_pack_version", None):
        console.print(
            "Prompt/Schema packs: "
            f"{getattr(report, 'prompt_pack_version', '-')}/{getattr(report, 'schema_pack_version', '-')}"
        )
    if getattr(report, "eval_gate_passed", None) is not None:
        status = "pass" if report.eval_gate_passed else "fail"
        console.print(f"Eval gate: {status}")
    if getattr(report, "greene_stats", None):
        stats = dict(report.greene_stats)
        if stats:
            console.print(
                "Greene stats: "
                f"captured={stats.get('captured', 0)}, keepers={stats.get('keepers', 0)}, "
                f"rejected={stats.get('rejected', 0)}"
            )
    if getattr(report, "staged_notes", None):
        console.print(f"Staged notes: {len(report.staged_notes)}")


def _resolve_project_path(settings, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.resolve()
    return settings.resolve(*candidate.parts).resolve()


def _run_started_at(repo: StorageRepo, run_id: str) -> str | None:
    run = repo.get_run(run_id)
    if not run:
        return None
    started = run.get("started_at")
    return str(started) if started else None


def _parse_iso(value: str) -> datetime | None:
    try:
        normalized = value
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _filter_since_run(rows: list[dict[str, Any]], since_started_at: str | None) -> list[dict[str, Any]]:
    if not since_started_at:
        return rows
    since_dt = _parse_iso(since_started_at)
    if not since_dt:
        return rows
    filtered: list[dict[str, Any]] = []
    for row in rows:
        event_dt = _parse_iso(str(row.get("created_at") or ""))
        if event_dt and event_dt >= since_dt:
            filtered.append(row)
    return filtered


def _set_attention(
    repo: StorageRepo,
    *,
    target_type: str,
    target_id: str,
    state: str,
    until: str | None = None,
) -> dict[str, str | None]:
    snoozed_until = until if state == "snoozed" else None
    repo.set_attention_state(
        target_type=target_type,
        target_id=target_id,
        state=state,
        snoozed_until=snoozed_until,
        updated_at=utc_now_iso(),
    )
    return {
        "target_type": target_type,
        "target_id": target_id,
        "state": state,
        "snoozed_until": snoozed_until,
    }


def _resolve_entity_query(settings, repo: StorageRepo, query: str) -> dict[str, Any] | None:
    overrides = load_entity_alias_overrides(settings)
    canonical_query = apply_entity_alias_override(query, overrides)
    return repo.resolve_entity(canonical_query) or repo.resolve_entity(query) or repo.get_entity(query)


def cmd_status(settings, console: Console, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        following = settings.resolve("config", "following.txt").read_text(encoding="utf-8").splitlines()
        following = [u.strip() for u in following if u.strip() and not u.strip().startswith("#")]

        rows: list[dict[str, str | int]] = []
        for username in following:
            row = repo.get_user(username) or {}
            count = repo.count_tweets(username)
            rows.append(
                {
                    "username": username,
                    "last_polled_at": str(row.get("last_polled_at") or "-"),
                    "last_seen_tweet_id": str(row.get("last_seen_tweet_id") or "-"),
                    "cached_tweets": count,
                }
            )

        if as_json:
            console.print_json(json.dumps({"users": rows}, sort_keys=True))
            return 0

        table = Table(show_header=True, header_style="bold")
        table.add_column("Username")
        table.add_column("Last Polled")
        table.add_column("Last Seen Tweet")
        table.add_column("Cached Tweets", justify="right")
        for item in rows:
            table.add_row(
                str(item["username"]),
                str(item["last_polled_at"]),
                str(item["last_seen_tweet_id"]),
                str(item["cached_tweets"]),
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_stories_status(settings, console: Console, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        stories = repo.list_stories(limit=200)
        if as_json:
            console.print_json(json.dumps({"stories": stories}, sort_keys=True))
            return 0

        table = Table(show_header=True, header_style="bold")
        table.add_column("Slug")
        table.add_column("Mentions", justify="right")
        table.add_column("Confidence")
        table.add_column("Attention")
        table.add_column("Last Run")
        for story in stories:
            attention = str(story.get("attention_state") or "active")
            if attention == "snoozed" and story.get("snoozed_until"):
                attention = f"snoozed:{story['snoozed_until']}"
            table.add_row(
                story["slug"],
                str(story["mention_count"]),
                story["confidence"],
                attention,
                story["last_seen_run_id"],
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_story_show(
    settings,
    console: Console,
    slug: str,
    *,
    since_run_id: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        story = repo.get_story_by_slug(slug)
        if not story:
            console.print(f"[red]Story not found:[/red] {slug}")
            return 1

        story_id = str(story["story_id"])
        sources = repo.list_story_sources(story_id, limit=240)
        since_started_at: str | None = None
        if since_run_id:
            since_started_at = _run_started_at(repo, since_run_id)
            if not since_started_at:
                console.print(f"[red]Run not found:[/red] {since_run_id}")
                return 1
            sources = _filter_since_run(list(sources), since_started_at)
        entities = repo.list_story_entities(story_id)
        aliases = repo.list_story_aliases(story_id)
        lineage = repo.list_story_lineage(story_id)
        payload = {
            "story": story,
            "sources": sources,
            "entities": entities,
            "aliases": aliases,
            "lineage": lineage,
            "since_run_id": since_run_id,
            "since_started_at": since_started_at,
        }

        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0

        console.print(f"[bold]{story['title']}[/bold] ({story['slug']})")
        console.print(f"Mentions: {story['mention_count']} | Confidence: {story['confidence']}")
        attention = story.get("attention_state") or "active"
        snoozed_until = story.get("snoozed_until")
        if attention == "snoozed" and snoozed_until:
            console.print(f"Attention: {attention} (until {snoozed_until})")
        else:
            console.print(f"Attention: {attention}")
        if since_run_id:
            console.print(f"Filtered since run: {since_run_id}")
        console.print(f"Entities: {', '.join(e['canonical_name'] for e in entities) if entities else 'none'}")
        if aliases:
            console.print(f"Aliases: {', '.join(aliases)}")
        if lineage:
            console.print(f"Lineage events: {len(lineage)}")
        table = Table(show_header=True, header_style="bold")
        table.add_column("Created At")
        table.add_column("Run")
        table.add_column("Source")
        for src in sources:
            username = src.get("username", "")
            tweet_id = src.get("tweet_id", "")
            table.add_row(
                str(src.get("created_at") or ""),
                str(src.get("run_id") or ""),
                f"{username}:{tweet_id}",
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_story_merge(
    settings,
    console: Console,
    source_a: str,
    source_b: str,
    *,
    into_slug: str,
    title: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        result = merge_stories(
            settings,
            repo,
            source_slug_a=source_a,
            source_slug_b=source_b,
            target_slug=into_slug,
            title=title,
            run_id=f"manual:{utc_now_iso()}",
            now_iso=utc_now_iso(),
        )
        rebuild_search_index(settings, repo)
        payload = {
            "target_story_id": result.target_story_id,
            "target_slug": result.target_slug,
            "merged_from": result.merged_from,
            "note_path": result.note_path,
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Merged into {result.target_slug} ({result.target_story_id})")
        console.print(f"Sources: {', '.join(result.merged_from)}")
        return 0
    except ValueError as exc:
        console.print(f"[red]Merge error:[/red] {exc}")
        return 1
    finally:
        repo.close()


def cmd_story_split(
    settings,
    console: Console,
    source: str,
    *,
    plan_path: str,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        result = split_story(
            settings,
            repo,
            source_slug=source,
            plan_path=Path(plan_path).resolve(),
            run_id=f"manual:{utc_now_iso()}",
            now_iso=utc_now_iso(),
        )
        rebuild_search_index(settings, repo)
        payload = {
            "parent_story_id": result.parent_story_id,
            "children": result.children,
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Split {source} into {len(result.children)} children")
        return 0
    except (ValueError, FileNotFoundError, json.JSONDecodeError) as exc:
        console.print(f"[red]Split error:[/red] {exc}")
        return 1
    finally:
        repo.close()


def cmd_story_attention(
    settings,
    console: Console,
    slug: str,
    *,
    state: str,
    until: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        story = repo.get_story_by_slug(slug)
        if not story:
            console.print(f"[red]Story not found:[/red] {slug}")
            return 1
        payload = _set_attention(
            repo,
            target_type="story",
            target_id=str(story["story_id"]),
            state=state,
            until=until,
        )
        rebuild_search_index(settings, repo)
        payload["slug"] = str(story["slug"])
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Story {story['slug']} -> {state}")
        return 0
    finally:
        repo.close()


def cmd_conflicts_list(
    settings,
    console: Console,
    *,
    status: str | None = None,
    limit: int = 50,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        rows = repo.list_conflicts(status=status, limit=max(1, limit))
        payload = {"status": status, "count": len(rows), "conflicts": rows}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        if not rows:
            console.print("No conflicts found.")
            return 0
        table = Table(show_header=True, header_style="bold")
        table.add_column("Conflict ID")
        table.add_column("Topic")
        table.add_column("Status")
        table.add_column("Updated")
        table.add_column("Sources", justify="right")
        for row in rows:
            table.add_row(
                str(row.get("conflict_id") or ""),
                str(row.get("topic") or ""),
                str(row.get("status") or "open"),
                str(row.get("updated_at") or row.get("created_at") or ""),
                str(len(row.get("source_refs") or [])),
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_conflicts_resolve(
    settings,
    console: Console,
    *,
    conflict_id: str,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        updated = repo.set_conflict_status(conflict_id, "resolved", updated_at=utc_now_iso())
        if not updated:
            console.print(f"[red]Conflict not found:[/red] {conflict_id}")
            return 1
        rebuild_search_index(settings, repo)
        row = next((r for r in repo.list_conflicts(limit=500) if r.get("conflict_id") == conflict_id), None)
        payload = {
            "conflict_id": conflict_id,
            "updated": updated,
            "status": "resolved",
            "conflict": row,
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Conflict {conflict_id} marked as resolved.")
        return 0
    finally:
        repo.close()


def cmd_entity_list(settings, console: Console, limit: int = 50, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        entities = repo.list_entities(limit=max(1, limit))
        if as_json:
            console.print_json(json.dumps({"entities": entities}, sort_keys=True))
            return 0

        table = Table(show_header=True, header_style="bold")
        table.add_column("Entity ID")
        table.add_column("Canonical Name")
        table.add_column("Attention")
        table.add_column("Last Seen")
        for entity in entities:
            attention = str(entity.get("attention_state") or "active")
            if attention == "snoozed" and entity.get("snoozed_until"):
                attention = f"snoozed:{entity['snoozed_until']}"
            table.add_row(
                str(entity["entity_id"]),
                str(entity["canonical_name"]),
                attention,
                str(entity["last_seen_at"]),
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_entity_attention(
    settings,
    console: Console,
    query: str,
    *,
    state: str,
    until: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        entity = _resolve_entity_query(settings, repo, query)
        if not entity:
            console.print(f"[red]Entity not found:[/red] {query}")
            return 1
        payload = _set_attention(
            repo,
            target_type="entity",
            target_id=str(entity["entity_id"]),
            state=state,
            until=until,
        )
        rebuild_search_index(settings, repo)
        payload["entity_id"] = str(entity["entity_id"])
        payload["canonical_name"] = str(entity["canonical_name"])
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Entity {entity['canonical_name']} -> {state}")
        return 0
    finally:
        repo.close()


def cmd_entity_show(
    settings,
    console: Console,
    query: str,
    *,
    days: int | None = None,
    since_run_id: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        entity = _resolve_entity_query(settings, repo, query)
        if not entity:
            console.print(f"[red]Entity not found:[/red] {query}")
            return 1

        entity_id = str(entity["entity_id"])
        timeline_days = days if days and days > 0 else settings.v7.timeline_default_days
        aliases = repo.get_entity_aliases(entity_id)
        timeline = repo.get_entity_timeline(entity_id, days=timeline_days, limit=500)
        since_started_at: str | None = None
        if since_run_id:
            since_started_at = _run_started_at(repo, since_run_id)
            if not since_started_at:
                console.print(f"[red]Run not found:[/red] {since_run_id}")
                return 1
            timeline = _filter_since_run(list(timeline), since_started_at)
        payload = {
            "entity": entity,
            "aliases": aliases,
            "days": timeline_days,
            "since_run_id": since_run_id,
            "since_started_at": since_started_at,
            "timeline": timeline,
        }

        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0

        console.print(f"[bold]{entity['canonical_name']}[/bold] ({entity_id})")
        console.print(f"Aliases: {', '.join(aliases) if aliases else 'none'}")
        attention = entity.get("attention_state") or "active"
        snoozed_until = entity.get("snoozed_until")
        if attention == "snoozed" and snoozed_until:
            console.print(f"Attention: {attention} (until {snoozed_until})")
        else:
            console.print(f"Attention: {attention}")
        console.print(f"Window: {timeline_days} days | Events: {len(timeline)}")
        if since_run_id:
            console.print(f"Filtered since run: {since_run_id}")
        table = Table(show_header=True, header_style="bold")
        table.add_column("When")
        table.add_column("Type")
        table.add_column("Reference")
        for row in timeline:
            ref_type = str(row.get("ref_type") or "")
            if ref_type == "tweet":
                username = row.get("username") or "unknown"
                tweet_id = row.get("ref_id") or ""
                reference = f"@{username}:{tweet_id}"
            elif ref_type == "story":
                reference = str(row.get("story_title") or row.get("ref_id") or "")
            else:
                reference = str(row.get("ref_id") or "")
            table.add_row(str(row.get("created_at") or ""), ref_type, reference)
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_editor_review(settings, console: Console, run_id: str, *, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        rows = repo.list_staged_notes(run_id, status="staged")
        payload: list[dict[str, object]] = []
        for row in rows:
            live_path = Path(str(row["live_path"]))
            staged_path = Path(str(row["staged_path"]))
            diff = build_diff_preview(live_path, staged_path, max_lines=settings.v13.max_diff_lines)
            payload.append(
                {
                    "run_id": run_id,
                    "note_type": row["note_type"],
                    "live_path": str(live_path),
                    "staged_path": str(staged_path),
                    "trigger_refs": row.get("trigger_refs", []),
                    "diff": diff,
                }
            )

        if as_json:
            console.print_json(json.dumps({"run_id": run_id, "items": payload}, sort_keys=True))
            return 0

        if not payload:
            console.print(f"No staged notes for run {run_id}.")
            return 0

        table = Table(show_header=True, header_style="bold")
        table.add_column("Type")
        table.add_column("Live Path")
        table.add_column("Added", justify="right")
        table.add_column("Removed", justify="right")
        table.add_column("Refs", justify="right")
        for item in payload:
            diff = item["diff"]
            trigger_refs = item.get("trigger_refs") or []
            table.add_row(
                str(item["note_type"]),
                str(item["live_path"]),
                str(diff["added_lines"]),
                str(diff["removed_lines"]),
                str(len(trigger_refs)),
            )
        console.print(table)
        for item in payload:
            console.rule(f"{item['note_type']} :: {item['live_path']}")
            refs = item.get("trigger_refs") or []
            if refs:
                ref_text = ", ".join(f"{r['username']}:{r['tweet_id']}" for r in refs[:12])
                console.print(f"Trigger refs: {ref_text}")
            diff_text = str(item["diff"]["diff"]).strip()
            console.print(diff_text if diff_text else "(no diff)")
        return 0
    finally:
        repo.close()


def cmd_editor_promote(settings, console: Console, run_id: str, *, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        result = promote_staged_run(repo, run_id, now_iso=utc_now_iso())
        rebuild_search_index(settings, repo)
        repo.patch_run_stats(run_id, {"approved_notes": result.promoted})
        payload = {
            "run_id": run_id,
            "promoted": result.promoted,
            "missing_staged_files": result.missing_staged_files,
            "snapshot_ids": result.snapshot_ids,
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Promoted notes: {len(result.promoted)}")
        if result.missing_staged_files:
            console.print(f"Missing staged files: {len(result.missing_staged_files)}")
        return 0
    finally:
        repo.close()


def cmd_editor_snapshots(settings, console: Console, note: str, *, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        note_path = _resolve_project_path(settings, note)
        rows = repo.list_note_snapshots(str(note_path), limit=50)
        payload = {"note_path": str(note_path), "snapshots": rows}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        if not rows:
            console.print(f"No snapshots found for {note_path}")
            return 0
        table = Table(show_header=True, header_style="bold")
        table.add_column("Snapshot ID", justify="right")
        table.add_column("Captured At")
        table.add_column("Reason")
        table.add_column("Run ID")
        for row in rows:
            table.add_row(
                str(row["snapshot_id"]),
                str(row["captured_at"]),
                str(row["reason"]),
                str(row.get("run_id") or "-"),
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_editor_rollback(
    settings,
    console: Console,
    note: str,
    *,
    snapshot_id: int | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        note_path = _resolve_project_path(settings, note)
        result = rollback_note(
            repo,
            note_path=str(note_path),
            now_iso=utc_now_iso(),
            snapshot_id=snapshot_id,
        )
        rebuild_search_index(settings, repo)
        payload = {
            "note_path": result.note_path,
            "restored_snapshot_id": result.restored_snapshot_id,
            "created_snapshot_id": result.created_snapshot_id,
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(
            f"Rollback complete for {result.note_path} using snapshot {result.restored_snapshot_id}"
        )
        return 0
    except ValueError as exc:
        console.print(f"[red]Rollback error:[/red] {exc}")
        return 1
    finally:
        repo.close()


def _load_lenses(settings) -> list[dict[str, object]]:
    path = settings.resolve("config", "lenses.yaml")
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    raw = payload.get("lenses", [])
    if not isinstance(raw, list):
        return []
    lenses: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        query = str(item.get("query") or "").strip()
        if not name or not query:
            continue
        lenses.append(
            {
                "name": name,
                "query": query,
                "type": item.get("type"),
                "days": item.get("days"),
            }
        )
    return lenses


def cmd_search(
    settings,
    console: Console,
    query: str,
    *,
    kind: str | None = None,
    days: int | None = None,
    limit: int = 20,
    include_muted: bool = False,
    reindex: bool = False,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        if reindex:
            rebuild_search_index(settings, repo)
        effective_include_muted = include_muted or not settings.v15.apply_muted_filters
        rows = search(
            settings,
            repo,
            query,
            kind=kind,
            limit=max(1, limit),
            days=days,
            include_muted=effective_include_muted,
            now_iso=utc_now_iso(),
        )
        payload = {
            "query": query,
            "type": kind,
            "days": days,
            "include_muted": effective_include_muted,
            "count": len(rows),
            "results": rows,
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        table = Table(show_header=True, header_style="bold")
        table.add_column("Type")
        table.add_column("Title")
        table.add_column("Ref")
        table.add_column("Sources")
        for row in rows:
            table.add_row(
                str(row.get("kind") or ""),
                str(row.get("title") or row.get("item_id") or ""),
                str(row.get("ref_path") or row.get("item_id") or ""),
                str(row.get("source_ids") or ""),
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_lens_list(settings, console: Console, *, as_json: bool = False) -> int:
    rows = _load_lenses(settings)
    if as_json:
        console.print_json(json.dumps({"lenses": rows}, sort_keys=True))
        return 0
    if not rows:
        console.print("No lenses configured.")
        return 0
    table = Table(show_header=True, header_style="bold")
    table.add_column("Name")
    table.add_column("Query")
    table.add_column("Type")
    table.add_column("Days")
    for row in rows:
        table.add_row(
            str(row["name"]),
            str(row["query"]),
            str(row.get("type") or "-"),
            str(row.get("days") or "-"),
        )
    console.print(table)
    return 0


def cmd_lens_run(
    settings,
    console: Console,
    name: str,
    *,
    limit: int = 20,
    include_muted: bool = False,
    reindex: bool = False,
    as_json: bool = False,
) -> int:
    rows = _load_lenses(settings)
    lens = next((row for row in rows if str(row.get("name")) == name), None)
    if not lens:
        console.print(f"[red]Lens not found:[/red] {name}")
        return 1
    kind = str(lens.get("type")) if lens.get("type") else None
    days = int(lens["days"]) if isinstance(lens.get("days"), int) else None
    return cmd_search(
        settings,
        console,
        query=str(lens["query"]),
        kind=kind,
        days=days,
        limit=limit,
        include_muted=include_muted,
        reindex=reindex,
        as_json=as_json,
    )


def cmd_brief(
    settings,
    console: Console,
    *,
    mode: str = "fast",
    date: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        row = repo.get_briefing_by_date(date) if date else repo.get_latest_briefing()
        if not row:
            if as_json:
                console.print_json(json.dumps({"found": False, "date": date, "mode": mode}, sort_keys=True))
            else:
                console.print("No briefing found.")
            return 1

        summary = dict(row.get("summary") or {})
        text = render_briefing(summary, mode=mode)
        payload = {
            "found": True,
            "brief_id": row.get("brief_id"),
            "brief_date": row.get("brief_date"),
            "run_id": row.get("run_id"),
            "mode": mode,
            "note_path": row.get("note_path"),
            "summary": summary,
            "text": text,
            "items": repo.list_briefing_items(str(row.get("brief_id") or "")),
        }
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(text)
        return 0
    finally:
        repo.close()


def cmd_greene_sync(settings, console: Console, *, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        settings.resolve("notes", "greene", "cards").mkdir(parents=True, exist_ok=True)
        settings.resolve("notes", "greene", "chapters").mkdir(parents=True, exist_ok=True)
        settings.resolve("notes", "greene", "argumentation").mkdir(parents=True, exist_ok=True)
        settings.resolve("notes", "greene", "gaps").mkdir(parents=True, exist_ok=True)
        settings.resolve("notes", "greene", "drafts").mkdir(parents=True, exist_ok=True)
        run_id = f"manual:{utc_now_iso()}"
        now_iso = utc_now_iso()
        payload = {"run_id": run_id}
        payload["v19"] = run_greene_cycle(settings, repo, run_id=run_id, now_iso=now_iso)
        if settings.v21.enabled:
            payload["v21"] = run_chapter_argument_gap_cycle(
                settings,
                repo,
                run_id=run_id,
                now_iso=now_iso,
            )
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Greene sync complete for {run_id}")
        console.print_json(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    finally:
        repo.close()


def cmd_greene_cards(
    settings,
    console: Console,
    *,
    state: str | None = None,
    week_key: str | None = None,
    limit: int = 50,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        rows = list_cards(repo, state=state, week_key=week_key, limit=max(1, limit))
        payload = {"count": len(rows), "state": state, "week_key": week_key, "cards": rows}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        if not rows:
            console.print("No Greene cards found.")
            return 0
        table = Table(show_header=True, header_style="bold")
        table.add_column("Card ID")
        table.add_column("Type")
        table.add_column("State")
        table.add_column("Theme")
        table.add_column("Score", justify="right")
        table.add_column("Title")
        for row in rows:
            title = str(row.get("title") or "")
            if len(title) > 64:
                title = title[:63] + "..."
            table.add_row(
                str(row.get("card_id") or ""),
                str(row.get("card_type") or ""),
                str(row.get("state") or ""),
                str(row.get("theme") or ""),
                str(row.get("score") or ""),
                title,
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_chapters_propose(
    settings,
    console: Console,
    *,
    topic: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        run_id = f"manual:{utc_now_iso()}"
        now_iso = utc_now_iso()
        settings.resolve("notes", "greene", "chapters").mkdir(parents=True, exist_ok=True)
        chapters = propose_chapters(settings, repo, run_id=run_id, now_iso=now_iso, topic=topic)
        note_text = render_chapter_note(chapters)
        note_path = settings.resolve("notes", "greene", "chapters", f"{now_iso[:10]}.md")
        note_res = update_note_file(
            note_path,
            note_type="greene",
            run_id=run_id,
            now_iso=now_iso,
            auto_body=note_text,
            note_title=f"Chapter Emergence - {now_iso[:10]}",
        )
        repo.upsert_note_index(
            NoteIndexUpsert(
                note_path=str(note_path),
                note_type="greene",
                username=None,
                created_at=note_res.created_at,
                updated_at=note_res.updated_at,
                last_run_id=run_id,
            )
        )
        payload = {"run_id": run_id, "topic": topic, "chapters": chapters, "note_path": str(note_path)}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(note_text)
        return 0
    finally:
        repo.close()


def cmd_argument(
    settings,
    console: Console,
    *,
    topic: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        run_id = f"manual:{utc_now_iso()}"
        now_iso = utc_now_iso()
        settings.resolve("notes", "greene", "argumentation").mkdir(parents=True, exist_ok=True)
        argument = build_argumentation(repo, topic=topic)
        text = render_argumentation(argument)
        note_path = settings.resolve("notes", "greene", "argumentation", f"{now_iso[:10]}.md")
        note_res = update_note_file(
            note_path,
            note_type="greene",
            run_id=run_id,
            now_iso=now_iso,
            auto_body=text,
            note_title=f"Argumentation - {now_iso[:10]}",
        )
        repo.upsert_note_index(
            NoteIndexUpsert(
                note_path=str(note_path),
                note_type="greene",
                username=None,
                created_at=note_res.created_at,
                updated_at=note_res.updated_at,
                last_run_id=run_id,
            )
        )
        payload = {"run_id": run_id, "topic": topic, "argument": argument, "note_path": str(note_path)}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(text)
        return 0
    finally:
        repo.close()


def cmd_gaps(
    settings,
    console: Console,
    *,
    topic: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        run_id = f"manual:{utc_now_iso()}"
        now_iso = utc_now_iso()
        settings.resolve("notes", "greene", "gaps").mkdir(parents=True, exist_ok=True)
        cards = repo.list_greene_cards(state="keeper", limit=5000)
        if topic:
            needle = topic.lower()
            cards = [
                c
                for c in cards
                if needle in " ".join(
                    [
                        str(c.get("theme") or ""),
                        str(c.get("title") or ""),
                        str(c.get("payload") or ""),
                    ]
                ).lower()
            ]
        gaps = detect_gaps(cards)
        text = render_gap_note(gaps)
        note_path = settings.resolve("notes", "greene", "gaps", f"{now_iso[:10]}.md")
        note_res = update_note_file(
            note_path,
            note_type="greene",
            run_id=run_id,
            now_iso=now_iso,
            auto_body=text,
            note_title=f"Gap Finder - {now_iso[:10]}",
        )
        repo.upsert_note_index(
            NoteIndexUpsert(
                note_path=str(note_path),
                note_type="greene",
                username=None,
                created_at=note_res.created_at,
                updated_at=note_res.updated_at,
                last_run_id=run_id,
            )
        )
        payload = {"run_id": run_id, "topic": topic, "gap_count": len(gaps), "gaps": gaps, "note_path": str(note_path)}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(text)
        return 0
    finally:
        repo.close()


def cmd_profile_init(settings, console: Console, *, as_json: bool = False) -> int:
    payload = ensure_profile_assets(settings)
    if as_json:
        console.print_json(json.dumps(payload, sort_keys=True))
        return 0
    console.print(f"Doctrine: {payload['doctrine_path']}")
    console.print(f"Tags: {payload['tags_path']}")
    return 0


def cmd_profile_show(settings, console: Console, *, as_json: bool = False) -> int:
    payload = ensure_profile_assets(settings)
    doctrine_path = Path(payload["doctrine_path"])
    tags_path = Path(payload["tags_path"])
    doctrine = doctrine_path.read_text(encoding="utf-8") if doctrine_path.exists() else ""
    tags = yaml.safe_load(tags_path.read_text(encoding="utf-8")) if tags_path.exists() else {}
    out = {"doctrine_path": str(doctrine_path), "tags_path": str(tags_path), "doctrine": doctrine, "tags": tags}
    if as_json:
        console.print_json(json.dumps(out, sort_keys=True))
        return 0
    console.print(f"[bold]Doctrine[/bold] {doctrine_path}")
    console.print(doctrine)
    console.print(f"[bold]Tags[/bold] {tags_path}")
    console.print_json(json.dumps(tags, indent=2, sort_keys=True))
    return 0


def cmd_feedback_mark(
    settings,
    console: Console,
    *,
    card_id: str,
    feedback_type: str,
    note: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        feedback_id = mark_card_feedback(
            repo,
            card_id=card_id,
            feedback=feedback_type,
            note=note,
            now_iso=utc_now_iso(),
        )
        payload = {"feedback_id": feedback_id, "card_id": card_id, "type": feedback_type, "note": note}
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(f"Feedback recorded: {feedback_id}")
        return 0
    finally:
        repo.close()


def cmd_draft_generate(
    settings,
    console: Console,
    *,
    mode: str,
    topic: str | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        run_id = f"manual:{utc_now_iso()}"
        now_iso = utc_now_iso()
        settings.resolve("notes", "greene", "drafts").mkdir(parents=True, exist_ok=True)
        payload = generate_draft(
            settings,
            repo,
            run_id=run_id,
            now_iso=now_iso,
            mode=mode,
            topic=topic,
        )
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(payload["text"])
        console.print(f"Output path: {payload['output_path']}")
        return 0
    finally:
        repo.close()


def cmd_action_run(settings, console: Console, *, name: str, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        payload = run_ai_action(settings, repo, action=name)
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0
        console.print(payload.get("text") or "")
        refs = payload.get("refs") or []
        if refs:
            ref_text = ", ".join(f"{r['username']}:{r['tweet_id']}" for r in refs)
            console.print(f"Sources: {ref_text}")
        return 0
    finally:
        repo.close()


def cmd_export(settings, fmt: str, console: Console) -> int:
    repo = _open_repo(settings)
    try:
        last_run = repo.get_last_run()
        if not last_run:
            console.print("No runs found.")
            return 1

        if fmt == "json":
            console.print_json(json.dumps(last_run.get("stats_json", {})))
            return 0

        latest_digest = repo.get_latest_digest_note()
        if not latest_digest:
            console.print("No digest notes found.")
            return 1

        digest_path = Path(latest_digest["note_path"])
        if not digest_path.exists():
            console.print(f"Digest path missing on disk: {digest_path}")
            return 1

        console.print(digest_path.read_text(encoding="utf-8"))
        return 0
    finally:
        repo.close()


def cmd_import_json(settings, file_path: str, default_username: str | None, console: Console) -> int:
    repo = _open_repo(settings)
    try:
        with run_lock(_lock_path(settings)):
            report = import_json_file(
                repo,
                Path(file_path),
                default_username=default_username,
            )
        console.print_json(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return 0
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Import error:[/red] {exc}")
        return 1
    finally:
        repo.close()


def cmd_sync(settings, console: Console, *, full: bool = False) -> int:
    token = require_x_bearer_token(settings)
    repo = _open_repo(settings)
    settings.resolve("data", "exports").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "logs").mkdir(parents=True, exist_ok=True)
    try:
        with run_lock(_lock_path(settings)):
            with XClient(
                token,
                timeout_s=settings.x.request_timeout_s,
                retry_max_attempts=settings.x.retry.max_attempts,
                backoff_s=settings.x.retry.backoff_s,
            ) as x_client:
                report = run_sync(settings, repo, x_client, full=full)

        table = Table(show_header=True, header_style="bold")
        table.add_column("Username")
        table.add_column("New Tweets", justify="right")
        for username, count in sorted(report.per_user_new_tweets.items()):
            table.add_row(username, str(count))
        console.print(f"Sync {report.run_id} ({report.mode})")
        console.print(table)
        return 0
    except XAPIError as exc:
        console.print(f"[red]X API error:[/red] {exc}")
        return 1
    finally:
        repo.close()


def cmd_eval(
    settings,
    console: Console,
    fixture: str | None = None,
    *,
    fixtures_dir: str | None = None,
    baseline: str | None = None,
    as_json: bool = False,
) -> int:
    try:
        fixture_path = Path(fixture).resolve() if fixture else None
        fixtures_dir_path = Path(fixtures_dir).resolve() if fixtures_dir else None
        baseline_path = Path(baseline).resolve() if baseline else None
        result = run_eval(
            settings,
            fixture_path=fixture_path,
            fixtures_dir=fixtures_dir_path,
            baseline_path=baseline_path,
        )
        payload = result.to_dict()
        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
        else:
            console.print(f"Eval fixture: {payload['fixture_path']}")
            table = Table(show_header=True, header_style="bold")
            table.add_column("Metric")
            table.add_column("Value", justify="right")
            for key, value in payload["metrics"].items():
                table.add_row(key, str(value))
            console.print(table)
            if payload.get("baseline_metrics"):
                base_table = Table(show_header=True, header_style="bold")
                base_table.add_column("Baseline Metric")
                base_table.add_column("Value", justify="right")
                for key, value in payload["baseline_metrics"].items():
                    base_table.add_row(key, str(value))
                console.print(base_table)
            failures = payload.get("failures") or []
            if failures:
                for failure in failures:
                    console.print(f"[red]Gate:[/red] {failure}")
            console.print(f"Passed: {'yes' if payload['passed'] else 'no'}")
        return 0 if payload["passed"] else 1
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Eval error:[/red] {exc}")
        return 1


def cmd_doctor(settings, console: Console, *, as_json: bool = False, online: bool = False) -> int:
    report = run_doctor(settings, online=online)
    payload = report.to_dict()
    if as_json:
        console.print_json(json.dumps(payload, sort_keys=True))
        return 0 if report.ok else 1

    table = Table(show_header=True, header_style="bold")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Message")
    table.add_column("Hint")
    for check in payload["checks"]:
        table.add_row(
            str(check["name"]),
            str(check["status"]),
            str(check["message"]),
            str(check.get("hint") or ""),
        )
    console.print(table)
    console.print(f"Doctor overall: {'OK' if report.ok else 'HAS_ERRORS'}")
    return 0 if report.ok else 1


def cmd_pipeline(
    settings,
    command: str,
    console: Console,
    *,
    from_db_only: bool = False,
    resume: bool = False,
) -> int:
    api_key = require_gemini_api_key(settings)
    repo = _open_repo(settings)

    settings.resolve("notes", "users").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "digests").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "ideas").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "shuffles").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "conflicts").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "entities").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "briefings").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "greene", "cards").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "greene", "chapters").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "greene", "argumentation").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "greene", "gaps").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "greene", "drafts").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "_staging").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "exports").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "logs").mkdir(parents=True, exist_ok=True)
    settings.resolve("profile").mkdir(parents=True, exist_ok=True)

    needs_x = command == "v1" or (command == "v2" and not from_db_only)
    x_client = None

    try:
        with run_lock(_lock_path(settings)):
            if needs_x:
                token = require_x_bearer_token(settings)
                x_client = XClient(
                    token,
                    timeout_s=settings.x.request_timeout_s,
                    retry_max_attempts=settings.x.retry.max_attempts,
                    backoff_s=settings.x.retry.backoff_s,
                )

            llm = GeminiSummarizer(settings.llm, repo, api_key=api_key, app_settings=settings)
            if command == "v1":
                if x_client is None:
                    raise RuntimeError("X client missing for v1")
                report = run_v1(settings, repo, x_client, llm, resume=resume)
            elif command == "build":
                report = run_build(settings, repo, llm)
            else:
                report = run_v2(settings, repo, x_client, llm, from_db_only=from_db_only, resume=resume)

        _print_report(console, report)
        return 0
    except XAPIError as exc:
        console.print(f"[red]X API error:[/red] {exc}")
        return 1
    except RuntimeError as exc:
        console.print(f"[red]Runtime error:[/red] {exc}")
        return 1
    finally:
        if x_client is not None:
            x_client.close()
        repo.close()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    settings = load_settings(args.base_dir)
    setup_logging(settings.log_level)
    console = Console()

    if args.command == "status":
        return cmd_status(settings, console, as_json=getattr(args, "json", False))
    if args.command == "export":
        return cmd_export(settings, args.format, console)
    if args.command == "import-json":
        return cmd_import_json(settings, args.file, args.default_username, console)
    if args.command == "sync":
        return cmd_sync(settings, console, full=args.full)
    if args.command == "stories":
        if args.stories_command == "status":
            return cmd_stories_status(settings, console, as_json=getattr(args, "json", False))
        if args.stories_command == "show":
            return cmd_story_show(
                settings,
                console,
                args.slug,
                since_run_id=getattr(args, "since_run_id", None),
                as_json=getattr(args, "json", False),
            )
        if args.stories_command == "merge":
            return cmd_story_merge(
                settings,
                console,
                source_a=args.source_a,
                source_b=args.source_b,
                into_slug=args.into,
                title=args.title,
                as_json=getattr(args, "json", False),
            )
        if args.stories_command == "split":
            return cmd_story_split(
                settings,
                console,
                source=args.source,
                plan_path=args.plan,
                as_json=getattr(args, "json", False),
            )
        if args.stories_command == "pin":
            return cmd_story_attention(settings, console, args.slug, state="pinned", as_json=args.json)
        if args.stories_command == "unpin":
            return cmd_story_attention(settings, console, args.slug, state="active", as_json=args.json)
        if args.stories_command == "mute":
            return cmd_story_attention(settings, console, args.slug, state="muted", as_json=args.json)
        if args.stories_command == "unmute":
            return cmd_story_attention(settings, console, args.slug, state="active", as_json=args.json)
        if args.stories_command == "snooze":
            return cmd_story_attention(
                settings,
                console,
                args.slug,
                state="snoozed",
                until=args.until,
                as_json=args.json,
            )
        parser.error("Unknown stories subcommand")
        return 2
    if args.command == "conflicts":
        if args.conflicts_command == "list":
            return cmd_conflicts_list(
                settings,
                console,
                status=getattr(args, "status", None),
                limit=getattr(args, "limit", 50),
                as_json=getattr(args, "json", False),
            )
        if args.conflicts_command == "resolve":
            return cmd_conflicts_resolve(
                settings,
                console,
                conflict_id=args.conflict_id,
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown conflicts subcommand")
        return 2
    if args.command == "entity":
        if args.entity_command == "list":
            return cmd_entity_list(
                settings,
                console,
                limit=getattr(args, "limit", 50),
                as_json=getattr(args, "json", False),
            )
        if args.entity_command == "show":
            return cmd_entity_show(
                settings,
                console,
                query=args.query,
                days=getattr(args, "days", None),
                since_run_id=getattr(args, "since_run_id", None),
                as_json=getattr(args, "json", False),
            )
        if args.entity_command == "pin":
            return cmd_entity_attention(settings, console, args.query, state="pinned", as_json=args.json)
        if args.entity_command == "unpin":
            return cmd_entity_attention(settings, console, args.query, state="active", as_json=args.json)
        if args.entity_command == "mute":
            return cmd_entity_attention(settings, console, args.query, state="muted", as_json=args.json)
        if args.entity_command == "unmute":
            return cmd_entity_attention(settings, console, args.query, state="active", as_json=args.json)
        if args.entity_command == "snooze":
            return cmd_entity_attention(
                settings,
                console,
                args.query,
                state="snoozed",
                until=args.until,
                as_json=args.json,
            )
        parser.error("Unknown entity subcommand")
        return 2
    if args.command == "search":
        return cmd_search(
            settings,
            console,
            args.query,
            kind=getattr(args, "type", None),
            days=getattr(args, "days", None),
            limit=getattr(args, "limit", 20),
            include_muted=getattr(args, "include_muted", False),
            reindex=getattr(args, "reindex", False),
            as_json=getattr(args, "json", False),
        )
    if args.command == "lens":
        if args.lens_command == "list":
            return cmd_lens_list(settings, console, as_json=getattr(args, "json", False))
        if args.lens_command == "run":
            return cmd_lens_run(
                settings,
                console,
                args.name,
                limit=getattr(args, "limit", 20),
                include_muted=getattr(args, "include_muted", False),
                reindex=getattr(args, "reindex", False),
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown lens subcommand")
        return 2
    if args.command == "brief":
        return cmd_brief(
            settings,
            console,
            mode=getattr(args, "mode", "fast"),
            date=getattr(args, "date", None),
            as_json=getattr(args, "json", False),
        )
    if args.command == "greene":
        if args.greene_command == "sync":
            return cmd_greene_sync(settings, console, as_json=getattr(args, "json", False))
        if args.greene_command == "cards":
            return cmd_greene_cards(
                settings,
                console,
                state=getattr(args, "state", None),
                week_key=getattr(args, "week_key", None),
                limit=getattr(args, "limit", 50),
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown greene subcommand")
        return 2
    if args.command == "chapters":
        if args.chapters_command == "propose":
            return cmd_chapters_propose(
                settings,
                console,
                topic=getattr(args, "topic", None),
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown chapters subcommand")
        return 2
    if args.command == "argument":
        return cmd_argument(
            settings,
            console,
            topic=getattr(args, "topic", None),
            as_json=getattr(args, "json", False),
        )
    if args.command == "gaps":
        return cmd_gaps(
            settings,
            console,
            topic=getattr(args, "topic", None),
            as_json=getattr(args, "json", False),
        )
    if args.command == "profile":
        if args.profile_command == "init":
            return cmd_profile_init(settings, console, as_json=getattr(args, "json", False))
        if args.profile_command == "show":
            return cmd_profile_show(settings, console, as_json=getattr(args, "json", False))
        parser.error("Unknown profile subcommand")
        return 2
    if args.command == "feedback":
        if args.feedback_command == "mark":
            return cmd_feedback_mark(
                settings,
                console,
                card_id=args.card,
                feedback_type=args.type,
                note=getattr(args, "note", None),
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown feedback subcommand")
        return 2
    if args.command == "draft":
        if args.draft_command == "generate":
            return cmd_draft_generate(
                settings,
                console,
                mode=args.mode,
                topic=getattr(args, "topic", None),
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown draft subcommand")
        return 2
    if args.command == "actions":
        if args.actions_command == "run":
            return cmd_action_run(
                settings,
                console,
                name=args.name,
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown actions subcommand")
        return 2
    if args.command == "editor":
        if args.editor_command == "review":
            return cmd_editor_review(settings, console, run_id=args.run_id, as_json=args.json)
        if args.editor_command == "promote":
            return cmd_editor_promote(settings, console, run_id=args.run_id, as_json=args.json)
        if args.editor_command == "snapshots":
            return cmd_editor_snapshots(settings, console, note=args.note, as_json=args.json)
        if args.editor_command == "rollback":
            return cmd_editor_rollback(
                settings,
                console,
                note=args.note,
                snapshot_id=args.snapshot_id,
                as_json=args.json,
            )
        parser.error("Unknown editor subcommand")
        return 2
    if args.command == "build":
        return cmd_pipeline(settings, "build", console, from_db_only=True)
    if args.command == "eval":
        return cmd_eval(
            settings,
            console,
            fixture=args.fixture,
            fixtures_dir=args.fixtures_dir,
            baseline=args.baseline,
            as_json=args.json,
        )
    if args.command == "doctor":
        return cmd_doctor(settings, console, as_json=args.json, online=args.online)
    if args.command in {"v1", "v2"}:
        return cmd_pipeline(
            settings,
            args.command,
            console,
            from_db_only=getattr(args, "from_db_only", False),
            resume=getattr(args, "resume", False),
        )

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
