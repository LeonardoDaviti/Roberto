from __future__ import annotations

import argparse
import json
from pathlib import Path

from rich.console import Console
from rich.table import Table

from roberto_app.llm.gemini import GeminiSummarizer
from roberto_app.logging_setup import setup_logging
from roberto_app.pipeline.build import run_build
from roberto_app.pipeline.doctor import run_doctor
from roberto_app.pipeline.editorial import build_diff_preview, promote_staged_run, rollback_note
from roberto_app.pipeline.eval import run_eval
from roberto_app.pipeline.import_json import import_json_file
from roberto_app.pipeline.lock import run_lock
from roberto_app.pipeline.sync import run_sync
from roberto_app.pipeline.v1 import run_v1
from roberto_app.pipeline.v2 import run_v2
from roberto_app.pipeline.common import utc_now_iso
from roberto_app.settings import (
    load_settings,
    require_gemini_api_key,
    require_x_bearer_token,
)
from roberto_app.storage.repo import StorageRepo
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
    stories_show.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    entity_cmd = sub.add_parser("entity", help="Entity index operations")
    entity_sub = entity_cmd.add_subparsers(dest="entity_command", required=True)
    entity_list = entity_sub.add_parser("list", help="List indexed entities")
    entity_list.add_argument("--limit", type=int, default=50)
    entity_list.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    entity_show = entity_sub.add_parser("show", help="Show one entity timeline")
    entity_show.add_argument("query", help="Entity alias, canonical name, or entity_id")
    entity_show.add_argument("--days", type=int, default=None, help="Window size in days")
    entity_show.add_argument("--json", action="store_true", help="Print machine-readable JSON")

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
    if getattr(report, "staged_notes", None):
        console.print(f"Staged notes: {len(report.staged_notes)}")


def _resolve_project_path(settings, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.resolve()
    return settings.resolve(*candidate.parts).resolve()


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
        table.add_column("Last Run")
        for story in stories:
            table.add_row(
                story["slug"],
                str(story["mention_count"]),
                story["confidence"],
                story["last_seen_run_id"],
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_story_show(settings, console: Console, slug: str, as_json: bool = False) -> int:
    repo = _open_repo(settings)
    try:
        story = repo.get_story_by_slug(slug)
        if not story:
            console.print(f"[red]Story not found:[/red] {slug}")
            return 1

        story_id = str(story["story_id"])
        sources = repo.list_story_sources(story_id, limit=120)
        entities = repo.list_story_entities(story_id)
        payload = {"story": story, "sources": sources, "entities": entities}

        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0

        console.print(f"[bold]{story['title']}[/bold] ({story['slug']})")
        console.print(f"Mentions: {story['mention_count']} | Confidence: {story['confidence']}")
        console.print(f"Entities: {', '.join(e['canonical_name'] for e in entities) if entities else 'none'}")
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
        table.add_column("Last Seen")
        for entity in entities:
            table.add_row(
                str(entity["entity_id"]),
                str(entity["canonical_name"]),
                str(entity["last_seen_at"]),
            )
        console.print(table)
        return 0
    finally:
        repo.close()


def cmd_entity_show(
    settings,
    console: Console,
    query: str,
    *,
    days: int | None = None,
    as_json: bool = False,
) -> int:
    repo = _open_repo(settings)
    try:
        entity = repo.resolve_entity(query) or repo.get_entity(query)
        if not entity:
            console.print(f"[red]Entity not found:[/red] {query}")
            return 1

        entity_id = str(entity["entity_id"])
        timeline_days = days if days and days > 0 else settings.v7.timeline_default_days
        aliases = repo.get_entity_aliases(entity_id)
        timeline = repo.get_entity_timeline(entity_id, days=timeline_days, limit=500)
        payload = {
            "entity": entity,
            "aliases": aliases,
            "days": timeline_days,
            "timeline": timeline,
        }

        if as_json:
            console.print_json(json.dumps(payload, sort_keys=True))
            return 0

        console.print(f"[bold]{entity['canonical_name']}[/bold] ({entity_id})")
        console.print(f"Aliases: {', '.join(aliases) if aliases else 'none'}")
        console.print(f"Window: {timeline_days} days | Events: {len(timeline)}")
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


def cmd_eval(settings, console: Console, fixture: str | None = None, as_json: bool = False) -> int:
    try:
        fixture_path = Path(fixture).resolve() if fixture else None
        result = run_eval(settings, fixture_path=fixture_path)
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
    settings.resolve("notes", "_staging").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "exports").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "logs").mkdir(parents=True, exist_ok=True)

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

            llm = GeminiSummarizer(settings.llm, repo, api_key=api_key)
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
            return cmd_story_show(settings, console, args.slug, as_json=getattr(args, "json", False))
        parser.error("Unknown stories subcommand")
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
                as_json=getattr(args, "json", False),
            )
        parser.error("Unknown entity subcommand")
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
        return cmd_eval(settings, console, fixture=args.fixture, as_json=args.json)
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
