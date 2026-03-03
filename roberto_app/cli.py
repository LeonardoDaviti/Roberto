from __future__ import annotations

import argparse
import json
from pathlib import Path

from rich.console import Console
from rich.table import Table

from roberto_app.llm.gemini import GeminiSummarizer
from roberto_app.logging_setup import setup_logging
from roberto_app.pipeline.import_json import import_json_file
from roberto_app.pipeline.v1 import run_v1
from roberto_app.pipeline.v2 import run_v2
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
    sub.add_parser("v1", help="Initial build pipeline")
    v2_cmd = sub.add_parser("v2", help="Incremental update pipeline")
    v2_cmd.add_argument(
        "--from-db-only",
        action="store_true",
        help="Skip X API fetch and update notes using only cached/imported tweets in SQLite",
    )
    sub.add_parser("status", help="Show cached status by followed user")

    export_cmd = sub.add_parser("export", help="Export last digest/story set")
    export_cmd.add_argument("--format", choices=["json", "md"], default="json")

    import_cmd = sub.add_parser("import-json", help="Import tweets from a local JSON file into SQLite")
    import_cmd.add_argument("--file", required=True, help="Path to JSON file")
    import_cmd.add_argument(
        "--default-username",
        default=None,
        help="Fallback username when records do not include username",
    )
    return parser


def _open_repo(settings) -> StorageRepo:
    db_path = settings.resolve("data", "roberto.db")
    return StorageRepo.from_path(db_path)


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


def cmd_status(settings, console: Console) -> int:
    repo = _open_repo(settings)
    try:
        following = settings.resolve("config", "following.txt").read_text(encoding="utf-8").splitlines()
        following = [u.strip() for u in following if u.strip() and not u.strip().startswith("#")]

        table = Table(show_header=True, header_style="bold")
        table.add_column("Username")
        table.add_column("Last Polled")
        table.add_column("Last Seen Tweet")
        table.add_column("Cached Tweets", justify="right")

        for username in following:
            row = repo.get_user(username) or {}
            count = repo.count_tweets(username)
            table.add_row(
                username,
                str(row.get("last_polled_at") or "-"),
                str(row.get("last_seen_tweet_id") or "-"),
                str(count),
            )

        console.print(table)
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


def cmd_pipeline(settings, command: str, console: Console, *, from_db_only: bool = False) -> int:
    api_key = require_gemini_api_key(settings)
    repo = _open_repo(settings)

    settings.resolve("notes", "users").mkdir(parents=True, exist_ok=True)
    settings.resolve("notes", "digests").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "exports").mkdir(parents=True, exist_ok=True)
    settings.resolve("data", "logs").mkdir(parents=True, exist_ok=True)

    needs_x = command == "v1" or (command == "v2" and not from_db_only)
    x_client = None

    try:
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
            report = run_v1(settings, repo, x_client, llm)
        else:
            report = run_v2(settings, repo, x_client, llm, from_db_only=from_db_only)

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
        return cmd_status(settings, console)
    if args.command == "export":
        return cmd_export(settings, args.format, console)
    if args.command == "import-json":
        return cmd_import_json(settings, args.file, args.default_username, console)
    if args.command in {"v1", "v2"}:
        return cmd_pipeline(
            settings,
            args.command,
            console,
            from_db_only=getattr(args, "from_db_only", False),
        )

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
