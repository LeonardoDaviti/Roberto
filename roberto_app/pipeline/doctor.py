from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import fcntl
import httpx

from roberto_app.storage.repo import StorageRepo


@dataclass
class DoctorCheck:
    name: str
    status: str
    message: str
    hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "hint": self.hint,
        }


@dataclass
class DoctorReport:
    checks: list[DoctorCheck]

    @property
    def ok(self) -> bool:
        return all(c.status != "error" for c in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "checks": [c.to_dict() for c in self.checks],
        }


def _check_file_exists(path: Path, name: str, hint: str) -> DoctorCheck:
    if path.exists():
        return DoctorCheck(name=name, status="ok", message=f"Found: {path}")
    return DoctorCheck(name=name, status="error", message=f"Missing: {path}", hint=hint)


def _check_env(name: str, value: str | None, hint: str) -> DoctorCheck:
    if value:
        return DoctorCheck(name=name, status="ok", message="Configured")
    return DoctorCheck(name=name, status="warn", message="Not configured", hint=hint)


def _check_timezone(tz_name: str) -> DoctorCheck:
    try:
        ZoneInfo(tz_name)
        return DoctorCheck(name="timezone", status="ok", message=f"Valid timezone: {tz_name}")
    except ZoneInfoNotFoundError:
        return DoctorCheck(
            name="timezone",
            status="error",
            message=f"Invalid timezone: {tz_name}",
            hint="Set notes.note_timezone to a valid IANA name (e.g. Asia/Tbilisi)",
        )


def _check_lock(path: Path) -> DoctorCheck:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            return DoctorCheck(name="lock", status="ok", message="No active lock holder")
        except BlockingIOError:
            return DoctorCheck(
                name="lock",
                status="warn",
                message="Another Roberto process appears to be active",
                hint="Wait for active run to finish or clear stale lock if process is gone",
            )


def _check_disk(path: Path) -> DoctorCheck:
    usage = shutil.disk_usage(path)
    free_mb = usage.free / (1024 * 1024)
    if free_mb < 256:
        return DoctorCheck(
            name="disk",
            status="warn",
            message=f"Low free disk: {free_mb:.1f} MB",
            hint="Free disk space to avoid run failures",
        )
    return DoctorCheck(name="disk", status="ok", message=f"Free disk: {free_mb:.1f} MB")


def _check_db(settings) -> DoctorCheck:
    required = {
        "users",
        "tweets",
        "runs",
        "note_index",
        "llm_cache",
        "stories",
        "story_sources",
        "llm_embeddings",
        "briefings",
        "briefing_items",
    }
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))
    try:
        rows = repo.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        existing = {str(r[0]) for r in rows}
        missing = sorted(required - existing)
        if missing:
            return DoctorCheck(
                name="db_schema",
                status="error",
                message=f"Missing tables: {', '.join(missing)}",
                hint="Run any Roberto command once to initialize/migrate schema",
            )

        last_run = repo.get_last_run()
        if last_run:
            return DoctorCheck(
                name="db_schema",
                status="ok",
                message=f"Schema healthy; last run: {last_run.get('run_id', '-')}",
            )
        return DoctorCheck(name="db_schema", status="ok", message="Schema healthy; no runs yet")
    finally:
        repo.close()


def _check_writable(path: Path, name: str) -> DoctorCheck:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".doctor_write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return DoctorCheck(name=name, status="ok", message=f"Writable: {path}")
    except OSError as exc:
        return DoctorCheck(name=name, status="error", message=f"Not writable: {path} ({exc})")


def _check_online_x(token: str | None) -> DoctorCheck:
    if not token:
        return DoctorCheck(
            name="x_online",
            status="warn",
            message="Skipped (no token)",
            hint="Set X_BEARER_TOKEN to run online X API checks",
        )

    try:
        with httpx.Client(timeout=10, headers={"Authorization": f"Bearer {token}"}) as client:
            resp = client.get("https://api.x.com/2/usage/tweets")
    except httpx.HTTPError as exc:
        return DoctorCheck(
            name="x_online",
            status="warn",
            message=f"Network check failed: {exc}",
            hint="Verify internet access and X API reachability",
        )

    if resp.status_code == 200:
        body = resp.json()
        usage = ((body.get("data") or {}).get("project_usage"))
        cap = ((body.get("data") or {}).get("project_cap"))
        return DoctorCheck(name="x_online", status="ok", message=f"X online check passed (usage={usage}, cap={cap})")
    if resp.status_code == 401:
        return DoctorCheck(name="x_online", status="error", message="X token unauthorized (401)", hint="Regenerate token in X developer portal")
    if resp.status_code == 402:
        return DoctorCheck(
            name="x_online",
            status="warn",
            message="X credits depleted (402)",
            hint="Add credits or adjust plan in X developer billing",
        )
    return DoctorCheck(name="x_online", status="warn", message=f"Unexpected X status: {resp.status_code}")


def run_doctor(settings, *, online: bool = False) -> DoctorReport:
    checks: list[DoctorCheck] = []

    checks.append(_check_file_exists(settings.resolve("config", "settings.yaml"), "config.settings", "Create config/settings.yaml"))
    checks.append(_check_file_exists(settings.resolve("config", "following.txt"), "config.following", "Create config/following.txt"))

    checks.append(
        _check_env(
            "env.x_bearer_token",
            settings.x_bearer_token,
            "Set X_BEARER_TOKEN in .env for sync/v1/v2 with API",
        )
    )
    checks.append(
        _check_env(
            "env.gemini_api_key",
            settings.gemini_api_key,
            "Set GEMINI_API_KEY in .env for summarization",
        )
    )

    checks.append(_check_timezone(settings.notes.note_timezone))
    checks.append(_check_disk(settings.base_dir))
    checks.append(_check_lock(settings.resolve("data", "roberto.lock")))

    checks.append(_check_writable(settings.resolve("data", "exports"), "fs.exports_dir"))
    checks.append(_check_writable(settings.resolve("data", "logs"), "fs.logs_dir"))
    checks.append(_check_writable(settings.resolve("notes", "users"), "fs.notes_users_dir"))
    checks.append(_check_writable(settings.resolve("notes", "digests"), "fs.notes_digests_dir"))
    checks.append(_check_writable(settings.resolve("notes", "stories"), "fs.notes_stories_dir"))
    checks.append(_check_writable(settings.resolve("notes", "briefings"), "fs.notes_briefings_dir"))

    checks.append(_check_db(settings))

    following = []
    try:
        following = [
            u.strip()
            for u in settings.resolve("config", "following.txt").read_text(encoding="utf-8").splitlines()
            if u.strip() and not u.strip().startswith("#")
        ]
    except OSError:
        pass
    if not following:
        checks.append(
            DoctorCheck(
                name="following",
                status="warn",
                message="No followed users configured",
                hint="Add usernames to config/following.txt",
            )
        )
    else:
        checks.append(DoctorCheck(name="following", status="ok", message=f"Configured users: {len(following)}"))

    if online:
        checks.append(_check_online_x(settings.x_bearer_token))

    return DoctorReport(checks=checks)
