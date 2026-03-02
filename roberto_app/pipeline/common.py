from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def local_now_iso(timezone_name: str) -> str:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = timezone.utc
    return datetime.now(tz).replace(microsecond=0).isoformat()


def run_id_now() -> str:
    # Include microseconds to avoid run_id collisions in rapid successive runs/tests.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S%fZ")


def read_following(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing following list: {path}")

    usernames: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        name = line.strip()
        if not name or name.startswith("#"):
            continue
        usernames.append(name)
    return usernames


def newest_tweet_id(tweet_ids: list[str]) -> str | None:
    if not tweet_ids:
        return None
    return str(max((int(t) for t in tweet_ids)))
