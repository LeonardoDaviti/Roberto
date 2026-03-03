from __future__ import annotations

from typing import Any


def x_source_ref(
    *,
    tweet_id: str,
    username: str | None = None,
    url: str | None = None,
    excerpt_hash: str | None = None,
    snapshot_hash: str | None = None,
    captured_at: str | None = None,
) -> dict[str, Any]:
    tweet_id = str(tweet_id).strip()
    username = str(username).strip() if username else ""
    if not url:
        if username:
            url = f"https://x.com/{username}/status/{tweet_id}"
        else:
            url = f"https://x.com/i/web/status/{tweet_id}"
    out: dict[str, Any] = {
        "provider": "x",
        "source_id": tweet_id,
        "url": url,
        "anchor_type": "id",
        "anchor": tweet_id,
        "excerpt_hash": excerpt_hash,
        "snapshot_hash": snapshot_hash,
        "captured_at": captured_at,
        "tweet_id": tweet_id,
    }
    if username:
        out["username"] = username
    return out


def coerce_source_ref(ref: dict[str, Any], *, fallback_username: str | None = None) -> dict[str, Any] | None:
    provider = str(ref.get("provider") or "").strip().lower()
    source_id = str(ref.get("source_id") or "").strip()
    anchor_type = str(ref.get("anchor_type") or "").strip().lower()
    anchor = str(ref.get("anchor") or "").strip()
    username = str(ref.get("username") or fallback_username or "").strip()
    tweet_id = str(ref.get("tweet_id") or "").strip()

    if not provider and (tweet_id or source_id or anchor):
        provider = "x"

    if provider == "x":
        source_id = source_id or tweet_id or anchor
        if not source_id:
            return None
        tweet_id = tweet_id or source_id
        anchor_type = "id"
        anchor = source_id
        return x_source_ref(
            tweet_id=source_id,
            username=username or None,
            url=str(ref.get("url") or "").strip() or None,
            excerpt_hash=(str(ref.get("excerpt_hash") or "").strip() or None),
            snapshot_hash=(str(ref.get("snapshot_hash") or "").strip() or None),
            captured_at=(str(ref.get("captured_at") or "").strip() or None),
        )

    if not provider:
        return None
    source_id = source_id or anchor
    if not source_id:
        return None
    anchor_type = anchor_type or "id"
    anchor = anchor or source_id
    return {
        "provider": provider,
        "source_id": source_id,
        "url": str(ref.get("url") or "").strip() or None,
        "anchor_type": anchor_type,
        "anchor": anchor,
        "excerpt_hash": (str(ref.get("excerpt_hash") or "").strip() or None),
        "snapshot_hash": (str(ref.get("snapshot_hash") or "").strip() or None),
        "captured_at": (str(ref.get("captured_at") or "").strip() or None),
    }


def dedupe_source_refs(refs: list[dict[str, Any]], *, fallback_username: str | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in refs:
        if not isinstance(row, dict):
            continue
        ref = coerce_source_ref(row, fallback_username=fallback_username)
        if not ref:
            continue
        key = (
            str(ref.get("provider") or ""),
            str(ref.get("source_id") or ""),
            str(ref.get("anchor_type") or ""),
            str(ref.get("anchor") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(ref)
    return out


def source_ref_username(ref: dict[str, Any]) -> str | None:
    provider = str(ref.get("provider") or "").strip().lower()
    if provider != "x":
        return None
    username = str(ref.get("username") or "").strip()
    if username:
        return username
    url = str(ref.get("url") or "").strip()
    parts = [p for p in url.split("/") if p]
    if len(parts) >= 2 and parts[-2] not in {"status", "web"}:
        return parts[-2]
    return None


def source_ref_tweet_id(ref: dict[str, Any]) -> str | None:
    provider = str(ref.get("provider") or "").strip().lower()
    if provider != "x":
        return None
    tweet_id = str(ref.get("tweet_id") or "").strip()
    if tweet_id:
        return tweet_id
    source_id = str(ref.get("source_id") or "").strip()
    return source_id or None


def source_ref_url(ref: dict[str, Any]) -> str | None:
    url = str(ref.get("url") or "").strip()
    if url:
        return url
    provider = str(ref.get("provider") or "").strip().lower()
    if provider == "x":
        tweet_id = source_ref_tweet_id(ref)
        if not tweet_id:
            return None
        username = source_ref_username(ref)
        if username:
            return f"https://x.com/{username}/status/{tweet_id}"
        return f"https://x.com/i/web/status/{tweet_id}"
    return None


def source_ref_label(ref: dict[str, Any]) -> str:
    provider = str(ref.get("provider") or "").strip().lower()
    if provider == "x":
        tweet_id = source_ref_tweet_id(ref)
        username = source_ref_username(ref)
        if username and tweet_id:
            return f"{username}:{tweet_id}"
        if tweet_id:
            return f"x:{tweet_id}"
    source_id = str(ref.get("source_id") or "").strip()
    if provider and source_id:
        return f"{provider}:{source_id}"
    if source_id:
        return source_id
    return "source"


def source_ref_markdown(ref: dict[str, Any]) -> str:
    label = source_ref_label(ref)
    url = source_ref_url(ref)
    if url:
        return f"[{label}]({url})"
    return label


def source_ref_search_id(ref: dict[str, Any]) -> str:
    provider = str(ref.get("provider") or "").strip().lower()
    if provider == "x":
        tweet_id = source_ref_tweet_id(ref)
        username = source_ref_username(ref)
        if username and tweet_id:
            return f"{username}:{tweet_id}"
        if tweet_id:
            return f"x:{tweet_id}"
    source_id = str(ref.get("source_id") or "").strip()
    if provider and source_id:
        return f"{provider}:{source_id}"
    if source_id:
        return source_id
    return ""


def source_ref_legacy_x(ref: dict[str, Any]) -> dict[str, str] | None:
    provider = str(ref.get("provider") or "").strip().lower()
    if provider != "x":
        return None
    username = source_ref_username(ref)
    tweet_id = source_ref_tweet_id(ref)
    if not username or not tweet_id:
        return None
    return {"username": username, "tweet_id": tweet_id}
