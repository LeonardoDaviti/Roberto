from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

AnchorType = Literal["id", "hash", "dom", "timecode", "chunk"]


@dataclass(frozen=True)
class SourceSnapshot:
    provider: str
    source_id: str
    url: str | None
    text: str
    metadata: dict[str, Any]
    snapshot_hash: str
    captured_at: str

    def to_record(self) -> dict[str, Any]:
        return {
            "snapshot_hash": self.snapshot_hash,
            "provider": self.provider,
            "source_id": self.source_id,
            "url": self.url,
            "text": self.text,
            "metadata_json": _to_json(self.metadata),
            "captured_at": self.captured_at,
        }


@dataclass(frozen=True)
class SourceRef:
    provider: str
    source_id: str
    url: str | None
    anchor_type: AnchorType
    anchor: str
    excerpt_hash: str | None
    snapshot_hash: str | None
    captured_at: str

    def ref_id(self) -> str:
        payload = {
            "provider": self.provider,
            "source_id": self.source_id,
            "anchor_type": self.anchor_type,
            "anchor": self.anchor,
        }
        return hashlib.sha256(_to_json(payload).encode("utf-8")).hexdigest()

    def to_record(self, *, username: str | None = None, tweet_id: str | None = None) -> dict[str, Any]:
        return {
            "ref_id": self.ref_id(),
            "provider": self.provider,
            "source_id": self.source_id,
            "url": self.url,
            "anchor_type": self.anchor_type,
            "anchor": self.anchor,
            "excerpt_hash": self.excerpt_hash,
            "snapshot_hash": self.snapshot_hash,
            "captured_at": self.captured_at,
            "username": username,
            "tweet_id": tweet_id,
        }


@dataclass(frozen=True)
class CanonicalPost:
    post_id: str
    username: str
    text: str
    created_at: str | None
    user_id: str | None
    display_name: str | None
    raw: dict[str, Any]
    provider: str = "x"

    def to_storage_dict(self) -> dict[str, Any]:
        payload = dict(self.raw)
        payload["id"] = self.post_id
        payload["text"] = self.text
        if self.created_at is not None:
            payload["created_at"] = self.created_at
        payload.setdefault("provider", self.provider)
        return payload


def build_x_source_artifacts(
    *,
    username: str,
    tweet_id: str,
    text: str,
    created_at: str | None,
    raw: dict[str, Any],
    captured_at: str | None = None,
) -> tuple[SourceRef, SourceSnapshot]:
    normalized_text = " ".join(text.split())
    excerpt_hash = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest() if normalized_text else None
    url = f"https://x.com/{username}/status/{tweet_id}"
    observed_at = captured_at or created_at or utc_now_iso()

    snapshot_payload = {
        "provider": "x",
        "source_id": tweet_id,
        "url": url,
        "text": normalized_text,
        "metadata": {
            "username": username,
            "created_at": created_at,
            "raw": raw,
        },
    }
    snapshot_hash = hashlib.sha256(_to_json(snapshot_payload).encode("utf-8")).hexdigest()

    snapshot = SourceSnapshot(
        provider="x",
        source_id=tweet_id,
        url=url,
        text=normalized_text,
        metadata=snapshot_payload["metadata"],
        snapshot_hash=snapshot_hash,
        captured_at=observed_at,
    )
    source_ref = SourceRef(
        provider="x",
        source_id=tweet_id,
        url=url,
        anchor_type="id",
        anchor=tweet_id,
        excerpt_hash=excerpt_hash,
        snapshot_hash=snapshot_hash,
        captured_at=observed_at,
    )
    return source_ref, snapshot


def _to_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
