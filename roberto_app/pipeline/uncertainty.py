from __future__ import annotations

import hashlib
from typing import Any

from roberto_app.llm.schemas import Story

CONF_RANK = {"low": 0, "medium": 1, "high": 2}


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def confidence_reason(
    *,
    previous: str | None,
    new: str,
    source_count: int,
) -> str:
    if previous is None:
        return f"Initial confidence from first synthesis with {source_count} cited sources."
    prev_rank = CONF_RANK.get(previous, 1)
    new_rank = CONF_RANK.get(new, 1)
    if new_rank > prev_rank:
        return f"Confidence rose due to stronger evidence in this run ({source_count} cited sources)."
    if new_rank < prev_rank:
        return f"Confidence dropped due to weaker or conflicting evidence ({source_count} cited sources)."
    return f"Confidence unchanged with refreshed evidence ({source_count} cited sources)."


def to_conflict_nodes(
    *,
    run_id: str,
    now_iso: str,
    conflict_cards: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for card in conflict_cards:
        tags = [str(tag) for tag in card.get("tags", [])]
        topic = ", ".join(tags[:2]) if tags else str(card.get("title") or "conflict")
        claim_a = card.get("claim_a", {})
        claim_b = card.get("claim_b", {})
        out.append(
            {
                "conflict_id": str(card["conflict_id"]),
                "run_id": run_id,
                "topic": topic,
                "claim_a": {
                    "username": str(claim_a.get("username") or ""),
                    "text": str(claim_a.get("hypothesis") or claim_a.get("title") or ""),
                },
                "claim_b": {
                    "username": str(claim_b.get("username") or ""),
                    "text": str(claim_b.get("hypothesis") or claim_b.get("title") or ""),
                },
                "source_refs": list(card.get("source_refs", [])),
                "status": "open",
                "created_at": str(card.get("created_at") or now_iso),
                "updated_at": now_iso,
            }
        )
    return out


def story_claims_from_story(
    *,
    story_id: str,
    story: Story,
    run_id: str,
    now_iso: str,
) -> list[dict[str, Any]]:
    evidence = [{"username": s.username, "tweet_id": s.tweet_id} for s in story.sources]
    claims: list[dict[str, Any]] = []
    primary_text = story.what_happened.strip()
    if primary_text:
        claim_id = _stable_id("claim", story_id, "what", primary_text)
        claims.append(
            {
                "claim_id": claim_id,
                "story_id": story_id,
                "run_id": run_id,
                "claim_text": primary_text,
                "evidence_refs": evidence,
                "confidence": story.confidence,
                "status": "active",
                "created_at": now_iso,
                "updated_at": now_iso,
            }
        )
    secondary_text = story.why_it_matters.strip()
    if secondary_text:
        claim_id = _stable_id("claim", story_id, "why", secondary_text)
        claims.append(
            {
                "claim_id": claim_id,
                "story_id": story_id,
                "run_id": run_id,
                "claim_text": secondary_text,
                "evidence_refs": evidence,
                "confidence": story.confidence,
                "status": "active",
                "created_at": now_iso,
                "updated_at": now_iso,
            }
        )
    return claims
