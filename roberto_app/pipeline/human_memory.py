from __future__ import annotations

import hashlib
import itertools
import re
from datetime import datetime
from typing import Any

from roberto_app.llm.schemas import UserNoteAutoBlock
from roberto_app.pipeline.taxonomy import normalize_tags

NEGATIVE_MARKERS = {"not", "no", "never", "cannot", "can't", "wont", "won't", "unlikely", "fails", "bad"}


def _stable_id(prefix: str, *parts: str) -> str:
    raw = "|".join(parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def _trim(text: str, limit: int = 280) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9_]+", text.lower()))


def _polarity(text: str) -> int:
    tokens = _tokenize(text)
    return -1 if tokens & NEGATIVE_MARKERS else 1


def propose_idea_cards(
    *,
    run_id: str,
    username: str,
    summary: UserNoteAutoBlock,
    now_iso: str,
    per_user_limit: int,
    tag_aliases: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    idea_cycle = itertools.cycle(["essay", "product", "experiment"])

    for card in summary.notecards[: max(0, per_user_limit)]:
        if not card.source_tweet_ids:
            continue
        idea_type = next(idea_cycle)
        title = f"{idea_type.title()} - {card.title}"
        hypothesis = _trim(card.payload)
        why_now = _trim(card.why_it_matters)
        source_refs = [{"username": username, "tweet_id": ref} for ref in card.source_tweet_ids]
        card_id = _stable_id("idea", username, idea_type, card.title, hypothesis, ",".join(card.source_tweet_ids))
        tags = card.tags or ["untagged"]
        if tag_aliases:
            tags = normalize_tags(tags, tag_aliases)
        cards.append(
            {
                "card_id": card_id,
                "run_id": run_id,
                "username": username,
                "idea_type": idea_type,
                "title": title,
                "hypothesis": hypothesis,
                "why_now": why_now,
                "tags": tags,
                "source_refs": source_refs,
                "created_at": now_iso,
            }
        )

    return cards


def detect_conflict_cards(
    *,
    run_id: str,
    cards: list[dict[str, Any]],
    now_iso: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()

    for i, left in enumerate(cards):
        for right in cards[i + 1 :]:
            if left["username"] == right["username"]:
                continue
            left_tags = set(left.get("tags", []))
            right_tags = set(right.get("tags", []))
            overlap_tags = sorted(left_tags & right_tags)
            if not overlap_tags:
                continue

            left_polarity = _polarity(f"{left.get('title', '')} {left.get('hypothesis', '')}")
            right_polarity = _polarity(f"{right.get('title', '')} {right.get('hypothesis', '')}")
            if left_polarity == right_polarity:
                continue

            key = tuple(sorted((left["card_id"], right["card_id"])))
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            title = f"Conflict on {', '.join(overlap_tags[:2])}"
            refs = list(left.get("source_refs", [])) + list(right.get("source_refs", []))
            conflict_id = _stable_id("conflict", left["card_id"], right["card_id"])
            out.append(
                {
                    "conflict_id": conflict_id,
                    "run_id": run_id,
                    "title": title,
                    "claim_a": {
                        "card_id": left["card_id"],
                        "username": left["username"],
                        "title": left["title"],
                        "hypothesis": left["hypothesis"],
                    },
                    "claim_b": {
                        "card_id": right["card_id"],
                        "username": right["username"],
                        "title": right["title"],
                        "hypothesis": right["hypothesis"],
                    },
                    "tags": overlap_tags,
                    "source_refs": refs,
                    "created_at": now_iso,
                }
            )

    return out


def select_shuffle_pack(
    *,
    cards: list[dict[str, Any]],
    max_cards: int,
    connection_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not cards:
        return [], []

    # Greedy diversity by tags, then fill remaining by recency.
    selected: list[dict[str, Any]] = []
    used_tags: set[str] = set()
    remaining = list(cards)

    for card in remaining:
        tags = set(card.get("tags", []))
        if tags - used_tags:
            selected.append(card)
            used_tags |= tags
            if len(selected) >= max_cards:
                break

    if len(selected) < max_cards:
        selected_ids = {c["card_id"] for c in selected}
        for card in remaining:
            if card["card_id"] in selected_ids:
                continue
            selected.append(card)
            if len(selected) >= max_cards:
                break

    selected = selected[:max_cards]

    # Build non-obvious connections from cross-tag card pairs with low lexical overlap.
    pairs: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    for i, left in enumerate(selected):
        left_tokens = _tokenize(f"{left.get('title', '')} {left.get('hypothesis', '')}")
        left_tags = set(left.get("tags", []))
        for right in selected[i + 1 :]:
            right_tags = set(right.get("tags", []))
            if left_tags & right_tags:
                continue
            right_tokens = _tokenize(f"{right.get('title', '')} {right.get('hypothesis', '')}")
            union = left_tokens | right_tokens
            if not union:
                continue
            jaccard = len(left_tokens & right_tokens) / len(union)
            # Lower overlap -> more non-obvious.
            pairs.append((jaccard, left, right))

    pairs.sort(key=lambda x: x[0])
    connections: list[dict[str, Any]] = []
    used_card_ids: set[str] = set()
    for _, left, right in pairs:
        if len(connections) >= connection_count:
            break
        if left["card_id"] in used_card_ids or right["card_id"] in used_card_ids:
            continue
        used_card_ids.add(left["card_id"])
        used_card_ids.add(right["card_id"])
        insight = (
            f"Possible bridge: '{left['title']}' ({left['username']}) and "
            f"'{right['title']}' ({right['username']}) may inform one combined hypothesis."
        )
        refs = list(left.get("source_refs", [])) + list(right.get("source_refs", []))
        refs = [r for r in refs if r.get("username") and r.get("tweet_id")]
        if not refs:
            continue
        connections.append({"insight": _trim(insight, 260), "source_refs": refs})

    return selected, connections


def render_idea_auto_block(cards: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("## Greene-Style Idea Cards")
    lines.append("")
    if not cards:
        lines.append("- No idea cards produced in this run.")
        return "\n".join(lines)

    for card in cards:
        lines.append(f"- **[{card['idea_type'].upper()}] {card['title']}**")
        lines.append(f"  - Hypothesis: {_trim(card['hypothesis'])}")
        lines.append(f"  - Why now: {_trim(card['why_now'])}")
        lines.append(f"  - Tags: {', '.join(card.get('tags', []))}")
        refs = ", ".join(
            f"[{r['username']}:{r['tweet_id']}](https://x.com/{r['username']}/status/{r['tweet_id']})"
            for r in card.get("source_refs", [])
        )
        lines.append(f"  - Sources: {refs if refs else 'none'}")
    return "\n".join(lines)


def render_conflict_auto_block(conflicts: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("## Conflict Cards")
    lines.append("")
    if not conflicts:
        lines.append("- No direct source-backed conflicts detected.")
        return "\n".join(lines)

    for card in conflicts:
        lines.append(f"- **{card['title']}**")
        a = card["claim_a"]
        b = card["claim_b"]
        lines.append(f"  - Claim A (@{a['username']}): {a['title']} - {_trim(a['hypothesis'])}")
        lines.append(f"  - Claim B (@{b['username']}): {b['title']} - {_trim(b['hypothesis'])}")
        lines.append(f"  - Shared tags: {', '.join(card.get('tags', []))}")
        refs = ", ".join(
            f"[{r['username']}:{r['tweet_id']}](https://x.com/{r['username']}/status/{r['tweet_id']})"
            for r in card.get("source_refs", [])
        )
        lines.append(f"  - Sources: {refs if refs else 'none'}")
    return "\n".join(lines)


def render_shuffle_auto_block(selected: list[dict[str, Any]], connections: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("## Weekly Shuffle Pack")
    lines.append("")
    lines.append(f"### Selected Cards ({len(selected)})")
    if not selected:
        lines.append("- No cards available for this week.")
    else:
        for card in selected:
            lines.append(f"- **{card['title']}** ({card['idea_type']}, @{card['username']})")
            lines.append(f"  - Tags: {', '.join(card.get('tags', []))}")
            refs = ", ".join(
                f"[{r['username']}:{r['tweet_id']}](https://x.com/{r['username']}/status/{r['tweet_id']})"
                for r in card.get("source_refs", [])
            )
            lines.append(f"  - Sources: {refs if refs else 'none'}")

    lines.append("")
    lines.append(f"### Non-Obvious Connections ({len(connections)})")
    if not connections:
        lines.append("- No high-quality cross-tag connections found.")
    else:
        for conn in connections:
            lines.append(f"- {conn['insight']}")
            refs = ", ".join(
                f"[{r['username']}:{r['tweet_id']}](https://x.com/{r['username']}/status/{r['tweet_id']})"
                for r in conn.get("source_refs", [])
            )
            lines.append(f"  - Sources: {refs if refs else 'none'}")

    return "\n".join(lines)


def week_key_from_iso(now_iso: str) -> str:
    dt = datetime.fromisoformat(now_iso)
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"
