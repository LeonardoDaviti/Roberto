from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import yaml

from roberto_app.notesys.updater import update_note_file
from roberto_app.pipeline.common import utc_now_iso
from roberto_app.pipeline.human_memory import week_key_from_iso
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo

CONF_RANK = {"low": 1.0, "medium": 2.0, "high": 3.0}


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def _trim(value: str, limit: int = 260) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


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


def _refs_md(refs: list[dict[str, str]]) -> str:
    return ", ".join(
        f"[{r['username']}:{r['tweet_id']}](https://x.com/{r['username']}/status/{r['tweet_id']})"
        for r in refs
    )


def _first_sentence(text: str) -> str:
    cleaned = _trim(text, 320)
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    return parts[0].strip() if parts else cleaned


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def ensure_profile_assets(settings) -> dict[str, str]:
    doctrine_path = settings.resolve(*Path(settings.v22.doctrine_path).parts)
    tags_path = settings.resolve(*Path(settings.v22.tags_path).parts)
    doctrine_path.parent.mkdir(parents=True, exist_ok=True)
    tags_path.parent.mkdir(parents=True, exist_ok=True)

    if not doctrine_path.exists():
        doctrine_path.write_text(
            (
                "# Roberto Doctrine\n\n"
                "## Signal Preferences\n"
                "- Prefer strategic claims with explicit evidence.\n"
                "- Prefer first-order mechanisms over vague predictions.\n\n"
                "## Ignore List\n"
                "- Repetitive hype without citations.\n"
                "- Low-evidence opinion loops.\n"
            ),
            encoding="utf-8",
        )
    if not tags_path.exists():
        tags_path.write_text(
            yaml.safe_dump(
                {
                    "focus_tags": ["strategy", "power", "psychology", "business", "tech"],
                    "ignore_tags": ["hype", "noise"],
                    "aliases": {"ai": "tech", "llm": "tech"},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    return {"doctrine_path": str(doctrine_path), "tags_path": str(tags_path)}


def _load_doctrine(settings) -> str:
    path = settings.resolve(*Path(settings.v22.doctrine_path).parts)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _load_profile_tags(settings) -> dict[str, Any]:
    path = settings.resolve(*Path(settings.v22.tags_path).parts)
    if not path.exists():
        return {"focus_tags": [], "ignore_tags": [], "aliases": {}}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {"focus_tags": [], "ignore_tags": [], "aliases": {}}
    return {
        "focus_tags": [str(x).lower() for x in payload.get("focus_tags", []) if str(x).strip()],
        "ignore_tags": [str(x).lower() for x in payload.get("ignore_tags", []) if str(x).strip()],
        "aliases": {str(k).lower(): str(v).lower() for k, v in dict(payload.get("aliases", {})).items()},
    }


def _infer_theme(title: str, payload: str, tags: list[str]) -> str:
    if tags:
        return str(tags[0]).lower()
    tokens = _tokenize(f"{title} {payload}")
    stop = {"the", "and", "with", "from", "that", "this", "about", "into", "for", "are", "was", "were"}
    for token in tokens:
        if len(token) >= 4 and token not in stop:
            return token
    return "general"


def _story_refs(repo: StorageRepo, story_id: str, limit: int = 40) -> list[dict[str, str]]:
    rows = repo.list_story_sources(story_id, limit=limit)
    return _dedupe_refs(
        [{"username": str(r.get("username") or ""), "tweet_id": str(r.get("tweet_id") or "")} for r in rows]
    )


def _capture_cards_from_stories(
    repo: StorageRepo,
    *,
    run_id: str,
    now_iso: str,
    week_key: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for story in repo.list_stories(limit=500):
        story_id = str(story["story_id"])
        summary = dict(story.get("summary_json") or {})
        refs = _story_refs(repo, story_id, limit=60)
        if not refs:
            continue
        tags = [str(tag).lower() for tag in summary.get("tags", []) if str(tag).strip()]
        what = str(summary.get("what_happened") or "")
        why = str(summary.get("why_it_matters") or "")
        title = str(story.get("title") or "Story")
        confidence = str(story.get("confidence") or "medium")

        if what:
            claim_id = _stable_id("greene", week_key, story_id, "claim", what)
            out.append(
                {
                    "card_id": claim_id,
                    "run_id": run_id,
                    "story_id": story_id,
                    "username": refs[0]["username"],
                    "week_key": week_key,
                    "card_type": "claim",
                    "title": f"{title} - Core Claim",
                    "payload": _trim(what, 420),
                    "why_it_matters": _trim(why or "Strategic relevance pending clarification.", 420),
                    "source_refs": refs,
                    "theme": _infer_theme(title, what, tags),
                    "principle": None,
                    "strategic_use_case": None,
                    "reusable_quote": None,
                    "confidence": confidence if confidence in {"high", "medium", "low"} else "medium",
                    "state": "captured",
                    "score": float(CONF_RANK.get(confidence, 2.0)) + min(2.0, len(refs) / 4.0),
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )
        if why:
            angle_id = _stable_id("greene", week_key, story_id, "angle", why)
            out.append(
                {
                    "card_id": angle_id,
                    "run_id": run_id,
                    "story_id": story_id,
                    "username": refs[0]["username"],
                    "week_key": week_key,
                    "card_type": "angle",
                    "title": f"{title} - Strategic Angle",
                    "payload": _trim(why, 420),
                    "why_it_matters": _trim(why, 420),
                    "source_refs": refs,
                    "theme": _infer_theme(title, why, tags),
                    "principle": None,
                    "strategic_use_case": None,
                    "reusable_quote": None,
                    "confidence": confidence if confidence in {"high", "medium", "low"} else "medium",
                    "state": "captured",
                    "score": float(CONF_RANK.get(confidence, 2.0)) + min(1.0, len(refs) / 8.0),
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )

        for claim in repo.list_story_claims(story_id, limit=6):
            claim_text = str(claim.get("claim_text") or "").strip()
            claim_refs = _dedupe_refs(list(claim.get("evidence_refs") or []))
            if not claim_text or not claim_refs:
                continue
            evidence_id = _stable_id("greene", week_key, story_id, "evidence", claim_text)
            out.append(
                {
                    "card_id": evidence_id,
                    "run_id": run_id,
                    "story_id": story_id,
                    "username": claim_refs[0]["username"],
                    "week_key": week_key,
                    "card_type": "evidence",
                    "title": f"{title} - Evidence",
                    "payload": _trim(claim_text, 420),
                    "why_it_matters": _trim("Evidence anchor for downstream arguments.", 220),
                    "source_refs": claim_refs,
                    "theme": _infer_theme(title, claim_text, tags),
                    "principle": None,
                    "strategic_use_case": None,
                    "reusable_quote": None,
                    "confidence": str(claim.get("confidence") or confidence),
                    "state": "captured",
                    "score": float(CONF_RANK.get(str(claim.get("confidence") or confidence), 2.0)) + min(2.0, len(claim_refs) / 3.0),
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )
    return out


def _distill_cards(
    cards: list[dict[str, Any]],
    *,
    doctrine: str,
    profile_tags: dict[str, Any],
    repo: StorageRepo,
    now_iso: str,
) -> list[dict[str, Any]]:
    doctrine_text = doctrine.lower()
    focus_tags = set(profile_tags.get("focus_tags", []))
    ignore_tags = set(profile_tags.get("ignore_tags", []))
    aliases = dict(profile_tags.get("aliases", {}))
    out: list[dict[str, Any]] = []
    for card in cards:
        row = dict(card)
        theme = str(row.get("theme") or _infer_theme(row.get("title", ""), row.get("payload", ""), []))
        theme = aliases.get(theme, theme)
        row["theme"] = theme
        row["principle"] = _trim(
            f"When '{theme}' signals appear, validate with independent evidence before committing resources.",
            220,
        )
        row["strategic_use_case"] = _trim(
            f"Use this card to pressure-test strategy decisions tied to {theme}.",
            220,
        )
        row["reusable_quote"] = _first_sentence(str(row.get("payload") or ""))
        row["state"] = "distilled" if str(row.get("state") or "captured") not in {"keeper", "rejected"} else row["state"]
        score = float(row.get("score") or 0.0)
        if theme in focus_tags:
            score += 1.2
        if theme in ignore_tags:
            score -= 1.5
        if theme and theme in doctrine_text:
            score += 0.7
        score += repo.feedback_score_for_card(str(row.get("card_id") or ""))
        row["score"] = round(score, 4)
        row["updated_at"] = now_iso
        out.append(row)
    return out


def _winnow_cards(
    cards: list[dict[str, Any]],
    *,
    cap_per_story_week: int,
    auto_reject_overflow: bool,
    now_iso: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for card in cards:
        key = (str(card.get("story_id") or "global"), str(card.get("week_key") or ""))
        grouped.setdefault(key, []).append(card)

    out: list[dict[str, Any]] = []
    for (_, _), rows in grouped.items():
        rows = sorted(rows, key=lambda r: (-float(r.get("score") or 0.0), str(r.get("card_id") or "")))
        keep = max(1, cap_per_story_week)
        for idx, row in enumerate(rows):
            item = dict(row)
            if idx < keep:
                item["state"] = "keeper"
            elif auto_reject_overflow:
                item["state"] = "rejected"
            else:
                item["state"] = "distilled"
            item["updated_at"] = now_iso
            out.append(item)
    return out


def render_cards_note(cards: list[dict[str, Any]], week_key: str) -> str:
    keepers = [c for c in cards if str(c.get("state")) == "keeper"]
    distilled = [c for c in cards if str(c.get("state")) == "distilled"]
    rejected = [c for c in cards if str(c.get("state")) == "rejected"]
    lines: list[str] = []
    lines.append(f"## Greene Cards - {week_key}")
    lines.append("")
    lines.append(f"- Keeper cards: **{len(keepers)}**")
    lines.append(f"- Distilled cards: **{len(distilled)}**")
    lines.append(f"- Rejected cards: **{len(rejected)}**")
    lines.append("")
    lines.append("### Keeper Deck")
    if not keepers:
        lines.append("- No keeper cards yet.")
    else:
        for card in keepers[:200]:
            lines.append(f"- **[{card['card_type'].upper()}] {card['title']}** (theme: {card.get('theme') or 'general'})")
            lines.append(f"  - Principle: {_trim(str(card.get('principle') or ''), 240)}")
            lines.append(f"  - Strategic use-case: {_trim(str(card.get('strategic_use_case') or ''), 240)}")
            lines.append(f"  - Reusable quote: \"{_trim(str(card.get('reusable_quote') or ''), 180)}\"")
            lines.append(f"  - Sources: {_refs_md(card.get('source_refs', [])) or 'none'}")
    return "\n".join(lines).rstrip()


def run_greene_cycle(settings, repo: StorageRepo, *, run_id: str, now_iso: str) -> dict[str, Any]:
    ensure_profile_assets(settings)
    week_key = week_key_from_iso(now_iso)
    captured = _capture_cards_from_stories(repo, run_id=run_id, now_iso=now_iso, week_key=week_key)
    if captured:
        repo.upsert_greene_cards(captured)

    week_cards = repo.list_greene_cards(week_key=week_key, limit=5000)
    doctrine = _load_doctrine(settings)
    profile_tags = _load_profile_tags(settings)
    distilled = _distill_cards(week_cards, doctrine=doctrine, profile_tags=profile_tags, repo=repo, now_iso=now_iso)
    if distilled:
        repo.upsert_greene_cards(distilled)

    winnowed = _winnow_cards(
        distilled,
        cap_per_story_week=settings.v19.keeper_cap_per_story_week,
        auto_reject_overflow=settings.v19.auto_reject_overflow,
        now_iso=now_iso,
    )
    if winnowed:
        repo.upsert_greene_cards(winnowed)

    cards_note = settings.resolve("notes", "greene", "cards", f"{week_key}.md")
    note_res = update_note_file(
        cards_note,
        note_type="greene",
        run_id=run_id,
        now_iso=now_iso,
        auto_body=render_cards_note(winnowed, week_key),
        note_title=f"Greene Card Deck - {week_key}",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(cards_note),
            note_type="greene",
            username=None,
            created_at=note_res.created_at,
            updated_at=note_res.updated_at,
            last_run_id=run_id,
        )
    )

    return {
        "week_key": week_key,
        "captured": len(captured),
        "distilled": len([c for c in winnowed if str(c.get("state")) in {"distilled", "keeper"}]),
        "keepers": len([c for c in winnowed if str(c.get("state")) == "keeper"]),
        "rejected": len([c for c in winnowed if str(c.get("state")) == "rejected"]),
        "note_path": str(cards_note),
    }


def list_cards(
    repo: StorageRepo,
    *,
    state: str | None = None,
    week_key: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    return repo.list_greene_cards(state=state, week_key=week_key, limit=limit)


def mark_card_feedback(
    repo: StorageRepo,
    *,
    card_id: str,
    feedback: str,
    note: str | None = None,
    now_iso: str | None = None,
) -> int:
    return repo.add_card_feedback(
        card_id=card_id,
        feedback=feedback,
        note=note,
        created_at=now_iso or utc_now_iso(),
    )


def _select_cards_for_topic(cards: list[dict[str, Any]], topic: str | None) -> list[dict[str, Any]]:
    if not topic:
        return cards
    needle = topic.lower().strip()
    out: list[dict[str, Any]] = []
    for card in cards:
        hay = " ".join(
            [
                str(card.get("theme") or ""),
                str(card.get("title") or ""),
                str(card.get("payload") or ""),
                str(card.get("story_id") or ""),
            ]
        ).lower()
        if needle in hay:
            out.append(card)
    return out


def detect_gaps(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for card in cards:
        refs = list(card.get("source_refs") or [])
        if len(refs) < 2:
            gaps.append(
                {
                    "card_id": str(card.get("card_id") or ""),
                    "kind": "evidence_depth",
                    "question": "Need at least one more independent evidence source for this claim.",
                }
            )
        payload = str(card.get("payload") or "").lower()
        if "because" not in payload and "mechanism" not in payload:
            gaps.append(
                {
                    "card_id": str(card.get("card_id") or ""),
                    "kind": "mechanism",
                    "question": "What mechanism explains this claim beyond observation?",
                }
            )
    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for gap in gaps:
        dedup[(gap["card_id"], gap["kind"])] = gap
    return list(dedup.values())


def propose_chapters(
    settings,
    repo: StorageRepo,
    *,
    run_id: str,
    now_iso: str,
    topic: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    keeper_cards = repo.list_greene_cards(state="keeper", limit=5000)
    keeper_cards = _select_cards_for_topic(keeper_cards, topic)
    cards_per_chapter = max(3, settings.v21.cards_per_chapter)
    chapter_count = max(1, settings.v21.chapter_count)

    by_theme: dict[str, list[dict[str, Any]]] = {}
    for card in keeper_cards:
        theme = str(card.get("theme") or "general")
        by_theme.setdefault(theme, []).append(card)
    for rows in by_theme.values():
        rows.sort(key=lambda r: (-float(r.get("score") or 0.0), str(r.get("card_id") or "")))

    theme_order = sorted(by_theme.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    thematic: list[dict[str, Any]] = []
    for theme, rows in theme_order[:chapter_count]:
        support = rows[:cards_per_chapter]
        refs = _dedupe_refs([ref for row in support for ref in row.get("source_refs", [])])
        thesis = f"{theme.title()} as a strategic lever"
        chapter_id = _stable_id("chapter", run_id, "thematic", theme, ",".join(r["card_id"] for r in support))
        thematic.append(
            {
                "chapter_id": chapter_id,
                "run_id": run_id,
                "toc_style": "thematic",
                "thesis": thesis,
                "supporting_cards": [row["card_id"] for row in support],
                "refs": refs,
                "gaps": detect_gaps(support)[:5],
                "created_at": now_iso,
            }
        )

    chronological_cards = sorted(
        keeper_cards,
        key=lambda r: (str(r.get("updated_at") or ""), str(r.get("card_id") or "")),
        reverse=True,
    )
    chronological: list[dict[str, Any]] = []
    for idx in range(min(chapter_count, max(1, (len(chronological_cards) + cards_per_chapter - 1) // cards_per_chapter))):
        chunk = chronological_cards[idx * cards_per_chapter : (idx + 1) * cards_per_chapter]
        if not chunk:
            continue
        refs = _dedupe_refs([ref for row in chunk for ref in row.get("source_refs", [])])
        thesis = f"Wave {idx + 1}: recent signal progression"
        chapter_id = _stable_id("chapter", run_id, "chronological", thesis, ",".join(r["card_id"] for r in chunk))
        chronological.append(
            {
                "chapter_id": chapter_id,
                "run_id": run_id,
                "toc_style": "chronological",
                "thesis": thesis,
                "supporting_cards": [row["card_id"] for row in chunk],
                "refs": refs,
                "gaps": detect_gaps(chunk)[:5],
                "created_at": now_iso,
            }
        )

    strategy_cards = sorted(
        keeper_cards,
        key=lambda r: (
            -float(r.get("score") or 0.0),
            -len(str(r.get("strategic_use_case") or "")),
            str(r.get("card_id") or ""),
        ),
    )
    strategy: list[dict[str, Any]] = []
    for idx in range(chapter_count):
        chunk = strategy_cards[idx * cards_per_chapter : (idx + 1) * cards_per_chapter]
        if not chunk:
            continue
        refs = _dedupe_refs([ref for row in chunk for ref in row.get("source_refs", [])])
        thesis = f"Strategic pattern {idx + 1}: where leverage compounds"
        chapter_id = _stable_id("chapter", run_id, "strategy", thesis, ",".join(r["card_id"] for r in chunk))
        strategy.append(
            {
                "chapter_id": chapter_id,
                "run_id": run_id,
                "toc_style": "strategy",
                "thesis": thesis,
                "supporting_cards": [row["card_id"] for row in chunk],
                "refs": refs,
                "gaps": detect_gaps(chunk)[:5],
                "created_at": now_iso,
            }
        )

    repo.replace_chapter_candidates(run_id=run_id, toc_style="thematic", rows=thematic, created_at=now_iso)
    repo.replace_chapter_candidates(run_id=run_id, toc_style="chronological", rows=chronological, created_at=now_iso)
    repo.replace_chapter_candidates(run_id=run_id, toc_style="strategy", rows=strategy, created_at=now_iso)

    return {"thematic": thematic, "chronological": chronological, "strategy": strategy}


def render_chapter_note(chapters: dict[str, list[dict[str, Any]]]) -> str:
    lines: list[str] = []
    lines.append("## Chapter Emergence")
    lines.append("")
    for style in ("thematic", "chronological", "strategy"):
        rows = list(chapters.get(style, []))
        lines.append(f"### {style.title()} TOC")
        if not rows:
            lines.append("- No chapter proposals.")
            lines.append("")
            continue
        for idx, row in enumerate(rows, start=1):
            lines.append(f"- {idx}. **{row['thesis']}**")
            lines.append(f"  - Supporting cards: {', '.join(row.get('supporting_cards', []))}")
            lines.append(f"  - Sources: {_refs_md(row.get('refs', [])) or 'none'}")
            if row.get("gaps"):
                lines.append("  - Missing evidence/gaps:")
                for gap in row["gaps"][:3]:
                    lines.append(f"    - {gap['question']}")
        lines.append("")
    return "\n".join(lines).rstrip()


def build_argumentation(
    repo: StorageRepo,
    *,
    topic: str | None = None,
) -> dict[str, Any]:
    cards = _select_cards_for_topic(repo.list_greene_cards(state="keeper", limit=5000), topic)
    if not cards:
        return {
            "topic": topic or "global",
            "strongest_argument": None,
            "strongest_counterargument": None,
            "what_would_change_my_mind": [],
            "watch_next": [],
            "assumptions": [],
        }
    cards = sorted(cards, key=lambda r: (-float(r.get("score") or 0.0), str(r.get("card_id") or "")))
    strongest = cards[0]
    counter = next((c for c in cards if c.get("card_type") == "angle"), cards[min(1, len(cards) - 1)])
    gaps = detect_gaps(cards[:8])
    assumptions = [
        {
            "assumption": _trim(f"{c.get('theme', 'general')} trend continues under current constraints.", 180),
            "supporting_cards": [str(c.get("card_id") or "")],
            "risk_level": "high" if len(c.get("source_refs", [])) < 2 else "medium",
        }
        for c in cards[:5]
    ]
    watch_next = [
        _trim(f"Watch for confirming/invalidating evidence linked to {c.get('title')}.", 180)
        for c in cards[:3]
    ]
    return {
        "topic": topic or "global",
        "strongest_argument": {
            "card_id": strongest.get("card_id"),
            "text": strongest.get("payload"),
            "refs": strongest.get("source_refs", []),
        },
        "strongest_counterargument": {
            "card_id": counter.get("card_id"),
            "text": counter.get("payload"),
            "refs": counter.get("source_refs", []),
        },
        "what_would_change_my_mind": [g["question"] for g in gaps[:6]],
        "watch_next": watch_next,
        "assumptions": assumptions,
    }


def render_argumentation(argument: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("## Argumentation Layer")
    lines.append("")
    lines.append(f"- Topic: **{argument.get('topic', 'global')}**")
    lines.append("")
    strongest = argument.get("strongest_argument")
    counter = argument.get("strongest_counterargument")
    lines.append("### Strongest Argument")
    if strongest:
        lines.append(f"- {strongest.get('text')}")
        lines.append(f"  - Sources: {_refs_md(strongest.get('refs', [])) or 'none'}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("### Strongest Counterargument")
    if counter:
        lines.append(f"- {counter.get('text')}")
        lines.append(f"  - Sources: {_refs_md(counter.get('refs', [])) or 'none'}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("### What Would Change My Mind")
    for row in argument.get("what_would_change_my_mind", []):
        lines.append(f"- {row}")
    if not argument.get("what_would_change_my_mind"):
        lines.append("- none")
    lines.append("")
    lines.append("### Watch Next")
    for row in argument.get("watch_next", []):
        lines.append(f"- {row}")
    if not argument.get("watch_next"):
        lines.append("- none")
    lines.append("")
    lines.append("### Assumption Register")
    for row in argument.get("assumptions", []):
        lines.append(f"- **{row['risk_level'].upper()}** {row['assumption']}")
        lines.append(f"  - Supporting cards: {', '.join(row.get('supporting_cards', []))}")
    if not argument.get("assumptions"):
        lines.append("- none")
    return "\n".join(lines).rstrip()


def render_gap_note(gaps: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("## Gap Finder")
    lines.append("")
    if not gaps:
        lines.append("- No major gaps detected.")
        return "\n".join(lines)
    for gap in gaps:
        lines.append(f"- **{gap['kind']}** ({gap['card_id']})")
        lines.append(f"  - Question: {gap['question']}")
    return "\n".join(lines).rstrip()


def _paragraph_from_card(card: dict[str, Any], *, include_sources: bool = True) -> str:
    body = f"{_trim(str(card.get('payload') or ''), 420)}"
    if include_sources:
        refs = _refs_md(card.get("source_refs", []))
        if refs:
            body += f"\n\nSources: {refs}"
    return body


def generate_draft(
    settings,
    repo: StorageRepo,
    *,
    run_id: str,
    now_iso: str,
    mode: str,
    topic: str | None = None,
) -> dict[str, Any]:
    cards = _select_cards_for_topic(repo.list_greene_cards(state="keeper", limit=5000), topic)
    cards = sorted(cards, key=lambda c: (-float(c.get("score") or 0.0), str(c.get("card_id") or "")))
    include_sources = mode != "compile"

    lines: list[str] = []
    lines.append(f"# Roberto Draft - {mode}")
    lines.append("")
    if topic:
        lines.append(f"- Topic: {topic}")
        lines.append("")

    if mode == "memo":
        lines.append("## 1-Page Memo")
        for card in cards[:6]:
            lines.append(f"### {card.get('title')}")
            lines.append(_paragraph_from_card(card, include_sources=include_sources))
            lines.append("")
    elif mode == "brief":
        lines.append("## 10-Bullet Brief")
        for idx, card in enumerate(cards[:10], start=1):
            lines.append(f"- {idx}. {card.get('title')}: {_trim(str(card.get('payload') or ''), 180)}")
            if include_sources:
                lines.append(f"  - Sources: {_refs_md(card.get('source_refs', [])) or 'none'}")
    elif mode == "essay-skeleton":
        lines.append("## Essay Skeleton")
        for idx, card in enumerate(cards[:8], start=1):
            lines.append(f"### Section {idx}: {card.get('title')}")
            lines.append(_paragraph_from_card(card, include_sources=include_sources))
            lines.append("")
    else:
        chapters = repo.list_chapter_candidates(limit=12)
        lines.append("## Chapter Draft")
        if not chapters:
            for idx, card in enumerate(cards[:8], start=1):
                lines.append(f"### Chapter Fragment {idx}")
                lines.append(_paragraph_from_card(card, include_sources=include_sources))
                lines.append("")
        else:
            for chapter in chapters[:6]:
                lines.append(f"### {chapter.get('thesis')}")
                for card_id in chapter.get("supporting_cards", [])[:6]:
                    card = repo.get_greene_card(str(card_id))
                    if not card:
                        continue
                    lines.append(_paragraph_from_card(card, include_sources=include_sources))
                    lines.append("")

    text = "\n".join(lines).rstrip() + "\n"
    date_part = now_iso[:10]
    mode_slug = mode.replace(" ", "-")
    output_path = settings.resolve("notes", "greene", "drafts", f"{date_part}__{mode_slug}.md")
    note_res = update_note_file(
        output_path,
        note_type="greene",
        run_id=run_id,
        now_iso=now_iso,
        auto_body=text,
        note_title=f"Roberto Draft - {mode}",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(output_path),
            note_type="greene",
            username=None,
            created_at=note_res.created_at,
            updated_at=note_res.updated_at,
            last_run_id=run_id,
        )
    )
    output_id = _stable_id("studio", run_id, mode, topic or "global", date_part)
    payload = {"mode": mode, "topic": topic, "cards_used": [c["card_id"] for c in cards[:20]], "text": text}
    repo.upsert_studio_output(
        output_id=output_id,
        run_id=run_id,
        mode=mode,
        topic=topic,
        output_path=str(output_path),
        payload=payload,
        created_at=now_iso,
    )
    return {"output_id": output_id, "mode": mode, "topic": topic, "output_path": str(output_path), "text": text}


def run_ai_action(
    settings,
    repo: StorageRepo,
    *,
    action: str,
) -> dict[str, Any]:
    if action == "one-issue":
        brief = repo.get_latest_briefing()
        if not brief:
            return {"action": action, "text": "No briefing available.", "refs": []}
        summary = brief.get("summary", {}) or {}
        stories = list(summary.get("story_deltas", []))
        if not stories:
            return {"action": action, "text": "No story deltas in latest briefing.", "refs": []}
        top = stories[0]
        text = (
            f"One issue today: {top.get('title')}.\n"
            f"Change: {top.get('what_changed')}\n"
            f"Why it matters: {top.get('why_it_matters')}"
        )
        return {"action": action, "text": text, "refs": top.get("refs", [])}
    if action == "challenge-thesis":
        arg = build_argumentation(repo, topic=None)
        counter = arg.get("strongest_counterargument") or {}
        text = f"Challenge thesis using: {counter.get('text') or 'No counterargument available.'}"
        return {"action": action, "text": text, "refs": counter.get("refs", [])}
    if action == "build-counter":
        conflicts = repo.list_conflicts(status="open", limit=3)
        if not conflicts:
            return {"action": action, "text": "No open conflicts to build counter from.", "refs": []}
        row = conflicts[0]
        claim_b = row.get("claim_b", {}) or {}
        text = f"Counter frame: {claim_b.get('text') or 'No claim text'}"
        return {"action": action, "text": text, "refs": row.get("source_refs", [])}
    if action == "impact-top":
        cards = repo.list_greene_cards(state="keeper", limit=1)
        if not cards:
            return {"action": action, "text": "No keeper cards yet.", "refs": []}
        card = cards[0]
        text = f"Top impact card: {card.get('title')} -> {card.get('strategic_use_case') or card.get('payload')}"
        return {"action": action, "text": text, "refs": card.get("source_refs", [])}
    raise ValueError(f"Unknown action: {action}")


def run_chapter_argument_gap_cycle(
    settings,
    repo: StorageRepo,
    *,
    run_id: str,
    now_iso: str,
    topic: str | None = None,
) -> dict[str, Any]:
    chapters = propose_chapters(settings, repo, run_id=run_id, now_iso=now_iso, topic=topic)
    chapter_text = render_chapter_note(chapters)
    chapter_path = settings.resolve("notes", "greene", "chapters", f"{now_iso[:10]}.md")
    chapter_res = update_note_file(
        chapter_path,
        note_type="greene",
        run_id=run_id,
        now_iso=now_iso,
        auto_body=chapter_text,
        note_title=f"Chapter Emergence - {now_iso[:10]}",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(chapter_path),
            note_type="greene",
            username=None,
            created_at=chapter_res.created_at,
            updated_at=chapter_res.updated_at,
            last_run_id=run_id,
        )
    )

    argument = build_argumentation(repo, topic=topic)
    argument_text = render_argumentation(argument)
    argument_path = settings.resolve("notes", "greene", "argumentation", f"{now_iso[:10]}.md")
    argument_res = update_note_file(
        argument_path,
        note_type="greene",
        run_id=run_id,
        now_iso=now_iso,
        auto_body=argument_text,
        note_title=f"Argumentation - {now_iso[:10]}",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(argument_path),
            note_type="greene",
            username=None,
            created_at=argument_res.created_at,
            updated_at=argument_res.updated_at,
            last_run_id=run_id,
        )
    )

    gaps = detect_gaps(_select_cards_for_topic(repo.list_greene_cards(state="keeper", limit=5000), topic))
    gap_text = render_gap_note(gaps)
    gap_path = settings.resolve("notes", "greene", "gaps", f"{now_iso[:10]}.md")
    gap_res = update_note_file(
        gap_path,
        note_type="greene",
        run_id=run_id,
        now_iso=now_iso,
        auto_body=gap_text,
        note_title=f"Gap Finder - {now_iso[:10]}",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(gap_path),
            note_type="greene",
            username=None,
            created_at=gap_res.created_at,
            updated_at=gap_res.updated_at,
            last_run_id=run_id,
        )
    )

    return {
        "chapter_path": str(chapter_path),
        "argument_path": str(argument_path),
        "gap_path": str(gap_path),
        "chapter_counts": {k: len(v) for k, v in chapters.items()},
        "gap_count": len(gaps),
    }
