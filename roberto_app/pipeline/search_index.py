from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from roberto_app.notesys.templates import AUTO_BEGIN, AUTO_END
from roberto_app.storage.repo import StorageRepo

SOURCE_RE = re.compile(r"https://x\.com/([A-Za-z0-9_]+)/status/([0-9]+)")


def _trim(value: str, limit: int = 4000) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def _extract_source_ids(text: str) -> str:
    refs = [f"{username}:{tweet_id}" for username, tweet_id in SOURCE_RE.findall(text)]
    seen: set[str] = set()
    ordered: list[str] = []
    for ref in refs:
        if ref in seen:
            continue
        seen.add(ref)
        ordered.append(ref)
    return ",".join(ordered)


def _extract_title_from_markdown(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("# "):
            return s[2:].strip()
    return ""


def _extract_auto_block(text: str) -> str:
    if AUTO_BEGIN not in text or AUTO_END not in text:
        return text
    start = text.find(AUTO_BEGIN)
    end = text.find(AUTO_END)
    if start < 0 or end < 0 or end < start:
        return text
    return text[start + len(AUTO_BEGIN) : end].strip()


def _fts_query(query: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9_]+", query.lower())
    if not tokens:
        return f'"{query.strip()}"'
    return " ".join(tokens)


def _note_docs(notes_root: Path, note_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    docs: list[dict[str, str]] = []
    for row in note_rows:
        note_path = Path(str(row["note_path"]))
        if not note_path.exists():
            continue
        text = note_path.read_text(encoding="utf-8")
        auto_text = _extract_auto_block(text)
        docs.append(
            {
                "kind": "note",
                "subtype": str(row.get("note_type") or ""),
                "item_id": str(note_path.name),
                "ref_path": str(note_path),
                "source_ids": _extract_source_ids(text),
                "title": _trim(_extract_title_from_markdown(text), 220),
                "body": _trim(auto_text, 8000),
                "tags": "",
                "username": str(row.get("username") or ""),
                "entity": "",
                "created_at": str(row.get("updated_at") or row.get("created_at") or ""),
            }
        )
    return docs


def rebuild_search_index(settings, repo: StorageRepo) -> int:
    docs: list[dict[str, str]] = []

    for tweet in repo.list_tweets_for_search(limit=100000):
        docs.append(
            {
                "kind": "tweet",
                "subtype": "",
                "item_id": str(tweet["tweet_id"]),
                "ref_path": f"https://x.com/{tweet['username']}/status/{tweet['tweet_id']}",
                "source_ids": f"{tweet['username']}:{tweet['tweet_id']}",
                "title": f"@{tweet['username']}:{tweet['tweet_id']}",
                "body": _trim(str(tweet.get("text") or ""), 4000),
                "tags": "",
                "username": str(tweet.get("username") or ""),
                "entity": "",
                "created_at": str(tweet.get("created_at") or ""),
            }
        )

    for story in repo.list_stories(limit=5000):
        summary = story.get("summary_json", {}) or {}
        body = f"{summary.get('what_happened', '')}\n{summary.get('why_it_matters', '')}"
        story_path = settings.resolve("notes", "stories", f"{story['slug']}.md")
        docs.append(
            {
                "kind": "story",
                "subtype": "",
                "item_id": str(story["story_id"]),
                "ref_path": str(story_path),
                "source_ids": "",
                "title": _trim(str(story.get("title") or ""), 260),
                "body": _trim(body, 5000),
                "tags": ",".join(story.get("tags_json", [])),
                "username": "",
                "entity": "",
                "created_at": str(story.get("updated_at") or story.get("created_at") or ""),
            }
        )
        claim_rows = repo.list_story_claims(str(story["story_id"]), limit=200)
        for claim in claim_rows:
            refs = ",".join(
                f"{r['username']}:{r['tweet_id']}"
                for r in claim.get("evidence_refs", [])
                if r.get("username") and r.get("tweet_id")
            )
            docs.append(
                {
                    "kind": "story",
                    "subtype": "claim",
                    "item_id": str(claim["claim_id"]),
                    "ref_path": str(story_path),
                    "source_ids": refs,
                    "title": _trim(f"{story.get('title', '')} claim", 260),
                    "body": _trim(str(claim.get("claim_text") or ""), 5000),
                    "tags": ",".join(story.get("tags_json", [])),
                    "username": "",
                    "entity": "",
                    "created_at": str(claim.get("updated_at") or claim.get("created_at") or ""),
                }
            )

    for card in repo.list_recent_idea_cards(days=3650, limit=100000):
        refs = ",".join(f"{r['username']}:{r['tweet_id']}" for r in card.get("source_refs", []))
        docs.append(
            {
                "kind": "idea",
                "subtype": str(card.get("idea_type") or ""),
                "item_id": str(card["card_id"]),
                "ref_path": str(settings.resolve("notes", "ideas", f"{card['username']}.md")),
                "source_ids": refs,
                "title": _trim(str(card.get("title") or ""), 260),
                "body": _trim(f"{card.get('hypothesis', '')}\n{card.get('why_now', '')}", 5000),
                "tags": ",".join(card.get("tags", [])),
                "username": str(card.get("username") or ""),
                "entity": "",
                "created_at": str(card.get("created_at") or ""),
            }
        )

    conflict_note_path = settings.resolve("notes", "conflicts", "latest.md")
    conflict_nodes = repo.list_conflicts(limit=100000)
    if conflict_nodes:
        for conflict in conflict_nodes:
            refs = ",".join(
                f"{r['username']}:{r['tweet_id']}"
                for r in conflict.get("source_refs", [])
                if r.get("username") and r.get("tweet_id")
            )
            claim_a = conflict.get("claim_a", {}) or {}
            claim_b = conflict.get("claim_b", {}) or {}
            claim_a_text = str(claim_a.get("text") or claim_a.get("title") or claim_a.get("hypothesis") or "")
            claim_b_text = str(claim_b.get("text") or claim_b.get("title") or claim_b.get("hypothesis") or "")
            status = str(conflict.get("status") or "open")
            docs.append(
                {
                    "kind": "conflict",
                    "subtype": status,
                    "item_id": str(conflict["conflict_id"]),
                    "ref_path": str(conflict_note_path),
                    "source_ids": refs,
                    "title": _trim(str(conflict.get("topic") or "Conflict"), 260),
                    "body": _trim(f"Status: {status}\nA: {claim_a_text}\nB: {claim_b_text}", 5000),
                    "tags": "",
                    "username": "",
                    "entity": "",
                    "created_at": str(conflict.get("updated_at") or conflict.get("created_at") or ""),
                }
            )
    else:
        for conflict in repo.list_recent_conflict_cards(days=3650, limit=100000):
            refs = ",".join(f"{r['username']}:{r['tweet_id']}" for r in conflict.get("source_refs", []))
            claim_a = conflict.get("claim_a", {}) or {}
            claim_b = conflict.get("claim_b", {}) or {}
            docs.append(
                {
                    "kind": "conflict",
                    "subtype": "",
                    "item_id": str(conflict["conflict_id"]),
                    "ref_path": str(conflict_note_path),
                    "source_ids": refs,
                    "title": _trim(str(conflict.get("title") or ""), 260),
                    "body": _trim(
                        f"A: {claim_a.get('title', '')} {claim_a.get('hypothesis', '')}\n"
                        f"B: {claim_b.get('title', '')} {claim_b.get('hypothesis', '')}",
                        5000,
                    ),
                    "tags": ",".join(conflict.get("tags", [])),
                    "username": "",
                    "entity": "",
                    "created_at": str(conflict.get("created_at") or ""),
                }
            )

    for entity in repo.list_entities(limit=20000):
        aliases = repo.get_entity_aliases(str(entity["entity_id"]))
        docs.append(
            {
                "kind": "entity",
                "subtype": "",
                "item_id": str(entity["entity_id"]),
                "ref_path": str(settings.resolve("notes", "entities", f"{entity['entity_id']}.md")),
                "source_ids": "",
                "title": _trim(str(entity.get("canonical_name") or ""), 200),
                "body": _trim(" ".join(aliases), 2000),
                "tags": "",
                "username": "",
                "entity": str(entity.get("canonical_name") or ""),
                "created_at": str(entity.get("last_seen_at") or entity.get("first_seen_at") or ""),
            }
        )

    for card in repo.list_greene_cards(limit=100000):
        refs = ",".join(f"{r['username']}:{r['tweet_id']}" for r in card.get("source_refs", []))
        docs.append(
            {
                "kind": "card",
                "subtype": str(card.get("state") or ""),
                "item_id": str(card.get("card_id") or ""),
                "ref_path": str(settings.resolve("notes", "greene", "cards", f"{card.get('week_key')}.md")),
                "source_ids": refs,
                "title": _trim(str(card.get("title") or ""), 260),
                "body": _trim(
                    f"{card.get('payload', '')}\n"
                    f"{card.get('principle', '')}\n"
                    f"{card.get('strategic_use_case', '')}",
                    5000,
                ),
                "tags": str(card.get("theme") or ""),
                "username": str(card.get("username") or ""),
                "entity": "",
                "created_at": str(card.get("updated_at") or card.get("created_at") or ""),
            }
        )

    docs.extend(_note_docs(settings.resolve("notes"), repo.list_note_index(limit=10000)))

    repo.reset_search_index()
    return repo.insert_search_docs(docs)


def search(
    settings,
    repo: StorageRepo,
    query: str,
    *,
    kind: str | None = None,
    limit: int = 20,
    days: int | None = None,
    include_muted: bool = False,
    now_iso: str | None = None,
):
    fts_query = _fts_query(query)
    if repo.count_search_docs() == 0:
        rebuild_search_index(settings, repo)
    return repo.search_docs(
        fts_query,
        kind=kind,
        limit=limit,
        days=days,
        include_muted=include_muted,
        now_iso=now_iso,
    )
