from __future__ import annotations

import re
from typing import Any

from roberto_app.llm.schemas import DailyDigestAutoBlock
from roberto_app.pipeline.story_memory import slugify_story_title
from roberto_app.storage.repo import StorageRepo

ENTITY_STOPWORDS = {
    "the",
    "this",
    "that",
    "there",
    "today",
    "tomorrow",
    "yesterday",
    "thread",
    "story",
    "stories",
    "update",
    "breaking",
    "news",
    "ai",
    "llm",
    "tag",
}


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.lower().strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value.strip())
    return out


def _canonical_name(raw: str) -> str | None:
    value = raw.strip()
    if not value:
        return None
    value = value.lstrip("@#$")
    value = re.sub(r"[_\-/]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return None

    lowered = value.lower()
    if lowered in ENTITY_STOPWORDS:
        return None
    if value.isdigit():
        return None

    if re.fullmatch(r"[A-Z0-9]{2,8}", value):
        return value
    if any(ch.isupper() for ch in value[1:]):
        return value
    return " ".join(part[:1].upper() + part[1:] for part in value.split(" "))


def _aliases_for_name(name: str) -> list[str]:
    compact = re.sub(r"\s+", "", name)
    return _dedupe_keep_order([name, name.lower(), compact, compact.lower()])


def _capitalized_entities(text: str) -> list[str]:
    pattern = re.compile(
        r"\b(?:[A-Z]{2,8}|[A-Z][a-z0-9]+(?:\s+[A-Z][a-z0-9]+){0,2})\b"
    )
    return [m.group(0) for m in pattern.finditer(text)]


def extract_entities_from_text(text: str, min_token_len: int = 3) -> list[str]:
    candidates: list[str] = []
    candidates.extend(re.findall(r"@([A-Za-z0-9_]{2,20})", text))
    candidates.extend(re.findall(r"#([A-Za-z0-9_]{2,40})", text))
    candidates.extend(re.findall(r"\$([A-Za-z]{2,8})", text))
    candidates.extend(_capitalized_entities(text))

    resolved: list[str] = []
    for raw in candidates:
        name = _canonical_name(raw)
        if not name:
            continue
        alpha_count = sum(ch.isalpha() for ch in name)
        if alpha_count < min_token_len:
            continue
        if name.lower() in ENTITY_STOPWORDS:
            continue
        resolved.append(name)
    return _dedupe_keep_order(resolved)


def extract_entities_from_tweet(tweet: dict[str, Any], min_token_len: int = 3) -> list[str]:
    candidates: list[str] = []
    payload = tweet.get("json", {}) or {}
    entities = payload.get("entities", {}) if isinstance(payload, dict) else {}
    if isinstance(entities, dict):
        for mention in entities.get("mentions", []) or []:
            username = mention.get("username")
            if username:
                candidates.append(str(username))
        for hashtag in entities.get("hashtags", []) or []:
            tag = hashtag.get("tag")
            if tag:
                candidates.append(str(tag))
        for cashtag in entities.get("cashtags", []) or []:
            tag = cashtag.get("tag")
            if tag:
                candidates.append(str(tag))
        for annotation in entities.get("annotations", []) or []:
            normalized = annotation.get("normalized_text")
            if normalized:
                candidates.append(str(normalized))

    text = str(tweet.get("text") or "")
    candidates.extend(extract_entities_from_text(text, min_token_len=min_token_len))

    resolved: list[str] = []
    for raw in candidates:
        name = _canonical_name(str(raw))
        if not name:
            continue
        alpha_count = sum(ch.isalpha() for ch in name)
        if alpha_count < min_token_len:
            continue
        resolved.append(name)
    return _dedupe_keep_order(resolved)


def index_entities_from_tweets(
    repo: StorageRepo,
    *,
    username: str,
    tweets: list[dict[str, Any]],
    now_iso: str,
    min_token_len: int,
) -> list[str]:
    entity_ids: list[str] = []
    for tweet in tweets:
        tweet_id = str(tweet.get("tweet_id") or tweet.get("id") or "")
        if not tweet_id:
            continue
        created_at = str(tweet.get("created_at") or now_iso)
        for name in extract_entities_from_tweet(tweet, min_token_len=min_token_len):
            entity_id = repo.upsert_entity(name, _aliases_for_name(name), now_iso=created_at)
            repo.link_entity_ref(entity_id, "tweet", tweet_id, username, created_at)
            entity_ids.append(entity_id)
    return _dedupe_keep_order(entity_ids)


def extract_entities_from_story_texts(story_title: str, story_text: str, tags: list[str], min_token_len: int) -> list[str]:
    candidates = extract_entities_from_text(story_title, min_token_len=min_token_len)
    candidates.extend(extract_entities_from_text(story_text, min_token_len=min_token_len))
    candidates.extend(tags)

    resolved: list[str] = []
    for raw in candidates:
        name = _canonical_name(str(raw))
        if not name:
            continue
        alpha_count = sum(ch.isalpha() for ch in name)
        if alpha_count < min_token_len:
            continue
        resolved.append(name)
    return _dedupe_keep_order(resolved)


def index_entities_from_digest(
    repo: StorageRepo,
    digest_block: DailyDigestAutoBlock,
    *,
    now_iso: str,
    min_token_len: int,
) -> list[str]:
    entity_ids: list[str] = []
    for story in digest_block.stories:
        slug = slugify_story_title(story.title)
        story_id = f"story:{slug}"
        story_text = f"{story.what_happened}\n{story.why_it_matters}"
        names = extract_entities_from_story_texts(
            story.title,
            story_text,
            story.tags,
            min_token_len=min_token_len,
        )
        for name in names:
            entity_id = repo.upsert_entity(name, _aliases_for_name(name), now_iso=now_iso)
            repo.link_story_entity(story_id, entity_id, created_at=now_iso)
            repo.link_entity_ref(entity_id, "story", story_id, None, now_iso)
            entity_ids.append(entity_id)
    return _dedupe_keep_order(entity_ids)


def render_entity_auto_block(
    *,
    canonical_name: str,
    aliases: list[str],
    timeline_rows: list[dict[str, Any]],
    days: int,
) -> str:
    lines: list[str] = []
    lines.append(f"## Entity Timeline ({days} days)")
    lines.append("")
    lines.append(f"- Canonical: **{canonical_name}**")
    lines.append(f"- Aliases: {', '.join(aliases) if aliases else 'none'}")
    lines.append(f"- Linked events: {len(timeline_rows)}")
    lines.append("")
    lines.append("### Events")

    if not timeline_rows:
        lines.append("- No links in the selected window.")
        return "\n".join(lines)

    for row in timeline_rows:
        created_at = row.get("created_at") or "-"
        ref_type = row.get("ref_type")
        if ref_type == "tweet":
            username = row.get("username") or "unknown"
            tweet_id = row.get("ref_id") or ""
            text = str(row.get("tweet_text") or "").strip()
            text = " ".join(text.split())
            if len(text) > 180:
                text = text[:179] + "..."
            lines.append(
                f"- {created_at} - tweet "
                f"[@{username}:{tweet_id}](https://x.com/{username}/status/{tweet_id})"
            )
            if text:
                lines.append(f"  - {text}")
            continue

        if ref_type == "story":
            story_id = row.get("ref_id") or ""
            story_title = row.get("story_title") or story_id
            lines.append(f"- {created_at} - story `{story_id}`: {story_title}")
            continue

        lines.append(f"- {created_at} - {ref_type}: {row.get('ref_id')}")

    return "\n".join(lines)
