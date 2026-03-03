from __future__ import annotations

from typing import Any

from roberto_app.llm.schemas import DailyDigestAutoBlock, SourceRef, Story, UserNoteAutoBlock
from roberto_app.sources.refs import dedupe_source_refs, source_ref_markdown, x_source_ref


def _trim_text(value: str, limit: int = 220) -> str:
    value = " ".join(value.split())
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "..."


def _refs_md(refs: list[dict[str, Any]]) -> str:
    normalized = dedupe_source_refs([dict(r) for r in refs if isinstance(r, dict)])
    return ", ".join(source_ref_markdown(ref) for ref in normalized)


def _ref_dicts(refs: list[Any], *, fallback_username: str | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ref in refs:
        if isinstance(ref, SourceRef):
            payload = ref.as_ref_dict()
        elif isinstance(ref, dict):
            payload = dict(ref)
        else:
            continue
        if fallback_username and payload.get("provider") == "x" and not payload.get("username"):
            payload["username"] = fallback_username
        out.append(payload)
    return out


def render_user_auto_block(username: str, block: UserNoteAutoBlock, tweets: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("## Roberto Summary (last 100 posts)")
    lines.append("")
    lines.append("### High-signal themes")
    if block.themes:
        for theme in block.themes:
            lines.append(f"- {theme}")
    else:
        lines.append("- No strong themes detected from available posts.")

    lines.append("")
    lines.append("### Notecards (atomic insights)")
    if block.notecards:
        for card in block.notecards:
            lines.append(f"- **{card.type.upper()}**: {card.title}")
            lines.append(f"  - Payload: {_trim_text(card.payload)}")
            lines.append(f"  - Why it matters: {_trim_text(card.why_it_matters)}")
            lines.append(f"  - Tags: {', '.join(card.tags) if card.tags else 'none'}")
            card_refs = _ref_dicts(list(card.source_refs), fallback_username=username)
            if not card_refs and card.source_tweet_ids:
                card_refs = [x_source_ref(username=username, tweet_id=tweet_id) for tweet_id in card.source_tweet_ids]
            if card_refs:
                refs = _refs_md(card_refs)
                lines.append(f"  - Sources: {refs}")
    else:
        lines.append("- No notecards generated.")

    lines.append("")
    lines.append("### Highlights")
    if block.highlights:
        for item in block.highlights:
            lines.append(f"- **{item.title}**: {_trim_text(item.summary)}")
            item_refs = _ref_dicts(list(item.source_refs), fallback_username=username)
            if not item_refs and item.source_tweet_ids:
                item_refs = [x_source_ref(username=username, tweet_id=tweet_id) for tweet_id in item.source_tweet_ids]
            if item_refs:
                refs = _refs_md(item_refs)
                lines.append(f"  - Sources: {refs}")
    else:
        lines.append("- No highlights generated.")

    lines.append("")
    lines.append("### Recent posts (for reference)")
    if tweets:
        for tweet in tweets[:20]:
            created_at = tweet.get("created_at") or "unknown-date"
            text = _trim_text(tweet.get("text", ""), 180)
            tweet_id = tweet.get("tweet_id", "")
            lines.append(
                f"- {created_at} - \"{text}\" "
                f"([tweet]({f'https://x.com/{username}/status/{tweet_id}'}), tweet_id: {tweet_id})"
            )
    else:
        lines.append("- No cached posts yet.")

    return "\n".join(lines).rstrip()


def render_digest_auto_block(block: DailyDigestAutoBlock) -> str:
    lines: list[str] = []
    lines.append("## Roberto Cross-User Digest")
    lines.append("")

    lines.append("### Stories")
    if block.stories:
        for story in block.stories:
            lines.append(f"- **{story.title}** ({story.confidence})")
            lines.append(f"  - What happened: {_trim_text(story.what_happened)}")
            lines.append(f"  - Why it matters: {_trim_text(story.why_it_matters)}")
            story_refs = _ref_dicts(list(story.source_refs))
            if not story_refs and story.sources:
                story_refs = [x_source_ref(username=s.username, tweet_id=s.tweet_id) for s in story.sources]
            sources = _refs_md(story_refs)
            lines.append(f"  - Sources: {sources if sources else 'none'}")
            lines.append(f"  - Tags: {', '.join(story.tags) if story.tags else 'none'}")
    else:
        lines.append("- No major cross-user stories in this run.")

    lines.append("")
    lines.append("### Connections")
    if block.connections:
        for conn in block.connections:
            lines.append(f"- {conn.insight}")
            support_refs = _ref_dicts(list(conn.source_refs))
            if not support_refs and conn.supports:
                support_refs = [x_source_ref(username=s.username, tweet_id=s.tweet_id) for s in conn.supports]
            supports = _refs_md(support_refs)
            lines.append(f"  - Supports: {supports if supports else 'none'}")
    else:
        lines.append("- No non-obvious connections found this run.")

    return "\n".join(lines).rstrip()


def render_story_auto_block(
    story: Story,
    history_sources: list[dict[str, Any]],
    mention_count: int,
    *,
    confidence_history: list[dict[str, Any]] | None = None,
    claims: list[dict[str, Any]] | None = None,
) -> str:
    lines: list[str] = []
    lines.append("## Story Snapshot")
    lines.append("")
    lines.append(f"- Confidence: **{story.confidence}**")
    lines.append(f"- Mentions across runs: **{mention_count}**")
    lines.append(f"- Tags: {', '.join(story.tags) if story.tags else 'none'}")
    lines.append("")
    lines.append("### What Happened")
    lines.append(f"- {_trim_text(story.what_happened, 500)}")
    lines.append("")
    lines.append("### Why It Matters")
    lines.append(f"- {_trim_text(story.why_it_matters, 500)}")
    lines.append("")
    lines.append("### Current Run Sources")
    current_refs = _ref_dicts(list(story.source_refs))
    if not current_refs and story.sources:
        current_refs = [x_source_ref(username=source.username, tweet_id=source.tweet_id) for source in story.sources]
    if current_refs:
        for source_ref in dedupe_source_refs(current_refs):
            lines.append(f"- {source_ref_markdown(source_ref)}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("### Historical Sources (recent)")
    if history_sources:
        for src in history_sources:
            run_id = src.get("run_id", "")
            lines.append(f"- {run_id} - {source_ref_markdown(dict(src))}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("### Confidence Evolution")
    if confidence_history:
        for event in confidence_history:
            prev = event.get("previous_confidence")
            new = event.get("new_confidence")
            reason = _trim_text(str(event.get("reason") or ""), 320)
            created_at = str(event.get("created_at") or "")
            if prev:
                lines.append(f"- {created_at}: {prev} -> {new}")
            else:
                lines.append(f"- {created_at}: {new}")
            lines.append(f"  - Reason: {reason}")
    else:
        lines.append("- No confidence transitions recorded yet.")

    lines.append("")
    lines.append("### Claim Ledger")
    if claims:
        for claim in claims:
            claim_text = _trim_text(str(claim.get("claim_text") or ""), 320)
            status = str(claim.get("status") or "active")
            confidence = str(claim.get("confidence") or story.confidence)
            lines.append(f"- **{status.upper()}** ({confidence}) {claim_text}")
            refs = _refs_md([dict(r) for r in claim.get("evidence_refs", []) if isinstance(r, dict)])
            lines.append(f"  - Evidence: {refs if refs else 'none'}")
    else:
        lines.append("- No claims extracted yet.")

    return "\n".join(lines).rstrip()
