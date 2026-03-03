from __future__ import annotations

from typing import Any

from roberto_app.llm.schemas import DailyDigestAutoBlock, Story, UserNoteAutoBlock
from roberto_app.sources.refs import dedupe_source_refs, source_ref_markdown, x_source_ref


def _trim_text(value: str, limit: int = 220) -> str:
    value = " ".join(value.split())
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "..."


def _refs_md(refs: list[dict[str, Any]]) -> str:
    normalized = dedupe_source_refs([dict(r) for r in refs if isinstance(r, dict)])
    return ", ".join(source_ref_markdown(ref) for ref in normalized)


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
            if card.source_tweet_ids:
                refs = _refs_md([x_source_ref(username=username, tweet_id=tweet_id) for tweet_id in card.source_tweet_ids])
                lines.append(f"  - Sources: {refs}")
    else:
        lines.append("- No notecards generated.")

    lines.append("")
    lines.append("### Highlights")
    if block.highlights:
        for item in block.highlights:
            lines.append(f"- **{item.title}**: {_trim_text(item.summary)}")
            if item.source_tweet_ids:
                refs = _refs_md([x_source_ref(username=username, tweet_id=tweet_id) for tweet_id in item.source_tweet_ids])
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
            sources = _refs_md([x_source_ref(username=s.username, tweet_id=s.tweet_id) for s in story.sources])
            lines.append(f"  - Sources: {sources if sources else 'none'}")
            lines.append(f"  - Tags: {', '.join(story.tags) if story.tags else 'none'}")
    else:
        lines.append("- No major cross-user stories in this run.")

    lines.append("")
    lines.append("### Connections")
    if block.connections:
        for conn in block.connections:
            lines.append(f"- {conn.insight}")
            supports = _refs_md([x_source_ref(username=s.username, tweet_id=s.tweet_id) for s in conn.supports])
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
    if story.sources:
        for source in story.sources:
            lines.append(f"- {source_ref_markdown(x_source_ref(username=source.username, tweet_id=source.tweet_id))}")
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
