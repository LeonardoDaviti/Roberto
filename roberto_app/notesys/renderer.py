from __future__ import annotations

from typing import Any

from roberto_app.llm.schemas import DailyDigestAutoBlock, Story, UserNoteAutoBlock


def _trim_text(value: str, limit: int = 220) -> str:
    value = " ".join(value.split())
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "..."


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
                refs = ", ".join(
                    f"[{tweet_id}](https://x.com/{username}/status/{tweet_id})"
                    for tweet_id in card.source_tweet_ids
                )
                lines.append(f"  - Sources: {refs}")
    else:
        lines.append("- No notecards generated.")

    lines.append("")
    lines.append("### Highlights")
    if block.highlights:
        for item in block.highlights:
            lines.append(f"- **{item.title}**: {_trim_text(item.summary)}")
            if item.source_tweet_ids:
                refs = ", ".join(
                    f"[{tweet_id}](https://x.com/{username}/status/{tweet_id})"
                    for tweet_id in item.source_tweet_ids
                )
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
            sources = ", ".join(
                f"[{s.username}:{s.tweet_id}](https://x.com/{s.username}/status/{s.tweet_id})"
                for s in story.sources
            )
            lines.append(f"  - Sources: {sources if sources else 'none'}")
            lines.append(f"  - Tags: {', '.join(story.tags) if story.tags else 'none'}")
    else:
        lines.append("- No major cross-user stories in this run.")

    lines.append("")
    lines.append("### Connections")
    if block.connections:
        for conn in block.connections:
            lines.append(f"- {conn.insight}")
            supports = ", ".join(
                f"[{s.username}:{s.tweet_id}](https://x.com/{s.username}/status/{s.tweet_id})"
                for s in conn.supports
            )
            lines.append(f"  - Supports: {supports if supports else 'none'}")
    else:
        lines.append("- No non-obvious connections found this run.")

    return "\n".join(lines).rstrip()


def render_story_auto_block(story: Story, history_sources: list[dict[str, Any]], mention_count: int) -> str:
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
            lines.append(
                f"- [{source.username}:{source.tweet_id}](https://x.com/{source.username}/status/{source.tweet_id})"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("### Historical Sources (recent)")
    if history_sources:
        for src in history_sources:
            username = src.get("username", "")
            tweet_id = src.get("tweet_id", "")
            run_id = src.get("run_id", "")
            lines.append(
                f"- {run_id} - [{username}:{tweet_id}](https://x.com/{username}/status/{tweet_id})"
            )
    else:
        lines.append("- none")

    return "\n".join(lines).rstrip()
