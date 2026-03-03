from __future__ import annotations

from roberto_app.llm.schemas import (
    Connection,
    ConnectionSupport,
    DailyDigestAutoBlock,
    Highlight,
    NoteCard,
    Story,
    StorySource,
    UserNoteAutoBlock,
)


def validate_user_auto_block(block: UserNoteAutoBlock, valid_tweet_ids: set[str]) -> UserNoteAutoBlock:
    valid_notecards: list[NoteCard] = []
    for card in block.notecards:
        refs = [ref for ref in card.source_tweet_ids if ref in valid_tweet_ids]
        if not refs:
            continue
        valid_notecards.append(
            NoteCard(
                type=card.type,
                title=card.title,
                payload=card.payload,
                why_it_matters=card.why_it_matters,
                tags=card.tags,
                source_tweet_ids=refs,
            )
        )

    valid_highlights: list[Highlight] = []
    for item in block.highlights:
        refs = [ref for ref in item.source_tweet_ids if ref in valid_tweet_ids]
        if not refs:
            continue
        valid_highlights.append(
            Highlight(
                title=item.title,
                summary=item.summary,
                source_tweet_ids=refs,
            )
        )

    return UserNoteAutoBlock(
        themes=block.themes,
        notecards=valid_notecards,
        highlights=valid_highlights,
    )


def validate_digest_auto_block(
    block: DailyDigestAutoBlock,
    valid_refs: set[tuple[str, str]],
) -> DailyDigestAutoBlock:
    valid_stories: list[Story] = []
    for story in block.stories:
        sources = [
            StorySource(username=src.username, tweet_id=src.tweet_id)
            for src in story.sources
            if (src.username, src.tweet_id) in valid_refs
        ]
        if not sources:
            continue
        valid_stories.append(
            Story(
                title=story.title,
                what_happened=story.what_happened,
                why_it_matters=story.why_it_matters,
                sources=sources,
                tags=story.tags,
                confidence=story.confidence,
            )
        )

    valid_connections: list[Connection] = []
    for conn in block.connections:
        supports = [
            ConnectionSupport(username=s.username, tweet_id=s.tweet_id)
            for s in conn.supports
            if (s.username, s.tweet_id) in valid_refs
        ]
        if not supports:
            continue
        valid_connections.append(Connection(insight=conn.insight, supports=supports))

    return DailyDigestAutoBlock(stories=valid_stories, connections=valid_connections)
