from __future__ import annotations

from typing import Any, Iterable

from roberto_app.llm.schemas import (
    Connection,
    DailyDigestAutoBlock,
    Highlight,
    NoteCard,
    SourceRef,
    Story,
    UserNoteAutoBlock,
)
from roberto_app.sources.refs import dedupe_source_refs, x_source_ref


def _ref_key(ref: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(ref.get("provider") or ""),
        str(ref.get("source_id") or ""),
        str(ref.get("anchor_type") or ""),
        str(ref.get("anchor") or ""),
    )


def _to_ref_dict(ref: Any) -> dict[str, Any] | None:
    if isinstance(ref, SourceRef):
        return ref.as_ref_dict()
    if isinstance(ref, dict):
        return dict(ref)
    return None


def _valid_ref_index(valid_refs: Iterable[Any]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    candidate_refs: list[dict[str, Any]] = []
    for item in valid_refs:
        if isinstance(item, tuple) and len(item) == 2:
            username = str(item[0] or "").strip()
            tweet_id = str(item[1] or "").strip()
            if username and tweet_id:
                candidate_refs.append(x_source_ref(username=username, tweet_id=tweet_id))
            continue
        if isinstance(item, str):
            tweet_id = item.strip()
            if tweet_id:
                candidate_refs.append(x_source_ref(tweet_id=tweet_id))
            continue
        payload = _to_ref_dict(item)
        if payload:
            candidate_refs.append(payload)
    index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for ref in dedupe_source_refs(candidate_refs):
        index[_ref_key(ref)] = ref
    return index


def _filter_refs(
    refs: list[SourceRef],
    valid_index: dict[tuple[str, str, str, str], dict[str, Any]],
) -> list[SourceRef]:
    kept: list[SourceRef] = []
    for ref in refs:
        key = _ref_key(ref.as_ref_dict())
        canonical = valid_index.get(key)
        if canonical:
            kept.append(SourceRef.model_validate(canonical))
    return kept


def validate_user_auto_block(block: UserNoteAutoBlock, valid_tweet_ids: Iterable[Any]) -> UserNoteAutoBlock:
    valid_index = _valid_ref_index(valid_tweet_ids)

    valid_notecards: list[NoteCard] = []
    for card in block.notecards:
        refs = _filter_refs(card.source_refs, valid_index)
        if not refs:
            continue
        valid_notecards.append(
            NoteCard(
                type=card.type,
                title=card.title,
                payload=card.payload,
                why_it_matters=card.why_it_matters,
                tags=card.tags,
                source_refs=[ref.as_ref_dict() for ref in refs],
            )
        )

    valid_highlights: list[Highlight] = []
    for item in block.highlights:
        refs = _filter_refs(item.source_refs, valid_index)
        if not refs:
            continue
        valid_highlights.append(
            Highlight(
                title=item.title,
                summary=item.summary,
                source_refs=[ref.as_ref_dict() for ref in refs],
            )
        )

    return UserNoteAutoBlock(
        themes=block.themes,
        notecards=valid_notecards,
        highlights=valid_highlights,
    )


def validate_digest_auto_block(
    block: DailyDigestAutoBlock,
    valid_refs: Iterable[Any],
) -> DailyDigestAutoBlock:
    valid_index = _valid_ref_index(valid_refs)

    valid_stories: list[Story] = []
    for story in block.stories:
        source_refs = _filter_refs(story.source_refs, valid_index)
        if not source_refs:
            continue
        valid_stories.append(
            Story(
                title=story.title,
                what_happened=story.what_happened,
                why_it_matters=story.why_it_matters,
                source_refs=[ref.as_ref_dict() for ref in source_refs],
                tags=story.tags,
                confidence=story.confidence,
            )
        )

    valid_connections: list[Connection] = []
    for conn in block.connections:
        source_refs = _filter_refs(conn.source_refs, valid_index)
        if not source_refs:
            continue
        valid_connections.append(Connection(insight=conn.insight, source_refs=[ref.as_ref_dict() for ref in source_refs]))

    return DailyDigestAutoBlock(stories=valid_stories, connections=valid_connections)
