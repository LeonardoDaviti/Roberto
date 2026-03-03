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
from roberto_app.llm.validation import validate_digest_auto_block, validate_user_auto_block
from roberto_app.sources.refs import x_source_ref


def test_validate_user_auto_block_filters_unknown_sources() -> None:
    block = UserNoteAutoBlock(
        themes=["t"],
        notecards=[
            NoteCard(
                type="claim",
                title="a",
                payload="p",
                why_it_matters="w",
                tags=[],
                source_tweet_ids=["1", "404"],
            ),
            NoteCard(
                type="evidence",
                title="b",
                payload="p",
                why_it_matters="w",
                tags=[],
                source_tweet_ids=["999"],
            ),
        ],
        highlights=[
            Highlight(title="h1", summary="s", source_tweet_ids=["1"]),
            Highlight(title="h2", summary="s", source_tweet_ids=["x"]),
        ],
    )

    cleaned = validate_user_auto_block(block, {"1"})

    assert len(cleaned.notecards) == 1
    assert cleaned.notecards[0].source_tweet_ids == ["1"]
    assert len(cleaned.highlights) == 1
    assert cleaned.highlights[0].source_tweet_ids == ["1"]


def test_validate_digest_auto_block_filters_unknown_refs() -> None:
    block = DailyDigestAutoBlock(
        stories=[
            Story(
                title="s1",
                what_happened="w",
                why_it_matters="m",
                sources=[StorySource(username="alice", tweet_id="1")],
                tags=[],
                confidence="high",
            ),
            Story(
                title="s2",
                what_happened="w",
                why_it_matters="m",
                sources=[StorySource(username="alice", tweet_id="404")],
                tags=[],
                confidence="low",
            ),
        ],
        connections=[
            Connection(insight="c1", supports=[ConnectionSupport(username="alice", tweet_id="1")]),
            Connection(insight="c2", supports=[ConnectionSupport(username="bob", tweet_id="999")]),
        ],
    )

    cleaned = validate_digest_auto_block(block, {("alice", "1")})

    assert len(cleaned.stories) == 1
    assert cleaned.stories[0].title == "s1"
    assert len(cleaned.connections) == 1
    assert cleaned.connections[0].insight == "c1"


def test_validate_user_auto_block_with_source_refs_primary() -> None:
    block = UserNoteAutoBlock(
        themes=["t"],
        notecards=[
            NoteCard(
                type="claim",
                title="a",
                payload="p",
                why_it_matters="w",
                tags=[],
                source_refs=[x_source_ref(username="alice", tweet_id="11")],
            ),
            NoteCard(
                type="evidence",
                title="b",
                payload="p",
                why_it_matters="w",
                tags=[],
                source_refs=[x_source_ref(username="alice", tweet_id="22")],
            ),
        ],
        highlights=[
            Highlight(title="h1", summary="s", source_refs=[x_source_ref(username="alice", tweet_id="11")]),
            Highlight(title="h2", summary="s", source_refs=[x_source_ref(username="alice", tweet_id="22")]),
        ],
    )

    cleaned = validate_user_auto_block(block, [x_source_ref(username="alice", tweet_id="11")])

    assert len(cleaned.notecards) == 1
    assert cleaned.notecards[0].source_tweet_ids == ["11"]
    assert len(cleaned.highlights) == 1
    assert cleaned.highlights[0].source_tweet_ids == ["11"]


def test_validate_digest_auto_block_with_source_refs_primary() -> None:
    block = DailyDigestAutoBlock(
        stories=[
            Story(
                title="s1",
                what_happened="w",
                why_it_matters="m",
                source_refs=[x_source_ref(username="alice", tweet_id="11")],
                tags=[],
                confidence="high",
            ),
            Story(
                title="s2",
                what_happened="w",
                why_it_matters="m",
                source_refs=[x_source_ref(username="bob", tweet_id="22")],
                tags=[],
                confidence="low",
            ),
        ],
        connections=[
            Connection(insight="c1", source_refs=[x_source_ref(username="alice", tweet_id="11")]),
            Connection(insight="c2", source_refs=[x_source_ref(username="bob", tweet_id="22")]),
        ],
    )

    cleaned = validate_digest_auto_block(block, [x_source_ref(username="alice", tweet_id="11")])

    assert len(cleaned.stories) == 1
    assert cleaned.stories[0].title == "s1"
    assert len(cleaned.connections) == 1
    assert cleaned.connections[0].insight == "c1"
