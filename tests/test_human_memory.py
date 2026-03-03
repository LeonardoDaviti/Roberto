from __future__ import annotations

from roberto_app.llm.schemas import NoteCard, UserNoteAutoBlock
from roberto_app.pipeline.human_memory import (
    detect_conflict_cards,
    propose_idea_cards,
    select_shuffle_pack,
    week_key_from_iso,
)


def test_propose_idea_cards_stable_ids_and_sources() -> None:
    summary = UserNoteAutoBlock(
        themes=["x"],
        highlights=[],
        notecards=[
            NoteCard(
                type="claim",
                title="Compiler speedup",
                payload="Build a compile cache",
                why_it_matters="Cuts iteration time",
                tags=["infra"],
                source_tweet_ids=["101"],
            )
        ],
    )
    run_a = propose_idea_cards(
        run_id="run-a",
        username="alice",
        summary=summary,
        now_iso="2026-03-03T12:00:00+00:00",
        per_user_limit=6,
    )
    run_b = propose_idea_cards(
        run_id="run-b",
        username="alice",
        summary=summary,
        now_iso="2026-03-03T12:00:00+00:00",
        per_user_limit=6,
    )

    assert len(run_a) == 1
    assert run_a[0]["card_id"] == run_b[0]["card_id"]
    assert run_a[0]["source_refs"] == [{"username": "alice", "tweet_id": "101"}]


def test_detect_conflict_cards_keeps_claims_separate() -> None:
    cards = [
        {
            "card_id": "idea:1",
            "username": "alice",
            "title": "GPU demand rising",
            "hypothesis": "Demand is rising quickly",
            "tags": ["chips"],
            "source_refs": [{"username": "alice", "tweet_id": "11"}],
        },
        {
            "card_id": "idea:2",
            "username": "bob",
            "title": "GPU demand not rising",
            "hypothesis": "Demand is not rising now",
            "tags": ["chips"],
            "source_refs": [{"username": "bob", "tweet_id": "22"}],
        },
    ]

    conflicts = detect_conflict_cards(
        run_id="run-x",
        cards=cards,
        now_iso="2026-03-03T12:00:00+00:00",
    )

    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict["claim_a"]["username"] == "alice"
    assert conflict["claim_b"]["username"] == "bob"
    assert conflict["source_refs"] == [
        {"username": "alice", "tweet_id": "11"},
        {"username": "bob", "tweet_id": "22"},
    ]


def test_shuffle_pack_connections_are_cited() -> None:
    cards = [
        {
            "card_id": "idea:a",
            "username": "alice",
            "idea_type": "essay",
            "title": "Compiler notes",
            "hypothesis": "Compile loop",
            "tags": ["compiler"],
            "source_refs": [{"username": "alice", "tweet_id": "1"}],
        },
        {
            "card_id": "idea:b",
            "username": "bob",
            "idea_type": "product",
            "title": "Infra orchestration",
            "hypothesis": "Orchestration loop",
            "tags": ["infra"],
            "source_refs": [{"username": "bob", "tweet_id": "2"}],
        },
        {
            "card_id": "idea:c",
            "username": "carol",
            "idea_type": "experiment",
            "title": "Agent eval",
            "hypothesis": "Evaluation loop",
            "tags": ["eval"],
            "source_refs": [{"username": "carol", "tweet_id": "3"}],
        },
    ]

    selected, connections = select_shuffle_pack(cards=cards, max_cards=2, connection_count=2)
    assert len(selected) == 2
    for conn in connections:
        assert conn["source_refs"]
        assert all(ref.get("username") and ref.get("tweet_id") for ref in conn["source_refs"])


def test_week_key_from_iso() -> None:
    assert week_key_from_iso("2026-03-03T10:00:00+00:00").startswith("2026-W")
