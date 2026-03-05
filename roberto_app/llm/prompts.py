from __future__ import annotations

import json
from typing import Any

from roberto_app.sources.refs import x_source_ref


DEFAULT_USER_TEMPLATE = (
    "You are Roberto, a strict analyst. Return valid JSON only.\n"
    "Rules:\n"
    "- Do not invent facts.\n"
    "- Every claim/opinion must cite source_refs from current input.\n"
    "- Keep notecards atomic and shuffleable.\n"
    "- Distinguish claim/evidence/angle using the enum type.\n"
    "- If evidence is weak, reduce confidence and say so.\n\n"
    "Username: @{username}\n"
    "Tweets JSON:\n{tweets_json}"
)

DEFAULT_DIGEST_TEMPLATE = (
    "You are Roberto digest builder. Return valid JSON only.\n"
    "Rules:\n"
    "- No invented facts.\n"
    "- Every story/connection must be backed by source_refs from input data.\n"
    "- Prefer non-obvious cross-user synthesis.\n"
    "- Keep concise and high-signal.\n\n"
    "Input JSON:\n{input_json}"
)

DEFAULT_BOOK_CHUNK_TEMPLATE = (
    "You are Roberto in book-reading mode. Return valid JSON only.\n"
    "Rules:\n"
    "- Distill into Greene-style reusable cards (atomic and strategic).\n"
    "- Do not invent facts beyond the provided chunk.\n"
    "- Every notecard must include source_refs from the provided source_refs list.\n"
    "- Return at most {max_notecards} notecards.\n"
    "- chunk_summary should be substantive (4-8 sentences) and explain key mechanisms, not just conclusions.\n"
    "- Each card summary should be concrete and include at least one textual clue from the chunk.\n"
    "- strategic_use_case must contain actionable tactics (steps, decisions, or warning checks).\n"
    "- example_application must include one practical example scenario; if hypothetical, label it as hypothetical.\n"
    "- Prefer strategic depth over breadth: fewer but richer cards.\n"
    "- tags: max 5 short tags per card.\n"
    "- Never copy long passages; paraphrase unless quote is very short.\n\n"
    "Book title: {book_title}\n"
    "Chunk id: {chunk_id}\n"
    "Page range: {page_range}\n"
    "Allowed source_refs JSON:\n{source_refs_json}\n\n"
    "Chunk text:\n{chunk_text}"
)


def _render_template(template: str, values: dict[str, str]) -> str:
    out = template
    for key, value in values.items():
        out = out.replace("{" + key + "}", value)
    return out


def build_user_prompt(
    username: str,
    tweets: list[dict[str, Any]],
    *,
    template: str | None = None,
) -> str:
    payload = [
        {
            "source_ref": x_source_ref(
                username=username,
                tweet_id=str(t.get("tweet_id") or t.get("id") or ""),
            ),
            "created_at": t.get("created_at"),
            "text": t.get("text"),
            "metrics": (t.get("json") or {}).get("public_metrics", {}),
        }
        for t in tweets
        if (t.get("tweet_id") or t.get("id"))
    ]
    return _render_template(
        template or DEFAULT_USER_TEMPLATE,
        {
            "username": username,
            "tweets_json": json.dumps(payload, ensure_ascii=True),
        },
    )


def build_user_prompt_with_context(
    username: str,
    tweets: list[dict[str, Any]],
    retrieval_context: list[dict[str, Any]] | None = None,
    *,
    template: str | None = None,
) -> str:
    base = build_user_prompt(username, tweets, template=template)
    if not retrieval_context:
        return base
    return (
        base
        + "\n\nRetrieved Prior Context (may help continuity; still cite only source_refs from current input):\n"
        + json.dumps(retrieval_context, ensure_ascii=True)
    )


def build_digest_prompt(
    highlights_by_user: list[dict[str, Any]],
    new_tweets_by_user: dict[str, list[dict[str, Any]]],
    *,
    template: str | None = None,
) -> str:
    payload = {
        "highlights_by_user": highlights_by_user,
        "new_tweets_by_user": new_tweets_by_user,
    }
    return _render_template(
        template or DEFAULT_DIGEST_TEMPLATE,
        {
            "input_json": json.dumps(payload, ensure_ascii=True),
        },
    )


def build_digest_prompt_with_context(
    highlights_by_user: list[dict[str, Any]],
    new_tweets_by_user: dict[str, list[dict[str, Any]]],
    retrieval_context: list[dict[str, Any]] | None = None,
    *,
    template: str | None = None,
) -> str:
    base = build_digest_prompt(
        highlights_by_user,
        new_tweets_by_user,
        template=template,
    )
    if not retrieval_context:
        return base
    return (
        base
        + "\n\nRetrieved Prior Story Context (for continuity only):\n"
        + json.dumps(retrieval_context, ensure_ascii=True)
    )


def build_book_chunk_prompt(
    *,
    book_title: str,
    chunk_id: str,
    page_range: str,
    chunk_text: str,
    source_refs: list[dict[str, Any]],
    max_notecards: int,
    template: str | None = None,
) -> str:
    return _render_template(
        template or DEFAULT_BOOK_CHUNK_TEMPLATE,
        {
            "book_title": book_title,
            "chunk_id": chunk_id,
            "page_range": page_range,
            "source_refs_json": json.dumps(source_refs, ensure_ascii=True),
            "max_notecards": str(max(1, int(max_notecards))),
            "chunk_text": chunk_text,
        },
    )
