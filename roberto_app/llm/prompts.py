from __future__ import annotations

import json
from typing import Any


DEFAULT_USER_TEMPLATE = (
    "You are Roberto, a strict analyst. Return valid JSON only.\n"
    "Rules:\n"
    "- Do not invent facts.\n"
    "- Every claim/opinion must cite tweet IDs in source_tweet_ids.\n"
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
    "- Every story/connection must be backed by tweet IDs in sources/supports.\n"
    "- Prefer non-obvious cross-user synthesis.\n"
    "- Keep concise and high-signal.\n\n"
    "Input JSON:\n{input_json}"
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
            "tweet_id": t.get("tweet_id") or t.get("id"),
            "created_at": t.get("created_at"),
            "text": t.get("text"),
            "metrics": (t.get("json") or {}).get("public_metrics", {}),
            "url": f"https://x.com/{username}/status/{t.get('tweet_id') or t.get('id')}",
        }
        for t in tweets
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
        + "\n\nRetrieved Prior Context (may help continuity; still cite only source tweet IDs from current input):\n"
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
