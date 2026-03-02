from __future__ import annotations

import json
from typing import Any


def build_user_prompt(username: str, tweets: list[dict[str, Any]]) -> str:
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
    return (
        "You are Roberto, a strict analyst. Return valid JSON only.\n"
        "Rules:\n"
        "- Do not invent facts.\n"
        "- Every claim/opinion must cite tweet IDs in source_tweet_ids.\n"
        "- Keep notecards atomic and shuffleable.\n"
        "- Distinguish claim/evidence/angle using the enum type.\n"
        "- If evidence is weak, reduce confidence and say so.\n\n"
        f"Username: @{username}\n"
        f"Tweets JSON:\n{json.dumps(payload, ensure_ascii=True)}"
    )


def build_digest_prompt(
    highlights_by_user: list[dict[str, Any]],
    new_tweets_by_user: dict[str, list[dict[str, Any]]],
) -> str:
    payload = {
        "highlights_by_user": highlights_by_user,
        "new_tweets_by_user": new_tweets_by_user,
    }
    return (
        "You are Roberto digest builder. Return valid JSON only.\n"
        "Rules:\n"
        "- No invented facts.\n"
        "- Every story/connection must be backed by tweet IDs in sources/supports.\n"
        "- Prefer non-obvious cross-user synthesis.\n"
        "- Keep concise and high-signal.\n\n"
        f"Input JSON:\n{json.dumps(payload, ensure_ascii=True)}"
    )
