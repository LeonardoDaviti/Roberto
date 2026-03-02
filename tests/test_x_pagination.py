from __future__ import annotations

from typing import Any

import httpx

from roberto_app.x_api.client import XClient


def test_fetch_user_tweets_uses_since_id_and_pagination() -> None:
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append({"path": request.url.path, "params": dict(request.url.params)})
        token = request.url.params.get("pagination_token")
        if token is None:
            payload = {
                "data": [
                    {"id": "103", "text": "new-3"},
                    {"id": "102", "text": "new-2"},
                ],
                "meta": {"next_token": "page2"},
            }
        else:
            payload = {
                "data": [
                    {"id": "102", "text": "dup-2"},
                    {"id": "101", "text": "new-1"},
                ],
                "meta": {},
            }
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = XClient(
        "token",
        timeout_s=1,
        retry_max_attempts=2,
        backoff_s=[0],
        transport=transport,
    )

    tweets = client.fetch_user_tweets(
        "42",
        since_id="100",
        max_results=100,
        exclude=["replies", "retweets"],
        tweet_fields=["id", "text", "created_at"],
        max_pages=2,
    )

    assert [t.id for t in tweets] == ["103", "102", "101"]
    assert len(calls) == 2
    assert calls[0]["params"]["since_id"] == "100"
    assert calls[1]["params"]["pagination_token"] == "page2"
