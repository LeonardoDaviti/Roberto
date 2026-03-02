from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from .errors import (
    ForbiddenError,
    NotFoundError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
    XAPIError,
)
from .models import XTweet, XUser

logger = logging.getLogger(__name__)


class XClient:
    def __init__(
        self,
        bearer_token: str,
        timeout_s: int = 20,
        retry_max_attempts: int = 5,
        backoff_s: list[int] | None = None,
        base_url: str = "https://api.x.com/2",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.retry_max_attempts = retry_max_attempts
        self.backoff_s = backoff_s or [1, 2, 4, 8, 16]
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout_s,
            headers={"Authorization": f"Bearer {bearer_token}"},
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "XClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def lookup_user(self, username: str) -> XUser:
        payload = self._request("GET", f"/users/by/username/{username}", params={"user.fields": "name,username"})
        data = payload.get("data")
        if not data:
            raise NotFoundError(f"Username not found: {username}")
        return XUser.model_validate(data)

    def fetch_user_tweets(
        self,
        user_id: str,
        *,
        since_id: str | None,
        max_results: int,
        exclude: list[str],
        tweet_fields: list[str],
        max_pages: int = 1,
    ) -> list[XTweet]:
        max_results = max(5, min(100, max_results))
        params: dict[str, str] = {
            "max_results": str(max_results),
            "exclude": ",".join(exclude),
            "tweet.fields": ",".join(tweet_fields),
        }
        if since_id:
            params["since_id"] = since_id

        all_tweets: list[XTweet] = []
        seen_ids: set[str] = set()
        token: str | None = None

        for _ in range(max_pages):
            if token:
                params["pagination_token"] = token
            else:
                params.pop("pagination_token", None)

            payload = self._request("GET", f"/users/{user_id}/tweets", params=params)
            rows = payload.get("data", [])
            for row in rows:
                tweet = XTweet.from_api(row)
                if tweet.id in seen_ids:
                    continue
                seen_ids.add(tweet.id)
                all_tweets.append(tweet)

            token = (payload.get("meta") or {}).get("next_token")
            if not token:
                break

        return all_tweets

    def _request(self, method: str, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        last_error: Exception | None = None

        for attempt in range(self.retry_max_attempts):
            try:
                response = self._client.request(method, path, params=params)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == self.retry_max_attempts - 1:
                    raise XAPIError(f"Transport error while calling X API: {exc}") from exc
                self._sleep_with_backoff(attempt)
                continue

            if response.status_code == 200:
                return response.json()
            if response.status_code == 401:
                raise UnauthorizedError("X API token missing or invalid (401)")
            if response.status_code == 403:
                raise ForbiddenError("X API access forbidden (403): check project access tier")
            if response.status_code == 402:
                raise PaymentRequiredError("X API credits depleted (402): top up or upgrade account access")
            if response.status_code == 404:
                raise NotFoundError(f"X API resource not found: {path}")
            if response.status_code == 429:
                if attempt == self.retry_max_attempts - 1:
                    raise RateLimitError("X API rate limit reached after retries (429)")
                self._sleep_for_rate_limit(response, attempt)
                continue
            if response.status_code >= 500:
                last_error = XAPIError(f"X API server error {response.status_code}: {response.text}")
                if attempt == self.retry_max_attempts - 1:
                    raise last_error
                self._sleep_with_backoff(attempt)
                continue

            raise XAPIError(f"X API error {response.status_code}: {response.text}")

        if last_error:
            raise XAPIError(str(last_error))
        raise XAPIError("Unknown X API error")

    def _sleep_for_rate_limit(self, response: httpx.Response, attempt: int) -> None:
        reset = response.headers.get("x-rate-limit-reset")
        if reset:
            try:
                wait_s = max(1, int(reset) - int(time.time()) + 1)
                logger.warning("Rate limited by X API. Waiting %ss until reset.", wait_s)
                time.sleep(wait_s)
                return
            except ValueError:
                pass
        self._sleep_with_backoff(attempt)

    def _sleep_with_backoff(self, attempt: int) -> None:
        idx = min(attempt, len(self.backoff_s) - 1)
        wait_s = self.backoff_s[idx]
        logger.warning("Retrying X API request in %ss", wait_s)
        time.sleep(wait_s)
