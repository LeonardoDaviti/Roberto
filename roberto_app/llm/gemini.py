from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import BaseModel

from roberto_app.llm.cache import build_cache_key
from roberto_app.llm.prompts import build_digest_prompt, build_user_prompt
from roberto_app.llm.schemas import DailyDigestAutoBlock, UserNoteAutoBlock
from roberto_app.settings import LLMSettings
from roberto_app.storage.repo import StorageRepo

logger = logging.getLogger(__name__)


class GeminiSummarizer:
    def __init__(self, config: LLMSettings, repo: StorageRepo, api_key: str | None = None) -> None:
        self.config = config
        self.repo = repo
        self.api_key = api_key
        self._client = None

    def summarize_user(self, username: str, tweets: list[dict[str, Any]]) -> UserNoteAutoBlock:
        if not tweets:
            return UserNoteAutoBlock()

        prompt = build_user_prompt(username, tweets)
        tweet_ids = [str(t.get("tweet_id") or t.get("id")) for t in tweets if t.get("tweet_id") or t.get("id")]
        cache_key = build_cache_key(self.config.model, prompt, tweet_ids)
        cached = self.repo.get_llm_cache(cache_key)
        if cached:
            return UserNoteAutoBlock.model_validate(cached)

        payload = self._generate_json(prompt, UserNoteAutoBlock)
        self.repo.set_llm_cache(cache_key, payload)
        return UserNoteAutoBlock.model_validate(payload)

    def summarize_digest(
        self,
        highlights_by_user: list[dict[str, Any]],
        new_tweets_by_user: dict[str, list[dict[str, Any]]],
    ) -> DailyDigestAutoBlock:
        if not highlights_by_user and not new_tweets_by_user:
            return DailyDigestAutoBlock()

        prompt = build_digest_prompt(highlights_by_user, new_tweets_by_user)
        tweet_ids: list[str] = []
        for tweets in new_tweets_by_user.values():
            for tweet in tweets:
                if tweet.get("tweet_id"):
                    tweet_ids.append(str(tweet["tweet_id"]))
        cache_key = build_cache_key(self.config.model, prompt, tweet_ids)
        cached = self.repo.get_llm_cache(cache_key)
        if cached:
            return DailyDigestAutoBlock.model_validate(cached)

        payload = self._generate_json(prompt, DailyDigestAutoBlock)
        self.repo.set_llm_cache(cache_key, payload)
        return DailyDigestAutoBlock.model_validate(payload)

    def _client_instance(self):
        if self._client is not None:
            return self._client
        from google import genai

        if self.api_key:
            self._client = genai.Client(api_key=self.api_key)
        else:
            self._client = genai.Client()
        return self._client

    def _generate_json(self, prompt: str, schema: type[BaseModel]) -> dict[str, Any]:
        client = self._client_instance()
        generation_config: dict[str, Any] = {
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_output_tokens,
        }
        if self.config.json_mode:
            generation_config["response_mime_type"] = "application/json"
            generation_config["response_schema"] = schema.model_json_schema()

        response = client.models.generate_content(
            model=self.config.model,
            contents=prompt,
            config=generation_config,
        )

        text = getattr(response, "text", None)
        if not text:
            raise RuntimeError("Gemini returned empty response")

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            logger.error("Gemini returned non-JSON text: %s", text)
            raise RuntimeError("Gemini returned invalid JSON") from exc

        schema.model_validate(parsed)
        return parsed
