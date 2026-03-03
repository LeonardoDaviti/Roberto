from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import BaseModel

from roberto_app.llm.cache import build_cache_key
from roberto_app.llm.prompts import build_digest_prompt_with_context, build_user_prompt_with_context
from roberto_app.llm.registry import PromptSchemaRegistry
from roberto_app.llm.schemas import DailyDigestAutoBlock, UserNoteAutoBlock
from roberto_app.settings import LLMSettings
from roberto_app.storage.repo import StorageRepo

logger = logging.getLogger(__name__)


def _tweet_cache_id(tweet: dict[str, Any]) -> str | None:
    source_ref = tweet.get("source_ref")
    if isinstance(source_ref, dict):
        provider = str(source_ref.get("provider") or "").strip()
        source_id = str(source_ref.get("source_id") or "").strip()
        if provider and source_id:
            return f"{provider}:{source_id}"
    tweet_id = tweet.get("tweet_id") or tweet.get("id")
    if tweet_id:
        return f"x:{tweet_id}"
    return None


class GeminiSummarizer:
    def __init__(
        self,
        config: LLMSettings,
        repo: StorageRepo,
        api_key: str | None = None,
        *,
        app_settings=None,
    ) -> None:
        self.config = config
        self.repo = repo
        self.api_key = api_key
        self._client = None
        self._registry: PromptSchemaRegistry | None = None
        if app_settings and getattr(app_settings, "v17", None) and app_settings.v17.enabled:
            self._registry = PromptSchemaRegistry(
                base_dir=app_settings.base_dir,
                prompt_pack_version=app_settings.v17.prompt_pack_version,
                schema_pack_version=app_settings.v17.schema_pack_version,
            )
            stamp = self._registry.stamp()
            self.prompt_pack_version = stamp.prompt_pack_version
            self.schema_pack_version = stamp.schema_pack_version
            self.prompt_pack_hash = stamp.prompt_pack_hash
            self.schema_pack_hash = stamp.schema_pack_hash
        else:
            self.prompt_pack_version = None
            self.schema_pack_version = None
            self.prompt_pack_hash = None
            self.schema_pack_hash = None

    def summarize_user(
        self,
        username: str,
        tweets: list[dict[str, Any]],
        *,
        retrieval_context: list[dict[str, Any]] | None = None,
    ) -> UserNoteAutoBlock:
        if not tweets:
            return UserNoteAutoBlock()

        user_template = self._registry.load_prompt("user_summary") if self._registry else None
        prompt = build_user_prompt_with_context(
            username,
            tweets,
            retrieval_context=retrieval_context,
            template=user_template,
        )
        cache_ids = [cache_id for t in tweets if (cache_id := _tweet_cache_id(t))]
        cache_key = build_cache_key(self.config.model, prompt, cache_ids)
        cached = self.repo.get_llm_cache(cache_key)
        if cached:
            return UserNoteAutoBlock.model_validate(cached)

        payload = self._generate_json(prompt, UserNoteAutoBlock, schema_name="user_note_auto_block")
        self.repo.set_llm_cache(cache_key, payload)
        return UserNoteAutoBlock.model_validate(payload)

    def summarize_digest(
        self,
        highlights_by_user: list[dict[str, Any]],
        new_tweets_by_user: dict[str, list[dict[str, Any]]],
        *,
        retrieval_context: list[dict[str, Any]] | None = None,
    ) -> DailyDigestAutoBlock:
        if not highlights_by_user and not new_tweets_by_user:
            return DailyDigestAutoBlock()

        digest_template = self._registry.load_prompt("digest") if self._registry else None
        prompt = build_digest_prompt_with_context(
            highlights_by_user,
            new_tweets_by_user,
            retrieval_context=retrieval_context,
            template=digest_template,
        )
        cache_ids: list[str] = []
        for tweets in new_tweets_by_user.values():
            for tweet in tweets:
                cache_id = _tweet_cache_id(tweet)
                if cache_id:
                    cache_ids.append(cache_id)
        cache_key = build_cache_key(self.config.model, prompt, cache_ids)
        cached = self.repo.get_llm_cache(cache_key)
        if cached:
            return DailyDigestAutoBlock.model_validate(cached)

        payload = self._generate_json(prompt, DailyDigestAutoBlock, schema_name="daily_digest_auto_block")
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

    def _generate_json(
        self,
        prompt: str,
        schema: type[BaseModel],
        *,
        schema_name: str,
    ) -> dict[str, Any]:
        client = self._client_instance()
        generation_config: dict[str, Any] = {
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_output_tokens,
        }
        if self.config.json_mode:
            generation_config["response_mime_type"] = "application/json"
            if self._registry:
                generation_config["response_schema"] = self._registry.load_schema(schema_name, schema)
            else:
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

    def registry_meta(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if self.prompt_pack_version:
            out["prompt_pack_version"] = str(self.prompt_pack_version)
        if self.schema_pack_version:
            out["schema_pack_version"] = str(self.schema_pack_version)
        if self.prompt_pack_hash:
            out["prompt_pack_hash"] = str(self.prompt_pack_hash)
        if self.schema_pack_hash:
            out["schema_pack_hash"] = str(self.schema_pack_hash)
        return out
