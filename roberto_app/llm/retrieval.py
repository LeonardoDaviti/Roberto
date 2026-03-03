from __future__ import annotations

import hashlib
import json
from typing import Any

from roberto_app.llm.embeddings import cosine_similarity, embed_text
from roberto_app.storage.repo import StorageRepo


class RetrievalContextBuilder:
    def __init__(self, repo: StorageRepo, config) -> None:
        self.repo = repo
        self.config = config

    def user_context(
        self,
        username: str,
        recent_tweets: list[dict[str, Any]],
        *,
        focus_tweet_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.config.enabled or not recent_tweets:
            return []

        focus_tweet_ids = focus_tweet_ids or set()
        query_text = "\n".join(
            t.get("text", "")
            for t in recent_tweets[: min(5, len(recent_tweets))]
            if str(t.get("tweet_id", "")) in focus_tweet_ids
        )
        if not query_text:
            query_text = "\n".join(t.get("text", "") for t in recent_tweets[:5])

        query_vec = embed_text(query_text)
        ranked: list[tuple[float, dict[str, Any]]] = []

        for tweet in recent_tweets:
            tweet_id = str(tweet.get("tweet_id", ""))
            if not tweet_id:
                continue
            text = tweet.get("text", "")
            if not text:
                continue

            self._ensure_embedding("tweet", f"{username}:{tweet_id}", text)
            emb = self.repo.get_embedding("tweet", f"{username}:{tweet_id}")
            if not emb:
                continue
            score = cosine_similarity(query_vec, emb["vector"])
            if tweet_id in focus_tweet_ids:
                score -= 1.0
            ranked.append((score, tweet))

        ranked.sort(key=lambda x: x[0], reverse=True)
        out: list[dict[str, Any]] = []
        for score, tweet in ranked[: self.config.top_k_user_context]:
            out.append(
                {
                    "tweet_id": str(tweet.get("tweet_id")),
                    "created_at": tweet.get("created_at"),
                    "text": self._trim(str(tweet.get("text", ""))),
                    "score": round(float(score), 4),
                }
            )
        return out

    def digest_context(
        self,
        highlights_by_user: list[dict[str, Any]],
        new_tweets_by_user: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        if not self.config.enabled:
            return []

        pieces: list[str] = []
        for item in highlights_by_user:
            for hl in item.get("highlights", []):
                pieces.append(str(hl.get("title", "")))
                pieces.append(str(hl.get("summary", "")))
        for tweets in new_tweets_by_user.values():
            for tweet in tweets[:2]:
                pieces.append(str(tweet.get("text", "")))

        query_text = "\n".join(p for p in pieces if p)
        if not query_text:
            return []

        query_vec = embed_text(query_text)
        stories = self.repo.list_stories(limit=200)
        ranked: list[tuple[float, dict[str, Any]]] = []

        for story in stories:
            story_id = str(story["story_id"])
            summary_json = story.get("summary_json", {})
            text = json.dumps(summary_json, sort_keys=True)
            self._ensure_embedding("story", story_id, text)
            emb = self.repo.get_embedding("story", story_id)
            if not emb:
                continue
            score = cosine_similarity(query_vec, emb["vector"])
            ranked.append((score, story))

        ranked.sort(key=lambda x: x[0], reverse=True)
        out: list[dict[str, Any]] = []
        for score, story in ranked[: self.config.top_k_story_context]:
            out.append(
                {
                    "story_id": story["story_id"],
                    "slug": story["slug"],
                    "title": story["title"],
                    "confidence": story["confidence"],
                    "mention_count": story["mention_count"],
                    "score": round(float(score), 4),
                }
            )
        return out

    def _ensure_embedding(self, kind: str, item_id: str, text: str) -> None:
        emb = self.repo.get_embedding(kind, item_id)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if emb and emb.get("text_hash") == text_hash:
            return
        vec = embed_text(text)
        self.repo.upsert_embedding(kind, item_id, text, vec)

    def _trim(self, text: str) -> str:
        text = " ".join(text.split())
        if len(text) <= self.config.max_context_chars:
            return text
        return text[: self.config.max_context_chars - 1] + "..."
