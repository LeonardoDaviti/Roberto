from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class XUser(BaseModel):
    id: str
    username: str
    name: str | None = None


class XTweet(BaseModel):
    id: str
    text: str
    created_at: datetime | None = None
    author_id: str | None = None
    conversation_id: str | None = None
    entities: dict[str, Any] | None = None
    public_metrics: dict[str, Any] | None = None
    referenced_tweets: list[dict[str, Any]] | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> "XTweet":
        obj = cls(**payload)
        obj.raw = payload
        return obj

    def created_at_iso(self) -> str | None:
        if self.created_at is None:
            return None
        return self.created_at.isoformat()
