from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CanonicalPost:
    post_id: str
    username: str
    text: str
    created_at: str | None
    user_id: str | None
    display_name: str | None
    raw: dict[str, Any]

    def to_storage_dict(self) -> dict[str, Any]:
        payload = dict(self.raw)
        payload["id"] = self.post_id
        payload["text"] = self.text
        if self.created_at is not None:
            payload["created_at"] = self.created_at
        return payload
