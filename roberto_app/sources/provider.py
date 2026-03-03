from __future__ import annotations

from typing import Protocol

from .models import CanonicalPost, SourceRef


class SourceProvider(Protocol):
    def fetch_latest(self, username: str, limit: int) -> list[CanonicalPost]:
        ...

    def fetch_since(self, username: str, since_id: str | None, limit: int) -> list[CanonicalPost]:
        ...

    def to_source_ref(self, post: CanonicalPost) -> SourceRef:
        ...
