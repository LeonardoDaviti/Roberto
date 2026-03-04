from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, computed_field, model_validator

from roberto_app.sources.refs import coerce_source_ref, dedupe_source_refs, source_ref_tweet_id, source_ref_username, x_source_ref


def _to_ref_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, BaseModel):
        return dict(value.model_dump(exclude_none=True))
    if isinstance(value, dict):
        return dict(value)
    return None


def _normalize_source_refs(values: list[Any], *, fallback_username: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in values:
        payload = _to_ref_dict(item)
        if payload is None:
            continue
        normalized = coerce_source_ref(payload, fallback_username=fallback_username)
        if normalized:
            rows.append(normalized)
    return dedupe_source_refs(rows, fallback_username=fallback_username)


class SourceRef(BaseModel):
    provider: str
    source_id: str
    url: str | None = None
    anchor_type: Literal["id", "hash", "dom", "timecode", "chunk"]
    anchor: str
    excerpt_hash: str | None = None
    snapshot_hash: str | None = None
    captured_at: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data: Any) -> Any:
        payload = _to_ref_dict(data)
        if payload is None:
            return data
        return coerce_source_ref(payload) or payload

    @computed_field(return_type=str | None)
    @property
    def tweet_id(self) -> str | None:
        if self.provider != "x":
            return None
        return self.source_id

    @computed_field(return_type=str | None)
    @property
    def username(self) -> str | None:
        if self.provider != "x":
            return None
        return source_ref_username(
            {
                "provider": self.provider,
                "source_id": self.source_id,
                "url": self.url,
                "anchor_type": self.anchor_type,
                "anchor": self.anchor,
            }
        )

    def as_ref_dict(self) -> dict[str, Any]:
        payload = {
            "provider": self.provider,
            "source_id": self.source_id,
            "url": self.url,
            "anchor_type": self.anchor_type,
            "anchor": self.anchor,
            "excerpt_hash": self.excerpt_hash,
            "snapshot_hash": self.snapshot_hash,
            "captured_at": self.captured_at,
        }
        if self.provider == "x":
            payload["tweet_id"] = self.source_id
            username = self.username
            if username:
                payload["username"] = username
        return payload


class StorySource(BaseModel):
    username: str
    tweet_id: str

    def to_source_ref(self) -> dict[str, Any]:
        return x_source_ref(username=self.username, tweet_id=self.tweet_id)


class ConnectionSupport(BaseModel):
    username: str
    tweet_id: str

    def to_source_ref(self) -> dict[str, Any]:
        return x_source_ref(username=self.username, tweet_id=self.tweet_id)


class NoteCard(BaseModel):
    type: Literal["claim", "evidence", "angle"]
    title: str
    payload: str
    why_it_matters: str
    tags: list[str] = Field(default_factory=list)
    source_refs: list[SourceRef] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_refs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        refs: list[Any] = []
        refs.extend(list(data.get("source_refs") or []))
        for tweet_id in list(data.get("source_tweet_ids") or []):
            if tweet_id:
                refs.append(x_source_ref(tweet_id=str(tweet_id)))
        data = dict(data)
        data["source_refs"] = _normalize_source_refs(refs)
        return data

    @computed_field(return_type=list[str])
    @property
    def source_tweet_ids(self) -> list[str]:
        out: list[str] = []
        for ref in self.source_refs:
            ref_dict = ref.as_ref_dict()
            tweet_id = source_ref_tweet_id(ref_dict)
            if tweet_id:
                out.append(tweet_id)
        return out


class Highlight(BaseModel):
    title: str
    summary: str
    source_refs: list[SourceRef] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_refs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        refs: list[Any] = []
        refs.extend(list(data.get("source_refs") or []))
        for tweet_id in list(data.get("source_tweet_ids") or []):
            if tweet_id:
                refs.append(x_source_ref(tweet_id=str(tweet_id)))
        data = dict(data)
        data["source_refs"] = _normalize_source_refs(refs)
        return data

    @computed_field(return_type=list[str])
    @property
    def source_tweet_ids(self) -> list[str]:
        out: list[str] = []
        for ref in self.source_refs:
            ref_dict = ref.as_ref_dict()
            tweet_id = source_ref_tweet_id(ref_dict)
            if tweet_id:
                out.append(tweet_id)
        return out


class UserNoteAutoBlock(BaseModel):
    themes: list[str] = Field(default_factory=list)
    notecards: list[NoteCard] = Field(default_factory=list)
    highlights: list[Highlight] = Field(default_factory=list)


class Story(BaseModel):
    title: str
    what_happened: str
    why_it_matters: str
    source_refs: list[SourceRef] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"]

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_sources(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        refs: list[Any] = []
        refs.extend(list(data.get("source_refs") or []))
        for source in list(data.get("sources") or []):
            source_payload = _to_ref_dict(source)
            if not source_payload:
                continue
            if source_payload.get("provider") or source_payload.get("source_id"):
                refs.append(source_payload)
                continue
            username = str(source_payload.get("username") or "").strip()
            tweet_id = str(source_payload.get("tweet_id") or "").strip()
            if username and tweet_id:
                refs.append(x_source_ref(username=username, tweet_id=tweet_id))
        data = dict(data)
        data["source_refs"] = _normalize_source_refs(refs)
        return data

    @computed_field(return_type=list[StorySource])
    @property
    def sources(self) -> list[StorySource]:
        out: list[StorySource] = []
        for ref in self.source_refs:
            ref_dict = ref.as_ref_dict()
            username = source_ref_username(ref_dict)
            tweet_id = source_ref_tweet_id(ref_dict)
            if username and tweet_id:
                out.append(StorySource(username=username, tweet_id=tweet_id))
        return out


class Connection(BaseModel):
    insight: str
    source_refs: list[SourceRef] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_supports(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        refs: list[Any] = []
        refs.extend(list(data.get("source_refs") or []))
        for support in list(data.get("supports") or []):
            support_payload = _to_ref_dict(support)
            if not support_payload:
                continue
            if support_payload.get("provider") or support_payload.get("source_id"):
                refs.append(support_payload)
                continue
            username = str(support_payload.get("username") or "").strip()
            tweet_id = str(support_payload.get("tweet_id") or "").strip()
            if username and tweet_id:
                refs.append(x_source_ref(username=username, tweet_id=tweet_id))
        data = dict(data)
        data["source_refs"] = _normalize_source_refs(refs)
        return data

    @computed_field(return_type=list[ConnectionSupport])
    @property
    def supports(self) -> list[ConnectionSupport]:
        out: list[ConnectionSupport] = []
        for ref in self.source_refs:
            ref_dict = ref.as_ref_dict()
            username = source_ref_username(ref_dict)
            tweet_id = source_ref_tweet_id(ref_dict)
            if username and tweet_id:
                out.append(ConnectionSupport(username=username, tweet_id=tweet_id))
        return out


class DailyDigestAutoBlock(BaseModel):
    stories: list[Story] = Field(default_factory=list)
    connections: list[Connection] = Field(default_factory=list)


class BookNotecard(BaseModel):
    type: Literal["claim", "evidence", "angle", "principle"]
    title: str
    summary: str
    strategic_use_case: str
    reusable_quote: str | None = None
    tags: list[str] = Field(default_factory=list)
    source_refs: list[SourceRef] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_refs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        refs = _normalize_source_refs(list(data.get("source_refs") or []))
        data = dict(data)
        data["source_refs"] = refs
        return data


class BookChunkAutoBlock(BaseModel):
    chunk_summary: str = ""
    themes: list[str] = Field(default_factory=list)
    notecards: list[BookNotecard] = Field(default_factory=list)
