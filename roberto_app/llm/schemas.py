from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class NoteCard(BaseModel):
    type: Literal["claim", "evidence", "angle"]
    title: str
    payload: str
    why_it_matters: str
    tags: list[str] = Field(default_factory=list)
    source_tweet_ids: list[str] = Field(default_factory=list)


class Highlight(BaseModel):
    title: str
    summary: str
    source_tweet_ids: list[str] = Field(default_factory=list)


class UserNoteAutoBlock(BaseModel):
    themes: list[str] = Field(default_factory=list)
    notecards: list[NoteCard] = Field(default_factory=list)
    highlights: list[Highlight] = Field(default_factory=list)


class StorySource(BaseModel):
    username: str
    tweet_id: str


class Story(BaseModel):
    title: str
    what_happened: str
    why_it_matters: str
    sources: list[StorySource] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"]


class ConnectionSupport(BaseModel):
    username: str
    tweet_id: str


class Connection(BaseModel):
    insight: str
    supports: list[ConnectionSupport] = Field(default_factory=list)


class DailyDigestAutoBlock(BaseModel):
    stories: list[Story] = Field(default_factory=list)
    connections: list[Connection] = Field(default_factory=list)
