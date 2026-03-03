from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class XRetrySettings(BaseModel):
    max_attempts: int = 5
    backoff_s: list[int] = Field(default_factory=lambda: [1, 2, 4, 8, 16])


class XSettings(BaseModel):
    exclude: list[str] = Field(default_factory=lambda: ["replies", "retweets"])
    max_results: int = 100
    tweet_fields: list[str] = Field(
        default_factory=lambda: [
            "id",
            "text",
            "created_at",
            "author_id",
            "conversation_id",
            "entities",
            "public_metrics",
            "referenced_tweets",
        ]
    )
    request_timeout_s: int = 20
    retry: XRetrySettings = Field(default_factory=XRetrySettings)


class LLMSettings(BaseModel):
    provider: str = "gemini"
    model: str = "gemini-flash-latest"
    temperature: float = 0.2
    max_output_tokens: int = 4096
    thinking_level: str = "low"
    json_mode: bool = True


class NotesSettings(BaseModel):
    per_user_note_enabled: bool = True
    digest_note_enabled: bool = True
    note_timezone: str = "Asia/Tbilisi"
    overwrite_mode: str = "markers_only"


class PipelineV1Settings(BaseModel):
    backfill_count: int = 100


class PipelineV2Settings(BaseModel):
    max_new_tweets_per_user: int = 200
    create_digest_each_run: bool = True


class PipelineSettings(BaseModel):
    v1: PipelineV1Settings = Field(default_factory=PipelineV1Settings)
    v2: PipelineV2Settings = Field(default_factory=PipelineV2Settings)


class V4RetrievalSettings(BaseModel):
    enabled: bool = True
    top_k_user_context: int = 5
    top_k_story_context: int = 5
    max_context_chars: int = 320


class V4EvalThresholds(BaseModel):
    citation_coverage_min: float = 0.7
    invalid_citation_rate_max: float = 0.3
    duplicate_notecard_rate_max: float = 0.5
    note_churn_max: float = 0.6
    story_continuity_score_min: float = 0.5


class V4EvalSettings(BaseModel):
    enabled: bool = True
    thresholds: V4EvalThresholds = Field(default_factory=V4EvalThresholds)


class V4Settings(BaseModel):
    retrieval: V4RetrievalSettings = Field(default_factory=V4RetrievalSettings)
    eval: V4EvalSettings = Field(default_factory=V4EvalSettings)


class AppSettings(BaseModel):
    x: XSettings
    llm: LLMSettings
    notes: NotesSettings
    pipeline: PipelineSettings
    v4: V4Settings = Field(default_factory=V4Settings)
    base_dir: Path
    x_bearer_token: str | None = None
    gemini_api_key: str | None = None
    log_level: str = "INFO"

    def resolve(self, *parts: str) -> Path:
        return self.base_dir.joinpath(*parts)


def load_settings(base_dir: str | Path | None = None) -> AppSettings:
    root = Path(base_dir or Path.cwd()).resolve()
    load_dotenv(root / ".env", override=False)

    settings_path = root / "config" / "settings.yaml"
    if not settings_path.exists():
        raise FileNotFoundError(f"Missing settings file: {settings_path}")

    with settings_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    return AppSettings(
        **raw,
        base_dir=root,
        x_bearer_token=os.getenv("X_BEARER_TOKEN") or os.getenv("BEARER_TOKEN"),
        gemini_api_key=os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def require_x_bearer_token(settings: AppSettings) -> str:
    token = settings.x_bearer_token
    if not token:
        raise RuntimeError("X bearer token missing. Set X_BEARER_TOKEN (or BEARER_TOKEN) in .env")
    return token


def require_gemini_api_key(settings: AppSettings) -> str:
    key = settings.gemini_api_key
    if not key:
        raise RuntimeError("Gemini API key missing. Set GEMINI_API_KEY in .env")
    return key
