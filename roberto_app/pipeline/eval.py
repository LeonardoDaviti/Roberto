from __future__ import annotations

import json
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from roberto_app.llm.schemas import DailyDigestAutoBlock, UserNoteAutoBlock
from roberto_app.llm.validation import validate_digest_auto_block, validate_user_auto_block
from roberto_app.pipeline.story_memory import slugify_story_title


@dataclass
class EvalResult:
    fixture_path: str
    metrics: dict[str, float]
    thresholds: dict[str, float]
    passed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "fixture_path": self.fixture_path,
            "metrics": self.metrics,
            "thresholds": self.thresholds,
            "passed": self.passed,
        }


def _default_fixture_path() -> Path:
    return Path(__file__).with_name("fixtures") / "eval_fixture.json"


def _user_ref_count(block: UserNoteAutoBlock) -> int:
    total = 0
    for card in block.notecards:
        total += len(card.source_tweet_ids)
    for item in block.highlights:
        total += len(item.source_tweet_ids)
    return total


def _duplicate_notecard_rate(block: UserNoteAutoBlock) -> float:
    if not block.notecards:
        return 0.0
    seen: set[str] = set()
    duplicates = 0
    for card in block.notecards:
        key = f"{card.type}|{card.title.strip().lower()}|{card.payload.strip().lower()}"
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates / len(block.notecards)


def _note_churn(previous_text: str, candidate_text: str) -> float:
    if not previous_text and not candidate_text:
        return 0.0
    ratio = SequenceMatcher(None, previous_text, candidate_text).ratio()
    return 1.0 - ratio


def _story_continuity_score(digest_block: DailyDigestAutoBlock, existing_story_slugs: set[str]) -> float:
    if not digest_block.stories:
        return 1.0
    matched = 0
    for story in digest_block.stories:
        if slugify_story_title(story.title) in existing_story_slugs:
            matched += 1
    return matched / len(digest_block.stories)


def evaluate_fixture_data(settings, fixture: dict[str, Any]) -> EvalResult:
    user_block = UserNoteAutoBlock.model_validate(fixture.get("user_block", {}))
    digest_block = DailyDigestAutoBlock.model_validate(fixture.get("digest_block", {}))

    valid_user_ids = {str(x) for x in fixture.get("valid_user_tweet_ids", [])}
    valid_digest_refs = {
        (str(x.get("username")), str(x.get("tweet_id")))
        for x in fixture.get("valid_digest_refs", [])
        if isinstance(x, dict)
    }

    cleaned_user = validate_user_auto_block(user_block, valid_user_ids)
    cleaned_digest = validate_digest_auto_block(digest_block, valid_digest_refs)

    total_refs = _user_ref_count(user_block)
    kept_refs = _user_ref_count(cleaned_user)
    citation_coverage = (kept_refs / total_refs) if total_refs else 1.0
    invalid_citation_rate = 1.0 - citation_coverage

    duplicate_notecard_rate = _duplicate_notecard_rate(user_block)

    previous_auto = str(fixture.get("previous_auto_block", ""))
    candidate_auto = str(fixture.get("candidate_auto_block", ""))
    note_churn = _note_churn(previous_auto, candidate_auto)

    existing_story_slugs = {str(s) for s in fixture.get("existing_story_slugs", [])}
    story_continuity_score = _story_continuity_score(cleaned_digest, existing_story_slugs)

    metrics = {
        "citation_coverage": round(citation_coverage, 4),
        "invalid_citation_rate": round(invalid_citation_rate, 4),
        "duplicate_notecard_rate": round(duplicate_notecard_rate, 4),
        "note_churn": round(note_churn, 4),
        "story_continuity_score": round(story_continuity_score, 4),
    }

    thresholds = {
        "citation_coverage_min": settings.v4.eval.thresholds.citation_coverage_min,
        "invalid_citation_rate_max": settings.v4.eval.thresholds.invalid_citation_rate_max,
        "duplicate_notecard_rate_max": settings.v4.eval.thresholds.duplicate_notecard_rate_max,
        "note_churn_max": settings.v4.eval.thresholds.note_churn_max,
        "story_continuity_score_min": settings.v4.eval.thresholds.story_continuity_score_min,
    }

    passed = (
        metrics["citation_coverage"] >= thresholds["citation_coverage_min"]
        and metrics["invalid_citation_rate"] <= thresholds["invalid_citation_rate_max"]
        and metrics["duplicate_notecard_rate"] <= thresholds["duplicate_notecard_rate_max"]
        and metrics["note_churn"] <= thresholds["note_churn_max"]
        and metrics["story_continuity_score"] >= thresholds["story_continuity_score_min"]
    )

    return EvalResult(
        fixture_path="inline",
        metrics=metrics,
        thresholds=thresholds,
        passed=passed,
    )


def run_eval(settings, fixture_path: Path | None = None) -> EvalResult:
    path = (fixture_path or _default_fixture_path()).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Eval fixture not found: {path}")

    fixture = json.loads(path.read_text(encoding="utf-8"))
    result = evaluate_fixture_data(settings, fixture)
    result.fixture_path = str(path)
    return result
