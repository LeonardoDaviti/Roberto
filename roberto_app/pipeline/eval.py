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
    baseline_metrics: dict[str, float] | None = None
    regression_deltas: dict[str, float] | None = None
    failures: list[str] | None = None
    fixture_results: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "fixture_path": self.fixture_path,
            "metrics": self.metrics,
            "thresholds": self.thresholds,
            "passed": self.passed,
            "baseline_metrics": self.baseline_metrics,
            "regression_deltas": self.regression_deltas,
            "failures": self.failures or [],
            "fixture_results": self.fixture_results or [],
        }


def _default_fixture_path() -> Path:
    return Path(__file__).with_name("fixtures") / "eval_fixture.json"


def _default_fixture_suite_dir() -> Path:
    return Path(__file__).with_name("fixtures") / "golden"


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


def _contradiction_precision(fixture: dict[str, Any]) -> float:
    predicted = {str(x) for x in fixture.get("predicted_conflict_ids", [])}
    truth = {str(x) for x in fixture.get("true_conflict_ids", [])}
    if not predicted:
        return 1.0
    true_positive = len(predicted & truth)
    return true_positive / len(predicted)


def _thresholds(settings) -> dict[str, float]:
    return {
        "citation_coverage_min": settings.v4.eval.thresholds.citation_coverage_min,
        "invalid_citation_rate_max": settings.v4.eval.thresholds.invalid_citation_rate_max,
        "duplicate_notecard_rate_max": settings.v4.eval.thresholds.duplicate_notecard_rate_max,
        "note_churn_max": settings.v4.eval.thresholds.note_churn_max,
        "story_continuity_score_min": settings.v4.eval.thresholds.story_continuity_score_min,
        "contradiction_precision_min": settings.v17.eval.contradiction_precision_min,
    }


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
    contradiction_precision = _contradiction_precision(fixture)

    metrics = {
        "citation_coverage": round(citation_coverage, 4),
        "invalid_citation_rate": round(invalid_citation_rate, 4),
        "duplicate_notecard_rate": round(duplicate_notecard_rate, 4),
        "note_churn": round(note_churn, 4),
        "story_continuity_score": round(story_continuity_score, 4),
        "contradiction_precision": round(contradiction_precision, 4),
    }

    thresholds = _thresholds(settings)

    passed = (
        metrics["citation_coverage"] >= thresholds["citation_coverage_min"]
        and metrics["invalid_citation_rate"] <= thresholds["invalid_citation_rate_max"]
        and metrics["duplicate_notecard_rate"] <= thresholds["duplicate_notecard_rate_max"]
        and metrics["note_churn"] <= thresholds["note_churn_max"]
        and metrics["story_continuity_score"] >= thresholds["story_continuity_score_min"]
        and metrics["contradiction_precision"] >= thresholds["contradiction_precision_min"]
    )

    return EvalResult(
        fixture_path="inline",
        metrics=metrics,
        thresholds=thresholds,
        passed=passed,
    )


def _evaluate_fixture_file(settings, path: Path) -> EvalResult:
    if not path.exists():
        raise FileNotFoundError(f"Eval fixture not found: {path}")
    fixture = json.loads(path.read_text(encoding="utf-8"))
    result = evaluate_fixture_data(settings, fixture)
    result.fixture_path = str(path)
    return result


def _aggregate_results(settings, suite_label: str, results: list[EvalResult]) -> EvalResult:
    if not results:
        raise ValueError("No eval fixtures found in suite")

    metric_names = sorted(results[0].metrics.keys())
    metrics: dict[str, float] = {}
    for name in metric_names:
        values = [float(row.metrics.get(name, 0.0)) for row in results]
        metrics[name] = round(sum(values) / len(values), 4)

    thresholds = _thresholds(settings)
    passed = all(row.passed for row in results)
    return EvalResult(
        fixture_path=suite_label,
        metrics=metrics,
        thresholds=thresholds,
        passed=passed,
        fixture_results=[
            {
                "fixture_path": row.fixture_path,
                "metrics": row.metrics,
                "passed": row.passed,
            }
            for row in results
        ],
    )


def _regression_failures(current: EvalResult, baseline: EvalResult, epsilon: float = 1e-9) -> tuple[list[str], dict[str, float]]:
    failures: list[str] = []
    deltas: dict[str, float] = {}

    higher_is_better = {"citation_coverage", "story_continuity_score", "contradiction_precision"}
    lower_is_better = {"invalid_citation_rate", "duplicate_notecard_rate", "note_churn"}

    for metric in sorted(set(current.metrics.keys()) & set(baseline.metrics.keys())):
        cur = float(current.metrics[metric])
        base = float(baseline.metrics[metric])
        deltas[metric] = round(cur - base, 4)
        if metric in higher_is_better and cur + epsilon < base:
            failures.append(f"{metric} regressed: current={cur:.4f} baseline={base:.4f}")
        if metric in lower_is_better and cur > base + epsilon:
            failures.append(f"{metric} regressed: current={cur:.4f} baseline={base:.4f}")

    return failures, deltas


def run_eval(
    settings,
    fixture_path: Path | None = None,
    *,
    fixtures_dir: Path | None = None,
    baseline_path: Path | None = None,
) -> EvalResult:
    suite_dir = (fixtures_dir or _default_fixture_suite_dir()).resolve() if fixtures_dir or _default_fixture_suite_dir().exists() else None
    if fixture_path:
        result = _evaluate_fixture_file(settings, fixture_path.resolve())
    elif suite_dir and suite_dir.exists():
        fixture_files = sorted(path for path in suite_dir.glob("*.json") if path.is_file())
        if not fixture_files:
            raise FileNotFoundError(f"No eval fixtures found in directory: {suite_dir}")
        results = [_evaluate_fixture_file(settings, path) for path in fixture_files]
        result = _aggregate_results(settings, f"suite:{suite_dir}", results)
    else:
        result = _evaluate_fixture_file(settings, _default_fixture_path().resolve())

    if not settings.v17.eval.enabled:
        return result

    baseline_candidate = baseline_path
    if baseline_candidate is None and settings.v17.eval.baseline_fixture:
        baseline_candidate = (settings.base_dir / settings.v17.eval.baseline_fixture).resolve()
    if baseline_candidate is None:
        return result

    baseline = _evaluate_fixture_file(settings, baseline_candidate.resolve())
    failures, deltas = _regression_failures(result, baseline)
    result.baseline_metrics = baseline.metrics
    result.regression_deltas = deltas
    merged_failures = list(result.failures or [])
    merged_failures.extend(failures)
    result.failures = merged_failures
    if failures:
        result.passed = False
    return result
