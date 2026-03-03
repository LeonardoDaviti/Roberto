from __future__ import annotations

import json
from pathlib import Path

from roberto_app.pipeline.eval import evaluate_fixture_data, run_eval
from roberto_app.settings import load_settings


def test_run_eval_default_fixture_passes(tmp_path) -> None:
    # Reuse project config but run from repo root settings.
    settings = load_settings(".")
    result = run_eval(settings)
    assert result.passed
    assert result.metrics["citation_coverage"] >= settings.v4.eval.thresholds.citation_coverage_min


def test_evaluate_fixture_can_fail() -> None:
    settings = load_settings(".")
    fixture = json.loads(
        """
        {
          "user_block": {
            "themes": [],
            "notecards": [
              {
                "type": "claim",
                "title": "x",
                "payload": "x",
                "why_it_matters": "x",
                "tags": [],
                "source_tweet_ids": ["404"]
              }
            ],
            "highlights": []
          },
          "valid_user_tweet_ids": ["100"],
          "digest_block": {"stories": [], "connections": []},
          "valid_digest_refs": [],
          "existing_story_slugs": [],
          "previous_auto_block": "a",
          "candidate_auto_block": "completely different"
        }
        """
    )
    result = evaluate_fixture_data(settings, fixture)
    assert not result.passed
    assert result.metrics["citation_coverage"] < settings.v4.eval.thresholds.citation_coverage_min


def _write_fixture(path: Path, *, valid_ids: list[str], source_ids: list[str]) -> None:
    payload = {
        "user_block": {
            "themes": [],
            "notecards": [
                {
                    "type": "claim",
                    "title": "signal",
                    "payload": "signal",
                    "why_it_matters": "signal",
                    "tags": [],
                    "source_tweet_ids": source_ids,
                }
            ],
            "highlights": [],
        },
        "valid_user_tweet_ids": valid_ids,
        "digest_block": {
            "stories": [
                {
                    "title": "Alpha Story",
                    "what_happened": "x",
                    "why_it_matters": "y",
                    "sources": [{"username": "alice", "tweet_id": "100"}],
                    "tags": ["alpha"],
                    "confidence": "high",
                }
            ],
            "connections": [],
        },
        "valid_digest_refs": [{"username": "alice", "tweet_id": "100"}],
        "existing_story_slugs": ["alpha-story"],
        "predicted_conflict_ids": ["c1"],
        "true_conflict_ids": ["c1"],
        "previous_auto_block": "same",
        "candidate_auto_block": "same",
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_run_eval_regression_gate_fails_with_baseline(tmp_path: Path) -> None:
    settings = load_settings(".")
    current = tmp_path / "current.json"
    baseline = tmp_path / "baseline.json"
    _write_fixture(current, valid_ids=["100", "101"], source_ids=["100", "404"])
    _write_fixture(baseline, valid_ids=["100", "101"], source_ids=["100"])

    result = run_eval(settings, fixture_path=current, baseline_path=baseline)
    assert not result.passed
    assert result.baseline_metrics is not None
    assert result.failures
    assert any("citation_coverage regressed" in msg for msg in result.failures)


def test_run_eval_regression_gate_passes_when_equal(tmp_path: Path) -> None:
    settings = load_settings(".")
    current = tmp_path / "current.json"
    baseline = tmp_path / "baseline.json"
    _write_fixture(current, valid_ids=["100"], source_ids=["100"])
    _write_fixture(baseline, valid_ids=["100"], source_ids=["100"])

    result = run_eval(settings, fixture_path=current, baseline_path=baseline)
    assert result.passed
    assert result.baseline_metrics is not None
