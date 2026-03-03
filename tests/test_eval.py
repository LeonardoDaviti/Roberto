from __future__ import annotations

import json

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
