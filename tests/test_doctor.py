from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.pipeline.doctor import run_doctor
from roberto_app.settings import load_settings


def _write_settings(root: Path, timezone: str = "Asia/Tbilisi") -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "users").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "digests").mkdir(parents=True, exist_ok=True)
    (root / "notes" / "stories").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)
    (root / "data" / "logs").mkdir(parents=True, exist_ok=True)

    (root / "config" / "following.txt").write_text("alice\n", encoding="utf-8")
    cfg = {
        "x": {
            "exclude": ["replies", "retweets"],
            "max_results": 100,
            "tweet_fields": ["id", "text", "created_at"],
            "request_timeout_s": 20,
            "retry": {"max_attempts": 5, "backoff_s": [1, 2, 4, 8, 16]},
        },
        "llm": {
            "provider": "gemini",
            "model": "gemini-flash-latest",
            "temperature": 0.2,
            "max_output_tokens": 4096,
            "thinking_level": "low",
            "json_mode": True,
        },
        "notes": {
            "per_user_note_enabled": True,
            "digest_note_enabled": True,
            "note_timezone": timezone,
            "overwrite_mode": "markers_only",
        },
        "pipeline": {
            "v1": {"backfill_count": 100},
            "v2": {"max_new_tweets_per_user": 200, "create_digest_each_run": True},
        },
        "v4": {
            "retrieval": {
                "enabled": True,
                "top_k_user_context": 5,
                "top_k_story_context": 5,
                "max_context_chars": 320,
            },
            "eval": {
                "enabled": True,
                "thresholds": {
                    "citation_coverage_min": 0.7,
                    "invalid_citation_rate_max": 0.3,
                    "duplicate_notecard_rate_max": 0.5,
                    "note_churn_max": 0.6,
                    "story_continuity_score_min": 0.5,
                },
            },
        },
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(cfg), encoding="utf-8")


def test_doctor_ok_offline(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    report = run_doctor(settings, online=False)
    assert report.ok
    names = {c.name for c in report.checks}
    assert "db_schema" in names
    assert "timezone" in names


def test_doctor_detects_invalid_timezone(tmp_path: Path) -> None:
    _write_settings(tmp_path, timezone="Invalid/Timezone")
    settings = load_settings(tmp_path)
    report = run_doctor(settings, online=False)
    assert not report.ok
    tz_checks = [c for c in report.checks if c.name == "timezone"]
    assert tz_checks and tz_checks[0].status == "error"
