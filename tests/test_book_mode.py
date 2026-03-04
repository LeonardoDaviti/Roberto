from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.llm.schemas import BookChunkAutoBlock, BookNotecard
from roberto_app.pipeline.books import run_book_mode
from roberto_app.settings import load_settings
from roberto_app.storage.repo import StorageRepo


class FakeBookLLM:
    def summarize_book_chunk(
        self,
        *,
        run_id: str | None,
        book_title: str,
        chunk_id: str,
        page_range: str,
        chunk_text: str,
        source_refs: list[dict],
        max_notecards: int = 6,
    ) -> BookChunkAutoBlock:
        return BookChunkAutoBlock(
            chunk_summary=f"{book_title} {chunk_id} summary",
            themes=["power", "strategy"],
            notecards=[
                BookNotecard(
                    type="principle",
                    title=f"{book_title} card",
                    summary="Strategic pattern extracted.",
                    strategic_use_case="Use as a framing device for future writing.",
                    tags=["power"],
                    source_refs=source_refs,
                )
            ],
        )


def _write_settings(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "data" / "exports").mkdir(parents=True, exist_ok=True)

    settings = {
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
            "note_timezone": "Asia/Tbilisi",
            "overwrite_mode": "markers_only",
        },
        "pipeline": {
            "v1": {"backfill_count": 100},
            "v2": {"max_new_tweets_per_user": 200, "create_digest_each_run": True},
        },
        "v26": {
            "enabled": True,
            "books_dir": "Books",
            "chunk_chars": 600,
            "max_chunks_per_book": 20,
            "cards_per_chunk": 4,
        },
    }
    (root / "config" / "settings.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")


def test_run_book_mode_writes_books_note_and_source_refs(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    book_path = tmp_path / "sample.txt"
    book_path.write_text(
        "Chapter 1\n"
        "Power depends on timing and leverage.\n\n"
        "Chapter 2\n"
        "Strategy requires long-term positioning and patience.\n",
        encoding="utf-8",
    )

    report = run_book_mode(
        settings,
        repo,
        FakeBookLLM(),
        book_path=book_path,
        title="Sample Book",
    )

    note_path = Path(report.note_path)
    assert note_path.exists()
    assert note_path.parent.name == "Books"
    content = note_path.read_text(encoding="utf-8")
    assert "Roberto Book Reading Mode (v26)" in content
    assert "Greene Notecards" in content
    assert "Sources:" in content
    assert report.cards_generated >= 1

    stats = repo.source_ref_stats()
    providers = {row["provider"]: row["refs"] for row in stats["providers"]}
    assert providers.get("book", 0) >= 1

    note_rows = repo.list_note_index(note_type="book")
    assert len(note_rows) == 1
    assert note_rows[0]["note_path"] == str(note_path)

    repo.close()
