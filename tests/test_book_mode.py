from __future__ import annotations

from pathlib import Path

import yaml

from roberto_app.llm.schemas import BookChunkAutoBlock, BookNotecard
from roberto_app.pipeline.books import _theme_matches_top, run_book_mode
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


class FailingBookLLM:
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
        raise RuntimeError("429 RESOURCE_EXHAUSTED: daily quota reached")


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
            "theme_notes_enabled": True,
            "theme_notes_max_cards": 240,
            "theme_min_cards_per_run": 1,
            "theme_allow_top_themes_only": False,
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
    note_paths = {str(row["note_path"]) for row in note_rows}
    assert str(note_path) in note_paths
    assert len(note_paths) >= 2

    theme_note = tmp_path / "Books" / "themes" / "power.md"
    assert theme_note.exists()
    theme_content = theme_note.read_text(encoding="utf-8")
    assert "Theme Memory: power" in theme_content
    assert "Sample Book" in theme_content

    repo.close()


def test_run_book_mode_falls_back_to_local_distillation(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    book_path = tmp_path / "fallback.txt"
    book_path.write_text(
        "Power appears where incentives align with timing. "
        "Leaders fail when they ignore structure and momentum.",
        encoding="utf-8",
    )

    report = run_book_mode(
        settings,
        repo,
        FailingBookLLM(),
        book_path=book_path,
        title="Fallback Book",
    )

    note_path = Path(report.note_path)
    assert note_path.exists()
    content = note_path.read_text(encoding="utf-8")
    assert "Roberto Book Reading Mode (v26)" in content
    assert "Greene Notecards" in content

    usage = repo.list_llm_query_usage(run_id=report.run_id, limit=50)
    assert usage
    assert any(str(row.get("model")) == "local-fallback" for row in usage)
    assert report.cards_generated >= 1

    repo.close()


def test_run_book_mode_chunk_window_offset_and_limit(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    # Force multiple chunks with deterministic markers so we can assert selected window.
    parts = [f"Section {idx}: " + ("alpha beta gamma " * 40) for idx in range(1, 7)]
    book_path = tmp_path / "window.txt"
    book_path.write_text("\n\n".join(parts), encoding="utf-8")

    report = run_book_mode(
        settings,
        repo,
        FakeBookLLM(),
        book_path=book_path,
        title="Window Book",
        chunk_offset=1,
        chunk_limit=2,
        chunk_chars_override=800,
    )

    assert report.chunks_processed == 2

    content = Path(report.note_path).read_text(encoding="utf-8")
    assert "chunk:0002:p1-1" in content
    assert "chunk:0003:p1-1" in content
    assert "chunk:0001:p1-1" not in content

    repo.close()


def test_run_book_mode_theme_notes_accumulate_across_books(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    settings = load_settings(tmp_path)
    repo = StorageRepo.from_path(settings.resolve("data", "roberto.db"))

    book_a = tmp_path / "book_a.txt"
    book_b = tmp_path / "book_b.txt"
    book_a.write_text("Power and strategy in first book.", encoding="utf-8")
    book_b.write_text("Power and strategy in second book.", encoding="utf-8")

    run_book_mode(settings, repo, FakeBookLLM(), book_path=book_a, title="Book A")
    run_book_mode(settings, repo, FakeBookLLM(), book_path=book_b, title="Book B")

    theme_note = tmp_path / "Books" / "themes" / "power.md"
    assert theme_note.exists()
    content = theme_note.read_text(encoding="utf-8")
    assert "Book A" in content
    assert "Book B" in content

    store_path = tmp_path / "data" / "books" / "themes" / "power.json"
    assert store_path.exists()

    repo.close()


def test_theme_matches_top_allows_token_overlap() -> None:
    top = {"the-nature-of-love", "dialectic-vs-rhetoric"}
    assert _theme_matches_top("love", top)
    assert _theme_matches_top("dialectic", top)
    assert not _theme_matches_top("justice", top)
