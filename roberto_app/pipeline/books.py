from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from roberto_app.llm.schemas import BookChunkAutoBlock, BookNotecard
from roberto_app.notesys.updater import update_note_file
from roberto_app.pipeline.common import local_now_iso, utc_now_iso
from roberto_app.sources.models import SourceRef as SourceRefModel
from roberto_app.sources.models import SourceSnapshot
from roberto_app.sources.refs import dedupe_source_refs, source_ref_markdown
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo

logger = logging.getLogger(__name__)


@dataclass
class BookChunk:
    chunk_index: int
    page_start: int
    page_end: int
    text: str

    @property
    def chunk_id(self) -> str:
        return f"chunk:{self.chunk_index:04d}:p{self.page_start}-{self.page_end}"


@dataclass
class BookRunReport:
    run_id: str
    book_title: str
    source_path: str
    book_id: str
    note_path: str
    created: bool
    updated: bool
    pages_processed: int
    chunks_processed: int
    cards_generated: int
    token_usage: list[dict[str, Any]]
    token_totals: dict[str, int]
    export_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "book_title": self.book_title,
            "source_path": self.source_path,
            "book_id": self.book_id,
            "note_path": self.note_path,
            "created": self.created,
            "updated": self.updated,
            "pages_processed": self.pages_processed,
            "chunks_processed": self.chunks_processed,
            "cards_generated": self.cards_generated,
            "token_usage": self.token_usage,
            "token_totals": self.token_totals,
            "export_path": self.export_path,
        }


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "book"


def _normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    non_empty = [line for line in lines if line]
    return "\n".join(non_empty).strip()


def _read_pdf_pages(path: Path, *, max_pages: int | None = None) -> list[tuple[int, str]]:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("PDF support requires pypdf. Install dependencies with `pip install -e .[dev]`.") from exc

    reader = PdfReader(str(path))
    pages: list[tuple[int, str]] = []
    for idx, page in enumerate(reader.pages, start=1):
        if max_pages is not None and idx > max_pages:
            break
        text = _normalize_text(page.extract_text() or "")
        if text:
            pages.append((idx, text))
    return pages


def _read_text_pages(path: Path) -> list[tuple[int, str]]:
    text = _normalize_text(path.read_text(encoding="utf-8"))
    if not text:
        return []
    return [(1, text)]


def _split_text_to_chunks(text: str, chunk_chars: int) -> list[str]:
    text = _normalize_text(text)
    if not text:
        return []
    if len(text) <= chunk_chars:
        return [text]
    out: list[str] = []
    cursor = 0
    while cursor < len(text):
        end = min(len(text), cursor + chunk_chars)
        if end < len(text):
            pivot = text.rfind("\n", cursor, end)
            if pivot > cursor + int(chunk_chars * 0.6):
                end = pivot
        chunk = _normalize_text(text[cursor:end])
        if chunk:
            out.append(chunk)
        cursor = max(end, cursor + 1)
    return out


def _chunk_pages(
    pages: list[tuple[int, str]],
    *,
    chunk_chars: int,
    max_chunks: int,
) -> list[BookChunk]:
    chunks: list[BookChunk] = []
    current_parts: list[str] = []
    current_start: int | None = None
    current_end: int | None = None

    def flush() -> None:
        nonlocal current_parts, current_start, current_end
        if not current_parts or current_start is None or current_end is None:
            return
        merged = _normalize_text("\n".join(current_parts))
        if merged:
            chunks.append(
                BookChunk(
                    chunk_index=len(chunks) + 1,
                    page_start=current_start,
                    page_end=current_end,
                    text=merged,
                )
            )
        current_parts = []
        current_start = None
        current_end = None

    for page_num, page_text in pages:
        parts = _split_text_to_chunks(page_text, chunk_chars=chunk_chars)
        for part in parts:
            if not current_parts:
                current_parts = [part]
                current_start = page_num
                current_end = page_num
                continue

            proposed = _normalize_text("\n".join(current_parts + [part]))
            if len(proposed) > chunk_chars:
                flush()
                current_parts = [part]
                current_start = page_num
                current_end = page_num
            else:
                current_parts.append(part)
                current_end = page_num

            if len(chunks) >= max_chunks:
                break
        if len(chunks) >= max_chunks:
            break

    if len(chunks) < max_chunks:
        flush()
    return chunks[:max_chunks]


def _load_book_pages(path: Path, *, max_pages: int | None = None) -> list[tuple[int, str]]:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return _read_pdf_pages(path, max_pages=max_pages)
    if ext in {".txt", ".md"}:
        return _read_text_pages(path)
    raise ValueError(f"Unsupported book format: {path.suffix}. Supported: .pdf, .txt, .md")


def _file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clip_words(text: str, limit: int) -> str:
    parts = [part.strip() for part in re.split(r"\s+", text.strip()) if part.strip()]
    if len(parts) <= max(1, limit):
        return " ".join(parts)
    return " ".join(parts[: max(1, limit)]).rstrip(",;:-") + "..."


def _extract_sentences(text: str) -> list[str]:
    normalized = _normalize_text(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[.!?])\s+", normalized)
    out = [part.strip() for part in parts if part and part.strip()]
    return out if out else [normalized]


def _extract_theme_candidates(text: str, *, limit: int = 8) -> list[str]:
    stopwords = {
        "the",
        "and",
        "for",
        "with",
        "that",
        "this",
        "from",
        "have",
        "will",
        "their",
        "they",
        "into",
        "about",
        "your",
        "than",
        "when",
        "were",
        "been",
        "being",
        "would",
        "could",
        "should",
        "there",
        "which",
        "what",
        "where",
        "while",
        "because",
        "these",
        "those",
        "after",
        "before",
        "through",
        "between",
        "against",
        "under",
        "over",
        "only",
        "also",
        "more",
        "most",
        "some",
        "such",
        "very",
        "much",
    }
    counts: Counter[str] = Counter()
    for raw in re.findall(r"[A-Za-z][A-Za-z-]{2,}", text.lower()):
        token = raw.strip("-")
        if not token or token in stopwords:
            continue
        counts[token] += 1
    return [token for token, _ in counts.most_common(max(1, limit))]


def _local_chunk_block(
    *,
    chunk: BookChunk,
    allowed_ref: dict[str, Any],
    max_notecards: int,
) -> BookChunkAutoBlock:
    sentences = _extract_sentences(chunk.text)
    summary = _clip_words(" ".join(sentences[:2]) if sentences else chunk.text, 80)
    themes = _extract_theme_candidates(chunk.text, limit=6)
    tags = themes[:3]

    notecards: list[BookNotecard] = []
    card_types = ["principle", "claim", "evidence", "angle"]
    for idx, sentence in enumerate(sentences[: max(1, max_notecards)]):
        title_words = _clip_words(sentence, 10).rstrip(".")
        if not title_words:
            title_words = f"{chunk.chunk_id} insight {idx + 1}"
        notecards.append(
            BookNotecard(
                type=card_types[idx % len(card_types)],
                title=title_words,
                summary=_clip_words(sentence, 40),
                strategic_use_case=(
                    "Use this as a framing principle when building arguments from this chapter; "
                    "extract the decision rule and test it against adjacent claims."
                ),
                example_application=(
                    "Hypothetical: apply this principle to a team decision memo and stress-test "
                    "tradeoffs before committing."
                ),
                tags=tags,
                source_refs=[allowed_ref],
            )
        )

    return BookChunkAutoBlock(
        chunk_summary=summary,
        themes=themes[:8],
        notecards=notecards[: max(1, max_notecards)],
    )


def _build_source_artifact(
    *,
    book_id: str,
    book_title: str,
    book_path: Path,
    chunk: BookChunk,
    now_iso: str,
) -> tuple[SourceRefModel, SourceSnapshot, dict[str, Any]]:
    excerpt_hash = hashlib.sha256(chunk.text.encode("utf-8")).hexdigest()
    snapshot_payload = {
        "provider": "book",
        "source_id": book_id,
        "anchor": chunk.chunk_id,
        "text": chunk.text,
        "title": book_title,
        "path": str(book_path.resolve()),
        "page_start": chunk.page_start,
        "page_end": chunk.page_end,
    }
    snapshot_hash = hashlib.sha256(json.dumps(snapshot_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    snapshot = SourceSnapshot(
        provider="book",
        source_id=book_id,
        url=str(book_path.resolve()),
        text=chunk.text,
        metadata={
            "title": book_title,
            "path": str(book_path.resolve()),
            "anchor": chunk.chunk_id,
            "page_start": chunk.page_start,
            "page_end": chunk.page_end,
        },
        snapshot_hash=snapshot_hash,
        captured_at=now_iso,
    )
    source_ref = SourceRefModel(
        provider="book",
        source_id=book_id,
        url=str(book_path.resolve()),
        anchor_type="chunk",
        anchor=chunk.chunk_id,
        excerpt_hash=excerpt_hash,
        snapshot_hash=snapshot_hash,
        captured_at=now_iso,
    )
    ref_dict = {
        "provider": source_ref.provider,
        "source_id": source_ref.source_id,
        "url": source_ref.url,
        "anchor_type": source_ref.anchor_type,
        "anchor": source_ref.anchor,
    }
    return source_ref, snapshot, ref_dict


def _render_book_auto_block(
    *,
    book_title: str,
    book_path: Path,
    book_id: str,
    chunks: list[BookChunk],
    chunk_summaries: list[dict[str, Any]],
    themes: list[str],
    notecards: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append("## Roberto Book Reading Mode (v26)")
    lines.append("")
    lines.append(f"- Book: **{book_title}**")
    lines.append(f"- Path: `{book_path.resolve()}`")
    lines.append(f"- Book ID: `{book_id}`")
    lines.append(f"- Pages processed: {max((c.page_end for c in chunks), default=0)}")
    lines.append(f"- Chunks processed: {len(chunks)}")
    lines.append(f"- Notecards generated: {len(notecards)}")
    lines.append("")
    lines.append("### High-Signal Themes")
    if themes:
        for theme in themes:
            lines.append(f"- {theme}")
    else:
        lines.append("- No strong themes extracted.")
    lines.append("")
    lines.append("### Greene Notecards")
    if not notecards:
        lines.append("- No citation-backed cards produced.")
    else:
        for card in notecards:
            lines.append(f"- **[{card['type'].upper()}] {card['title']}**")
            lines.append(f"  - Summary: {card['summary']}")
            lines.append(f"  - Strategic use: {card['strategic_use_case']}")
            example = str(card.get("example_application") or "").strip()
            if example:
                lines.append(f"  - Example: {example}")
            quote = str(card.get("reusable_quote") or "").strip()
            if quote:
                lines.append(f"  - Reusable quote: \"{quote}\"")
            tags = card.get("tags") or []
            lines.append(f"  - Tags: {', '.join(tags) if tags else 'none'}")
            refs = ", ".join(source_ref_markdown(ref) for ref in dedupe_source_refs(list(card.get("source_refs", []))))
            lines.append(f"  - Sources: {refs if refs else 'none'}")
    lines.append("")
    lines.append("### Chunk Summaries")
    if chunk_summaries:
        for item in chunk_summaries:
            lines.append(f"- **{item['chunk_id']}** (pages {item['page_range']})")
            lines.append(f"  - {item['summary']}")
    else:
        lines.append("- No chunk summaries generated.")
    return "\n".join(lines).rstrip()


def run_book_mode(
    settings,
    repo: StorageRepo,
    llm,
    *,
    book_path: Path,
    title: str | None = None,
    max_pages: int | None = None,
    chunk_offset: int = 0,
    chunk_limit: int | None = None,
    chunk_chars_override: int | None = None,
    cards_per_chunk_override: int | None = None,
) -> BookRunReport:
    if not book_path.exists():
        raise FileNotFoundError(f"Book file not found: {book_path}")
    if not book_path.is_file():
        raise ValueError(f"Book path must be a file: {book_path}")
    if not settings.v26.enabled:
        raise RuntimeError("v26 is disabled in config/settings.yaml")

    now_local = local_now_iso(settings.notes.note_timezone)
    now_utc = utc_now_iso()
    run_id = f"book_{now_utc.replace('-', '').replace(':', '').replace('T', '_').replace('Z', 'Z')}"
    book_title = title.strip() if title and title.strip() else book_path.stem.strip()
    file_hash = _file_sha(book_path)
    book_id = f"book:{file_hash[:20]}"
    pages = _load_book_pages(book_path, max_pages=max_pages)
    if not pages:
        raise ValueError(f"No extractable text found in: {book_path}")

    if chunk_offset < 0:
        raise ValueError("chunk_offset must be >= 0")
    if chunk_limit is not None and int(chunk_limit) <= 0:
        raise ValueError("chunk_limit must be > 0 when provided")
    if chunk_chars_override is not None and int(chunk_chars_override) < 800:
        raise ValueError("chunk_chars_override must be >= 800 when provided")
    if cards_per_chunk_override is not None and int(cards_per_chunk_override) <= 0:
        raise ValueError("cards_per_chunk_override must be > 0 when provided")

    effective_chunk_chars = max(800, int(chunk_chars_override or settings.v26.chunk_chars))
    effective_chunk_limit = max(1, int(chunk_limit or settings.v26.max_chunks_per_book))
    prelimit = max(1, int(chunk_offset) + effective_chunk_limit)

    pre_chunks = _chunk_pages(
        pages,
        chunk_chars=effective_chunk_chars,
        max_chunks=prelimit,
    )
    if not pre_chunks:
        raise ValueError("No chunks could be produced from the book text")
    if chunk_offset >= len(pre_chunks):
        raise ValueError(
            f"chunk_offset={chunk_offset} is out of range for available chunks={len(pre_chunks)} "
            f"(with chunk_chars={effective_chunk_chars})"
        )
    chunks = pre_chunks[chunk_offset : chunk_offset + effective_chunk_limit]

    cards: list[dict[str, Any]] = []
    chunk_summaries: list[dict[str, Any]] = []
    theme_counter: Counter[str] = Counter()

    for chunk in chunks:
        source_ref_model, snapshot, allowed_ref = _build_source_artifact(
            book_id=book_id,
            book_title=book_title,
            book_path=book_path,
            chunk=chunk,
            now_iso=now_utc,
        )
        repo.upsert_source_artifact(source_ref_model, snapshot=snapshot)

        max_notecards = max(1, int(cards_per_chunk_override or settings.v26.cards_per_chunk))
        try:
            block = llm.summarize_book_chunk(
                run_id=run_id,
                book_title=book_title,
                chunk_id=chunk.chunk_id,
                page_range=f"{chunk.page_start}-{chunk.page_end}",
                chunk_text=chunk.text,
                source_refs=[allowed_ref],
                max_notecards=max_notecards,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Book chunk %s failed on LLM path (%s). Falling back to deterministic local distillation.",
                chunk.chunk_id,
                exc,
            )
            repo.log_llm_query_usage(
                run_id=run_id,
                query_kind="book_chunk_summary",
                query_ref=chunk.chunk_id,
                model="local-fallback",
                cached=False,
                prompt_chars=len(chunk.text),
                prompt_tokens=0,
                output_tokens=0,
                total_tokens=0,
                created_at=utc_now_iso(),
            )
            block = _local_chunk_block(
                chunk=chunk,
                allowed_ref=allowed_ref,
                max_notecards=max_notecards,
            )

        for theme in block.themes:
            value = str(theme).strip()
            if value:
                theme_counter[value] += 1

        allowed_key = (
            allowed_ref["provider"],
            allowed_ref["source_id"],
            allowed_ref["anchor_type"],
            allowed_ref["anchor"],
        )

        for card in block.notecards[:max_notecards]:
            refs = [
                ref.as_ref_dict()
                for ref in card.source_refs
                if (
                    ref.provider,
                    ref.source_id,
                    ref.anchor_type,
                    ref.anchor,
                )
                == allowed_key
            ]
            if not refs:
                refs = [allowed_ref]
            cards.append(
                {
                    "type": card.type,
                    "title": str(card.title).strip(),
                    "summary": str(card.summary).strip(),
                    "strategic_use_case": str(card.strategic_use_case).strip(),
                    "example_application": str(card.example_application or "").strip(),
                    "tags": [str(tag).strip() for tag in card.tags if str(tag).strip()],
                    "source_refs": refs,
                    "chunk_id": chunk.chunk_id,
                }
            )

        summary = str(block.chunk_summary).strip()
        if summary:
            chunk_summaries.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "page_range": f"{chunk.page_start}-{chunk.page_end}",
                    "summary": summary,
                }
            )

    top_themes = [theme for theme, _ in theme_counter.most_common(12)]
    auto_body = _render_book_auto_block(
        book_title=book_title,
        book_path=book_path,
        book_id=book_id,
        chunks=chunks,
        chunk_summaries=chunk_summaries,
        themes=top_themes,
        notecards=cards,
    )

    books_dir = settings.resolve(settings.v26.books_dir)
    books_dir.mkdir(parents=True, exist_ok=True)
    note_path = books_dir / f"{_slugify(book_title)}.md"
    note_res = update_note_file(
        note_path,
        note_type="book",
        run_id=run_id,
        now_iso=now_local,
        auto_body=auto_body,
        note_title=f"{book_title} - Roberto Book Notes",
    )
    repo.upsert_note_index(
        NoteIndexUpsert(
            note_path=str(note_path),
            note_type="book",
            username=None,
            created_at=note_res.created_at,
            updated_at=note_res.updated_at,
            last_run_id=run_id,
        )
    )

    token_rows = repo.list_llm_query_usage(run_id=run_id, limit=max(200, len(chunks) + 20))
    prompt_total = sum(int(row["prompt_tokens"]) for row in token_rows if row.get("prompt_tokens") is not None)
    output_total = sum(int(row["output_tokens"]) for row in token_rows if row.get("output_tokens") is not None)
    total_total = sum(int(row["total_tokens"]) for row in token_rows if row.get("total_tokens") is not None)
    token_totals = {
        "queries": len(token_rows),
        "cached_queries": sum(1 for row in token_rows if int(row.get("cached") or 0) == 1),
        "prompt_tokens": prompt_total,
        "output_tokens": output_total,
        "total_tokens": total_total,
    }

    exports_dir = settings.resolve("data", "exports")
    exports_dir.mkdir(parents=True, exist_ok=True)
    export_path = exports_dir / f"book_{run_id}.json"
    export_payload = {
        "run_id": run_id,
        "book_title": book_title,
        "source_path": str(book_path.resolve()),
        "book_id": book_id,
        "chunk_offset": int(chunk_offset),
        "chunk_limit": effective_chunk_limit,
        "chunk_chars": effective_chunk_chars,
        "cards_per_chunk": max(1, int(cards_per_chunk_override or settings.v26.cards_per_chunk)),
        "pages_processed": max(page for page, _ in pages),
        "chunks_processed": len(chunks),
        "cards_generated": len(cards),
        "themes": top_themes,
        "token_usage": token_rows,
        "token_totals": token_totals,
        "note_path": str(note_path),
    }
    export_path.write_text(json.dumps(export_payload, indent=2, sort_keys=True), encoding="utf-8")

    return BookRunReport(
        run_id=run_id,
        book_title=book_title,
        source_path=str(book_path.resolve()),
        book_id=book_id,
        note_path=str(note_path),
        created=note_res.created,
        updated=note_res.updated,
        pages_processed=max(page for page, _ in pages),
        chunks_processed=len(chunks),
        cards_generated=len(cards),
        token_usage=token_rows,
        token_totals=token_totals,
        export_path=str(export_path),
    )
