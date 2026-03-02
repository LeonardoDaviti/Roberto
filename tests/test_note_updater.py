from __future__ import annotations

from pathlib import Path

from roberto_app.notesys.updater import replace_auto_block, update_note_file


def test_replace_auto_block_only_between_markers() -> None:
    original = """---
type: user
username: alice
created_at: 2026-01-01T00:00:00+00:00
updated_at: 2026-01-01T00:00:00+00:00
last_run_id: 2026-01-01T000000Z
---
# @alice - Roberto Notes

## Manual (you write here)
- keep this line

<!-- ROBERTO:AUTO:BEGIN -->
old auto block
<!-- ROBERTO:AUTO:END -->
"""

    new_body = "## Roberto Summary\n- new data"
    updated = replace_auto_block(original, new_body)

    assert "keep this line" in updated
    assert "old auto block" not in updated
    assert "new data" in updated


def test_update_note_file_preserves_manual_section(tmp_path: Path) -> None:
    path = tmp_path / "alice.md"

    first = update_note_file(
        path,
        note_type="user",
        run_id="2026-03-02T120000Z",
        now_iso="2026-03-02T16:00:00+04:00",
        auto_body="## Auto\n- one",
        username="alice",
    )
    assert first.created

    text = path.read_text(encoding="utf-8")
    text = text.replace("- My own hypotheses, links, TODOs...", "- my manual note")
    path.write_text(text, encoding="utf-8")

    second = update_note_file(
        path,
        note_type="user",
        run_id="2026-03-02T130000Z",
        now_iso="2026-03-02T17:00:00+04:00",
        auto_body="## Auto\n- two",
        username="alice",
    )
    assert not second.created
    assert second.updated

    updated_text = path.read_text(encoding="utf-8")
    assert "- my manual note" in updated_text
    assert "- one" not in updated_text
    assert "- two" in updated_text
