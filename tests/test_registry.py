from __future__ import annotations

import json
from pathlib import Path

import pytest

from roberto_app.llm.registry import PromptSchemaRegistry
from roberto_app.llm.schemas import UserNoteAutoBlock
from roberto_app.settings import load_settings


def test_prompt_schema_registry_loads_v1_pack() -> None:
    settings = load_settings(".")
    registry = PromptSchemaRegistry(
        base_dir=settings.base_dir,
        prompt_pack_version=settings.v17.prompt_pack_version,
        schema_pack_version=settings.v17.schema_pack_version,
    )
    prompt = registry.load_prompt("user_summary")
    assert "{username}" in prompt
    schema = registry.load_schema("user_note_auto_block", UserNoteAutoBlock)
    assert isinstance(schema, dict)
    assert schema.get("type") == "object"
    stamp = registry.stamp().to_dict()
    assert stamp["prompt_pack_version"] == "v1"
    assert stamp["schema_pack_version"] == "v1"
    assert stamp["prompt_pack_hash"]
    assert stamp["schema_pack_hash"]


def test_registry_detects_manifest_hash_mismatch(tmp_path: Path) -> None:
    prompts_dir = tmp_path / "prompts" / "v9"
    schemas_dir = tmp_path / "schemas" / "v9"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    schemas_dir.mkdir(parents=True, exist_ok=True)

    (prompts_dir / "user_summary.md").write_text("Prompt {username}", encoding="utf-8")
    (schemas_dir / "user_note_auto_block.json").write_text(
        json.dumps(UserNoteAutoBlock.model_json_schema()),
        encoding="utf-8",
    )

    (prompts_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": "v9",
                "pack_hash": "bad",
                "files": {"user_summary.md": "bad_hash"},
            }
        ),
        encoding="utf-8",
    )
    (schemas_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": "v9",
                "pack_hash": "ok",
                "files": {
                    "user_note_auto_block.json": "x",
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError):
        PromptSchemaRegistry(
            base_dir=tmp_path,
            prompt_pack_version="v9",
            schema_pack_version="v9",
        )
