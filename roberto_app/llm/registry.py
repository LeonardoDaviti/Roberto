from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class PackStamp:
    prompt_pack_version: str
    prompt_pack_hash: str
    schema_pack_version: str
    schema_pack_hash: str

    def to_dict(self) -> dict[str, str]:
        return {
            "prompt_pack_version": self.prompt_pack_version,
            "prompt_pack_hash": self.prompt_pack_hash,
            "schema_pack_version": self.schema_pack_version,
            "schema_pack_hash": self.schema_pack_hash,
        }


class PromptSchemaRegistry:
    def __init__(
        self,
        *,
        base_dir: Path,
        prompt_pack_version: str,
        schema_pack_version: str,
    ) -> None:
        self.base_dir = base_dir
        self.prompt_pack_version = prompt_pack_version
        self.schema_pack_version = schema_pack_version

        self.prompt_pack_dir = self.base_dir / "prompts" / self.prompt_pack_version
        self.schema_pack_dir = self.base_dir / "schemas" / self.schema_pack_version
        self._prompt_manifest = self._load_manifest(self.prompt_pack_dir, self.prompt_pack_version)
        self._schema_manifest = self._load_manifest(self.schema_pack_dir, self.schema_pack_version)

    def _load_manifest(self, pack_dir: Path, expected_version: str) -> dict[str, Any]:
        if not pack_dir.exists():
            raise RuntimeError(f"Pack directory missing: {pack_dir}")
        manifest_path = pack_dir / "manifest.json"
        if not manifest_path.exists():
            raise RuntimeError(f"Pack manifest missing: {manifest_path}")
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_version = str(payload.get("version") or "")
        if manifest_version != expected_version:
            raise RuntimeError(
                f"Pack version mismatch in {manifest_path}: expected {expected_version}, got {manifest_version}"
            )
        files = payload.get("files", {})
        if not isinstance(files, dict) or not files:
            raise RuntimeError(f"Pack manifest has no files: {manifest_path}")
        for rel_path, expected_hash in files.items():
            file_path = pack_dir / str(rel_path)
            if not file_path.exists():
                raise RuntimeError(f"Pack file missing: {file_path}")
            actual_hash = _sha256_path(file_path)
            if actual_hash != str(expected_hash):
                raise RuntimeError(
                    f"Pack hash mismatch for {file_path}; update pack version and manifest before running"
                )
        return payload

    def load_prompt(self, prompt_name: str) -> str:
        path = self.prompt_pack_dir / f"{prompt_name}.md"
        if not path.exists():
            raise RuntimeError(f"Prompt file missing: {path}")
        return path.read_text(encoding="utf-8")

    def load_schema(self, schema_name: str, fallback_model: type[BaseModel]) -> dict[str, Any]:
        path = self.schema_pack_dir / f"{schema_name}.json"
        if not path.exists():
            return fallback_model.model_json_schema()
        return json.loads(path.read_text(encoding="utf-8"))

    def stamp(self) -> PackStamp:
        return PackStamp(
            prompt_pack_version=self.prompt_pack_version,
            prompt_pack_hash=str(self._prompt_manifest.get("pack_hash") or ""),
            schema_pack_version=self.schema_pack_version,
            schema_pack_hash=str(self._schema_manifest.get("pack_hash") or ""),
        )
