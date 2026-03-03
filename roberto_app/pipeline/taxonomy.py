from __future__ import annotations

from pathlib import Path

import yaml


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    return payload


def load_entity_alias_overrides(settings) -> dict[str, str]:
    path = settings.resolve("config", "entity_alias_overrides.yaml")
    payload = _read_yaml(path)
    rows = payload.get("aliases", {})
    if not isinstance(rows, dict):
        return {}
    out: dict[str, str] = {}
    for alias, canonical in rows.items():
        alias_key = str(alias).strip().lower()
        canonical_value = str(canonical).strip()
        if not alias_key or not canonical_value:
            continue
        out[alias_key] = canonical_value
    return out


def apply_entity_alias_override(name: str, overrides: dict[str, str]) -> str:
    key = name.strip().lower()
    return overrides.get(key, name)


def load_tag_aliases(settings) -> dict[str, str]:
    path = settings.resolve("config", "taxonomy.yaml")
    payload = _read_yaml(path)
    rows = payload.get("tag_aliases", {})
    if not isinstance(rows, dict):
        return {}
    out: dict[str, str] = {}
    for alias, canonical in rows.items():
        alias_key = str(alias).strip().lower()
        canonical_value = str(canonical).strip().lower()
        if not alias_key or not canonical_value:
            continue
        out[alias_key] = canonical_value
    return out


def normalize_tags(tags: list[str], tag_aliases: dict[str, str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for tag in tags:
        key = str(tag).strip().lower()
        if not key:
            continue
        normalized = tag_aliases.get(key, key)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out
