from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

from roberto_app.settings import LLMSettings


def _status_code(exc: Exception) -> int | None:
    value = getattr(exc, "status_code", None)
    if value is None:
        match = re.match(r"\s*([0-9]{3})\b", str(exc))
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _model_alias(model: str) -> str:
    aliases = {
        "gemini-3.0-flash": "gemini-3-flash-preview",
    }
    return aliases.get(model, model)


def _coerce_model_name(value: Any) -> str:
    name = str(value or "").strip()
    if name.startswith("models/"):
        name = name.split("/", 1)[1]
    return _model_alias(name)


def _extract_usage(response: Any) -> tuple[int | None, int | None, int | None]:
    usage = getattr(response, "usage_metadata", None)
    if usage is None:
        return None, None, None
    prompt = getattr(usage, "prompt_token_count", None)
    output = getattr(usage, "candidates_token_count", None)
    total = getattr(usage, "total_token_count", None)
    try:
        prompt_i = int(prompt) if prompt is not None else None
    except (TypeError, ValueError):
        prompt_i = None
    try:
        output_i = int(output) if output is not None else None
    except (TypeError, ValueError):
        output_i = None
    try:
        total_i = int(total) if total is not None else None
    except (TypeError, ValueError):
        total_i = None
    return prompt_i, output_i, total_i


def classify_probe_error(exc: Exception) -> tuple[str, str]:
    code = _status_code(exc)
    text = str(exc).strip()
    lower = text.lower()
    if "temporary failure in name resolution" in lower or "name resolution" in lower:
        return "dns_resolution_failed", text
    if code in {401, 403} or "api key not valid" in lower or "permission denied" in lower:
        return "auth_error", text
    if code == 404 or ("model" in lower and "not found" in lower):
        return "model_not_found", text
    if code == 429 or "resource_exhausted" in lower or ("quota" in lower and ("limit" in lower or "exceeded" in lower)):
        return "quota_exhausted", text
    if "timeout" in lower or "timed out" in lower:
        return "timeout", text
    if code in {500, 502, 503, 504}:
        return "server_error", text
    if "connection" in lower or "network" in lower or "transport" in lower:
        return "network_error", text
    return "unknown_error", text


def configured_models(config: LLMSettings) -> list[str]:
    out: list[str] = []
    for raw in [config.model, *config.model_fallbacks]:
        name = _coerce_model_name(raw)
        if not name:
            continue
        if name not in out:
            out.append(name)
    return out


def _model_supports_generate(item: Any) -> bool:
    actions = getattr(item, "supported_actions", None)
    if actions is None and isinstance(item, dict):
        actions = item.get("supported_actions")
    if not actions:
        return True
    values = [str(v).lower() for v in actions]
    return any("generatecontent" in v or "generate_content" in v for v in values)


def discover_flash_models(client: Any) -> tuple[list[str], tuple[str, str] | None]:
    try:
        seen: list[str] = []
        for item in client.models.list():
            raw_name = getattr(item, "name", None)
            if raw_name is None and isinstance(item, dict):
                raw_name = item.get("name")
            name = _coerce_model_name(raw_name)
            if not name:
                continue
            lower = name.lower()
            if "gemini" not in lower or "flash" not in lower:
                continue
            if not _model_supports_generate(item):
                continue
            if name not in seen:
                seen.append(name)
        return seen, None
    except Exception as exc:  # noqa: BLE001
        return [], classify_probe_error(exc)


def _probe_models(client: Any, *, models: list[str], prompt: str, max_output_tokens: int) -> list["ModelProbeResult"]:
    try:
        from google.genai import types as genai_types

        config_obj = genai_types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=max(8, int(max_output_tokens)),
        )
    except Exception:  # noqa: BLE001
        config_obj = {"temperature": 0.0, "max_output_tokens": max(8, int(max_output_tokens))}

    results: list[ModelProbeResult] = []
    for model in models:
        started = time.perf_counter()
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=config_obj,
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            text = (getattr(response, "text", None) or "").strip()
            prompt_tokens, output_tokens, total_tokens = _extract_usage(response)
            results.append(
                ModelProbeResult(
                    model=model,
                    ok=True,
                    latency_ms=latency_ms,
                    output_text=text,
                    prompt_tokens=prompt_tokens,
                    output_tokens=output_tokens,
                    total_tokens=total_tokens,
                )
            )
        except Exception as exc:  # noqa: BLE001
            latency_ms = int((time.perf_counter() - started) * 1000)
            error_type, error_message = classify_probe_error(exc)
            results.append(
                ModelProbeResult(
                    model=model,
                    ok=False,
                    latency_ms=latency_ms,
                    error_type=error_type,
                    error_message=error_message,
                )
            )
    return results


@dataclass
class ModelProbeResult:
    model: str
    ok: bool
    latency_ms: int
    output_text: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    prompt_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "ok": self.ok,
            "latency_ms": self.latency_ms,
            "output_text": self.output_text,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "prompt_tokens": self.prompt_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass
class GeminiProbeReport:
    prompt: str
    scope: str
    configured: list[str]
    listed: list[str]
    list_error: tuple[str, str] | None
    results: list[ModelProbeResult]

    @property
    def success_count(self) -> int:
        return sum(1 for row in self.results if row.ok)

    @property
    def failure_count(self) -> int:
        return sum(1 for row in self.results if not row.ok)

    def to_dict(self) -> dict[str, Any]:
        return {
            "prompt": self.prompt,
            "scope": self.scope,
            "configured_models": self.configured,
            "listed_models": self.listed,
            "list_error_type": self.list_error[0] if self.list_error else None,
            "list_error_message": self.list_error[1] if self.list_error else None,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "results": [row.to_dict() for row in self.results],
        }


def run_gemini_probe(
    *,
    config: LLMSettings,
    api_key: str,
    prompt: str,
    scope: str = "both",
    max_output_tokens: int = 64,
    client: Any | None = None,
) -> GeminiProbeReport:
    if scope not in {"configured", "listed", "both"}:
        raise ValueError(f"Invalid scope: {scope}")

    if client is None:
        from google import genai

        client = genai.Client(api_key=api_key)

    configured = configured_models(config)
    listed, list_error = discover_flash_models(client)

    if scope == "configured":
        targets = list(configured)
    elif scope == "listed":
        targets = list(listed) if listed else list(configured)
    else:
        targets = list(configured)
        for model in listed:
            if model not in targets:
                targets.append(model)

    results = _probe_models(
        client,
        models=targets,
        prompt=prompt,
        max_output_tokens=max_output_tokens,
    )
    return GeminiProbeReport(
        prompt=prompt,
        scope=scope,
        configured=configured,
        listed=listed,
        list_error=list_error,
        results=results,
    )
