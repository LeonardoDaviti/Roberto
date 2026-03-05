from __future__ import annotations

from typing import Any

from roberto_app.llm.probe import classify_probe_error, run_gemini_probe
from roberto_app.settings import LLMSettings


class _FakeUsage:
    def __init__(self, prompt: int, output: int, total: int) -> None:
        self.prompt_token_count = prompt
        self.candidates_token_count = output
        self.total_token_count = total


class _FakeResponse:
    def __init__(self, text: str, *, prompt: int = 0, output: int = 0, total: int = 0) -> None:
        self.text = text
        self.usage_metadata = _FakeUsage(prompt=prompt, output=output, total=total)


class _FakeModelInfo:
    def __init__(self, name: str, supported_actions: list[str] | None = None) -> None:
        self.name = name
        self.supported_actions = supported_actions or ["generateContent"]


class _FakeModels:
    def __init__(self, listed: list[_FakeModelInfo], generated: dict[str, Any]) -> None:
        self._listed = listed
        self._generated = generated

    def list(self):
        return list(self._listed)

    def generate_content(self, **kwargs: Any):
        model = str(kwargs.get("model") or "")
        if model not in self._generated:
            raise RuntimeError(f"404 model not found: {model}")
        value = self._generated[model]
        if isinstance(value, Exception):
            raise value
        return value


class _FakeClient:
    def __init__(self, listed: list[_FakeModelInfo], generated: dict[str, Any]) -> None:
        self.models = _FakeModels(listed, generated)


def test_classify_probe_error_dns() -> None:
    error_type, message = classify_probe_error(RuntimeError("[Errno -3] Temporary failure in name resolution"))
    assert error_type == "dns_resolution_failed"
    assert "name resolution" in message.lower()


def test_run_gemini_probe_combines_configured_and_listed_models() -> None:
    settings = LLMSettings(
        model="gemini-3.1-flash-lite-preview",
        model_fallbacks=["gemini-3.0-flash", "gemini-2.5-flash"],
    )
    listed = [
        _FakeModelInfo("models/gemini-2.5-flash"),
        _FakeModelInfo("models/gemini-2.5-flash-lite"),
        _FakeModelInfo("models/gemini-2.5-pro"),  # filtered: not flash
    ]
    generated = {
        "gemini-3.1-flash-lite-preview": _FakeResponse("ok 3.1", prompt=11, output=4, total=15),
        "gemini-3-flash-preview": RuntimeError("404 model not found"),
        "gemini-2.5-flash": RuntimeError("429 RESOURCE_EXHAUSTED quota"),
        "gemini-2.5-flash-lite": _FakeResponse("ok 2.5 lite", prompt=9, output=3, total=12),
    }
    report = run_gemini_probe(
        config=settings,
        api_key="fake-key",
        prompt="Say hello in one sentence.",
        scope="both",
        client=_FakeClient(listed, generated),
    )

    models = [row.model for row in report.results]
    assert "gemini-3.1-flash-lite-preview" in models
    assert "gemini-3-flash-preview" in models
    assert "gemini-2.5-flash" in models
    assert "gemini-2.5-flash-lite" in models
    assert "gemini-2.5-pro" not in models

    by_model = {row.model: row for row in report.results}
    assert by_model["gemini-3.1-flash-lite-preview"].ok
    assert by_model["gemini-3.1-flash-lite-preview"].total_tokens == 15
    assert not by_model["gemini-3-flash-preview"].ok
    assert by_model["gemini-3-flash-preview"].error_type == "model_not_found"
    assert not by_model["gemini-2.5-flash"].ok
    assert by_model["gemini-2.5-flash"].error_type == "quota_exhausted"
    assert by_model["gemini-2.5-flash-lite"].ok


def test_run_gemini_probe_listed_scope_falls_back_to_configured_when_listing_fails() -> None:
    settings = LLMSettings(
        model="gemini-2.5-flash",
        model_fallbacks=[],
    )

    class _BrokenListModels(_FakeModels):
        def list(self):
            raise RuntimeError("[Errno -3] Temporary failure in name resolution")

    class _BrokenListClient(_FakeClient):
        def __init__(self) -> None:
            self.models = _BrokenListModels(
                listed=[],
                generated={"gemini-2.5-flash": _FakeResponse("ok fallback", prompt=7, output=2, total=9)},
            )

    report = run_gemini_probe(
        config=settings,
        api_key="fake-key",
        prompt="One line.",
        scope="listed",
        client=_BrokenListClient(),
    )

    assert report.list_error is not None
    assert report.list_error[0] == "dns_resolution_failed"
    assert len(report.results) == 1
    assert report.results[0].model == "gemini-2.5-flash"
    assert report.results[0].ok
