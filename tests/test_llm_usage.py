from __future__ import annotations

from pathlib import Path
from typing import Any

from roberto_app.llm.gemini import GeminiSummarizer
from roberto_app.settings import LLMSettings
from roberto_app.storage.repo import StorageRepo


class _FakeUsage:
    def __init__(self, prompt: int, output: int, total: int) -> None:
        self.prompt_token_count = prompt
        self.candidates_token_count = output
        self.total_token_count = total


class _FakeResponse:
    def __init__(self, payload: str, prompt: int, output: int, total: int) -> None:
        self.text = payload
        self.usage_metadata = _FakeUsage(prompt=prompt, output=output, total=total)


class _FakeModels:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)

    def generate_content(self, **kwargs: Any) -> _FakeResponse:
        if not self._responses:
            raise AssertionError("No fake responses left")
        return self._responses.pop(0)


class _FakeClient:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.models = _FakeModels(responses)


class _StubGemini(GeminiSummarizer):
    def __init__(self, repo: StorageRepo, fake_client: _FakeClient) -> None:
        super().__init__(LLMSettings(), repo, api_key=None, app_settings=None)
        self._fake_client = fake_client

    def _client_instance(self):
        return self._fake_client


def test_gemini_logs_token_usage_per_query(tmp_path: Path) -> None:
    repo = StorageRepo.from_path(tmp_path / "roberto.db")
    payload = '{"themes":["t"],"notecards":[],"highlights":[]}'
    fake_client = _FakeClient([_FakeResponse(payload, prompt=44, output=19, total=63)])
    llm = _StubGemini(repo, fake_client)

    tweets = [
        {
            "tweet_id": "101",
            "created_at": "2026-03-01T00:00:00Z",
            "text": "signal",
            "json": {},
        }
    ]

    block = llm.summarize_user("alice", tweets, run_id="run-token-test")
    assert block.themes == ["t"]

    # Second call should use cache and still write usage row.
    block_cached = llm.summarize_user("alice", tweets, run_id="run-token-test")
    assert block_cached.themes == ["t"]

    rows = repo.list_llm_query_usage(run_id="run-token-test", limit=10)
    assert len(rows) == 2

    first = rows[0]
    second = rows[1]
    assert first["query_kind"] == "user_summary"
    assert first["cached"] == 0
    assert first["prompt_tokens"] == 44
    assert first["output_tokens"] == 19
    assert first["total_tokens"] == 63

    assert second["query_kind"] == "user_summary"
    assert second["cached"] == 1
    assert second["prompt_tokens"] is None
    assert second["output_tokens"] is None
    assert second["total_tokens"] is None

    repo.close()
