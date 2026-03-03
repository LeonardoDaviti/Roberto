from __future__ import annotations

from roberto_app.pipeline.v2 import run_v2
from roberto_app.storage.repo import StorageRepo


def run_build(settings, repo: StorageRepo, llm):
    return run_v2(
        settings,
        repo,
        x_client=None,
        llm=llm,
        from_db_only=True,
    )
