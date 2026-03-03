from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RunReport:
    run_id: str
    mode: str
    started_at: str
    finished_at: str | None = None
    created_notes: list[str] = field(default_factory=list)
    updated_notes: list[str] = field(default_factory=list)
    staged_notes: list[str] = field(default_factory=list)
    approved_notes: list[str] = field(default_factory=list)
    rolled_back_notes: list[str] = field(default_factory=list)
    per_user_new_tweets: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "created_notes": self.created_notes,
            "updated_notes": self.updated_notes,
            "staged_notes": self.staged_notes,
            "approved_notes": self.approved_notes,
            "rolled_back_notes": self.rolled_back_notes,
            "per_user_new_tweets": self.per_user_new_tweets,
        }

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
