from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from roberto_app.pipeline.common import utc_now_iso


@dataclass
class RecoveryState:
    mode: str
    run_id: str
    started_at: str
    completed_users: set[str]
    failed_users: dict[str, str]


class RunJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **payload: Any) -> None:
        row = {
            "ts": utc_now_iso(),
            "event": event,
            "payload": payload,
        }
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, payload: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()


class ReliabilityKernel:
    def __init__(
        self,
        *,
        mode: str,
        journal: RunJournal,
        checkpoint: CheckpointStore,
        resume: bool,
    ) -> None:
        self.mode = mode
        self.journal = journal
        self.checkpoint = checkpoint
        self.resume = resume
        self._state: RecoveryState | None = None

    def start(self, usernames: list[str], run_id_factory) -> RecoveryState:
        cp = self.checkpoint.load()
        if self.resume and cp and cp.get("mode") == self.mode:
            run_id = str(cp["run_id"])
            started_at = str(cp["started_at"])
            completed_users = {str(u) for u in cp.get("completed_users", [])}
            failed_users = {str(k): str(v) for k, v in (cp.get("failed_users") or {}).items()}
            self._state = RecoveryState(
                mode=self.mode,
                run_id=run_id,
                started_at=started_at,
                completed_users=completed_users,
                failed_users=failed_users,
            )
            self.journal.write("run_resumed", mode=self.mode, run_id=run_id, completed=len(completed_users))
            return self._state

        run_id = run_id_factory()
        started_at = utc_now_iso()
        self._state = RecoveryState(
            mode=self.mode,
            run_id=run_id,
            started_at=started_at,
            completed_users=set(),
            failed_users={},
        )
        self._persist(usernames)
        self.journal.write("run_started", mode=self.mode, run_id=run_id, users=len(usernames))
        return self._state

    def should_skip_user(self, username: str) -> bool:
        if self._state is None:
            return False
        return username in self._state.completed_users

    def mark_user_started(self, username: str) -> None:
        self.journal.write("user_started", username=username)

    def mark_user_completed(self, usernames: list[str], username: str) -> None:
        if self._state is None:
            return
        self._state.completed_users.add(username)
        self._state.failed_users.pop(username, None)
        self._persist(usernames)
        self.journal.write("user_completed", username=username)

    def mark_user_failed(self, usernames: list[str], username: str, error: str) -> None:
        if self._state is None:
            return
        self._state.failed_users[username] = error
        self._persist(usernames)
        self.journal.write("user_failed", username=username, error=error)

    def finish(self, users: list[str], *, success: bool) -> None:
        if self._state is None:
            return
        self.journal.write(
            "run_finished",
            mode=self.mode,
            run_id=self._state.run_id,
            success=success,
            completed=len(self._state.completed_users),
            failed=len(self._state.failed_users),
        )
        if success and self._state.completed_users.issuperset(users):
            self.checkpoint.clear()

    @property
    def state(self) -> RecoveryState:
        if self._state is None:
            raise RuntimeError("Reliability kernel not started")
        return self._state

    def _persist(self, users: list[str]) -> None:
        if self._state is None:
            return
        payload = {
            "mode": self._state.mode,
            "run_id": self._state.run_id,
            "started_at": self._state.started_at,
            "updated_at": utc_now_iso(),
            "users": users,
            "completed_users": sorted(self._state.completed_users),
            "failed_users": self._state.failed_users,
        }
        self.checkpoint.save(payload)


def build_reliability_kernel(settings, mode: str, resume: bool) -> ReliabilityKernel:
    logs_dir = settings.resolve("data", "logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = utc_now_iso().replace(":", "").replace("-", "")
    journal = RunJournal(logs_dir / f"journal_{mode}_{timestamp}.jsonl")
    checkpoint = CheckpointStore(logs_dir / f"checkpoint_{mode}.json")
    return ReliabilityKernel(mode=mode, journal=journal, checkpoint=checkpoint, resume=resume)
