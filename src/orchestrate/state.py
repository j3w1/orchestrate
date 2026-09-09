"""Host-local intentions, delivery journal, evidence, and per-Run locks."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid
from typing import Any

from .errors import OrchestrateError


STATE_SCHEMA = "orchestrate-state/v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def state_home(environment: dict[str, str] | None = None) -> Path:
    env = os.environ if environment is None else environment
    override = env.get("ORCHESTRATE_HOME", "").strip()
    if override:
        return Path(override).resolve()
    if sys.platform == "win32":
        base = env.get("LOCALAPPDATA", "").strip()
        if not base:
            raise OrchestrateError("LOCALAPPDATA is unavailable", code="state_home_unavailable")
        return Path(base) / "orchestrate"
    base = env.get("XDG_STATE_HOME", "").strip()
    return (Path(base) if base else Path.home() / ".local" / "state") / "orchestrate"


def project_key(root: Path) -> str:
    normalized = os.path.normcase(os.fspath(root.resolve())).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


@dataclass(frozen=True, slots=True)
class RunRecord:
    local_id: str
    project_key: str
    native_run_id: str | None
    objective: str
    profile_digest: str
    source_digest: str
    task_id: str | None
    dispatch_id: str | None
    phase: str
    worker_outcome: str | None
    verification_status: str
    delivery_id: str | None


class RunLock(AbstractContextManager["RunLock"]):
    """A non-blocking host-local process lock, distinct from Orca ownership."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._file: Any = None

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+b")
        self._file.seek(0)
        if self._file.tell() == 0:
            self._file.write(b"0")
            self._file.flush()
        try:
            if sys.platform == "win32":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._file.close()
            self._file = None
            raise OrchestrateError(
                "Another controller holds this host-local Run lock",
                code="controller_contention",
                data={"lock": self.path.name},
            ) from exc
        return self

    def __exit__(self, *_: object) -> None:
        if self._file is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None


class StateStore(AbstractContextManager["StateStore"]):
    def __init__(self, root: Path, *, home: Path | None = None) -> None:
        self.root = root.resolve()
        self.home = state_home() if home is None else home.resolve()
        self.project_key = project_key(self.root)
        self.directory = self.home / "projects" / self.project_key
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "state.sqlite3"
        self.connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def __enter__(self) -> "StateStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.close()

    def _migrate(self) -> None:
        existing = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'"
        ).fetchone()
        if existing is not None:
            schema = self.connection.execute("SELECT value FROM meta WHERE key = 'schema'").fetchone()
            if schema is not None and schema["value"] != STATE_SCHEMA:
                raise OrchestrateError(
                    f"Unsupported host-local state schema: {schema['value']}",
                    code="state_schema_unsupported",
                )
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                local_id TEXT PRIMARY KEY,
                project_key TEXT NOT NULL,
                native_run_id TEXT UNIQUE,
                objective TEXT NOT NULL,
                profile_digest TEXT NOT NULL,
                source_digest TEXT NOT NULL,
                task_id TEXT,
                dispatch_id TEXT,
                phase TEXT NOT NULL,
                worker_outcome TEXT,
                verification_status TEXT NOT NULL DEFAULT 'pending',
                delivery_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS intentions (
                id TEXT PRIMARY KEY,
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                operation TEXT NOT NULL,
                arguments_json TEXT NOT NULL,
                request_id TEXT,
                status TEXT NOT NULL,
                response_json TEXT,
                error_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS packets (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                task_id TEXT NOT NULL,
                packet_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_id)
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                delivery_id TEXT NOT NULL,
                response_json TEXT NOT NULL,
                acked INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, delivery_id)
            );
            CREATE TABLE IF NOT EXISTS delivery_messages (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                delivery_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                message_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                effect_status TEXT NOT NULL,
                PRIMARY KEY (run_local_id, delivery_id, message_id)
            );
            CREATE TABLE IF NOT EXISTS questions (
                message_id TEXT PRIMARY KEY,
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                delivery_id TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL,
                answer TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS evidence (
                id TEXT PRIMARY KEY,
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                subject TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self.connection.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema', ?)",
            (STATE_SCHEMA,),
        )

    def lock(self, identity: str) -> RunLock:
        safe = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        return RunLock(self.directory / "locks" / f"{safe}.lock")

    def create_run(self, *, objective: str, profile_digest: str, source_digest: str) -> RunRecord:
        now = utc_now()
        local_id = f"local_{uuid.uuid4().hex}"
        self.connection.execute(
            """INSERT INTO runs(
                local_id, project_key, objective, profile_digest, source_digest,
                phase, verification_status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'preparing', 'pending', ?, ?)""",
            (local_id, self.project_key, objective, profile_digest, source_digest, now, now),
        )
        return self.get_run(local_id)

    def _row_to_run(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(**{field: row[field] for field in RunRecord.__dataclass_fields__})

    def get_run(self, identity: str) -> RunRecord:
        row = self.connection.execute(
            "SELECT * FROM runs WHERE local_id = ? OR native_run_id = ?",
            (identity, identity),
        ).fetchone()
        if row is None:
            raise OrchestrateError(f"Unknown local Run: {identity}", code="run_not_found")
        return self._row_to_run(row)

    def active_runs(self) -> list[RunRecord]:
        terminal = ("worker_succeeded", "worker_failed", "blocked", "completed")
        placeholders = ",".join("?" for _ in terminal)
        rows = self.connection.execute(
            f"SELECT * FROM runs WHERE phase NOT IN ({placeholders}) ORDER BY created_at",
            terminal,
        ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def list_runs(self) -> list[RunRecord]:
        rows = self.connection.execute("SELECT * FROM runs ORDER BY created_at").fetchall()
        return [self._row_to_run(row) for row in rows]

    def select_run(self, identity: str | None) -> RunRecord:
        if identity:
            return self.get_run(identity)
        active = self.active_runs()
        if len(active) == 1:
            return active[0]
        if not active:
            raise OrchestrateError("No resumable local Run exists", code="run_not_found")
        raise OrchestrateError(
            "More than one local Run is active; pass --run explicitly",
            code="run_selection_ambiguous",
            data={"runs": [item.native_run_id or item.local_id for item in active]},
        )

    def select_for_read(self, identity: str | None) -> RunRecord:
        if identity:
            return self.get_run(identity)
        rows = self.list_runs()
        if len(rows) == 1:
            return rows[0]
        if not rows:
            raise OrchestrateError("No local Run exists", code="run_not_found")
        raise OrchestrateError(
            "More than one local Run exists; pass --run explicitly",
            code="run_selection_ambiguous",
            data={"runs": [item.native_run_id or item.local_id for item in rows]},
        )

    def update_run(self, local_id: str, **fields: object) -> RunRecord:
        allowed = {
            "native_run_id",
            "task_id",
            "dispatch_id",
            "phase",
            "worker_outcome",
            "verification_status",
            "delivery_id",
            "source_digest",
        }
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"Unsupported run fields: {sorted(invalid)}")
        fields["updated_at"] = utc_now()
        assignments = ", ".join(f"{key} = ?" for key in fields)
        self.connection.execute(
            f"UPDATE runs SET {assignments} WHERE local_id = ?",
            (*fields.values(), local_id),
        )
        return self.get_run(local_id)

    def prepare_intention(self, run_local_id: str, operation: str, arguments: list[str]) -> str:
        intention_id = f"intent_{uuid.uuid4().hex}"
        now = utc_now()
        self.connection.execute(
            """INSERT INTO intentions(
                id, run_local_id, operation, arguments_json, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'prepared', ?, ?)""",
            (intention_id, run_local_id, operation, json.dumps(arguments), now, now),
        )
        return intention_id

    def mark_intention(self, intention_id: str, status: str, *, request_id: str | None = None, response: object = None, error: object = None) -> None:
        self.connection.execute(
            """UPDATE intentions SET status = ?, request_id = COALESCE(?, request_id),
               response_json = ?, error_json = ?, updated_at = ? WHERE id = ?""",
            (
                status,
                request_id,
                json.dumps(response) if response is not None else None,
                json.dumps(error) if error is not None else None,
                utc_now(),
                intention_id,
            ),
        )

    def unsettled_intentions(self, run_local_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM intentions WHERE run_local_id = ? AND status != 'applied' ORDER BY created_at",
            (run_local_id,),
        ).fetchall()

    def save_packet(self, run_local_id: str, task_id: str, packet: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO packets VALUES (?, ?, ?, ?)",
            (run_local_id, task_id, json.dumps(packet, sort_keys=True), utc_now()),
        )

    def get_packet(self, run_local_id: str, task_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT packet_json FROM packets WHERE run_local_id = ? AND task_id = ?",
            (run_local_id, task_id),
        ).fetchone()
        if row is None:
            raise OrchestrateError("No packet is stored for that exact Task", code="packet_not_found")
        return json.loads(row["packet_json"])

    def journal_delivery(self, run_local_id: str, delivery_id: str, response: dict[str, Any], messages: list[dict[str, Any]]) -> None:
        now = utc_now()
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO deliveries VALUES (?, ?, ?, 0, ?)",
                (run_local_id, delivery_id, json.dumps(response, sort_keys=True), now),
            )
            for ordinal, message in enumerate(messages):
                message_id = message.get("id")
                if not isinstance(message_id, str):
                    message_id = f"ordinal-{ordinal}-{hashlib.sha256(json.dumps(message, sort_keys=True).encode()).hexdigest()}"
                message_type = message.get("type") if isinstance(message.get("type"), str) else "malformed"
                self.connection.execute(
                    "INSERT OR IGNORE INTO delivery_messages VALUES (?, ?, ?, ?, ?, ?, 'observed')",
                    (run_local_id, delivery_id, message_id, ordinal, message_type, json.dumps(message, sort_keys=True)),
                )

    def messages(self, run_local_id: str, delivery_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM delivery_messages WHERE run_local_id = ? AND delivery_id = ? ORDER BY ordinal",
            (run_local_id, delivery_id),
        ).fetchall()

    def mark_message(self, run_local_id: str, delivery_id: str, message_id: str, status: str) -> None:
        self.connection.execute(
            "UPDATE delivery_messages SET effect_status = ? WHERE run_local_id = ? AND delivery_id = ? AND message_id = ?",
            (status, run_local_id, delivery_id, message_id),
        )

    def save_question(self, *, message_id: str, run_local_id: str, delivery_id: str, body: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO questions VALUES (?, ?, ?, ?, 'pending', NULL, ?)",
            (message_id, run_local_id, delivery_id, body, utc_now()),
        )

    def answer_question(self, message_id: str, answer: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM questions WHERE message_id = ?", (message_id,)).fetchone()
        if row is None:
            raise OrchestrateError(f"Unknown question: {message_id}", code="question_not_found")
        if row["status"] == "answered" and row["answer"] != answer:
            raise OrchestrateError("Question was already answered with different text", code="question_already_answered")
        self.connection.execute(
            "UPDATE questions SET status = 'answered', answer = ?, updated_at = ? WHERE message_id = ?",
            (answer, utc_now(), message_id),
        )
        return self.connection.execute("SELECT * FROM questions WHERE message_id = ?", (message_id,)).fetchone()

    def pending_questions(self, run_local_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM questions WHERE run_local_id = ? AND status = 'pending' ORDER BY updated_at",
            (run_local_id,),
        ).fetchall()

    def mark_delivery_acked(self, run_local_id: str, delivery_id: str) -> None:
        self.connection.execute(
            "UPDATE deliveries SET acked = 1 WHERE run_local_id = ? AND delivery_id = ?",
            (run_local_id, delivery_id),
        )

    def add_evidence(self, run_local_id: str, *, kind: str, status: str, subject: str, payload: object) -> None:
        self.connection.execute(
            "INSERT INTO evidence VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"evidence_{uuid.uuid4().hex}", run_local_id, kind, status, subject, json.dumps(payload, sort_keys=True), utc_now()),
        )

    def evidence(self, run_local_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT kind, status, subject, payload_json, created_at FROM evidence WHERE run_local_id = ? ORDER BY created_at",
            (run_local_id,),
        ).fetchall()
        return [
            {
                "kind": row["kind"],
                "status": row["status"],
                "subject": row["subject"],
                "payload": json.loads(row["payload_json"]),
                "recordedAt": row["created_at"],
            }
            for row in rows
        ]
