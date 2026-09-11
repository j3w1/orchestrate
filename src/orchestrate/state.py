"""Host-local intentions, delivery journal, evidence, and per-Run locks."""

from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import uuid
from typing import Any, Iterator

from .errors import OrchestrateError


STATE_SCHEMA = "orchestrate-state/v1"
SYNC_STATE_COMPONENTS = {"box", "dropbox", "google drive", "googledrive", "iclouddrive", "syncthing"}
REQUIRED_STATE_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "meta": frozenset({"key", "value"}),
    "runs": frozenset(
        {
            "local_id",
            "project_key",
            "native_run_id",
            "objective",
            "profile_digest",
            "source_digest",
            "task_id",
            "dispatch_id",
            "phase",
            "worker_outcome",
            "verification_status",
            "delivery_id",
            "created_at",
            "updated_at",
        }
    ),
    "intentions": frozenset(
        {
            "id",
            "run_local_id",
            "operation",
            "arguments_json",
            "request_id",
            "status",
            "returncode",
            "response_json",
            "error_json",
            "created_at",
            "updated_at",
        }
    ),
    "worker_resource_bindings": frozenset(
        {
            "run_local_id",
            "dispatch_id",
            "resource_id",
            "terminal_handle",
            "worktree_id",
            "readback_json",
            "created_at",
        }
    ),
    "packets": frozenset({"run_local_id", "task_id", "packet_json", "created_at"}),
    "preflight_observations": frozenset(
        {
            "run_local_id",
            "run_id",
            "task_id",
            "dispatch_id",
            "outcome",
            "observation_json",
            "created_at",
        }
    ),
    "deliveries": frozenset(
        {"run_local_id", "delivery_id", "response_json", "acked", "created_at"}
    ),
    "delivery_messages": frozenset(
        {
            "run_local_id",
            "delivery_id",
            "message_id",
            "ordinal",
            "message_type",
            "payload_json",
            "effect_status",
        }
    ),
    "questions": frozenset(
        {"message_id", "run_local_id", "delivery_id", "body", "status", "answer", "updated_at"}
    ),
    "evidence": frozenset(
        {"id", "run_local_id", "kind", "status", "subject", "payload_json", "created_at"}
    ),
    "interventions": frozenset(
        {
            "run_local_id",
            "task_key",
            "record_json",
            "correction_key",
            "evidence_digest",
            "correction_count",
            "diagnosis_status",
            "created_at",
            "updated_at",
        }
    ),
    "milestone_task_bindings": frozenset(
        {
            "run_local_id",
            "task_key",
            "task_id",
            "candidate_digest",
            "contract_digest",
            "spec",
            "dependencies_json",
            "created_at",
        }
    ),
    "milestone_plan_bindings": frozenset(
        {
            "run_local_id",
            "relative_path",
            "plan_digest",
            "plan_json",
            "candidate_digest",
            "contract_digest",
            "status",
            "created_at",
            "updated_at",
        }
    ),
    "milestone_gate_bindings": frozenset(
        {
            "run_local_id",
            "task_key",
            "task_id",
            "gate_id",
            "gate_kind",
            "question",
            "status",
            "resolution",
            "created_at",
            "updated_at",
        }
    ),
    "milestone_worker_bindings": frozenset(
        {
            "run_local_id",
            "task_key",
            "task_id",
            "dispatch_id",
            "role",
            "agent",
            "resource_id",
            "terminal_handle",
            "worktree_id",
            "outcome",
            "result_outcome",
            "result_digest",
            "release_state",
            "readback_json",
            "created_at",
            "updated_at",
        }
    ),
}
STATE_TABLES = frozenset(REQUIRED_STATE_TABLE_COLUMNS)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def state_home(environment: dict[str, str] | None = None) -> Path:
    env = os.environ if environment is None else environment
    override = env.get("ORCHESTRATE_HOME", "").strip()
    if override:
        return Path(os.path.abspath(override))
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


def require_private_state_target(project_root: Path, target: Path) -> Path:
    """Reject positively identified project-contained or synchronized state."""

    project = project_root.resolve()
    lexical_project = Path(os.path.abspath(os.fspath(project_root)))
    lexical = Path(os.path.abspath(os.fspath(target)))
    selected = lexical.resolve()
    try:
        inside_project = (
            lexical == lexical_project
            or lexical.is_relative_to(lexical_project)
            or selected == project
            or selected.is_relative_to(project)
        )
    except (OSError, ValueError):
        inside_project = False
    if inside_project:
        raise OrchestrateError(
            "Host-local orchestrate state cannot be stored inside the project tree",
            code="state_storage_unsafe",
        )
    if sys.platform == "win32" and os.fspath(lexical).startswith("\\\\"):
        raise OrchestrateError(
            "Host-local orchestrate state cannot use a UNC network root",
            code="state_storage_unsafe",
        )

    lowered = tuple(part.casefold() for part in selected.parts)
    component_sync = any(
        part in SYNC_STATE_COMPONENTS or part == "onedrive" or part.startswith("onedrive ")
        for part in lowered
    )
    environment_sync = False
    for name in ("OneDrive", "OneDriveCommercial", "OneDriveConsumer"):
        raw = os.environ.get(name, "").strip()
        if not raw:
            continue
        try:
            sync_root = Path(raw).resolve()
            if selected == sync_root or selected.is_relative_to(sync_root):
                environment_sync = True
                break
        except (OSError, ValueError):
            continue
    if component_sync or environment_sync:
        raise OrchestrateError(
            "Host-local orchestrate state cannot use a recognized synchronized-storage root",
            code="state_storage_unsafe",
        )
    return selected


def make_private_state_directory(project_root: Path, target: Path) -> Path:
    directory = require_private_state_target(project_root, target)
    directory.mkdir(parents=True, exist_ok=True)
    try:
        directory.chmod(0o700)
    except OSError:
        pass
    return directory


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


@dataclass(frozen=True, slots=True)
class WorkerResourceBinding:
    run_local_id: str
    dispatch_id: str
    resource_id: str
    terminal_handle: str
    worktree_id: str
    readback_json: str


class RunLock(AbstractContextManager["RunLock"]):
    """A non-blocking host-local process lock, distinct from Orca ownership."""

    def __init__(
        self,
        path: Path,
        *,
        timeout_seconds: float = 0.0,
        contention_code: str = "controller_contention",
        contention_message: str = "Another controller holds this host-local Run lock",
    ) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.contention_code = contention_code
        self.contention_message = contention_message
        self._file: Any = None

    def _acquire(self) -> None:
        if sys.platform == "win32":
            import msvcrt

            self._file.seek(0)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+b")
        self._file.seek(0)
        if self._file.tell() == 0:
            self._file.write(b"0")
            self._file.flush()
        deadline = time.monotonic() + self.timeout_seconds
        try:
            while True:
                try:
                    self._acquire()
                    break
                except OSError as exc:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise OrchestrateError(
                            self.contention_message,
                            code=self.contention_code,
                            data={"lock": self.path.name},
                        ) from exc
                    time.sleep(min(0.01, remaining))
        except BaseException:
            self._file.close()
            self._file = None
            raise
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


class AdmissionEffectFence(RunLock):
    """Bounded Run-scoped ordering for managed admission evidence and effects.

    This is deliberately a separate OS lock from controller ownership. A worker
    may therefore complete its first preflight while implement/resume owns the
    controller Run lock, while a concurrent preflight and admitted controller
    effect cannot cross. The operating system releases the byte-range lock when
    a process exits, including an ungraceful exit.
    """

    def __init__(self, path: Path, *, timeout_seconds: float = 5.0) -> None:
        super().__init__(
            path,
            timeout_seconds=timeout_seconds,
            contention_code="admission_effect_contention",
            contention_message="Timed out waiting for the host-local admission/effect fence",
        )


class StateStore(AbstractContextManager["StateStore"]):
    def __init__(
        self,
        root: Path,
        *,
        home: Path | None = None,
        read_only: bool = False,
    ) -> None:
        self.root = root.resolve()
        self.home = state_home() if home is None else Path(os.path.abspath(os.fspath(home)))
        self.project_key = project_key(self.root)
        target = self.home / "projects" / self.project_key
        self.directory = (
            require_private_state_target(self.root, target)
            if read_only
            else make_private_state_directory(self.root, target)
        )
        self.path = self.directory / "state.sqlite3"
        self.read_only = read_only
        if read_only:
            if not self.path.is_file():
                raise OrchestrateError(
                    "No host-local state database exists for this project",
                    code="state_not_found",
                )
            sidecars = [
                candidate.name
                for suffix in ("-wal", "-shm")
                for candidate in (Path(str(self.path) + suffix),)
                if candidate.exists()
            ]
            if sidecars:
                raise OrchestrateError(
                    "Host-local state has live SQLite sidecars and cannot be opened as an immutable read-only snapshot",
                    code="state_read_snapshot_unavailable",
                    data={"sidecars": sidecars},
                )
            try:
                try:
                    self.connection = sqlite3.connect(
                        self.path.resolve().as_uri() + "?mode=ro&immutable=1",
                        uri=True,
                        timeout=5,
                        isolation_level=None,
                    )
                except sqlite3.DatabaseError as exc:
                    raise OrchestrateError(
                        "Host-local state database is unreadable in read-only mode",
                        code="state_unreadable",
                    ) from exc
                self.connection.row_factory = sqlite3.Row
                self.connection.execute("PRAGMA query_only=ON")
                self._validate_read_schema()
            except BaseException:
                connection = getattr(self, "connection", None)
                if connection is not None:
                    connection.close()
                raise
        else:
            self.connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()

    @classmethod
    def open_read_only(cls, root: Path, *, home: Path | None = None) -> "StateStore":
        """Open existing current-schema state without creating or changing any file."""

        return cls(root, home=home, read_only=True)

    def __enter__(self) -> "StateStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Provide a real transaction while the connection otherwise autocommits."""

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

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
                returncode INTEGER,
                response_json TEXT,
                error_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS worker_resource_bindings (
                run_local_id TEXT PRIMARY KEY REFERENCES runs(local_id),
                dispatch_id TEXT NOT NULL UNIQUE,
                resource_id TEXT NOT NULL,
                terminal_handle TEXT NOT NULL,
                worktree_id TEXT NOT NULL,
                readback_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS packets (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                task_id TEXT NOT NULL,
                packet_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_id)
            );
            CREATE TABLE IF NOT EXISTS preflight_observations (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                run_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                dispatch_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                observation_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_id, dispatch_id)
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
            CREATE TABLE IF NOT EXISTS interventions (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                task_key TEXT NOT NULL,
                record_json TEXT NOT NULL,
                correction_key TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                correction_count INTEGER NOT NULL,
                diagnosis_status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_key)
            );
            CREATE TABLE IF NOT EXISTS milestone_task_bindings (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                task_key TEXT NOT NULL,
                task_id TEXT NOT NULL UNIQUE,
                candidate_digest TEXT NOT NULL,
                contract_digest TEXT NOT NULL,
                spec TEXT NOT NULL,
                dependencies_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_key)
            );
            CREATE TABLE IF NOT EXISTS milestone_plan_bindings (
                run_local_id TEXT PRIMARY KEY REFERENCES runs(local_id),
                relative_path TEXT NOT NULL,
                plan_digest TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                candidate_digest TEXT,
                contract_digest TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS milestone_gate_bindings (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                task_key TEXT NOT NULL,
                task_id TEXT NOT NULL,
                gate_id TEXT NOT NULL UNIQUE,
                gate_kind TEXT NOT NULL,
                question TEXT NOT NULL,
                status TEXT NOT NULL,
                resolution TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_key)
            );
            CREATE TABLE IF NOT EXISTS milestone_worker_bindings (
                run_local_id TEXT NOT NULL REFERENCES runs(local_id),
                task_key TEXT NOT NULL,
                task_id TEXT NOT NULL,
                dispatch_id TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                agent TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                terminal_handle TEXT NOT NULL,
                worktree_id TEXT NOT NULL,
                outcome TEXT,
                result_outcome TEXT,
                result_digest TEXT,
                release_state TEXT NOT NULL,
                readback_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_local_id, task_key)
            );
            """
        )
        intention_columns = {
            row["name"] for row in self.connection.execute("PRAGMA table_info(intentions)").fetchall()
        }
        if "returncode" not in intention_columns:
            self.connection.execute("ALTER TABLE intentions ADD COLUMN returncode INTEGER")
        self.connection.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema', ?)",
            (STATE_SCHEMA,),
        )

    def _validate_read_schema(self) -> None:
        """Reject state that the writable path would have to create or migrate."""

        try:
            tables = {
                row["name"]
                for row in self.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if not STATE_TABLES <= tables:
                raise OrchestrateError(
                    "Host-local state requires schema creation or migration before read-only reporting",
                    code="state_schema_migration_required",
                    data={"missingTables": sorted(STATE_TABLES - tables)},
                )
            for table, required_columns in REQUIRED_STATE_TABLE_COLUMNS.items():
                observed_columns = {
                    row["name"]
                    for row in self.connection.execute(f'PRAGMA table_info("{table}")').fetchall()
                }
                missing_columns = sorted(required_columns - observed_columns)
                if missing_columns:
                    raise OrchestrateError(
                        "Host-local state requires schema migration before read-only reporting",
                        code="state_schema_migration_required",
                        data={"table": table, "missingColumns": missing_columns},
                    )
            schema = self.connection.execute(
                "SELECT value FROM meta WHERE key = 'schema'"
            ).fetchone()
            if schema is None:
                raise OrchestrateError(
                    "Host-local state requires schema metadata migration before read-only reporting",
                    code="state_schema_migration_required",
                )
            if schema["value"] != STATE_SCHEMA:
                raise OrchestrateError(
                    f"Unsupported host-local state schema: {schema['value']}",
                    code="state_schema_unsupported",
                )
        except sqlite3.DatabaseError as exc:
            raise OrchestrateError(
                "Host-local state cannot be read without repair or migration",
                code="state_schema_migration_required",
            ) from exc

    def lock(self, identity: str) -> RunLock:
        safe = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        return RunLock(self.directory / "locks" / f"{safe}.lock")

    def admission_effect_fence(self, identity: str) -> AdmissionEffectFence:
        safe = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        return AdmissionEffectFence(self.directory / "admission-effect-fences" / f"{safe}.lock")

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
        terminal = ("worker_succeeded", "worker_failed", "worker_unadmitted", "completed")
        placeholders = ",".join("?" for _ in terminal)
        rows = self.connection.execute(
            f"""SELECT * FROM runs
                  WHERE phase NOT IN ({placeholders})
                     OR delivery_id IS NOT NULL
                     OR (
                         phase = 'worker_succeeded'
                         AND EXISTS (
                             SELECT 1 FROM milestone_plan_bindings p
                             WHERE p.run_local_id = runs.local_id AND p.status != 'review_accepted'
                         )
                     )
                  ORDER BY created_at""",
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

    def mark_intention(
        self,
        intention_id: str,
        status: str,
        *,
        request_id: str | None = None,
        returncode: int | None = None,
        response: object = None,
        error: object = None,
    ) -> None:
        if returncode is not None and (isinstance(returncode, bool) or not isinstance(returncode, int)):
            raise ValueError("The subprocess return code must be an integer")
        self.connection.execute(
            """UPDATE intentions SET status = ?, request_id = COALESCE(?, request_id),
               returncode = COALESCE(?, returncode), response_json = ?, error_json = ?,
               updated_at = ? WHERE id = ?""",
            (
                status,
                request_id,
                returncode,
                json.dumps(response) if response is not None else None,
                json.dumps(error) if error is not None else None,
                utc_now(),
                intention_id,
            ),
        )

    def record_worker_resource_binding(
        self,
        run_local_id: str,
        *,
        dispatch_id: str,
        resource_id: str,
        terminal_handle: str,
        worktree_id: str,
        readback: object,
    ) -> WorkerResourceBinding:
        values = (dispatch_id, resource_id, terminal_handle, worktree_id)
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError("Worker resource identities must be non-empty strings")
        existing = self.connection.execute(
            "SELECT * FROM worker_resource_bindings WHERE run_local_id = ?",
            (run_local_id,),
        ).fetchone()
        if existing is not None:
            observed = tuple(existing[name] for name in ("dispatch_id", "resource_id", "terminal_handle", "worktree_id"))
            if observed != values:
                raise OrchestrateError(
                    "The worker terminal resource conflicts with its immutable local binding",
                    code="worker_resource_binding_mismatch",
                )
            return WorkerResourceBinding(
                **{field: existing[field] for field in WorkerResourceBinding.__dataclass_fields__}
            )
        encoded = json.dumps(readback, sort_keys=True, separators=(",", ":"))
        self.connection.execute(
            "INSERT INTO worker_resource_bindings VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_local_id, *values, encoded, utc_now()),
        )
        return self.get_worker_resource_binding(run_local_id)

    def get_worker_resource_binding(self, run_local_id: str) -> WorkerResourceBinding | None:
        row = self.connection.execute(
            "SELECT * FROM worker_resource_bindings WHERE run_local_id = ?",
            (run_local_id,),
        ).fetchone()
        if row is None:
            return None
        return WorkerResourceBinding(
            **{field: row[field] for field in WorkerResourceBinding.__dataclass_fields__}
        )

    def unsettled_intentions(self, run_local_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM intentions WHERE run_local_id = ? AND status != 'applied' ORDER BY created_at",
            (run_local_id,),
        ).fetchall()

    def save_packet(self, run_local_id: str, task_id: str, packet_json: str) -> None:
        try:
            packet = json.loads(packet_json)
        except json.JSONDecodeError as exc:
            raise OrchestrateError(
                "The immutable Task packet is not valid JSON",
                code="packet_identity_conflict",
            ) from exc
        if not isinstance(packet, dict):
            raise OrchestrateError(
                "The immutable Task packet must be a JSON object",
                code="packet_identity_conflict",
            )
        existing = self.connection.execute(
            "SELECT packet_json FROM packets WHERE run_local_id = ? AND task_id = ?",
            (run_local_id, task_id),
        ).fetchone()
        if existing is not None:
            legacy_json = json.dumps(packet, sort_keys=True)
            if existing["packet_json"] not in {packet_json, legacy_json}:
                raise OrchestrateError(
                    "The immutable Task packet conflicts with its stored identity",
                    code="packet_identity_conflict",
                )
            # Pre-canonical rows remain immutable. The controller derives canonical
            # bytes in memory and still requires exact native Task-spec readback.
            return
        self.connection.execute(
            "INSERT INTO packets VALUES (?, ?, ?, ?)",
            (run_local_id, task_id, packet_json, utc_now()),
        )

    def get_packet_json(self, run_local_id: str, task_id: str) -> str:
        row = self.connection.execute(
            "SELECT packet_json FROM packets WHERE run_local_id = ? AND task_id = ?",
            (run_local_id, task_id),
        ).fetchone()
        if row is None:
            raise OrchestrateError("No packet is stored for that exact Task", code="packet_not_found")
        return str(row["packet_json"])

    def get_packet(self, run_local_id: str, task_id: str) -> dict[str, Any]:
        try:
            packet = json.loads(self.get_packet_json(run_local_id, task_id))
        except json.JSONDecodeError as exc:
            raise OrchestrateError(
                "The stored immutable Task packet is not valid JSON",
                code="packet_identity_conflict",
            ) from exc
        if not isinstance(packet, dict):
            raise OrchestrateError(
                "The stored immutable Task packet is not a JSON object",
                code="packet_identity_conflict",
            )
        return packet

    def record_preflight(
        self,
        run_local_id: str,
        *,
        run_id: str,
        task_id: str,
        dispatch_id: str,
        observation: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        outcome = observation.get("outcome")
        if outcome not in {"passed", "rejected"}:
            raise ValueError("Preflight outcome must be passed or rejected")
        encoded = json.dumps(observation, sort_keys=True, separators=(",", ":"))
        with self.transaction():
            existing = self.connection.execute(
                """SELECT observation_json FROM preflight_observations
                   WHERE run_local_id = ? AND task_id = ? AND dispatch_id = ?""",
                (run_local_id, task_id, dispatch_id),
            ).fetchone()
            if existing is not None:
                return json.loads(existing["observation_json"]), False
            self.connection.execute(
                "INSERT INTO preflight_observations VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run_local_id, run_id, task_id, dispatch_id, outcome, encoded, utc_now()),
            )
        return observation, True

    @staticmethod
    def _decode_preflight(encoded: object) -> dict[str, Any]:
        try:
            decoded = json.loads(encoded)  # type: ignore[arg-type]
        except (json.JSONDecodeError, TypeError) as exc:
            raise OrchestrateError(
                "Stored preflight observation is malformed",
                code="preflight_identity_conflict",
            ) from exc
        if not isinstance(decoded, dict):
            raise OrchestrateError("Stored preflight observation is malformed", code="preflight_identity_conflict")
        return decoded

    def get_preflight(self, run_local_id: str, task_id: str, dispatch_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT observation_json FROM preflight_observations
               WHERE run_local_id = ? AND task_id = ? AND dispatch_id = ?""",
            (run_local_id, task_id, dispatch_id),
        ).fetchone()
        if row is None:
            return None
        return self._decode_preflight(row["observation_json"])

    def list_preflights(self, run_local_id: str, task_id: str) -> list[dict[str, Any]]:
        """Every immutable preflight observation for one local Run/Task, any Dispatch.

        Rows are read-only evidence: a later controller Dispatch binding never
        hides, overwrites or clears an observation recorded under another
        Dispatch. Malformed rows fail closed instead of being skipped.
        """

        rows = self.connection.execute(
            """SELECT dispatch_id, outcome, observation_json, created_at FROM preflight_observations
               WHERE run_local_id = ? AND task_id = ?
               ORDER BY created_at, dispatch_id""",
            (run_local_id, task_id),
        ).fetchall()
        return [
            {
                "dispatchId": row["dispatch_id"],
                "outcome": row["outcome"],
                "createdAt": row["created_at"],
                "observation": self._decode_preflight(row["observation_json"]),
            }
            for row in rows
        ]

    def journal_delivery(self, run_local_id: str, delivery_id: str, response: dict[str, Any], messages: list[dict[str, Any]]) -> None:
        now = utc_now()
        encoded_response = json.dumps(response, sort_keys=True)
        normalized: list[tuple[str, int, str, str]] = []
        for ordinal, message in enumerate(messages):
            encoded_message = json.dumps(message, sort_keys=True)
            message_id = message.get("id")
            if not isinstance(message_id, str):
                message_id = f"ordinal-{ordinal}-{hashlib.sha256(encoded_message.encode()).hexdigest()}"
            message_type = message.get("type") if isinstance(message.get("type"), str) else "malformed"
            normalized.append((message_id, ordinal, message_type, encoded_message))
        with self.transaction():
            existing = self.connection.execute(
                "SELECT response_json FROM deliveries WHERE run_local_id = ? AND delivery_id = ?",
                (run_local_id, delivery_id),
            ).fetchone()
            if existing is not None:
                existing_messages = self.connection.execute(
                    """SELECT message_id, ordinal, message_type, payload_json FROM delivery_messages
                       WHERE run_local_id = ? AND delivery_id = ? ORDER BY ordinal""",
                    (run_local_id, delivery_id),
                ).fetchall()
                existing_normalized = [
                    (row["message_id"], row["ordinal"], row["message_type"], row["payload_json"])
                    for row in existing_messages
                ]
                if existing["response_json"] != encoded_response or existing_normalized != normalized:
                    raise OrchestrateError(
                        "The immutable FIFO Delivery conflicts with its stored identity",
                        code="delivery_identity_conflict",
                    )
                return
            self.connection.execute(
                "INSERT INTO deliveries VALUES (?, ?, ?, 0, ?)",
                (run_local_id, delivery_id, encoded_response, now),
            )
            for message_id, ordinal, message_type, encoded_message in normalized:
                self.connection.execute(
                    "INSERT INTO delivery_messages VALUES (?, ?, ?, ?, ?, ?, 'observed')",
                    (run_local_id, delivery_id, message_id, ordinal, message_type, encoded_message),
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

    def delivery_messages(self, run_local_id: str, delivery_id: str) -> list[dict[str, Any]]:
        rows = self.messages(run_local_id, delivery_id)
        return [json.loads(row["payload_json"]) for row in rows]

    def finalize_worker_message(
        self,
        *,
        run_local_id: str,
        delivery_id: str,
        message_id: str,
        outcome: str,
        subject: str,
        payload: object,
        admitted: bool = True,
        admission_detail: object = None,
    ) -> RunRecord:
        """Commit evidence, terminal phase, and message effect atomically."""

        evidence_id = "evidence_" + hashlib.sha256(
            f"{run_local_id}\0{delivery_id}\0{message_id}\0worker-claim".encode("utf-8")
        ).hexdigest()
        now = utc_now()
        phase = ("worker_succeeded" if outcome == "succeeded" else "worker_failed") if admitted else "worker_unadmitted"
        verification = "pending" if admitted and outcome == "succeeded" else "not_run"
        with self.transaction():
            self.connection.execute(
                "INSERT OR IGNORE INTO evidence VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    evidence_id,
                    run_local_id,
                    "worker-claim",
                    outcome,
                    subject,
                    json.dumps(payload, sort_keys=True),
                    now,
                ),
            )
            if not admitted:
                admission_id = "evidence_" + hashlib.sha256(
                    f"{run_local_id}\0{delivery_id}\0{message_id}\0admission".encode("utf-8")
                ).hexdigest()
                self.connection.execute(
                    "INSERT OR IGNORE INTO evidence VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        admission_id,
                        run_local_id,
                        "worker-admission",
                        "rejected",
                        "worker completion lacked joined preflight admission",
                        json.dumps(admission_detail, sort_keys=True),
                        now,
                    ),
                )
            self.connection.execute(
                """UPDATE runs SET worker_outcome = ?, phase = ?, verification_status = ?, updated_at = ?
                   WHERE local_id = ?""",
                (outcome, phase, verification, now, run_local_id),
            )
            self.connection.execute(
                """UPDATE delivery_messages SET effect_status = 'processed'
                   WHERE run_local_id = ? AND delivery_id = ? AND message_id = ?""",
                (run_local_id, delivery_id, message_id),
            )
        return self.get_run(run_local_id)

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
        with self.transaction():
            self.connection.execute(
                "UPDATE deliveries SET acked = 1 WHERE run_local_id = ? AND delivery_id = ?",
                (run_local_id, delivery_id),
            )
            self.connection.execute(
                "UPDATE runs SET delivery_id = NULL, updated_at = ? WHERE local_id = ? AND delivery_id = ?",
                (utc_now(), run_local_id, delivery_id),
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
