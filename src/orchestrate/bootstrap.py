"""Launch and recover a controller in its own ordinary Orca terminal."""

from __future__ import annotations

import base64
from collections.abc import Mapping
import json
import os
from pathlib import Path
import shlex
import sqlite3
import sys
from typing import Any
import uuid

from .errors import OrchestrateError
from .identity import require_bootstrap_caller
from .orca import OrcaClient, OrcaCommandError
from .state import RunLock, make_private_state_directory, project_key, state_home, utc_now


BOOTSTRAP_SCHEMA = "orchestrate-bootstrap/v1"
BOOTSTRAP_STATE_SCHEMA = "orchestrate-bootstrap-state/v1"


def encode_payload(arguments: list[str]) -> str:
    raw = json.dumps({"schema": BOOTSTRAP_SCHEMA, "arguments": arguments}, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_payload(value: str) -> list[str]:
    try:
        decoded = json.loads(base64.urlsafe_b64decode(value.encode("ascii")))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError("Invalid controller bootstrap payload", code="bootstrap_payload_invalid") from exc
    arguments = decoded.get("arguments") if isinstance(decoded, dict) and decoded.get("schema") == BOOTSTRAP_SCHEMA else None
    if not isinstance(arguments, list) or not all(isinstance(item, str) for item in arguments):
        raise OrchestrateError("Invalid controller bootstrap arguments", code="bootstrap_payload_invalid")
    return arguments


def _powershell_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def controller_command(
    payload: str,
    result_path: Path,
    *,
    python: str | None = None,
    platform: str | None = None,
) -> str:
    executable = python or sys.executable
    selected = sys.platform if platform is None else platform
    if selected == "win32":
        return (
            "$env:ORCHESTRATE_CONTROLLER_BOOTSTRAP='1'; "
            f"& {_powershell_quote(executable)} -m orchestrate _controller --payload {_powershell_quote(payload)} "
            f"--result-path {_powershell_quote(os.fspath(result_path))}; "
            "exit $LASTEXITCODE"
        )
    return (
        "ORCHESTRATE_CONTROLLER_BOOTSTRAP=1 exec "
        f"{shlex.quote(executable)} -m orchestrate _controller --payload {shlex.quote(payload)} "
        f"--result-path {shlex.quote(os.fspath(result_path))}"
    )


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


def _terminal(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    terminal = _result(payload).get("terminal")
    if not isinstance(terminal, Mapping) or not isinstance(terminal.get("handle"), str):
        raise OrchestrateError("terminal create omitted result.terminal.handle", code="orca_contract_error")
    required_strings = ("worktreeId", "executionHostId", "hostPlatform", "surface")
    if any(not isinstance(terminal.get(key), str) or not terminal.get(key) for key in required_strings):
        raise OrchestrateError("terminal create returned an unknown public shape", code="orca_contract_error")
    return terminal


def _exit_code(payload: Mapping[str, Any], *, expected_handle: str | None = None) -> int:
    wait = _result(payload).get("wait")
    if not isinstance(wait, Mapping):
        raise OrchestrateError("terminal wait omitted result.wait", code="bootstrap_exit_unproven")
    if (
        wait.get("condition") != "exit"
        or wait.get("satisfied") is not True
        or wait.get("status") != "exited"
        or (expected_handle is not None and wait.get("handle") != expected_handle)
    ):
        raise OrchestrateError("terminal wait did not prove the exact exited controller", code="bootstrap_exit_unproven")
    exit_code = wait.get("exitCode")
    cause = wait.get("exitCause")
    if (
        not isinstance(exit_code, int)
        or not isinstance(cause, Mapping)
        or cause.get("kind") != "exited"
        or cause.get("exitCode") != exit_code
    ):
        raise OrchestrateError("terminal exit receipts disagree", code="bootstrap_exit_unproven")
    return exit_code


def _read_result(path: Path, expected_exit_code: int) -> str:
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError(
            "The controller exited without a durable result record",
            code="bootstrap_result_unavailable",
        ) from exc
    if not isinstance(record, dict) or record.get("schema") != BOOTSTRAP_SCHEMA:
        raise OrchestrateError("The controller result record is malformed", code="bootstrap_result_unavailable")
    if record.get("exitCode") != expected_exit_code or not isinstance(record.get("stdout"), str):
        raise OrchestrateError("Controller and terminal exit receipts disagree", code="bootstrap_result_unavailable")
    return record["stdout"]


class BootstrapJournal:
    """One host-local durable invocation per project, never a scheduler."""

    def __init__(self, root: Path) -> None:
        self.directory = make_private_state_directory(root, state_home() / "bootstrap")
        make_private_state_directory(root, self.directory / "results")
        make_private_state_directory(root, self.directory / "locks")
        self.connection = sqlite3.connect(self.directory / "state.sqlite3", timeout=5, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        current = self.connection.execute("SELECT value FROM meta WHERE key = 'schema'").fetchone()
        if current is not None and current["value"] != BOOTSTRAP_STATE_SCHEMA:
            raise OrchestrateError("Unsupported bootstrap journal schema", code="bootstrap_state_unsupported")
        self.connection.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema', ?)",
            (BOOTSTRAP_STATE_SCHEMA,),
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS invocations(
                id TEXT PRIMARY KEY,
                project_key TEXT NOT NULL,
                root TEXT NOT NULL,
                arguments_json TEXT NOT NULL,
                result_path TEXT NOT NULL,
                create_arguments_json TEXT NOT NULL,
                phase TEXT NOT NULL,
                handle TEXT,
                exit_code INTEGER,
                create_response_json TEXT,
                interrupt_response_json TEXT,
                close_response_json TEXT,
                error_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )

    def close(self) -> None:
        self.connection.close()

    def lock(self, root: Path) -> RunLock:
        return RunLock(self.directory / "locks" / f"{project_key(root)}.lock")

    def begin(self, root: Path, arguments: list[str]) -> sqlite3.Row:
        key = project_key(root)
        encoded = json.dumps(arguments, separators=(",", ":"))
        existing = self.connection.execute(
            "SELECT * FROM invocations WHERE project_key = ? AND phase != 'reported' ORDER BY created_at LIMIT 1",
            (key,),
        ).fetchone()
        if existing is not None:
            if existing["arguments_json"] != encoded:
                raise OrchestrateError(
                    "A prior bootstrap invocation is unresolved; different controller arguments cannot replace it",
                    code="bootstrap_invocation_active",
                    data={"invocationId": existing["id"], "phase": existing["phase"]},
                )
            return existing
        invocation_id = f"bootstrap_{uuid.uuid4().hex}"
        result_path = self.directory / "results" / f"{invocation_id}.json"
        payload = encode_payload(arguments)
        create_arguments = [
            "terminal",
            "create",
            "--worktree",
            f"path:{root.resolve()}",
            "--title",
            "orchestrate controller",
            "--command",
            controller_command(payload, result_path),
            "--focus",
        ]
        now = utc_now()
        self.connection.execute(
            """INSERT INTO invocations VALUES (?, ?, ?, ?, ?, ?, 'create_prepared',
               NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)""",
            (
                invocation_id,
                key,
                os.fspath(root.resolve()),
                encoded,
                os.fspath(result_path),
                json.dumps(create_arguments, separators=(",", ":")),
                now,
                now,
            ),
        )
        return self.get(invocation_id)

    def get(self, invocation_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM invocations WHERE id = ?", (invocation_id,)).fetchone()
        if row is None:
            raise OrchestrateError("Bootstrap invocation disappeared", code="bootstrap_state_unavailable")
        return row

    def update(self, invocation_id: str, **fields: object) -> sqlite3.Row:
        allowed = {
            "phase",
            "handle",
            "exit_code",
            "create_response_json",
            "interrupt_response_json",
            "close_response_json",
            "error_json",
        }
        if set(fields) - allowed:
            raise ValueError("Unsupported bootstrap journal field")
        fields["updated_at"] = utc_now()
        assignments = ", ".join(f"{key} = ?" for key in fields)
        self.connection.execute(
            f"UPDATE invocations SET {assignments} WHERE id = ?",
            (*fields.values(), invocation_id),
        )
        return self.get(invocation_id)


def _unknown_effect(row: sqlite3.Row, operation: str) -> OrchestrateError:
    return OrchestrateError(
        f"A prior bootstrap {operation} may have taken effect without a public recovery identity",
        code="bootstrap_effect_uncertain",
        data={"invocationId": row["id"], "phase": row["phase"], "terminalHandle": row["handle"]},
    )


def _call_bootstrap_mutation(
    client: OrcaClient,
    journal: BootstrapJournal,
    row: sqlite3.Row,
    *,
    operation: str,
    arguments: list[str],
) -> Mapping[str, Any]:
    row = journal.update(row["id"], phase=f"{operation}_invoking", error_json=None)
    try:
        response = client.run_json(*arguments, "--json")
    except (OrcaCommandError, KeyboardInterrupt) as exc:
        if isinstance(exc, OrcaCommandError) and exc.result is not None and exc.result.payload is not None:
            error: object = exc.result.payload
        else:
            error = {"code": "interrupted" if isinstance(exc, KeyboardInterrupt) else "orca_command_failed"}
        journal.update(
            row["id"],
            phase=f"{operation}_uncertain",
            error_json=json.dumps(error, sort_keys=True),
        )
        raise _unknown_effect(journal.get(row["id"]), operation) from exc
    return response


def _confirm_created_terminal(
    client: OrcaClient,
    journal: BootstrapJournal,
    row: sqlite3.Row,
    root: Path,
) -> sqlite3.Row:
    handle = row["handle"]
    if not isinstance(handle, str):
        raise OrchestrateError("Bootstrap journal omitted its terminal handle", code="bootstrap_state_unavailable")
    shown = client.run_json("terminal", "show", "--terminal", handle, "--json")
    terminal = _result(shown).get("terminal")
    if (
        not isinstance(terminal, Mapping)
        or terminal.get("handle") != handle
        or not isinstance(terminal.get("worktreePath"), str)
        or Path(terminal["worktreePath"]).resolve() != root.resolve()
        or terminal.get("worktreeId") != json.loads(row["create_response_json"])["result"]["terminal"]["worktreeId"]
        or (terminal.get("agentIdentity") is not None and terminal.get("agentIdentity") != "")
        or terminal.get("connected") is not True
        or terminal.get("writable") is not True
    ):
        raise OrchestrateError(
            "The created controller terminal identity could not be proven",
            code="bootstrap_terminal_unproven",
        )
    return journal.update(row["id"], phase="waiting")


def _wait_for_exit(client: OrcaClient, handle: str) -> Mapping[str, Any] | None:
    try:
        return client.run_json(
            "terminal",
            "wait",
            "--terminal",
            handle,
            "--for",
            "exit",
            "--timeout-ms",
            "1000",
            "--json",
            timeout_seconds=3,
        )
    except OrcaCommandError as exc:
        if exc.code == "timeout" or "timed out" in str(exc).lower():
            return None
        raise


def launch_controller(root: Path, arguments: list[str], *, client: OrcaClient) -> int:
    """Create or resume one journaled terminal and reproduce its exact result."""

    root = root.resolve()
    require_bootstrap_caller(client, root)
    journal = BootstrapJournal(root)
    try:
        with journal.lock(root):
            row = journal.begin(root, arguments)
            while True:
                phase = row["phase"]
                if phase == "create_prepared":
                    create_arguments = json.loads(row["create_arguments_json"])
                    response = _call_bootstrap_mutation(
                        client,
                        journal,
                        row,
                        operation="create",
                        arguments=create_arguments,
                    )
                    terminal = _terminal(response)
                    row = journal.update(
                        row["id"],
                        phase="create_confirmed",
                        handle=terminal["handle"],
                        create_response_json=json.dumps(response, sort_keys=True),
                    )
                    continue
                if phase in {"create_invoking", "create_uncertain"}:
                    raise _unknown_effect(row, "terminal create")
                if phase == "create_confirmed":
                    row = _confirm_created_terminal(client, journal, row, root)
                    continue
                if phase in {"waiting", "interrupt_uncertain"}:
                    handle = row["handle"]
                    if not isinstance(handle, str):
                        raise OrchestrateError("Bootstrap journal omitted its terminal handle", code="bootstrap_state_unavailable")
                    try:
                        waited = _wait_for_exit(client, handle)
                    except KeyboardInterrupt:
                        if phase == "interrupt_uncertain":
                            raise _unknown_effect(row, "terminal interrupt")
                        row = journal.update(row["id"], phase="interrupt_prepared")
                        continue
                    if waited is None:
                        continue
                    exit_code = _exit_code(waited, expected_handle=handle)
                    _read_result(Path(row["result_path"]), exit_code)
                    row = journal.update(row["id"], phase="exited", exit_code=exit_code)
                    continue
                if phase == "interrupt_prepared":
                    handle = row["handle"]
                    response = _call_bootstrap_mutation(
                        client,
                        journal,
                        row,
                        operation="interrupt",
                        arguments=["terminal", "send", "--terminal", str(handle), "--interrupt"],
                    )
                    sent = _result(response).get("send")
                    if (
                        not isinstance(sent, Mapping)
                        or sent.get("handle") != handle
                        or sent.get("accepted") is not True
                        or sent.get("bytesWritten") != 1
                    ):
                        journal.update(row["id"], phase="interrupt_uncertain", error_json=json.dumps(response, sort_keys=True))
                        raise _unknown_effect(journal.get(row["id"]), "terminal interrupt")
                    row = journal.update(
                        row["id"],
                        phase="waiting",
                        interrupt_response_json=json.dumps(response, sort_keys=True),
                    )
                    continue
                if phase == "interrupt_invoking":
                    row = journal.update(row["id"], phase="interrupt_uncertain")
                    continue
                if phase == "exited":
                    _read_result(Path(row["result_path"]), int(row["exit_code"]))
                    row = journal.update(row["id"], phase="close_prepared")
                    continue
                if phase == "close_prepared":
                    handle = row["handle"]
                    response = _call_bootstrap_mutation(
                        client,
                        journal,
                        row,
                        operation="close",
                        arguments=["terminal", "close", "--terminal", str(handle), "--tab"],
                    )
                    closed = _result(response).get("close")
                    if (
                        not isinstance(closed, Mapping)
                        or closed.get("handle") != handle
                        or closed.get("closeMode") != "tab"
                        or not isinstance(closed.get("tabId"), str)
                        or not isinstance(closed.get("ptyKilled"), bool)
                    ):
                        journal.update(row["id"], phase="close_uncertain", error_json=json.dumps(response, sort_keys=True))
                        raise _unknown_effect(journal.get(row["id"]), "terminal close")
                    row = journal.update(
                        row["id"],
                        phase="closed",
                        close_response_json=json.dumps(response, sort_keys=True),
                    )
                    continue
                if phase in {"close_invoking", "close_uncertain"}:
                    raise _unknown_effect(row, "terminal close")
                if phase == "closed":
                    output = _read_result(Path(row["result_path"]), int(row["exit_code"]))
                    print(output, end="", flush=True)
                    row = journal.update(row["id"], phase="reported")
                    try:
                        Path(row["result_path"]).unlink()
                    except FileNotFoundError:
                        pass
                    return int(row["exit_code"])
                raise OrchestrateError(f"Unknown bootstrap journal phase: {phase}", code="bootstrap_state_unsupported")
    finally:
        journal.close()
