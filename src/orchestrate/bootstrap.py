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
            f"& {_powershell_quote(executable)} -I -m orchestrate _controller --payload {_powershell_quote(payload)} "
            f"--result-path {_powershell_quote(os.fspath(result_path))}; "
            "exit $LASTEXITCODE"
        )
    return (
        "ORCHESTRATE_CONTROLLER_BOOTSTRAP=1 exec "
        f"{shlex.quote(executable)} -I -m orchestrate _controller --payload {shlex.quote(payload)} "
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
        type(exit_code) is not int
        or not isinstance(cause, Mapping)
        or cause.get("kind") != "exited"
        or type(cause.get("exitCode")) is not int
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
    if (
        type(expected_exit_code) is not int
        or type(record.get("exitCode")) is not int
        or record.get("exitCode") != expected_exit_code
        or not isinstance(record.get("stdout"), str)
    ):
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
                exit_response_json TEXT,
                create_response_json TEXT,
                interrupt_response_json TEXT,
                close_response_json TEXT,
                cleanup_observation_json TEXT,
                error_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(invocations)").fetchall()}
        for name in ("exit_response_json", "cleanup_observation_json"):
            if name not in columns:
                self.connection.execute(f"ALTER TABLE invocations ADD COLUMN {name} TEXT")

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
            """INSERT INTO invocations(
                   id, project_key, root, arguments_json, result_path, create_arguments_json,
                   phase, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, 'create_prepared', ?, ?)""",
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
            "exit_response_json",
            "create_response_json",
            "interrupt_response_json",
            "close_response_json",
            "cleanup_observation_json",
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


def _runtime_id(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("_meta")
    runtime_id = metadata.get("runtimeId") if isinstance(metadata, Mapping) else None
    if not isinstance(runtime_id, str) or not runtime_id:
        raise OrchestrateError("Bootstrap readback omitted runtime identity", code="bootstrap_effect_uncertain")
    return runtime_id


def _reconcile_uncertain_close(
    client: OrcaClient,
    journal: BootstrapJournal,
    row: sqlite3.Row,
    root: Path,
) -> sqlite3.Row:
    """Confirm one observed exited-and-absent disposition without repeating close."""

    try:
        created_payload = json.loads(row["create_response_json"])
        created = _terminal(created_payload)
        handle = created["handle"]
        tab_id = created.get("tabId")
        incarnation_id = created.get("incarnationId")
        worktree_id = created.get("worktreeId")
        host_id = created.get("executionHostId")
        host_platform = created.get("hostPlatform")
        runtime_id = _runtime_id(created_payload)
        if (
            not all(isinstance(item, str) and item for item in (tab_id, incarnation_id, worktree_id, host_id))
            or host_id != "local"
            or host_platform != "win32"
        ):
            raise OrchestrateError("Bootstrap create receipt omitted cleanup identity", code="bootstrap_effect_uncertain")
        expected_exit = row["exit_code"]
        if type(expected_exit) is not int:
            raise OrchestrateError("Bootstrap journal omitted exact exit evidence", code="bootstrap_effect_uncertain")
        _read_result(Path(row["result_path"]), expected_exit)
        waited = _wait_for_exit(client, handle)
        if waited is None or _runtime_id(waited) != runtime_id or _exit_code(waited, expected_handle=handle) != expected_exit:
            raise OrchestrateError("Bootstrap exit could not be re-proven", code="bootstrap_effect_uncertain")
        shown_payload = client.run_json("terminal", "show", "--terminal", handle, "--json")
        shown = _result(shown_payload).get("terminal")
        if (
            _runtime_id(shown_payload) != runtime_id
            or not isinstance(shown, Mapping)
            or shown.get("handle") != handle
            or shown.get("tabId") != tab_id
            or shown.get("incarnationId") != incarnation_id
            or shown.get("worktreeId") != worktree_id
            or shown.get("executionHostId") != host_id
            or shown.get("hostPlatform") != host_platform
            or not isinstance(shown.get("worktreePath"), str)
            or Path(shown["worktreePath"]).resolve() != root.resolve()
            or shown.get("connected") is not False
            or shown.get("writable") is not False
            or shown.get("orphaned") is not False
        ):
            raise OrchestrateError("Historical terminal identity did not prove exited state", code="bootstrap_effect_uncertain")
        inventory_payload = client.run_json(
            "terminal", "list", "--worktree", f"path:{root.resolve()}", "--include-visual-layouts", "--json"
        )
        inventory = _result(inventory_payload)
        terminals = inventory.get("terminals")
        layouts = inventory.get("visualLayouts")
        host_scope = inventory.get("hostScope")
        topology_revisions = inventory.get("topologyRevisions")
        if (
            _runtime_id(inventory_payload) != runtime_id
            or inventory.get("truncated") is not False
            or not isinstance(terminals, list)
            or not all(isinstance(item, Mapping) for item in terminals)
            or type(inventory.get("totalCount")) is not int
            or inventory.get("totalCount") != len(terminals)
            or not isinstance(layouts, list)
            or not isinstance(host_scope, Mapping)
            or host_scope.get("hostIds") != [host_id]
            or host_scope.get("omittedHostIds") != []
            or not isinstance(topology_revisions, Mapping)
            or type(topology_revisions.get(worktree_id)) is not int
        ):
            raise OrchestrateError("Terminal inventory was not complete for exact cleanup", code="bootstrap_effect_uncertain")
        if any(
            item.get("worktreeId") != worktree_id
            or item.get("executionHostId") != host_id
            or not isinstance(item.get("worktreePath"), str)
            or Path(item["worktreePath"]).resolve() != root.resolve()
            for item in terminals
        ):
            raise OrchestrateError("Terminal inventory crossed the expected workspace", code="bootstrap_effect_uncertain")
        if any(
            item.get("handle") == handle or item.get("incarnationId") == incarnation_id
            for item in terminals
        ):
            raise OrchestrateError("Created terminal remains in the complete inventory", code="bootstrap_effect_uncertain")
        seen_tabs: set[str] = set()
        seen_handles: set[str] = set()
        for layout in layouts:
            if (
                layout.get("worktreeId") != worktree_id
                or not isinstance(layout.get("worktreePath"), str)
                or Path(layout["worktreePath"]).resolve() != root.resolve()
            ):
                raise OrchestrateError("Terminal layout belongs to another workspace", code="bootstrap_effect_uncertain")
            layout_root = layout.get("root")
            tabs = layout_root.get("tabs") if isinstance(layout_root, Mapping) and layout_root.get("type") == "group" else None
            if not isinstance(tabs, list) or not all(isinstance(item, Mapping) for item in tabs):
                raise OrchestrateError("Unsupported terminal layout shape", code="bootstrap_effect_uncertain")
            for tab in tabs:
                current_tab = tab.get("tabId")
                panes = tab.get("panes")
                if not isinstance(current_tab, str) or not isinstance(panes, Mapping) or panes.get("type") != "terminal":
                    raise OrchestrateError("Unsupported terminal pane shape", code="bootstrap_effect_uncertain")
                pane_handle = panes.get("handle")
                pane_tab = panes.get("tabId")
                if not isinstance(pane_handle, str) or pane_tab != current_tab:
                    raise OrchestrateError("Terminal layout identity is malformed", code="bootstrap_effect_uncertain")
                seen_tabs.add(current_tab)
                seen_handles.add(pane_handle)
        if tab_id in seen_tabs or handle in seen_handles:
            raise OrchestrateError("Created tab remains in the complete layout", code="bootstrap_effect_uncertain")
        observation = {
            "schema": "orchestrate-bootstrap-cleanup/v1",
            "outcome": "observed-exited-and-absent",
            "runtimeId": runtime_id,
            "expected": {
                "handle": handle, "tabId": tab_id, "incarnationId": incarnation_id,
                "worktreeId": worktree_id, "executionHostId": host_id, "hostPlatform": host_platform,
            },
            "exit": waited,
            "historicalTerminal": shown_payload,
            "completeInventory": inventory_payload,
            "observedAt": utc_now(),
        }
        return journal.update(
            row["id"],
            phase="closed",
            exit_response_json=json.dumps(waited, sort_keys=True),
            cleanup_observation_json=json.dumps(observation, sort_keys=True),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, OrchestrateError, OrcaCommandError) as exc:
        raise _unknown_effect(journal.get(row["id"]), "terminal close") from exc


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
                    row = journal.update(
                        row["id"], phase="exited", exit_code=exit_code,
                        exit_response_json=json.dumps(waited, sort_keys=True),
                    )
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
                    row = _reconcile_uncertain_close(client, journal, row, root)
                    continue
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
