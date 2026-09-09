"""Launch the controller in its own ordinary Orca terminal."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
from typing import Any

from .errors import OrchestrateError
from .orca import OrcaClient, OrcaCommandError
from .state import state_home


BOOTSTRAP_SCHEMA = "orchestrate-bootstrap/v1"


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


def _terminal_handle(payload: dict[str, Any]) -> str:
    result = payload.get("result")
    if isinstance(result, dict):
        terminal = result.get("terminal")
        if isinstance(terminal, dict) and isinstance(terminal.get("handle"), str):
            return terminal["handle"]
        for key in ("handle", "terminalHandle"):
            if isinstance(result.get(key), str):
                return result[key]
    raise OrchestrateError("terminal create omitted the terminal handle", code="orca_contract_error")


def _exit_code(payload: dict[str, Any]) -> int:
    result = payload.get("result")
    wait = result.get("wait") if isinstance(result, dict) else None
    if not isinstance(wait, dict):
        raise OrchestrateError("terminal wait omitted result.wait", code="bootstrap_exit_unproven")
    if wait.get("condition") != "exit" or wait.get("satisfied") is not True or wait.get("status") != "exited":
        raise OrchestrateError("terminal wait did not prove an exited controller", code="bootstrap_exit_unproven")
    exit_code = wait.get("exitCode")
    if not isinstance(exit_code, int):
        raise OrchestrateError("terminal wait omitted the controller exit code", code="bootstrap_exit_unproven")
    cause = wait.get("exitCause")
    if isinstance(cause, dict) and cause.get("kind") == "exited" and cause.get("exitCode") != exit_code:
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


def launch_controller(root: Path, arguments: list[str], *, client: OrcaClient) -> int:
    """Create one dedicated terminal, mirror its output, and forward Ctrl-C."""

    if os.environ.get("ORCA_AGENT_HOOK_TOKEN") or os.environ.get("ORCA_AGENT_LAUNCH_TOKEN"):
        raise OrchestrateError(
            "Agent terminals may not bootstrap around Orca dispatch depth; run this from an ordinary shell",
            code="bootstrap_from_agent_forbidden",
        )
    payload = encode_payload(arguments)
    result_directory = state_home() / "bootstrap"
    result_directory.mkdir(parents=True, exist_ok=True)
    descriptor, raw_path = tempfile.mkstemp(prefix="result-", suffix=".json", dir=result_directory)
    os.close(descriptor)
    result_path = Path(raw_path)
    result_path.unlink()
    created = client.run_json(
        "terminal",
        "create",
        "--worktree",
        f"path:{root.resolve()}",
        "--title",
        "orchestrate controller",
        "--command",
        controller_command(payload, result_path),
        "--focus",
        "--json",
    )
    handle = _terminal_handle(created)
    interrupted = False
    try:
        while True:
            try:
                waited = client.run_json(
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
            except KeyboardInterrupt:
                interrupted = True
                client.run_json("terminal", "send", "--terminal", handle, "--interrupt", "--json")
                continue
            except OrcaCommandError as exc:
                if exc.code == "timeout" or "timed out" in str(exc).lower():
                    continue
                raise
            exit_code = _exit_code(waited)
            if result_path.exists():
                output = _read_result(result_path, exit_code)
                print(output, end="", flush=True)
            elif not interrupted:
                raise OrchestrateError(
                    "The controller exited without a durable result record",
                    code="bootstrap_result_unavailable",
                )
            client.run_json("terminal", "close", "--terminal", handle, "--tab", "--json")
            return exit_code
    finally:
        try:
            result_path.unlink()
        except FileNotFoundError:
            pass
