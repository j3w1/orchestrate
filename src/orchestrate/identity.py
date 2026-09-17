"""Exact native caller, workspace, and Run binding checks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Any

from .errors import OrchestrateError
from .host import SUPPORTED_NATIVE_PLATFORMS, is_wsl
from .orca import OrcaClient


SETTLED_DISPATCH_STATUSES = {"completed", "failed"}


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


@dataclass(frozen=True, slots=True)
class ControllerIdentity:
    terminal_handle: str
    worktree_id: str
    worktree_path: Path
    execution_host_id: str
    current_run_id: str | None


def require_plain_controller(
    client: OrcaClient,
    root: Path,
    *,
    expected_run_id: str | None = None,
    allow_expected_unbound: bool = False,
    environment: Mapping[str, str] | None = None,
) -> ControllerIdentity:
    """Prove an ordinary exact-worktree caller with no active Dispatch."""

    env = os.environ if environment is None else environment
    handle = env.get("ORCA_TERMINAL_HANDLE", "").strip()
    if not handle:
        raise OrchestrateError(
            "No owned Orca terminal is present; use the bootstrap command",
            code="controller_terminal_missing",
        )
    if sys.platform not in SUPPORTED_NATIVE_PLATFORMS or is_wsl(env):
        raise OrchestrateError(
            "Controller identity supports only native Windows and Linux; WSL must use the Windows transport boundary",
            code="controller_host_unsupported",
        )

    terminal_payload = client.run_json("terminal", "show", "--terminal", handle, "--json")
    terminal = _result(terminal_payload).get("terminal")
    if not isinstance(terminal, Mapping) or terminal.get("handle") != handle:
        raise OrchestrateError("terminal show did not prove the invoking terminal", code="orca_contract_error")
    execution_host_id = terminal.get("executionHostId")
    if execution_host_id != "local" or (
        "hostPlatform" in terminal and terminal.get("hostPlatform") != sys.platform
    ):
        raise OrchestrateError(
            "The controller terminal is not on the local native host platform",
            code="controller_host_unsupported",
            data={
                "expectedExecutionHostId": "local",
                "actualExecutionHostId": execution_host_id,
                "expectedHostPlatform": sys.platform,
                "actualHostPlatform": terminal.get("hostPlatform"),
            },
        )
    agent_identity = terminal.get("agentIdentity")
    if agent_identity is not None and agent_identity != "":
        raise OrchestrateError(
            "A reasoning-agent terminal cannot act as the persistent controller; use a dedicated ordinary Orca terminal",
            code="agent_terminal_not_controller",
        )
    path = terminal.get("worktreePath")
    worktree_id = terminal.get("worktreeId")
    if (
        not isinstance(path, str)
        or Path(path).resolve() != root.resolve()
        or not isinstance(worktree_id, str)
        or not worktree_id
        or terminal.get("connected") is not True
        or terminal.get("writable") is not True
    ):
        raise OrchestrateError(
            "The ordinary controller terminal is not a connected writable terminal in the exact project worktree",
            code="controller_worktree_mismatch",
        )

    current_payload = client.run_json("worktree", "current", "--json")
    current_worktree = _result(current_payload).get("worktree")
    if (
        not isinstance(current_worktree, Mapping)
        or current_worktree.get("id") != worktree_id
        or not isinstance(current_worktree.get("path"), str)
        or Path(current_worktree["path"]).resolve() != root.resolve()
    ):
        raise OrchestrateError("worktree current disagrees with terminal show", code="controller_worktree_mismatch")

    workers_payload = client.run_json("orchestration", "worker-list", "--json")
    workers = _result(workers_payload).get("workers")
    if not isinstance(workers, list) or not all(isinstance(item, Mapping) for item in workers):
        raise OrchestrateError("worker-list omitted its exact worker inventory", code="orca_contract_error")
    matching = [item for item in workers if item.get("agentTerminalHandle") == handle]
    for worker in matching:
        status = worker.get("dispatchStatus")
        if not isinstance(status, str):
            raise OrchestrateError("worker-list omitted Dispatch status", code="orca_contract_error")
        if status not in SETTLED_DISPATCH_STATUSES:
            raise OrchestrateError(
                "The invoking terminal owns an active or context-only Dispatch and cannot become a controller",
                code="agent_terminal_not_controller",
                data={"dispatchId": worker.get("dispatchId"), "dispatchStatus": status},
            )

    run_payload = client.run_json("orchestration", "run-current", "--json")
    run = _result(run_payload).get("run")
    if run is None:
        current_run_id = None
    elif isinstance(run, Mapping) and isinstance(run.get("id"), str):
        current_run_id = run["id"]
    else:
        raise OrchestrateError("run-current returned an unknown binding shape", code="orca_contract_error")
    if expected_run_id is None:
        if current_run_id is not None:
            raise OrchestrateError(
                "The ordinary controller terminal is already bound to another native Run",
                code="controller_run_conflict",
                data={"currentRunId": current_run_id},
            )
    elif current_run_id != expected_run_id and not (allow_expected_unbound and current_run_id is None):
        raise OrchestrateError(
            "The ordinary controller terminal is bound to a different native Run",
            code="controller_run_conflict",
            data={"expectedRunId": expected_run_id, "currentRunId": current_run_id},
        )
    return ControllerIdentity(handle, worktree_id, root.resolve(), execution_host_id, current_run_id)


def require_bootstrap_caller(client: OrcaClient, root: Path) -> None:
    """Reject an actual agent/Dispatch while allowing a truly outside shell."""

    if not os.environ.get("ORCA_TERMINAL_HANDLE", "").strip():
        return
    require_plain_controller(client, root)
