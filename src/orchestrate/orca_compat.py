"""Strict public readback shapes for the supported Orca 1.4.198/1.4.199 range."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class WorkerShowShapeError(ValueError):
    """The readback is neither one exact supported shape nor a consistent alias pair."""


@dataclass(frozen=True, slots=True)
class DispatchIdentity:
    version: str
    run_id: object
    task_id: object
    last_failure: object


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    dispatch: DispatchIdentity
    dispatch_id: object
    worktree_id: object
    terminal_handle: object
    last_error: object


@dataclass(frozen=True, slots=True)
class TerminalResourceIdentity:
    resource_id: str
    terminal_handle: str
    worktree_id: str


def worker_show_dispatch_identity(dispatch: Mapping[str, Any]) -> DispatchIdentity:
    """Read only the two version-matched Dispatch identity encodings."""

    if "runId" in dispatch:
        required = ("runId", "taskId", "task_id", "lastFailure")
        forbidden = ("run_id", "last_failure")
        if any(name not in dispatch for name in required) or any(name in dispatch for name in forbidden):
            raise WorkerShowShapeError("mixed or incomplete Orca 1.4.199 Dispatch identity")
        if dispatch["taskId"] != dispatch["task_id"]:
            raise WorkerShowShapeError("conflicting Orca 1.4.199 Task aliases")
        return DispatchIdentity(
            version="1.4.199",
            run_id=dispatch["runId"],
            task_id=dispatch["taskId"],
            last_failure=dispatch["lastFailure"],
        )
    if "run_id" in dispatch:
        required = ("run_id", "task_id", "last_failure")
        forbidden = ("runId", "taskId", "lastFailure")
        if any(name not in dispatch for name in required) or any(name in dispatch for name in forbidden):
            raise WorkerShowShapeError("mixed or incomplete Orca 1.4.198 Dispatch identity")
        return DispatchIdentity(
            version="1.4.198",
            run_id=dispatch["run_id"],
            task_id=dispatch["task_id"],
            last_failure=dispatch["last_failure"],
        )
    raise WorkerShowShapeError("worker-show omitted a supported Dispatch identity")


def worker_show_identity(
    dispatch: Mapping[str, Any],
    worker: Mapping[str, Any],
) -> WorkerIdentity:
    """Read the corresponding exact worker identity without broad key normalization."""

    dispatch_identity = worker_show_dispatch_identity(dispatch)
    if dispatch_identity.version == "1.4.199":
        required = ("dispatchId", "worktreeId", "agentTerminalHandle", "lastError")
        forbidden = ("dispatch_id", "worktree_id", "agent_terminal_handle", "last_error")
        if any(name not in worker for name in required) or any(name in worker for name in forbidden):
            raise WorkerShowShapeError("mixed or incomplete Orca 1.4.199 worker identity")
        if worker["dispatchId"] != dispatch.get("id"):
            raise WorkerShowShapeError("Orca 1.4.199 worker and Dispatch identities conflict")
        return WorkerIdentity(
            dispatch=dispatch_identity,
            dispatch_id=worker["dispatchId"],
            worktree_id=worker["worktreeId"],
            terminal_handle=worker["agentTerminalHandle"],
            last_error=worker["lastError"],
        )
    forbidden = ("dispatchId", "worktreeId", "agentTerminalHandle", "lastError")
    required = ("worktree_id", "agent_terminal_handle", "last_error")
    if any(name not in worker for name in required) or any(name in worker for name in forbidden):
        raise WorkerShowShapeError("mixed or incomplete Orca 1.4.198 worker identity")
    return WorkerIdentity(
        dispatch=dispatch_identity,
        dispatch_id=dispatch.get("id"),
        worktree_id=worker["worktree_id"],
        terminal_handle=worker["agent_terminal_handle"],
        last_error=worker["last_error"],
    )


def worker_terminal_resource_identity(
    resource: Mapping[str, Any],
    *,
    dispatch_id: str,
) -> TerminalResourceIdentity:
    """Bind the immutable public worker resource identity owned by one Dispatch."""

    resource_id = resource.get("id")
    terminal_handle = resource.get("terminalHandle")
    worktree_id = resource.get("worktreeId")
    if (
        not isinstance(resource_id, str)
        or not resource_id
        or not isinstance(terminal_handle, str)
        or not terminal_handle
        or not isinstance(worktree_id, str)
        or not worktree_id
        or resource.get("originDispatchId") != dispatch_id
        or resource.get("ownerDispatchId") != dispatch_id
    ):
        raise WorkerShowShapeError("worker-show omitted the exact Dispatch-owned terminal resource identity")
    return TerminalResourceIdentity(
        resource_id=resource_id,
        terminal_handle=terminal_handle,
        worktree_id=worktree_id,
    )
