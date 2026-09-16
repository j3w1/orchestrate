from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from orchestrate.admission import joined_preflight_status, worker_preflight
from orchestrate.controller import (
    _ack_if_resolved,
    _finish_prompt_stall_cleanup,
    _mutation,
    _process_delivery,
    _record_input_submission_diagnostic,
    _recover_milestone_worker_projections,
    _release_disposition,
    _request_id,
    _run_summary,
    _validate_worker_start_readback,
    answer,
    explain,
    implement,
    packet,
    reconcile_intentions,
    resume,
    status,
)
from orchestrate.cli import main as cli_main
from orchestrate.errors import OrchestrateError
from orchestrate.coordination import MilestonePlan, MilestoneTask, SharedContract
from orchestrate.identity import require_plain_controller
from orchestrate.orca import OrcaCommandError, OrcaCommandResult, OrcaJsonResponse
from orchestrate.packets import canonical_packet_json, expected_packet_id, make_packet, packet_spec
from orchestrate.profile import ProjectProfile, setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from orchestrate.state import AdmissionEffectFence, StateStore


LIVENESS_TIMEOUT = 120.0  # Outer deadlock detector, not a semantic progress budget.
requires_native_windows_admission = unittest.skipUnless(
    sys.platform == "win32",
    "managed worker admission is win32-only by design",
)


def git(root: Path, *arguments: str) -> bytes:
    return subprocess.run(("git", "-C", str(root), *arguments), check=True, capture_output=True).stdout


Response = dict[str, object] | Callable[["FakeClient", tuple[str, ...]], dict[str, object]]


class FakeClient:
    def __init__(self, responses: list[Response], *, default_returncode: int | None = 0) -> None:
        self.responses = responses
        self.default_returncode = default_returncode
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if not self.responses:
            raise AssertionError(f"Unexpected Orca call: {arguments}")
        response = self.responses.pop(0)
        resolved = response(self, arguments) if callable(response) else response
        if isinstance(resolved, OrcaJsonResponse) or self.default_returncode is None:
            return resolved
        return OrcaJsonResponse(resolved, returncode=self.default_returncode)


def mutation(request_id: str, **result: object) -> dict[str, object]:
    return {
        "result": {
            **result,
            "mutation": {"requestId": request_id, "replayed": False},
        }
    }


def acknowledgement(
    delivery_id: str,
    *,
    request_id: str = "request_ack",
    **result: object,
) -> dict[str, object]:
    values: dict[str, object] = {
        "acknowledged": delivery_id,
        "deliveryId": None,
        "messages": [],
    }
    values.update(result)
    return mutation(request_id, **values)


def lifecycle_message(
    message_type: str,
    payload: Mapping[str, object],
    *,
    message_id: str,
    run_id: str = "run_1",
    dispatch_id: str = "dispatch_1",
    from_handle: str | None = None,
    subject: str | None = None,
    body: str | None = None,
) -> dict[str, object]:
    encoded_payload = dict(payload)
    if message_type == "question":
        body = "Synthetic question" if body is None else body
        encoded_payload.setdefault("question", body)
        encoded_payload.setdefault("options", [])
    message: dict[str, object] = {
        "id": message_id,
        "run_id": run_id,
        "delivery_contract": "current_delivery",
        "from_handle": (
            f"dispatch:{dispatch_id}"
            if message_type == "question"
            else "term_worker"
        ) if from_handle is None else from_handle,
        "to_handle": f"run:{run_id}",
        "type": message_type,
        "priority": "normal",
        "thread_id": message_id if message_type == "question" else None,
        "payload": json.dumps(encoded_payload, separators=(",", ":")),
        "created_at": "2026-01-01T00:00:00Z",
        "delivered_at": None,
    }
    if subject is not None:
        message["subject"] = subject
    if body is not None:
        message["body"] = body
    return message


def request_show(
    request_id: str,
    state: str = "completed",
    method: str = "orchestration.run-create",
) -> dict[str, object]:
    return {
        "result": {
            "requestId": request_id,
            "state": state,
            "method": method,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:01Z",
            "receipt": {},
            "interpretation": "synthetic",
        }
    }


def _task_readback(client: FakeClient, _: tuple[str, ...]) -> dict[str, object]:
    call = next(item for item in client.calls if "task-create" in item)
    spec = call[call.index("--spec") + 1]
    title = call[call.index("--task-title") + 1]
    return {
        "result": {
            "tasks": [
                {
                    "id": "task_1",
                    "run_id": "run_1",
                    "task_title": title,
                    "spec": spec,
                    "status": "ready",
                }
            ]
        }
    }


def _worker_start(
    root: Path,
    *,
    terminal_handle: str = "term_worker",
    worktree_id: str | None = None,
    dispatch_id: str = "dispatch_1",
) -> OrcaJsonResponse:
    worktree_id = f"repo::{root.resolve()}" if worktree_id is None else worktree_id
    terminal_id = terminal_handle
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": terminal_id}
    response = mutation(
        "request_worker",
        runId="run_1",
        taskId="task_1",
        dispatchId=dispatch_id,
        state="ready",
        stage="input_accepted",
        setup={"state": "not_applicable"},
        launch=launch,
        effects=[
            {"kind": "worktree", "action": "reused", "id": worktree_id},
            {"kind": "setup", "action": "not_applicable", "state": "not_applicable"},
            terminal_effect,
            {"kind": "dispatch_input", "role": "agent", "id": terminal_id, "state": "accepted"},
        ],
        residualResources=[terminal_effect],
    )
    response["_meta"] = {"runtimeId": "runtime_test"}
    return OrcaJsonResponse(response, returncode=0)


def _worker_start_readback(
    root: Path,
    *,
    current_shape: bool = False,
    dispatch_last_failure: object = None,
    worker_last_error: object = None,
    resource_id: str = "terminal-resource-1",
    terminal_handle: str = "term_worker",
    worktree_id: str | None = None,
    dispatch_id: str = "dispatch_1",
) -> dict[str, object]:
    worktree_id = f"repo::{root.resolve()}" if worktree_id is None else worktree_id
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": terminal_handle}
    dispatch: dict[str, object] = {
        "id": dispatch_id,
        "status": "dispatched",
    }
    worker: dict[str, object] = {
        "state": "ready",
        "stage": "input_accepted",
        "effects": [],
        "residualResources": [terminal_effect],
        "startOptions": {
            "worktree": f"path:{root.resolve()}",
            "resolvedWorktreeId": worktree_id,
            "terminal": None,
            "agent": "codex",
            "launch": launch,
            "setup": "not_applicable",
            "setupSource": "existing_worktree",
        },
    }
    if current_shape:
        dispatch.update(
            runId="run_1",
            taskId="task_1",
            task_id="task_1",
            lastFailure=dispatch_last_failure,
        )
        worker.update(
            dispatchId=dispatch_id,
            worktreeId=worktree_id,
            agentTerminalHandle=terminal_handle,
            lastError=worker_last_error,
        )
    else:
        dispatch.update(
            run_id="run_1",
            task_id="task_1",
            last_failure=dispatch_last_failure,
        )
        worker.update(
            worktree_id=worktree_id,
            agent_terminal_handle=terminal_handle,
            last_error=worker_last_error,
        )
    return {
        "result": {
            "dispatch": dispatch,
            "worker": worker,
            "terminalResource": {
                "id": resource_id,
                "ownershipState": "owned",
                "releaseState": "not_requested",
                "retainedReason": None,
                "originDispatchId": dispatch_id,
                "ownerDispatchId": dispatch_id,
                "terminalHandle": terminal_handle,
                "worktreeId": worktree_id,
            },
        },
        "_meta": {"runtimeId": "runtime_test"},
    }


class RealPreflightClient:
    """Public readbacks visible to a worker before worker-start returns."""

    def __init__(self, root: Path, packet: dict[str, object], *, current_shape: bool) -> None:
        self.root = root
        self.packet = packet
        self.current_shape = current_shape
        self.calls: list[tuple[str, ...]] = []

    @staticmethod
    def _wrap(result: dict[str, object]) -> dict[str, object]:
        return {"result": result, "_meta": {"runtimeId": "runtime_test"}}

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        worktree_id = f"repo::{self.root.resolve()}"
        if arguments[:2] == ("terminal", "show"):
            return self._wrap({
                "terminal": {
                    "handle": "term_worker",
                    "worktreeId": worktree_id,
                    "worktreePath": str(self.root.resolve()),
                    "executionHostId": "local",
                    "agentIdentity": "codex",
                    "connected": True,
                    "writable": True,
                    "hostPlatform": "win32",
                }
            })
        if arguments[:2] == ("worktree", "current"):
            return self._wrap({"worktree": {"id": worktree_id, "path": str(self.root.resolve())}})
        if arguments[:2] == ("orchestration", "worker-show"):
            return _worker_start_readback(self.root, current_shape=self.current_shape)
        if arguments[:2] == ("orchestration", "task-list"):
            return self._wrap({
                "tasks": [{
                    "id": "task_1",
                    "run_id": "run_1",
                    "status": "dispatched",
                    "spec": packet_spec(self.packet),
                }]
            })
        raise AssertionError(arguments)


def _worker_start_with_real_preflight(
    root: Path,
    *,
    current_shape: bool,
    later_resource_id: str = "terminal-resource-1",
    later_terminal_handle: str = "term_worker",
    later_worktree_id: str | None = None,
    later_dispatch_id: str = "dispatch_1",
    second_preflight_dispatch_id: str | None = None,
) -> Response:
    def respond(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
        with StateStore(root) as store:
            run = store.select_run(None)
            packet = store.get_packet(run.local_id, str(run.task_id))
            if store.get_worker_resource_binding(run.local_id) is not None:
                raise AssertionError("controller binding existed before the real worker preflight")
        preflight_client = RealPreflightClient(root, packet, current_shape=current_shape)
        result = worker_preflight(
            root,
            run_id="run_1",
            task_id="task_1",
            dispatch_id="dispatch_1",
            packet_id=str(packet["packetId"]),
            client=preflight_client,  # type: ignore[arg-type]
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
            platform="win32",
        )
        if result.get("status") != "admitted" or len(preflight_client.calls) != 4:
            raise AssertionError("real pre-receipt worker preflight did not pass exact public readbacks")
        with StateStore(root) as store:
            run = store.select_run(None)
            if store.get_worker_resource_binding(run.local_id) is not None:
                raise AssertionError("preflight wrote the controller resource binding")
            if joined_preflight_status(store, run) != "pending":
                raise AssertionError("pre-receipt preflight was not a pending join")
        if second_preflight_dispatch_id is not None:
            # A second worker attempt preflights the same Run/Task under another
            # Dispatch before any controller receipt exists.
            second_client = RealPreflightClient(root, packet, current_shape=current_shape)
            try:
                worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id=second_preflight_dispatch_id,
                    packet_id=str(packet["packetId"]),
                    client=second_client,  # type: ignore[arg-type]
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            except OrchestrateError as exc:
                cause = exc.data["cause"] if isinstance(exc.data, dict) else None
                if exc.code != "preflight_rejected" or cause != "preflight_dispatch_conflict":
                    raise AssertionError(
                        f"second-Dispatch preflight was not rejected as a Dispatch conflict: {exc.code}/{cause}"
                    )
            else:
                raise AssertionError("second-Dispatch preflight issued a fresh editing grant")
            if second_client.calls:
                raise AssertionError("second-Dispatch preflight performed native readbacks")
        return _worker_start(
            root,
            terminal_handle=later_terminal_handle,
            worktree_id=later_worktree_id,
            dispatch_id=later_dispatch_id,
        )

    return respond


def _prompt_stall_start(root: Path, *, include_ok: bool = False) -> OrcaJsonResponse:
    worktree_id = f"repo::{root.resolve()}"
    terminal_id = "term_worker"
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": terminal_id}
    response = mutation(
        "request_worker",
        runId="run_1",
        taskId="task_1",
        dispatchId="dispatch_1",
        state="failed",
        stage="dispatch_input",
        failedStage="dispatch_input",
        lastError="agent_prompt_stalled",
        setup={"state": "not_applicable"},
        launch=launch,
        effects=[
            {"kind": "worktree", "action": "reused", "id": worktree_id},
            {"kind": "setup", "action": "not_applicable", "state": "not_applicable"},
            terminal_effect,
        ],
        residualResources=[terminal_effect],
    )
    response["_meta"] = {"runtimeId": "runtime_test"}
    if include_ok:
        response["ok"] = True
    return OrcaJsonResponse(response, returncode=1)


def _prompt_stall_readback(root: Path, *, current_shape: bool = False) -> dict[str, object]:
    worktree_id = f"repo::{root.resolve()}"
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": "term_worker"}
    dispatch: dict[str, object] = {"id": "dispatch_1", "status": "failed"}
    worker: dict[str, object] = {
        "state": "failed",
        "stage": "dispatch_input",
        "residualResources": [terminal_effect],
        "startOptions": {
            "worktree": f"path:{root.resolve()}",
            "resolvedWorktreeId": worktree_id,
            "terminal": None,
            "agent": "codex",
            "launch": launch,
            "setup": "not_applicable",
            "setupSource": "existing_worktree",
        },
    }
    if current_shape:
        dispatch.update(
            runId="run_1",
            taskId="task_1",
            task_id="task_1",
            lastFailure="agent_prompt_stalled",
        )
        worker.update(
            dispatchId="dispatch_1",
            lastError="agent_prompt_stalled",
            worktreeId=worktree_id,
            agentTerminalHandle="term_worker",
        )
    else:
        dispatch.update(
            run_id="run_1",
            task_id="task_1",
            last_failure="agent_prompt_stalled",
        )
        worker.update(
            last_error="agent_prompt_stalled",
            worktree_id=worktree_id,
            agent_terminal_handle="term_worker",
        )
    return {
        "result": {
            "dispatch": dispatch,
            "worker": worker,
            "terminalResource": {
                "id": "terminal-resource-1",
                "ownershipState": "owned",
                "releaseState": "not_requested",
                "retainedReason": None,
                "originDispatchId": "dispatch_1",
                "ownerDispatchId": "dispatch_1",
                "terminalHandle": "term_worker",
                "worktreeId": worktree_id,
            },
        }
    }


def _released_prompt_stall_readback(root: Path, *, current_shape: bool = False) -> dict[str, object]:
    worktree_id = f"repo::{root.resolve()}"
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": "term_worker"}
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    dispatch: dict[str, object] = {"id": "dispatch_1", "status": "failed"}
    worker: dict[str, object] = {
        "state": "failed",
        "stage": "dispatch_input",
        "effects": [
            {"kind": "worktree", "action": "reused", "id": worktree_id},
            {"kind": "setup", "action": "not_applicable", "state": "not_applicable"},
            terminal_effect,
        ],
        "residualResources": [terminal_effect],
        "startOptions": {
            "worktree": f"path:{root.resolve()}",
            "resolvedWorktreeId": worktree_id,
            "terminal": None,
            "agent": "codex",
            "launch": launch,
            "setup": "not_applicable",
            "setupSource": "existing_worktree",
        },
    }
    if current_shape:
        dispatch.update(
            runId="run_1",
            taskId="task_1",
            task_id="task_1",
            lastFailure="agent_prompt_stalled",
        )
        worker.update(
            dispatchId="dispatch_1",
            worktreeId=worktree_id,
            agentTerminalHandle="term_worker",
            lastError="agent_prompt_stalled",
        )
    else:
        dispatch.update(
            run_id="run_1",
            task_id="task_1",
            last_failure="agent_prompt_stalled",
        )
        worker.update(
            worktree_id=worktree_id,
            agent_terminal_handle="term_worker",
            last_error="agent_prompt_stalled",
        )
    return {
        "result": {
            "dispatch": dispatch,
            "worker": worker,
            "projection": {
                "id": "dispatch_1",
                "dispatchId": "dispatch_1",
                "taskId": "task_1",
                "runId": "run_1",
                "stage": {
                    "worker": "failed",
                    "dispatch": "failed",
                    "detail": "dispatch_input",
                    "activity": "unknown",
                },
                "outcome": "failed",
                "liveness": {"verdict": "exited", "source": "resource_release"},
                "resource": {
                    "state": "released",
                    "id": "terminal-resource-1",
                    "ownerDispatchId": "dispatch_1",
                    "releaseState": "released",
                    "terminalState": "released",
                }
            },
            "terminal": None,
            "observation": {"status": "missing", "exactWorker": False},
            "terminalResource": {
                "id": "terminal-resource-1",
                "ownershipState": "released",
                "releaseState": "released",
                "retainedReason": None,
                "originDispatchId": "dispatch_1",
                "ownerDispatchId": "dispatch_1",
                "terminalHandle": "term_worker",
                "worktreeId": worktree_id,
                "endpointId": None,
                "endpointIncarnation": None,
                "releaseRequestedAt": "2026-01-01T00:00:00Z",
                "releaseCompletedAt": "2026-01-01T00:00:01Z",
                "releaseError": None,
                "recoveryAttemptCount": 0,
                "lastRecoveryAt": None,
                "archive": {"source": "transcript", "status": "captured"},
            },
        }
    }


def _release_worker_show(
    resource: Mapping[str, object],
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    status: str = "completed",
    current_shape: bool = False,
) -> dict[str, object]:
    last_failure = None if status == "completed" else "worker_failed"
    dispatch: dict[str, object] = {"id": dispatch_id, "status": status}
    worker: dict[str, object] = {"state": "succeeded" if status == "completed" else "failed", "stage": "settled"}
    if current_shape:
        dispatch.update(
            runId=run_id,
            taskId=task_id,
            task_id=task_id,
            lastFailure=last_failure,
        )
        worker.update(
            dispatchId=dispatch_id,
            worktreeId=resource.get("worktreeId"),
            agentTerminalHandle=resource.get("terminalHandle"),
            lastError=last_failure,
        )
    else:
        dispatch.update(run_id=run_id, task_id=task_id, last_failure=last_failure)
        worker.update(
            worktree_id=resource.get("worktreeId"),
            agent_terminal_handle=resource.get("terminalHandle"),
            last_error=last_failure,
        )
    return {
        "result": {
            "dispatch": dispatch,
            "worker": worker,
            "terminal": None if resource.get("releaseState") == "released" else {"handle": resource.get("terminalHandle")},
            "terminalResource": dict(resource),
        }
    }


def _released_resource(
    dispatch_id: str,
    *,
    resource_id: str = "terminal-resource-1",
    terminal_handle: str = "term_worker",
    worktree_id: str = "repo::fixture",
) -> dict[str, object]:
    return {
        "id": resource_id,
        "ownershipState": "released",
        "releaseState": "released",
        "retainedReason": None,
        "originDispatchId": dispatch_id,
        "ownerDispatchId": dispatch_id,
        "terminalHandle": terminal_handle,
        "worktreeId": worktree_id,
        "releaseRequestedAt": "2026-01-01T00:00:00Z",
        "releaseCompletedAt": "2026-01-01T00:00:01Z",
        "releaseError": None,
        "archive": {"source": "transcript", "status": "captured"},
    }


def _contradict_release_semantics(
    payload: dict[str, object],
    *,
    current_shape: bool,
    field: str,
    original_terminal: str = "term_worker",
) -> None:
    result = payload["result"]
    assert isinstance(result, dict)
    dispatch = result["dispatch"]
    worker = result["worker"]
    assert isinstance(dispatch, dict)
    assert isinstance(worker, dict)
    if field == "dispatch_status":
        dispatch["status"] = "dispatched"
    elif field == "dispatch_last_failure":
        dispatch["lastFailure" if current_shape else "last_failure"] = "contradictory_failure"
    elif field == "worker_state":
        worker["state"] = "ready"
    elif field == "worker_stage":
        worker["stage"] = "input_accepted"
    elif field == "worker_last_error":
        worker["lastError" if current_shape else "last_error"] = "contradictory_failure"
    elif field == "worker_terminal_handle":
        worker["agentTerminalHandle" if current_shape else "agent_terminal_handle"] = None
    elif field == "attached_terminal":
        result["terminal"] = {"handle": original_terminal}
    else:  # pragma: no cover - test helper guard
        raise AssertionError(f"Unknown semantic contradiction: {field}")


def _record_fixture_binding(
    store: StateStore,
    run_local_id: str,
    *,
    dispatch_id: str = "dispatch_1",
    resource_id: str = "terminal-resource-1",
    terminal_handle: str = "term_worker",
    worktree_id: str = "repo::fixture",
) -> None:
    store.record_worker_resource_binding(
        run_local_id,
        dispatch_id=dispatch_id,
        resource_id=resource_id,
        terminal_handle=terminal_handle,
        worktree_id=worktree_id,
        readback={"fixture": "validated-start-readback"},
    )


def _worker_start_with_preflight(root: Path) -> Response:
    def respond(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
        response = _worker_start(root)
        with StateStore(root) as store:
            run = store.select_run(None)
            packet_json = store.get_packet_json(run.local_id, str(run.task_id))
            packet = store.get_packet(run.local_id, str(run.task_id))
            launch = packet["admission"]["launch"]
            reader = packet["reader"]
            observation = {
                "schema": "orchestrate-worker-preflight/v1",
                "outcome": "passed",
                "runId": "run_1",
                "taskId": "task_1",
                "dispatchId": "dispatch_1",
                "packetId": packet["packetId"],
                "packetJsonSha256": hashlib.sha256(packet_json.encode("utf-8")).hexdigest(),
                "expectedSourceDigest": run.source_digest,
                "observedSourceDigest": run.source_digest,
                "profileDigest": packet["profileDigest"],
                "routingDigest": hashlib.sha256(
                    json.dumps(reader, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                "candidate": packet["candidate"],
                "native": {
                    "runtimeId": "runtime_test",
                    "actor": launch["agent"],
                    "terminalHandle": "term_worker",
                    "terminalResourceId": "terminal-resource-1",
                    "executionHostId": "local",
                    "hostPlatform": "win32",
                    "worktreeId": f"repo::{root.resolve()}",
                    "worktreeRoot": str(root.resolve()),
                    "launch": launch,
                },
                "observedAt": "2026-01-01T00:00:00Z",
                "limitations": [],
                "mismatches": [],
            }
            store.record_preflight(
                run.local_id,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                observation=observation,
            )
        return response

    return respond


class MilestoneClient:
    """Stateful public-contract fixture for the production --plan path."""

    def __init__(
        self,
        root: Path,
        *,
        verification_result: str = "accepted",
        review_result: str = "accepted",
        uncertain_task: str | None = None,
        pause_after_owner: bool = False,
    ) -> None:
        self.root = root
        self.verification_result = verification_result
        self.review_result = review_result
        self.uncertain_task = uncertain_task
        self.calls: list[tuple[str, ...]] = []
        self.tasks: dict[str, dict[str, object]] = {}
        self.gates: dict[str, dict[str, object]] = {}
        self.workers: dict[str, dict[str, object]] = {}
        self.next_task = 1
        self.next_gate = 1
        self.next_delivery = 1
        self.current_run = "run_1"
        self.peak_active_workers = 0
        self.pause_after_owner = pause_after_owner
        self.deliveries_paused = False

    def _wrapped(self, value: dict[str, object], *, returncode: int = 0) -> OrcaJsonResponse:
        value.setdefault("_meta", {"runtimeId": "runtime_test"})
        return OrcaJsonResponse(value, returncode=returncode)

    def _refresh_tasks(self) -> None:
        for task_id, task in self.tasks.items():
            if task["status"] in {"completed", "failed", "dispatched"}:
                continue
            dependencies = task["deps"]
            deps_done = all(self.tasks[item]["status"] == "completed" for item in dependencies)  # type: ignore[union-attr]
            gate = next((item for item in self.gates.values() if item["task_id"] == task_id), None)
            gate_open = gate is None or (gate["status"] == "resolved" and gate["resolution"] == "accepted")
            task["status"] = "ready" if deps_done and gate_open else "pending"

    def _launch(self, task_id: str, arguments: tuple[str, ...]) -> OrcaJsonResponse:
        if self.uncertain_task == task_id:
            self.uncertain_task = None
            raise OrcaCommandError(
                "synthetic lost milestone launch receipt",
                OrcaCommandResult(arguments, -1, "", "lost", None),
            )
        dispatch_id = f"dispatch_{task_id.removeprefix('task_')}"
        terminal = "term_worker" if task_id == "task_1" else f"term_{task_id.removeprefix('task_')}"
        resource = f"resource_{task_id.removeprefix('task_')}"
        worktree = f"repo::{self.root.resolve()}"
        requested = {"agent": arguments[arguments.index("--agent") + 1]}
        if "--model" in arguments:
            requested["model"] = arguments[arguments.index("--model") + 1]
        if "--effort" in arguments:
            requested["effort"] = arguments[arguments.index("--effort") + 1]
        launch = {"requested": requested, "effective": dict(requested)}
        self.tasks[task_id]["status"] = "dispatched"
        self.workers[dispatch_id] = {
            "task_id": task_id,
            "terminal": terminal,
            "resource": resource,
            "worktree": worktree,
            "launch": launch,
            "state": "ready",
            "released": False,
            "delivery_sent": False,
        }
        self.peak_active_workers = max(
            self.peak_active_workers,
            sum(record["state"] == "ready" for record in self.workers.values()),
        )
        response = mutation(
            f"request_start_{task_id}",
            runId="run_1",
            taskId=task_id,
            dispatchId=dispatch_id,
            state="ready",
            stage="input_accepted",
            setup={"state": "not_applicable"},
            launch=launch,
            effects=[
                {"kind": "worktree", "action": "reused", "id": worktree},
                {"kind": "setup", "action": "not_applicable", "state": "not_applicable"},
                {"kind": "terminal", "role": "agent", "action": "created", "id": terminal},
                {"kind": "dispatch_input", "role": "agent", "id": terminal, "state": "accepted"},
            ],
            residualResources=[{"kind": "terminal", "role": "agent", "action": "created", "id": terminal}],
        )
        response["_meta"] = {"runtimeId": "runtime_test"}
        self._record_preflight(task_id, dispatch_id)
        return self._wrapped(response)

    def _record_preflight(self, task_id: str, dispatch_id: str) -> None:
        with StateStore(self.root) as store:
            run = store.select_run(None)
            packet = store.get_packet(run.local_id, task_id)
            packet_id = packet["packetId"]
        worker_preflight(
            self.root,
            run_id="run_1",
            task_id=task_id,
            dispatch_id=dispatch_id,
            packet_id=packet_id,
            client=self,  # type: ignore[arg-type]
            environment={"ORCA_TERMINAL_HANDLE": str(self.workers[dispatch_id]["terminal"])},
            platform="win32",
        )

    def _worker_show(self, dispatch_id: str) -> dict[str, object]:
        record = self.workers[dispatch_id]
        task_id = record["task_id"]
        outcome = record["state"]
        settled = outcome in {"succeeded", "failed"}
        failed = outcome == "failed"
        released = bool(record["released"])
        terminal = record["terminal"]
        worktree = record["worktree"]
        resource = record["resource"]
        return {
            "result": {
                "dispatch": {
                    "id": dispatch_id,
                    "runId": "run_1",
                    "taskId": task_id,
                    "task_id": task_id,
                    "status": "failed" if failed else "completed" if settled else "dispatched",
                    "lastFailure": "worker_failed" if failed else None,
                },
                "worker": {
                    "dispatchId": dispatch_id,
                    "worktreeId": worktree,
                    "agentTerminalHandle": terminal,
                    "agent": record["launch"]["requested"]["agent"],  # type: ignore[index]
                    "lastError": "worker_failed" if failed else None,
                    "state": outcome if settled else "ready",
                    "stage": "settled" if settled else "input_accepted",
                    "residualResources": [
                        {"kind": "terminal", "role": "agent", "action": "created", "id": terminal}
                    ],
                    "startOptions": {
                        "worktree": f"path:{self.root.resolve()}",
                        "resolvedWorktreeId": worktree,
                        "terminal": None,
                        "agent": record["launch"]["requested"]["agent"],  # type: ignore[index]
                        "launch": record["launch"],
                        "setup": "not_applicable",
                        "setupSource": "existing_worktree",
                    },
                },
                "terminal": None if released else {
                    "handle": terminal,
                    "ptyId": f"pty_{dispatch_id}",
                    "incarnationId": f"incarnation_{dispatch_id}",
                    "worktreeId": worktree,
                    "executionHostId": "local",
                    "agentIdentity": record["launch"]["requested"]["agent"],  # type: ignore[index]
                },
                "terminalResource": {
                    "id": resource,
                    "ownershipState": "released" if released else "owned",
                    "releaseState": "released" if released else "not_requested",
                    "retainedReason": None,
                    "originDispatchId": dispatch_id,
                    "ownerDispatchId": dispatch_id,
                    "terminalHandle": terminal,
                    "worktreeId": worktree,
                    "endpointId": f"endpoint_{dispatch_id}",
                    "endpointIncarnation": f"endpoint_incarnation_{dispatch_id}",
                    "releaseRequestedAt": "2026-01-01T00:00:00Z" if released else None,
                    "releaseCompletedAt": "2026-01-01T00:00:01Z" if released else None,
                    "releaseError": None,
                    "archive": {"source": "transcript", "status": "captured"} if released else None,
                },
            },
            "_meta": {"runtimeId": "runtime_test"},
        }

    def _delivery(self) -> dict[str, object]:
        active = next(
            record
            for record in self.workers.values()
            if record["state"] in {"ready", "succeeded", "failed"} and not record["delivery_sent"]
        )
        task_id = active["task_id"]
        dispatch_id = next(key for key, value in self.workers.items() if value is active)
        if active["state"] == "ready":
            active["state"] = "succeeded"
        outcome = str(active["state"])
        active["delivery_sent"] = True
        self.tasks[task_id]["status"] = "completed" if outcome == "succeeded" else "failed"
        payload: dict[str, object] = {"taskId": task_id, "dispatchId": dispatch_id, "outcome": outcome}
        if task_id != "task_1" and outcome == "succeeded":
            with StateStore(self.root) as store:
                run = store.select_run(None)
                packet = store.get_packet(run.local_id, task_id)
            role = packet["scope"]["role"]
            result_outcome = self.verification_result if role == "specialist" else self.review_result
            if result_outcome != "missing":
                result_path = Path(packet["outputs"]["milestoneResult"]["path"])
                result_path.write_text(
                    json.dumps(
                        {
                            "schema": "orchestrate-milestone-result/v1",
                            "taskKey": packet["scope"]["taskKey"],
                            "taskId": task_id,
                            "dispatchId": dispatch_id,
                            "candidateDigest": packet["milestone"]["candidateDigest"],
                            "contractDigest": packet["milestone"]["contractDigest"],
                            "outcome": result_outcome,
                        }
                    ),
                    encoding="utf-8",
                )
                payload["reportPath"] = str(result_path)
        delivery_id = f"delivery_{self.next_delivery}"
        self.next_delivery += 1
        return {
            "result": {
                "deliveryId": delivery_id,
                "messages": [
                    lifecycle_message(
                        "worker_done",
                        payload,
                        message_id=f"message_{task_id}",
                        dispatch_id=dispatch_id,
                        from_handle=active["terminal"],  # type: ignore[arg-type]
                        subject="done",
                    )
                ],
            }
        }

    def settle_without_delivery(self, dispatch_id: str, *, outcome: str) -> None:
        if outcome not in {"succeeded", "failed"}:
            raise AssertionError(f"unsupported synthetic outcome: {outcome}")
        record = self.workers[dispatch_id]
        if record["delivery_sent"]:
            raise AssertionError("synthetic worker Delivery was already consumed")
        record["state"] = outcome
        self.tasks[str(record["task_id"])]["status"] = "completed" if outcome == "succeeded" else "failed"

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        command = arguments[:2]
        if command == ("terminal", "show"):
            terminal = arguments[arguments.index("--terminal") + 1]
            record = next(item for item in self.workers.values() if item["terminal"] == terminal)
            return self._wrapped(
                {
                    "result": {
                        "terminal": {
                            "handle": terminal,
                            "connected": True,
                            "writable": True,
                            "executionHostId": "local",
                            "agentIdentity": record["launch"]["requested"]["agent"],  # type: ignore[index]
                            "hostPlatform": "win32",
                            "worktreeId": record["worktree"],
                            "worktreePath": str(self.root.resolve()),
                        }
                    },
                    "_meta": {"runtimeId": "runtime_test"},
                }
            )
        if command == ("worktree", "current"):
            record = next(item for item in self.workers.values() if item["state"] == "ready")
            return self._wrapped(
                {
                    "result": {
                        "worktree": {
                            "id": record["worktree"],
                            "path": str(self.root.resolve()),
                        }
                    },
                    "_meta": {"runtimeId": "runtime_test"},
                }
            )
        if command == ("orchestration", "run-create"):
            return self._wrapped(mutation("request_run", run={"id": "run_1"}))
        if command == ("orchestration", "run-show"):
            return self._wrapped({"result": {"run": {"id": "run_1", "objective": self._objective()}}})
        if command == ("orchestration", "run-use"):
            return self._wrapped(mutation("request_run_use", run={"id": "run_1"}))
        if command == ("orchestration", "run-current"):
            return self._wrapped({"result": {"run": {"id": "run_1"}}})
        if command == ("orchestration", "task-create"):
            task_id = f"task_{self.next_task}"
            self.next_task += 1
            dependencies = json.loads(arguments[arguments.index("--deps") + 1]) if "--deps" in arguments else []
            self.tasks[task_id] = {
                "id": task_id,
                "run_id": "run_1",
                "task_title": arguments[arguments.index("--task-title") + 1],
                "spec": arguments[arguments.index("--spec") + 1],
                "deps": dependencies,
                "status": "pending" if dependencies else "ready",
            }
            self._refresh_tasks()
            return self._wrapped(mutation(f"request_{task_id}", task={"id": task_id}))
        if command == ("orchestration", "task-list"):
            self._refresh_tasks()
            rows = list(self.tasks.values())
            if "--ready" in arguments:
                rows = [row for row in rows if row["status"] == "ready"]
            return self._wrapped({"result": {"tasks": [dict(row) for row in rows]}})
        if command == ("orchestration", "gate-create"):
            gate_id = f"gate_{self.next_gate}"
            self.next_gate += 1
            task_id = arguments[arguments.index("--task") + 1]
            gate = {
                "id": gate_id,
                "task_id": task_id,
                "question": arguments[arguments.index("--question") + 1],
                "status": "pending",
                "resolution": None,
            }
            self.gates[gate_id] = gate
            self._refresh_tasks()
            return self._wrapped(mutation(f"request_{gate_id}", gate=dict(gate)))
        if command == ("orchestration", "gate-list"):
            task_id = arguments[arguments.index("--task") + 1]
            return self._wrapped(
                {"result": {"gates": [dict(gate) for gate in self.gates.values() if gate["task_id"] == task_id]}}
            )
        if command == ("orchestration", "gate-resolve"):
            gate = self.gates[arguments[arguments.index("--id") + 1]]
            gate["status"] = "resolved"
            gate["resolution"] = arguments[arguments.index("--resolution") + 1]
            self._refresh_tasks()
            return self._wrapped(mutation(f"request_resolve_{gate['id']}", gate=dict(gate)))
        if command == ("orchestration", "worker-start"):
            return self._launch(arguments[arguments.index("--task") + 1], arguments)
        if command == ("orchestration", "worker-show"):
            return self._wrapped(self._worker_show(arguments[arguments.index("--dispatch") + 1]))
        if command == ("orchestration", "worker-release"):
            dispatch_id = arguments[arguments.index("--dispatch") + 1]
            self.workers[dispatch_id]["released"] = True
            return self._wrapped(
                mutation(
                    f"request_release_{dispatch_id}",
                    dispatchId=dispatch_id,
                    state="released",
                    processAction="closed",
                    archive={"source": "transcript", "status": "captured"},
                )
            )
        if command == ("orchestration", "check") and "--ack" in arguments:
            if self.pause_after_owner and arguments[arguments.index("--ack") + 1] == "delivery_1":
                self.deliveries_paused = True
            return self._wrapped(acknowledgement(arguments[arguments.index("--ack") + 1]))
        if command == ("orchestration", "check"):
            if self.deliveries_paused:
                return self._wrapped({"result": {"deliveryId": None, "messages": [], "timedOut": True}})
            return self._wrapped(self._delivery())
        raise AssertionError(arguments)

    def _objective(self) -> str:
        with StateStore(self.root) as store:
            return store.select_for_read(None).objective


class ReportingClient:
    """Non-mutating native readback stub for status-only tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if arguments[:2] == ("orchestration", "run-show"):
            return {"result": {"run": {"id": arguments[arguments.index("--id") + 1]}}}
        if arguments[:2] == ("orchestration", "task-list"):
            return {"result": {"tasks": []}}
        if arguments[:2] == ("orchestration", "worker-show"):
            dispatch = arguments[arguments.index("--dispatch") + 1]
            return {"result": {"dispatch": {"id": dispatch}}}
        raise AssertionError(arguments)


def settlement_responses(
    outcome: str = "succeeded",
    *,
    worktree_id: str = "repo::fixture",
    current_shape: bool = False,
    release_state: str = "released",
) -> list[Response]:
    native_status = "completed" if outcome == "succeeded" else "failed"
    dispatch: dict[str, object] = {"id": "dispatch_1", "status": native_status}
    released_worker: dict[str, object] = {"state": outcome, "stage": "settled"}
    if current_shape:
        dispatch.update(
            runId="run_1",
            taskId="task_1",
            task_id="task_1",
            lastFailure=None if outcome == "succeeded" else "worker_failed",
        )
        released_worker.update(
            dispatchId="dispatch_1",
            worktreeId=worktree_id,
            agentTerminalHandle="term_worker",
            lastError=None if outcome == "succeeded" else "worker_failed",
        )
    else:
        dispatch.update(
            run_id="run_1",
            task_id="task_1",
            last_failure=None if outcome == "succeeded" else "worker_failed",
        )
        released_worker.update(
            worktree_id=worktree_id,
            agent_terminal_handle="term_worker",
            last_error=None if outcome == "succeeded" else "worker_failed",
        )
    return [
        {
            "result": {
                "dispatch": dispatch,
            }
        },
        {"result": {"tasks": [{"id": "task_1", "run_id": "run_1", "status": native_status}]}},
        mutation(
            "request_release",
            dispatchId="dispatch_1",
            state=release_state,
            processAction="closed",
            archive=None,
        ),
        {
            "result": {
                "dispatch": dispatch,
                "worker": released_worker,
                "terminal": None,
                "terminalResource": {
                    "id": "terminal-resource-1",
                    "ownershipState": "released",
                    "releaseState": "released",
                    "retainedReason": None,
                    "originDispatchId": "dispatch_1",
                    "ownerDispatchId": "dispatch_1",
                    "terminalHandle": "term_worker",
                    "worktreeId": worktree_id,
                    "releaseRequestedAt": "2026-01-01T00:00:00Z",
                    "releaseCompletedAt": "2026-01-01T00:00:01Z",
                    "releaseError": None,
                    "archive": {"source": "transcript", "status": "captured"},
                },
            }
        },
    ]


def completion_responses(
    root: Path,
    objective: str,
    outcome: str = "succeeded",
    *,
    delivery_observed: threading.Event | None = None,
) -> list[Response]:
    delivery_payload = {
        "result": {
            "deliveryId": "delivery_1",
            "messages": [
                lifecycle_message(
                    "worker_done",
                    {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": outcome},
                    message_id="message_done",
                    subject="done",
                )
            ],
        }
    }
    delivery: Response = delivery_payload
    if delivery_observed is not None:
        def observed_delivery(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            delivery_observed.set()
            return delivery_payload

        delivery = observed_delivery
    return [
        mutation("request_run", run={"id": "run_1"}),
        {"result": {"run": {"id": "run_1", "objective": objective}}},
        mutation("request_task", task={"id": "task_1"}),
        _task_readback,
        _worker_start_with_preflight(root),
        _worker_start_readback(root),
        delivery,
        *settlement_responses(outcome, worktree_id=f"repo::{root.resolve()}"),
        acknowledgement("delivery_1"),
    ]


def identity_responses(
    root: Path,
    *,
    agent: str | None = None,
    workers: list[dict[str, object]] | None = None,
    run_id: str | None = None,
) -> list[Response]:
    terminal: dict[str, object] = {
        "handle": "term_plain",
        "worktreeId": "worktree_1",
        "worktreePath": str(root),
        "executionHostId": "local",
        "connected": True,
        "writable": True,
    }
    if agent is not None:
        terminal["agentIdentity"] = agent
    return [
        {"result": {"terminal": terminal}},
        {"result": {"worktree": {"id": "worktree_1", "path": str(root)}}},
        {"result": {"workers": [] if workers is None else workers}},
        {"result": {"run": None if run_id is None else {"id": run_id}}},
    ]


class MilestoneRepo(unittest.TestCase):
    def setUp(self) -> None:
        self.project_temp = tempfile.TemporaryDirectory()
        self.state_temp = tempfile.TemporaryDirectory()
        self.root = Path(self.project_temp.name)
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Preserve WIP.\n", encoding="utf-8")
        (self.root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")
        self.environment = patch.dict(
            os.environ,
            {
                "ORCHESTRATE_HOME": self.state_temp.name,
                "ORCA_TERMINAL_HANDLE": "term_plain",
                "ORCA_AGENT_HOOK_TOKEN": "ordinary-hook-credential",
                "ORCA_AGENT_HOOK_ENDPOINT": "ordinary-hook-endpoint",
                "ORCA_AGENT_LAUNCH_TOKEN": "",
            },
        )
        self.environment.start()
        setup_project(self.root)

    def tearDown(self) -> None:
        self.environment.stop()
        self.project_temp.cleanup()
        self.state_temp.cleanup()

    def _run_event_backed(
        self,
        action: Callable[[], dict[str, object]],
        observed: threading.Event,
        *,
        description: str,
    ) -> dict[str, object]:
        completed = threading.Event()
        outcome: dict[str, object] = {}

        def run_action() -> None:
            try:
                outcome["report"] = action()
            except BaseException as exc:
                outcome["error"] = exc
            finally:
                completed.set()

        worker = threading.Thread(target=run_action, daemon=True)
        worker.start()
        self.assertTrue(
            completed.wait(LIVENESS_TIMEOUT),
            f"{description} exceeded its outer liveness bound",
        )
        if "error" in outcome:
            raise outcome["error"]  # type: ignore[misc]
        report = outcome.get("report")
        if not isinstance(report, dict):
            self.fail(f"{description} did not return a report")
        self.assertTrue(observed.is_set(), f"{description} did not observe its completion event")
        return report

    def _write_milestone_plan(
        self,
        objective: str,
        *,
        specialist_keys: tuple[str, ...] = ("verify",),
        max_workers: int = 2,
    ) -> str:
        relative = "milestone-plan.json"
        specialist_tasks = [
            {
                "key": key,
                "title": f"Verify the integrated candidate ({key})",
                "spec": f"Read only the exact candidate and report focused verification for {key}.",
                "role": "specialist",
                "dependencies": ["owner"],
                "gate": "verification",
            }
            for key in specialist_keys
        ]
        (self.root / relative).write_text(
            json.dumps(
                {
                    "schema": "orchestrate-milestone-plan/v1",
                    "contract": {"interface": "frozen-v1", "checks": ["focused"]},
                    "maxWorkers": max_workers,
                    "tasks": [
                        {
                            "key": "owner",
                            "title": "Integrate the authorized objective",
                            "spec": objective,
                            "role": "owner",
                            "dependencies": [],
                            "gate": "integration",
                        },
                        *specialist_tasks,
                        {
                            "key": "review",
                            "title": "Review the verified candidate",
                            "spec": "Independently review only the exact verified candidate.",
                            "role": "reviewer",
                            "dependencies": ["owner", *specialist_keys],
                            "gate": "review",
                        },
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        git(self.root, "add", relative)
        git(self.root, "commit", "-qm", "add milestone plan")
        return relative


class ControllerTests(MilestoneRepo):
    def test_packet_uses_the_existing_read_only_state_store(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="read one packet", profile_digest="p", source_digest="s")
            run = store.update_run(run.local_id, task_id="task_packet")
            store.save_packet(run.local_id, "task_packet", '{"schema":"fixture-packet/v1"}')

        observed_modes: list[bool] = []
        original_init = StateStore.__init__

        def observe_init(instance: StateStore, *arguments: object, **keywords: object) -> None:
            observed_modes.append(keywords.get("read_only") is True)
            original_init(instance, *arguments, **keywords)  # type: ignore[arg-type]

        with patch.object(StateStore, "__init__", new=observe_init):
            observed = packet(self.root, run.local_id, "task_packet")
        self.assertEqual(observed, {"schema": "fixture-packet/v1"})
        self.assertEqual(observed_modes, [True])

    def test_public_implement_prelaunch_accepts_bound_milestone_plan_source(self) -> None:
        objective = "Validate the bound milestone plan before native launch"
        plan = self._write_milestone_plan(objective)

        class PrelaunchReached(RuntimeError):
            pass

        class PrelaunchProbe(MilestoneClient):
            def _launch(self, task_id: str, arguments: tuple[str, ...]) -> OrcaJsonResponse:
                raise PrelaunchReached(f"validated {task_id}")

        client = PrelaunchProbe(self.root)
        with self.assertRaisesRegex(PrelaunchReached, "validated task_1"):
            implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertTrue(any(call[:2] == ("orchestration", "worker-start") for call in client.calls))

    def test_public_implement_records_exceptional_grant_before_native_launch(self) -> None:
        objective = "Grant four bounded workers before the first launch"
        plan = self._write_milestone_plan(
            objective,
            specialist_keys=("verify_a", "verify_b", "verify_c"),
            max_workers=4,
        )

        class PrelaunchReached(RuntimeError):
            pass

        class PrelaunchProbe(MilestoneClient):
            def _launch(self, task_id: str, arguments: tuple[str, ...]) -> OrcaJsonResponse:
                with StateStore(self.root, home=Path(self.state_home)) as store:
                    run = store.select_run(None)
                    grants = [item for item in store.evidence(run.local_id) if item["kind"] == "capacity-grant"]
                self.asserted_grants = grants
                raise PrelaunchReached(f"validated {task_id}")

        client = PrelaunchProbe(self.root)
        client.state_home = self.state_temp.name
        with self.assertRaisesRegex(PrelaunchReached, "validated task_1"):
            implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                allow_exceptional_capacity=True,
                capacity_reason="four independent bounded checks",
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(len(client.asserted_grants), 1)
        payload = client.asserted_grants[0]["payload"]
        self.assertEqual(payload["runId"], "run_1")
        self.assertEqual(payload["limit"], 4)
        self.assertEqual(payload["reason"], "four independent bounded checks")

    def test_public_resume_can_record_exceptional_grant_before_first_launch(self) -> None:
        objective = "Resume a held four-worker Run with an exact grant"
        plan = self._write_milestone_plan(
            objective,
            specialist_keys=("verify_a", "verify_b", "verify_c"),
            max_workers=4,
        )
        client = MilestoneClient(self.root)
        with self.assertRaises(OrchestrateError) as held:
            implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(held.exception.code, "exceptional_capacity_required")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in client.calls))

        class PrelaunchReached(RuntimeError):
            pass

        original_launch = client._launch

        def stop_at_launch(task_id: str, arguments: tuple[str, ...]) -> OrcaJsonResponse:
            with StateStore(self.root, home=Path(self.state_temp.name)) as store:
                run = store.select_run(None)
                grants = [item for item in store.evidence(run.local_id) if item["kind"] == "capacity-grant"]
            self.assertEqual(len(grants), 1)
            self.assertEqual(grants[0]["payload"]["limit"], 4)
            raise PrelaunchReached(f"validated {task_id}")

        client._launch = stop_at_launch  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(PrelaunchReached, "validated task_1"):
                resume(
                    self.root,
                    None,
                    client=client,  # type: ignore[arg-type]
                    milestone_plan=plan,
                    allow_exceptional_capacity=True,
                    capacity_reason="resume four independent bounded checks",
                    wait_timeout_ms=60_000,
                    require_context=False,
                )
        finally:
            client._launch = original_launch  # type: ignore[method-assign]

    @requires_native_windows_admission
    def test_production_controller_executes_tracked_native_milestone_plan(self) -> None:
        objective = "Integrate the exact bounded milestone"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )

        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "review_accepted")
        self.assertEqual(report["admission"], "admitted")
        self.assertEqual(len([call for call in client.calls if call[:2] == ("orchestration", "task-create")]), 3)
        self.assertEqual(len([call for call in client.calls if call[:2] == ("orchestration", "gate-create")]), 2)
        self.assertEqual(len([call for call in client.calls if call[:2] == ("orchestration", "gate-resolve")]), 2)
        self.assertEqual(len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")]), 3)
        self.assertEqual(
            [item["result_outcome"] for item in report["milestone"]["tasks"]],
            ["accepted", "accepted"],
        )
        self.assertEqual(
            [item["admission"] for item in report["milestone"]["tasks"]],
            ["admitted", "admitted"],
        )

    @requires_native_windows_admission
    def test_production_controller_executes_wide_ready_frontier_in_bounded_waves(self) -> None:
        objective = "Execute every specialist in deterministic bounded waves"
        specialists = ("verify_a", "verify_b", "verify_c")
        plan = self._write_milestone_plan(objective, specialist_keys=specialists)
        client = MilestoneClient(self.root)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )

        starts = [call for call in client.calls if call[:2] == ("orchestration", "worker-start")]
        started_task_ids = [call[call.index("--task") + 1] for call in starts]
        self.assertEqual(started_task_ids, ["task_1", "task_2", "task_3", "task_4", "task_5"])
        self.assertEqual(client.peak_active_workers, 2)
        third_specialist_start = next(
            index
            for index, call in enumerate(client.calls)
            if call[:2] == ("orchestration", "worker-start")
            and call[call.index("--task") + 1] == "task_4"
        )
        first_specialist_ack = next(
            index
            for index, call in enumerate(client.calls)
            if call[:2] == ("orchestration", "check")
            and "--ack" in call
            and call[call.index("--ack") + 1] == "delivery_2"
        )
        reviewer_start = next(
            index
            for index, call in enumerate(client.calls)
            if call[:2] == ("orchestration", "worker-start")
            and call[call.index("--task") + 1] == "task_5"
        )
        final_specialist_ack = next(
            index
            for index, call in enumerate(client.calls)
            if call[:2] == ("orchestration", "check")
            and "--ack" in call
            and call[call.index("--ack") + 1] == "delivery_4"
        )
        self.assertGreater(third_specialist_start, first_specialist_ack)
        self.assertGreater(reviewer_start, final_specialist_ack)
        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "review_accepted")
        self.assertEqual(
            [(item["task_key"], item["result_outcome"]) for item in report["milestone"]["tasks"]],
            [
                ("verify_a", "accepted"),
                ("verify_b", "accepted"),
                ("verify_c", "accepted"),
                ("review", "accepted"),
            ],
        )

    @requires_native_windows_admission
    def test_resume_executes_valid_mixed_later_waves_under_remaining_capacity(self) -> None:
        objective = "Resume every valid later specialist wave"
        specialists = ("verify_a", "verify_b", "verify_c", "verify_d", "verify_e")
        plan = self._write_milestone_plan(objective, specialist_keys=specialists)
        client = MilestoneClient(self.root, pause_after_owner=True)

        with patch(
            "orchestrate.controller._monotonic",
            side_effect=(0.0, 0.0, 0.0, 61.0),
        ) as controlled_clock:
            paused = implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )

        self.assertEqual(paused["status"], "milestone_waiting")
        self.assertEqual(controlled_clock.call_count, 4)
        self.assertEqual(client.tasks["task_1"]["status"], "completed")
        self.assertTrue(
            any(
                call[:2] == ("orchestration", "check")
                and "--ack" in call
                and call[call.index("--ack") + 1] == "delivery_1"
                for call in client.calls
            )
        )
        self.assertEqual(client.peak_active_workers, 2)
        self.assertEqual(
            [
                call[call.index("--task") + 1]
                for call in client.calls
                if call[:2] == ("orchestration", "worker-start")
            ],
            ["task_1", "task_2", "task_3"],
        )
        client.deliveries_paused = False
        client.pause_after_owner = False

        resumed = resume(
            self.root,
            str(paused["localRunId"]),
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )

        self.assertEqual(resumed["status"], "worker_succeeded")
        self.assertEqual(resumed["verification"], "review_accepted")
        self.assertEqual(client.peak_active_workers, 2)
        self.assertEqual(
            [
                call[call.index("--task") + 1]
                for call in client.calls
                if call[:2] == ("orchestration", "worker-start")
            ],
            ["task_1", "task_2", "task_3", "task_4", "task_5", "task_6", "task_7"],
        )
        self.assertTrue(
            all(
                "--terminal" not in call
                for call in client.calls
                if call[:2] == ("orchestration", "worker-start")
            )
        )

    @requires_native_windows_admission
    def test_status_and_explain_retract_accepted_review_for_all_consulted_source_drift(self) -> None:
        objective = "Invalidate accepted review as soon as consulted source identity changes"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root)
        accepted = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        local_run_id = str(accepted["localRunId"])
        agents_path = self.root / "AGENTS.md"
        original_agents = agents_path.read_bytes()

        def apply_drift(kind: str) -> None:
            if kind == "unstaged":
                agents_path.write_text("Unstaged candidate restriction.\n", encoding="utf-8")
            elif kind == "staged":
                agents_path.write_text("Staged candidate restriction.\n", encoding="utf-8")
                git(self.root, "add", "AGENTS.md")
            elif kind == "untracked":
                nested = self.root / "nested"
                nested.mkdir()
                (nested / "AGENTS.md").write_text("Untracked nested restriction.\n", encoding="utf-8")
            elif kind == "deleted":
                agents_path.unlink()
            elif kind == "replaced":
                replacement = self.root / "replacement-policy.tmp"
                replacement.write_text("Atomically replaced restriction.\n", encoding="utf-8")
                replacement.replace(agents_path)
            else:
                raise AssertionError(kind)

        def restore_drift(kind: str) -> None:
            if kind == "staged":
                git(self.root, "restore", "--staged", "--", "AGENTS.md")
            if kind == "untracked":
                nested_agents = self.root / "nested" / "AGENTS.md"
                nested_agents.unlink()
                nested_agents.parent.rmdir()
            agents_path.write_bytes(original_agents)

        for kind in ("unstaged", "staged", "untracked", "deleted", "replaced"):
            with self.subTest(kind=kind):
                apply_drift(kind)
                try:
                    status_report = status(
                        self.root,
                        local_run_id,
                        client=client,  # type: ignore[arg-type]
                    )
                    explain_report = explain(self.root, local_run_id)
                    for report in (status_report, explain_report):
                        self.assertEqual(report["status"], "stale_review")
                        self.assertEqual(report["verification"], "review_stale")
                        self.assertNotEqual(report["milestone"]["plan"]["status"], "review_accepted")
                        self.assertEqual(report["staleEvidence"]["storedVerification"], "review_accepted")
                        self.assertIn("new independent review", report["nextObligation"])
                        self.assertIsNot(report["sourceBinding"]["unchanged"], True)
                    for command, report in (("status", status_report), ("explain", explain_report)):
                        with patch("orchestrate.cli._execute", return_value=(report, True)), patch("builtins.print"):
                            self.assertEqual(
                                cli_main([command, "--project", str(self.root), "--run", local_run_id, "--json"]),
                                1,
                            )
                    if kind == "unstaged":
                        mutations_before = [
                            call
                            for call in client.calls
                            if call[:2]
                            in {
                                ("orchestration", "gate-create"),
                                ("orchestration", "gate-resolve"),
                                ("orchestration", "task-create"),
                                ("orchestration", "worker-release"),
                                ("orchestration", "worker-start"),
                            }
                        ]
                        with self.assertRaises(OrchestrateError) as stale_resume:
                            resume(
                                self.root,
                                local_run_id,
                                client=client,  # type: ignore[arg-type]
                                milestone_plan=plan,
                                wait_timeout_ms=60_000,
                                require_context=False,
                            )
                        self.assertEqual(stale_resume.exception.code, "stale_review")
                        mutations_after = [
                            call
                            for call in client.calls
                            if call[:2]
                            in {
                                ("orchestration", "gate-create"),
                                ("orchestration", "gate-resolve"),
                                ("orchestration", "task-create"),
                                ("orchestration", "worker-release"),
                                ("orchestration", "worker-start"),
                            }
                        ]
                        self.assertEqual(mutations_after, mutations_before)
                finally:
                    restore_drift(kind)

                restored = explain(self.root, local_run_id)
                self.assertEqual(restored["status"], "worker_succeeded")
                self.assertEqual(restored["verification"], "review_accepted")
                self.assertIs(restored["sourceBinding"]["unchanged"], True)

    @requires_native_windows_admission
    def test_status_and_explain_reject_every_corrupt_or_unbound_candidate_packet_identity(self) -> None:
        objective = "Reject corrupt immutable candidate packet identity"
        plan = self._write_milestone_plan(objective)
        accepted = implement(
            self.root,
            objective,
            client=MilestoneClient(self.root),  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        local_run_id = str(accepted["localRunId"])
        with StateStore(self.root) as store:
            run = store.get_run(local_run_id)
            rows = store.connection.execute(
                """SELECT p.task_id, p.packet_json, b.task_key
                   FROM packets p JOIN milestone_task_bindings b
                     ON b.run_local_id = p.run_local_id AND b.task_id = p.task_id
                   WHERE p.run_local_id = ? ORDER BY b.created_at""",
                (run.local_id,),
            ).fetchall()
            originals = {row["task_id"]: row["packet_json"] for row in rows}
            first_task, second_task = list(originals)[:2]
            first_key = rows[0]["task_key"]
            original_worktree = store.connection.execute(
                """SELECT worktree_id FROM milestone_worker_bindings
                   WHERE run_local_id = ? AND task_key = ?""",
                (run.local_id, first_key),
            ).fetchone()["worktree_id"]

        def canonical_changed(task_id: str, change: Callable[[dict[str, object]], None]) -> str:
            packet_value = json.loads(originals[task_id])
            change(packet_value)
            packet_value["packetId"] = expected_packet_id(packet_value)
            return canonical_packet_json(packet_value)

        def corrupt(kind: str) -> None:
            with StateStore(self.root) as store:
                if kind == "malformed_sources_and_stale_packet_id":
                    packet_value = json.loads(originals[first_task])
                    packet_value["sources"] = [
                        {"path": item["path"]}
                        for item in packet_value["sources"]
                    ]
                    value = canonical_packet_json(packet_value)
                    store.connection.execute(
                        "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                        (value, local_run_id, first_task),
                    )
                elif kind == "duplicate_source_identity":
                    value = canonical_changed(
                        first_task,
                        lambda packet: packet["sources"].append(deepcopy(packet["sources"][0])),  # type: ignore[union-attr,index]
                    )
                    store.connection.execute(
                        "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                        (value, local_run_id, first_task),
                    )
                elif kind == "conflicting_contract":
                    value = canonical_changed(
                        first_task,
                        lambda packet: packet["milestone"].update(contractDigest="contract_sha256_conflict"),  # type: ignore[union-attr]
                    )
                    store.connection.execute(
                        "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                        (value, local_run_id, first_task),
                    )
                elif kind == "duplicate_packet":
                    store.connection.execute(
                        "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                        (originals[first_task], local_run_id, second_task),
                    )
                elif kind == "unbound_packet_row":
                    store.connection.execute(
                        "INSERT INTO packets VALUES (?, 'task_unbound', ?, '2026-01-01T00:00:00Z')",
                        (local_run_id, originals[first_task]),
                    )
                elif kind == "conflicting_workspace":
                    store.connection.execute(
                        """UPDATE milestone_worker_bindings SET worktree_id = 'worktree_conflict'
                           WHERE run_local_id = ? AND task_key = ?""",
                        (local_run_id, first_key),
                    )
                else:
                    raise AssertionError(kind)

        def restore() -> None:
            with StateStore(self.root) as store:
                store.connection.execute(
                    "DELETE FROM packets WHERE run_local_id = ? AND task_id = 'task_unbound'",
                    (local_run_id,),
                )
                for task_id, packet_json in originals.items():
                    store.connection.execute(
                        "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                        (packet_json, local_run_id, task_id),
                    )
                store.connection.execute(
                    """UPDATE milestone_worker_bindings SET worktree_id = ?
                       WHERE run_local_id = ? AND task_key = ?""",
                    (original_worktree, local_run_id, first_key),
                )

        for kind in (
            "malformed_sources_and_stale_packet_id",
            "duplicate_source_identity",
            "conflicting_contract",
            "duplicate_packet",
            "unbound_packet_row",
            "conflicting_workspace",
        ):
            with self.subTest(kind=kind):
                corrupt(kind)
                try:
                    reports = (
                        status(self.root, local_run_id, client=ReportingClient()),  # type: ignore[arg-type]
                        explain(self.root, local_run_id),
                    )
                    for report in reports:
                        self.assertEqual(report["status"], "stale_review")
                        self.assertEqual(report["verification"], "review_stale")
                        self.assertIsNone(report["sourceBinding"]["current"])
                        self.assertEqual(report["sourceBinding"]["error"]["code"], "packet_identity_conflict")
                        self.assertEqual(report["staleEvidence"]["storedVerification"], "review_accepted")
                    for command, report in (("status", reports[0]), ("explain", reports[1])):
                        with patch("orchestrate.cli._execute", return_value=(report, True)), patch("builtins.print"):
                            self.assertEqual(
                                cli_main([command, "--project", str(self.root), "--run", local_run_id, "--json"]),
                                1,
                            )
                finally:
                    restore()

        restored = explain(self.root, local_run_id)
        self.assertEqual(restored["status"], "worker_succeeded")
        self.assertEqual(restored["verification"], "review_accepted")
        self.assertIs(restored["sourceBinding"]["unchanged"], True)

    @requires_native_windows_admission
    def test_accepted_review_requires_exact_canonical_plan_and_complete_native_history(self) -> None:
        objective = "Reject altered or missing milestone plan authority"
        plan_path = self._write_milestone_plan(objective)
        accepted = implement(
            self.root,
            objective,
            client=MilestoneClient(self.root),  # type: ignore[arg-type]
            milestone_plan=plan_path,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        local_run_id = str(accepted["localRunId"])
        with StateStore(self.root) as store:
            state_path = store.path
            plan_row = dict(
                store.connection.execute(
                    "SELECT * FROM milestone_plan_bindings WHERE run_local_id = ?",
                    (local_run_id,),
                ).fetchone()
            )
            plan_schema = store.connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'milestone_plan_bindings'"
            ).fetchone()["sql"]
            task_dependencies = {
                row["task_key"]: row["dependencies_json"]
                for row in store.connection.execute(
                    "SELECT task_key, dependencies_json FROM milestone_task_bindings WHERE run_local_id = ?",
                    (local_run_id,),
                ).fetchall()
            }
            task_specs = {
                row["task_key"]: row["spec"]
                for row in store.connection.execute(
                    "SELECT task_key, spec FROM milestone_task_bindings WHERE run_local_id = ?",
                    (local_run_id,),
                ).fetchall()
            }
            task_ids = {"owner": str(accepted["taskId"])}
            task_ids.update(
                {
                    row["task_key"]: row["task_id"]
                    for row in store.connection.execute(
                        "SELECT task_key, task_id FROM milestone_task_bindings WHERE run_local_id = ?",
                        (local_run_id,),
                    ).fetchall()
                }
            )
            task_creation_rows = [
                dict(row)
                for row in store.connection.execute(
                    "SELECT * FROM intentions WHERE run_local_id = ? ORDER BY created_at, id",
                    (local_run_id,),
                ).fetchall()
                if row["operation"] == "task-create"
                or row["operation"].startswith("milestone-task-create:")
            ]
            task_creation_by_operation = {row["operation"]: row for row in task_creation_rows}
            gate_kinds = {
                row["task_key"]: row["gate_kind"]
                for row in store.connection.execute(
                    "SELECT task_key, gate_kind FROM milestone_gate_bindings WHERE run_local_id = ?",
                    (local_run_id,),
                ).fetchall()
            }

        initial_current = explain(self.root, local_run_id)
        self.assertEqual(
            initial_current["status"],
            "worker_succeeded",
            initial_current.get("sourceBinding"),
        )

        def canonical(value: object) -> str:
            return json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )

        def digest(value: str, prefix: str) -> str:
            return f"{prefix}_sha256_" + hashlib.sha256(value.encode("utf-8")).hexdigest()

        def replace_plan(value: dict[str, object], *, contract_digest: str | None = None) -> None:
            encoded = canonical(value)
            with StateStore(self.root) as store:
                store.connection.execute(
                    """UPDATE milestone_plan_bindings
                       SET plan_json = ?, plan_digest = ?, contract_digest = COALESCE(?, contract_digest)
                       WHERE run_local_id = ?""",
                    (encoded, digest(encoded, "plan"), contract_digest, local_run_id),
                )

        def duplicate_task_creation(operation: str) -> None:
            original = task_creation_by_operation[operation]
            with StateStore(self.root) as store:
                store.connection.execute(
                    """INSERT INTO intentions(
                           id, run_local_id, operation, arguments_json, request_id, status,
                           returncode, response_json, error_json, created_at, updated_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        f"{original['id']}_duplicate",
                        original["run_local_id"],
                        original["operation"],
                        original["arguments_json"],
                        original["request_id"],
                        original["status"],
                        original["returncode"],
                        original["response_json"],
                        original["error_json"],
                        original["created_at"],
                        original["updated_at"],
                    ),
                )

        def alter_task_creation_argument(operation: str, flag: str, value: str) -> None:
            arguments = json.loads(task_creation_by_operation[operation]["arguments_json"])
            arguments[arguments.index(flag) + 1] = value
            with StateStore(self.root) as store:
                store.connection.execute(
                    "UPDATE intentions SET arguments_json = ? WHERE run_local_id = ? AND operation = ?",
                    (json.dumps(arguments), local_run_id, operation),
                )

        def corrupt(kind: str) -> None:
            if kind == "duplicate_plan_history":
                connection = sqlite3.connect(state_path)
                try:
                    connection.execute(
                        "CREATE TABLE milestone_plan_bindings_duplicate AS SELECT * FROM milestone_plan_bindings"
                    )
                    connection.execute("DROP TABLE milestone_plan_bindings")
                    connection.execute(
                        "ALTER TABLE milestone_plan_bindings_duplicate RENAME TO milestone_plan_bindings"
                    )
                    connection.execute(
                        "INSERT INTO milestone_plan_bindings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        tuple(plan_row.values()),
                    )
                    connection.commit()
                finally:
                    connection.close()
                return
            if kind == "duplicate_owner_task_creation":
                duplicate_task_creation("task-create")
                return
            if kind == "duplicate_followup_task_creation":
                duplicate_task_creation("milestone-task-create:verify")
                return
            if kind == "altered_owner_task_creation":
                alter_task_creation_argument("task-create", "--task-title", "Altered owner title")
                return
            if kind == "altered_followup_task_creation":
                alter_task_creation_argument(
                    "milestone-task-create:verify",
                    "--spec",
                    "Altered follow-up Task creation spec.",
                )
                return
            with StateStore(self.root) as store:
                if kind == "missing_plan_row":
                    store.connection.execute(
                        "DELETE FROM milestone_plan_bindings WHERE run_local_id = ?",
                        (local_run_id,),
                    )
                elif kind == "plan_digest_mismatch":
                    store.connection.execute(
                        "UPDATE milestone_plan_bindings SET plan_digest = 'plan_sha256_conflict' WHERE run_local_id = ?",
                        (local_run_id,),
                    )
                elif kind == "candidate_digest_mismatch":
                    store.connection.execute(
                        "UPDATE milestone_plan_bindings SET candidate_digest = 'candidate_conflict' WHERE run_local_id = ?",
                        (local_run_id,),
                    )
                elif kind == "contract_digest_mismatch":
                    store.connection.execute(
                        "UPDATE milestone_plan_bindings SET contract_digest = 'contract_sha256_conflict' WHERE run_local_id = ?",
                        (local_run_id,),
                    )
                elif kind == "noncanonical_plan_json":
                    noncanonical = json.dumps(json.loads(plan_row["plan_json"]), separators=(",", ":"))
                    self.assertNotEqual(noncanonical, plan_row["plan_json"])
                    store.connection.execute(
                        "UPDATE milestone_plan_bindings SET plan_json = ? WHERE run_local_id = ?",
                        (noncanonical, local_run_id),
                    )
                elif kind == "invalid_plan_json":
                    store.connection.execute(
                        "UPDATE milestone_plan_bindings SET plan_json = '{invalid' WHERE run_local_id = ?",
                        (local_run_id,),
                    )
                elif kind == "missing_owner_task_creation":
                    store.connection.execute(
                        "DELETE FROM intentions WHERE run_local_id = ? AND operation = 'task-create'",
                        (local_run_id,),
                    )
                elif kind == "missing_followup_task_creation":
                    store.connection.execute(
                        """DELETE FROM intentions
                           WHERE run_local_id = ? AND operation = 'milestone-task-create:verify'""",
                        (local_run_id,),
                    )
                elif kind == "malformed_owner_task_creation":
                    store.connection.execute(
                        """UPDATE intentions SET arguments_json = '{invalid'
                           WHERE run_local_id = ? AND operation = 'task-create'""",
                        (local_run_id,),
                    )
                elif kind == "malformed_followup_task_creation":
                    store.connection.execute(
                        """UPDATE intentions SET response_json = '{invalid'
                           WHERE run_local_id = ? AND operation = 'milestone-task-create:verify'""",
                        (local_run_id,),
                    )
                elif kind == "conflicting_owner_task_creation":
                    response = json.loads(task_creation_by_operation["task-create"]["response_json"])
                    response["result"]["task"]["id"] = "task_conflict"
                    store.connection.execute(
                        """UPDATE intentions SET response_json = ?
                           WHERE run_local_id = ? AND operation = 'task-create'""",
                        (json.dumps(response), local_run_id),
                    )
                elif kind == "conflicting_followup_task_creation":
                    store.connection.execute(
                        """UPDATE intentions SET request_id = 'request_conflict'
                           WHERE run_local_id = ? AND operation = 'milestone-task-create:verify'""",
                        (local_run_id,),
                    )
                elif kind == "unapplied_owner_task_creation":
                    store.connection.execute(
                        """UPDATE intentions SET status = 'prepared'
                           WHERE run_local_id = ? AND operation = 'task-create'""",
                        (local_run_id,),
                    )
                elif kind == "unapplied_followup_task_creation":
                    store.connection.execute(
                        """UPDATE intentions SET status = 'prepared'
                           WHERE run_local_id = ? AND operation = 'milestone-task-create:verify'""",
                        (local_run_id,),
                    )
                elif kind == "changed_native_dependencies":
                    store.connection.execute(
                        """UPDATE milestone_task_bindings SET dependencies_json = '[]'
                           WHERE run_local_id = ? AND task_key = 'review'""",
                        (local_run_id,),
                    )
                elif kind == "changed_gate_binding":
                    store.connection.execute(
                        """UPDATE milestone_gate_bindings SET gate_kind = 'review'
                           WHERE run_local_id = ? AND task_key = 'verify'""",
                        (local_run_id,),
                    )
                elif kind == "unbound_task_history":
                    store.connection.execute(
                        """INSERT INTO milestone_task_bindings
                           VALUES (?, 'unbound', 'task_unbound_plan', ?, ?, 'unbound', '[]', '2026-01-01T00:00:00Z')""",
                        (
                            local_run_id,
                            plan_row["candidate_digest"],
                            plan_row["contract_digest"],
                        ),
                    )
                elif kind in {
                    "changed_plan_contract",
                    "changed_plan_task_title",
                    "changed_plan_task_spec",
                    "changed_plan_task_dependencies",
                    "changed_plan_task_topology",
                }:
                    pass
                else:
                    raise AssertionError(kind)
            original_value = json.loads(plan_row["plan_json"])
            if kind == "changed_plan_contract":
                changed_contract = {"interface": "frozen-v2", "checks": ["focused", "changed"]}
                original_value["contract"] = changed_contract
                replace_plan(
                    original_value,
                    contract_digest=digest(canonical(changed_contract), "contract"),
                )
            elif kind == "changed_plan_task_title":
                tasks = original_value["tasks"]
                assert isinstance(tasks, list)
                specialist = next(task for task in tasks if task["key"] == "verify")
                specialist["title"] = "Tampered accepted verification title"
                replace_plan(original_value)
            elif kind == "changed_plan_task_spec":
                tasks = original_value["tasks"]
                assert isinstance(tasks, list)
                specialist = next(task for task in tasks if task["key"] == "verify")
                changed_spec = "Read only a recomputed but historically uncreated specification."
                specialist["spec"] = changed_spec
                replace_plan(original_value)
                with StateStore(self.root) as store:
                    store.connection.execute(
                        """UPDATE milestone_task_bindings SET spec = ?
                           WHERE run_local_id = ? AND task_key = 'verify'""",
                        (changed_spec, local_run_id),
                    )
            elif kind == "changed_plan_task_dependencies":
                tasks = original_value["tasks"]
                assert isinstance(tasks, list)
                reviewer = next(task for task in tasks if task["key"] == "review")
                reviewer["dependencies"] = ["verify", "owner"]
                replace_plan(original_value)
                with StateStore(self.root) as store:
                    store.connection.execute(
                        """UPDATE milestone_task_bindings SET dependencies_json = ?
                           WHERE run_local_id = ? AND task_key = 'review'""",
                        (
                            json.dumps([task_ids["verify"], task_ids["owner"]], separators=(",", ":")),
                            local_run_id,
                        ),
                    )
            elif kind == "changed_plan_task_topology":
                tasks = original_value["tasks"]
                assert isinstance(tasks, list)
                reviewer = next(task for task in tasks if task["key"] == "review")
                reviewer["dependencies"].append("verify_extra")
                tasks.insert(
                    -1,
                    {
                        "key": "verify_extra",
                        "title": "Verify an altered topology",
                        "spec": "Read only the altered topology.",
                        "role": "specialist",
                        "dependencies": ["owner"],
                        "gate": "verification",
                    },
                )
                replace_plan(original_value)

        def restore(*, duplicate_schema: bool = False) -> None:
            if duplicate_schema:
                connection = sqlite3.connect(state_path)
                try:
                    connection.execute("ALTER TABLE milestone_plan_bindings RENAME TO corrupt_plan_bindings")
                    connection.execute(plan_schema)
                    connection.execute(
                        "INSERT INTO milestone_plan_bindings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        tuple(plan_row.values()),
                    )
                    connection.execute("DROP TABLE corrupt_plan_bindings")
                    connection.commit()
                finally:
                    connection.close()
            with StateStore(self.root) as store:
                store.connection.execute(
                    "DELETE FROM milestone_plan_bindings WHERE run_local_id = ?",
                    (local_run_id,),
                )
                store.connection.execute(
                    "INSERT INTO milestone_plan_bindings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    tuple(plan_row.values()),
                )
                store.connection.execute(
                    "DELETE FROM milestone_task_bindings WHERE run_local_id = ? AND task_key = 'unbound'",
                    (local_run_id,),
                )
                for task_key, dependencies_json in task_dependencies.items():
                    store.connection.execute(
                        """UPDATE milestone_task_bindings SET dependencies_json = ?
                           WHERE run_local_id = ? AND task_key = ?""",
                        (dependencies_json, local_run_id, task_key),
                    )
                for task_key, task_spec in task_specs.items():
                    store.connection.execute(
                        """UPDATE milestone_task_bindings SET spec = ?
                           WHERE run_local_id = ? AND task_key = ?""",
                        (task_spec, local_run_id, task_key),
                    )
                for task_key, gate_kind in gate_kinds.items():
                    store.connection.execute(
                        """UPDATE milestone_gate_bindings SET gate_kind = ?
                           WHERE run_local_id = ? AND task_key = ?""",
                        (gate_kind, local_run_id, task_key),
                    )
                store.connection.execute(
                    """DELETE FROM intentions
                       WHERE run_local_id = ?
                         AND (operation = 'task-create' OR operation GLOB 'milestone-task-create:*')""",
                    (local_run_id,),
                )
                for original in task_creation_rows:
                    store.connection.execute(
                        """INSERT INTO intentions(
                               id, run_local_id, operation, arguments_json, request_id, status,
                               returncode, response_json, error_json, created_at, updated_at
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        tuple(original.values()),
                    )

        kinds = (
            "missing_plan_row",
            "plan_digest_mismatch",
            "candidate_digest_mismatch",
            "contract_digest_mismatch",
            "noncanonical_plan_json",
            "invalid_plan_json",
            "changed_plan_contract",
            "changed_plan_task_title",
            "changed_plan_task_spec",
            "changed_plan_task_dependencies",
            "changed_plan_task_topology",
            "missing_owner_task_creation",
            "missing_followup_task_creation",
            "altered_owner_task_creation",
            "altered_followup_task_creation",
            "duplicate_owner_task_creation",
            "duplicate_followup_task_creation",
            "malformed_owner_task_creation",
            "malformed_followup_task_creation",
            "conflicting_owner_task_creation",
            "conflicting_followup_task_creation",
            "unapplied_owner_task_creation",
            "unapplied_followup_task_creation",
            "changed_native_dependencies",
            "changed_gate_binding",
            "unbound_task_history",
            "duplicate_plan_history",
        )
        for kind in kinds:
            with self.subTest(kind=kind):
                corrupt(kind)
                try:
                    reports = (
                        status(self.root, local_run_id, client=ReportingClient()),  # type: ignore[arg-type]
                        explain(self.root, local_run_id),
                    )
                    for report in reports:
                        self.assertEqual(report["status"], "stale_review")
                        self.assertEqual(report["verification"], "review_stale")
                        self.assertIsNone(report["sourceBinding"]["current"])
                        self.assertEqual(report["sourceBinding"]["error"]["code"], "packet_identity_conflict")
                        self.assertEqual(report["staleEvidence"]["storedVerification"], "review_accepted")
                    for command, report in (("status", reports[0]), ("explain", reports[1])):
                        with patch("orchestrate.cli._execute", return_value=(report, True)), patch("builtins.print"):
                            self.assertEqual(
                                cli_main([command, "--project", str(self.root), "--run", local_run_id, "--json"]),
                                1,
                            )
                finally:
                    restore(duplicate_schema=kind == "duplicate_plan_history")

        restored = explain(self.root, local_run_id)
        self.assertEqual(restored["status"], "worker_succeeded")
        self.assertEqual(restored["verification"], "review_accepted")
        self.assertIs(restored["sourceBinding"]["unchanged"], True)

    @requires_native_windows_admission
    def test_accepted_review_projects_unavailable_profile_identity_as_stale_until_exact_restoration(self) -> None:
        objective = "Project unavailable current profile identity as stale"
        plan = self._write_milestone_plan(objective)
        accepted = implement(
            self.root,
            objective,
            client=MilestoneClient(self.root),  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        local_run_id = str(accepted["localRunId"])
        profile_path = self.root / ".orchestrate.json"
        original_profile = profile_path.read_bytes()

        for kind in ("missing", "malformed", "unreadable", "reparse"):
            with self.subTest(kind=kind):
                profile_path.write_bytes(original_profile)
                if kind == "missing":
                    profile_path.unlink()
                    context = nullcontext()
                elif kind == "malformed":
                    profile_path.write_bytes(b"{")
                    context = nullcontext()
                elif kind == "unreadable":
                    context = patch("orchestrate.profile.read_project_bytes", side_effect=OSError("synthetic unreadable"))
                else:
                    context = patch(
                        "orchestrate.profile.read_project_bytes",
                        side_effect=OrchestrateError("synthetic reparse boundary", code="source_reparse_unresolved"),
                    )
                try:
                    with context:
                        reports = (
                            status(self.root, local_run_id, client=ReportingClient()),  # type: ignore[arg-type]
                            explain(self.root, local_run_id),
                        )
                    for report in reports:
                        self.assertEqual(report["status"], "stale_review")
                        self.assertEqual(report["verification"], "review_stale")
                        self.assertIsNone(report["sourceBinding"]["current"])
                        self.assertEqual(report["staleEvidence"]["reason"], "source_binding_unavailable")
                finally:
                    profile_path.write_bytes(original_profile)

                restored = explain(self.root, local_run_id)
                self.assertEqual(restored["status"], "worker_succeeded")
                self.assertEqual(restored["verification"], "review_accepted")
                self.assertIs(restored["sourceBinding"]["unchanged"], True)

    def test_profile_failure_without_bound_milestone_history_remains_blocked(self) -> None:
        with StateStore(self.root) as store:
            run = store.create_run(objective="ordinary reporting", profile_digest="p", source_digest="s")
        (self.root / ".orchestrate.json").unlink()

        for operation in (
            lambda: status(self.root, run.local_id, client=ReportingClient()),  # type: ignore[arg-type]
            lambda: explain(self.root, run.local_id),
        ):
            with self.assertRaises(OrchestrateError) as caught:
                operation()
            self.assertEqual(caught.exception.code, "profile_missing")

    @requires_native_windows_admission
    def test_status_and_explain_leave_closed_state_database_and_sidecars_unchanged(self) -> None:
        objective = "Report without mutating host-local state"
        plan = self._write_milestone_plan(objective)
        accepted = implement(
            self.root,
            objective,
            client=MilestoneClient(self.root),  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        local_run_id = str(accepted["localRunId"])
        with StateStore(self.root) as store:
            state_path = store.path

        def snapshot() -> dict[str, tuple[bool, str | None]]:
            return {
                suffix: (
                    candidate.exists(),
                    hashlib.sha256(candidate.read_bytes()).hexdigest() if candidate.exists() else None,
                )
                for suffix in ("", "-wal", "-shm")
                for candidate in (Path(str(state_path) + suffix),)
            }

        before = snapshot()
        status_report = status(self.root, local_run_id, client=ReportingClient())  # type: ignore[arg-type]
        after_status = snapshot()
        explain_report = explain(self.root, local_run_id)
        after_explain = snapshot()

        self.assertEqual(status_report["status"], "worker_succeeded")
        self.assertEqual(explain_report["status"], "worker_succeeded")
        self.assertEqual(after_status, before)
        self.assertEqual(after_explain, before)

    def test_untracked_milestone_plan_has_zero_orca_effects(self) -> None:
        objective = "Reject an untracked milestone authority packet"
        tracked = self._write_milestone_plan(objective)
        untracked = "untracked-milestone-plan.json"
        (self.root / untracked).write_bytes((self.root / tracked).read_bytes())
        client = MilestoneClient(self.root)

        with self.assertRaises(OrchestrateError) as caught:
            implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                milestone_plan=untracked,
                wait_timeout_ms=60_000,
                require_context=False,
            )

        self.assertEqual(caught.exception.code, "milestone_plan_untracked")
        self.assertEqual(client.calls, [])

    @requires_native_windows_admission
    def test_false_verification_success_keeps_review_gate_blocking(self) -> None:
        objective = "Integrate then reject false verifier success"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, verification_result="missing")

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )

        self.assertEqual(report["status"], "milestone_blocked")
        review_gate = next(item for item in report["milestone"]["gates"] if item["task_key"] == "review")
        self.assertEqual(review_gate["status"], "pending")
        self.assertFalse(
            any(
                call[:2] == ("orchestration", "worker-start")
                and call[call.index("--task") + 1] == "task_3"
                for call in client.calls
            )
        )
        self.assertTrue(any("--ack" in call for call in client.calls))
        mutating_before = [
            call
            for call in client.calls
            if call[:2]
            in {
                ("orchestration", "gate-create"),
                ("orchestration", "gate-resolve"),
                ("orchestration", "task-create"),
                ("orchestration", "worker-release"),
                ("orchestration", "worker-start"),
            }
        ]
        resumed = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        mutating_after = [
            call
            for call in client.calls
            if call[:2]
            in {
                ("orchestration", "gate-create"),
                ("orchestration", "gate-resolve"),
                ("orchestration", "task-create"),
                ("orchestration", "worker-release"),
                ("orchestration", "worker-start"),
            }
        ]
        self.assertEqual(resumed["status"], "milestone_blocked")
        self.assertEqual(mutating_after, mutating_before)

    @requires_native_windows_admission
    def test_rejected_exact_review_never_completes_the_milestone(self) -> None:
        objective = "Integrate then preserve rejected review evidence"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, review_result="rejected")

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )

        self.assertEqual(report["status"], "milestone_blocked")
        review = next(item for item in report["milestone"]["tasks"] if item["task_key"] == "review")
        self.assertEqual(review["outcome"], "succeeded")
        self.assertEqual(review["result_outcome"], "rejected")
        self.assertNotEqual(report["verification"], "review_accepted")

    @requires_native_windows_admission
    def test_uncertain_milestone_launch_is_not_duplicated_on_resume(self) -> None:
        objective = "Integrate then preserve uncertain follow-up launch"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, uncertain_task="task_2")

        with self.assertRaises(OrchestrateError) as first:
            implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(first.exception.code, "mutation_outcome_uncertain")
        starts_before = len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")])

        with self.assertRaises(OrchestrateError) as resumed:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(resumed.exception.code, "unknown_external_effect")
        starts_after = len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")])
        self.assertEqual(starts_after, starts_before)

    def test_unresolved_milestone_launch_precedes_native_ready_validation(self) -> None:
        contract = SharedContract.draft({"interface": "frozen-v1"}).settle()
        candidate = "candidate_exact"
        plan = MilestonePlan(
            contract,
            candidate,
            (
                MilestoneTask(
                    "owner",
                    "Owner",
                    "Own integration",
                    "owner",
                    gate="integration",
                    integration_owner=True,
                    writes_shared_contract=True,
                    candidate_digest=candidate,
                    contract_digest=contract.digest,
                ),
                MilestoneTask(
                    "verify",
                    "Verify",
                    "Verify exact candidate",
                    "specialist",
                    dependencies=("owner",),
                    gate="verification",
                    independently_useful=True,
                    candidate_digest=candidate,
                    contract_digest=contract.digest,
                ),
            ),
        )
        client = FakeClient([])
        with StateStore(self.root) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1")
            intention = store.prepare_intention(
                run.local_id,
                "milestone-worker-start:verify",
                ["orchestration", "worker-start"],
            )
            store.mark_intention(intention, "uncertain", request_id="request_unknown")
            with self.assertRaises(OrchestrateError) as caught:
                _recover_milestone_worker_projections(
                    client,  # type: ignore[arg-type]
                    store,
                    run,
                    ProjectProfile.load(self.root),
                    plan,
                    worktree_id=None,
                )
        self.assertEqual(caught.exception.code, "unknown_external_effect")
        self.assertEqual(client.calls, [])

    @requires_native_windows_admission
    def test_resume_reconstructs_applied_milestone_start_before_queued_completion(self) -> None:
        objective = "Integrate then recover the exact applied follow-up launch"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, pause_after_owner=True)
        first = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        self.assertEqual(first["status"], "milestone_waiting")
        starts_before = [call for call in client.calls if call[:2] == ("orchestration", "worker-start")]
        self.assertEqual(sum(call[call.index("--task") + 1] == "task_2" for call in starts_before), 1)

        queued = client._delivery()
        delivery_id = queued["result"]["deliveryId"]  # type: ignore[index]
        messages = queued["result"]["messages"]  # type: ignore[index]
        with StateStore(self.root) as store:
            run = store.select_run(None)
            store.journal_delivery(run.local_id, delivery_id, queued, messages)  # type: ignore[arg-type]
            store.update_run(run.local_id, delivery_id=delivery_id)
            store.connection.execute(
                "DELETE FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            )
            store.connection.execute(
                """DELETE FROM evidence WHERE run_local_id = ? AND kind = 'efficiency-event'
                   AND payload_json LIKE '%\"identity\":\"dispatch_2\"%'""",
                (run.local_id,),
            )
        client.deliveries_paused = False

        resumed = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        self.assertEqual(resumed["status"], "worker_succeeded")
        self.assertEqual(resumed["verification"], "review_accepted")
        starts_after = [call for call in client.calls if call[:2] == ("orchestration", "worker-start")]
        self.assertEqual(sum(call[call.index("--task") + 1] == "task_2" for call in starts_after), 1)
        releases = [call for call in client.calls if call[:2] == ("orchestration", "worker-release")]
        self.assertEqual(sum(call[call.index("--dispatch") + 1] == "dispatch_2" for call in releases), 1)

    def _resume_settled_milestone_before_delivery(
        self,
        *,
        outcome: str,
        remove_binding: bool,
    ) -> tuple[dict[str, object], MilestoneClient]:
        objective = f"Integrate then receive {outcome} after native settlement"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, pause_after_owner=True)
        first = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        self.assertEqual(first["status"], "milestone_waiting")
        client.settle_without_delivery("dispatch_2", outcome=outcome)
        with StateStore(self.root) as store:
            run = store.select_run(None)
            run_local_id = run.local_id
            self.assertIsNone(run.delivery_id)
            self.assertIsNone(
                store.connection.execute(
                    "SELECT 1 FROM deliveries WHERE run_local_id = ? AND delivery_id = 'delivery_2'",
                    (run.local_id,),
                ).fetchone()
            )
            if remove_binding:
                store.connection.execute(
                    "DELETE FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                    (run.local_id,),
                )
        starts_before = len(
            [
                call
                for call in client.calls
                if call[:2] == ("orchestration", "worker-start")
                and call[call.index("--task") + 1] == "task_2"
            ]
        )
        client.deliveries_paused = False
        report = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        starts_after = len(
            [
                call
                for call in client.calls
                if call[:2] == ("orchestration", "worker-start")
                and call[call.index("--task") + 1] == "task_2"
            ]
        )
        self.assertEqual(starts_before, 1)
        self.assertEqual(starts_after, starts_before)
        releases = [
            call
            for call in client.calls
            if call[:2] == ("orchestration", "worker-release")
            and call[call.index("--dispatch") + 1] == "dispatch_2"
        ]
        self.assertEqual(len(releases), 1)
        with StateStore(self.root) as store:
            run = store.get_run(run_local_id)
            worker = store.connection.execute(
                "SELECT outcome, release_state FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual(worker["outcome"], outcome)
            self.assertEqual(worker["release_state"], "released")
        return report, client

    @requires_native_windows_admission
    def test_resume_receives_success_settled_before_delivery_with_original_binding(self) -> None:
        report, _ = self._resume_settled_milestone_before_delivery(
            outcome="succeeded",
            remove_binding=False,
        )
        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "review_accepted")

    @requires_native_windows_admission
    def test_resume_receives_success_settled_before_delivery_with_reconstructed_binding(self) -> None:
        report, _ = self._resume_settled_milestone_before_delivery(
            outcome="succeeded",
            remove_binding=True,
        )
        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "review_accepted")

    @requires_native_windows_admission
    def test_resume_receives_failure_settled_before_delivery_with_original_binding(self) -> None:
        report, client = self._resume_settled_milestone_before_delivery(
            outcome="failed",
            remove_binding=False,
        )
        self.assertEqual(report["status"], "milestone_blocked")
        self.assertEqual(report["verification"], "not_run")
        self.assertNotIn("dispatch_3", client.workers)

    @requires_native_windows_admission
    def test_resume_receives_failure_settled_before_delivery_with_reconstructed_binding(self) -> None:
        report, client = self._resume_settled_milestone_before_delivery(
            outcome="failed",
            remove_binding=True,
        )
        self.assertEqual(report["status"], "milestone_blocked")
        self.assertEqual(report["verification"], "not_run")
        self.assertNotIn("dispatch_3", client.workers)

    @requires_native_windows_admission
    def test_settled_before_delivery_still_requires_exact_native_task_identity(self) -> None:
        objective = "Integrate then reject changed settled Task identity"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        client.settle_without_delivery("dispatch_2", outcome="succeeded")
        client.tasks["task_2"]["run_id"] = "run_replacement"
        client.deliveries_paused = False
        with self.assertRaises(OrchestrateError) as caught:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "native_task_binding_mismatch")
        self.assertFalse(
            any(
                call[:2] == ("orchestration", "worker-release")
                and call[call.index("--dispatch") + 1] == "dispatch_2"
                for call in client.calls
            )
        )

    @requires_native_windows_admission
    def test_settled_reconstruction_rejects_replacement_worker_identity(self) -> None:
        objective = "Integrate then reject replaced settled worker identity"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        client.settle_without_delivery("dispatch_2", outcome="failed")
        with StateStore(self.root) as store:
            run = store.select_run(None)
            store.connection.execute(
                "DELETE FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            )
        client.workers["dispatch_2"]["worktree"] = "repo::replacement"
        client.deliveries_paused = False
        with self.assertRaises(OrchestrateError) as caught:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "native_worker_binding_mismatch")
        self.assertFalse(
            any(
                call[:2] == ("orchestration", "worker-release")
                and call[call.index("--dispatch") + 1] == "dispatch_2"
                for call in client.calls
            )
        )

    @requires_native_windows_admission
    def test_delivery_revalidates_task_identity_after_check_before_success_effects(self) -> None:
        objective = "Revalidate the complete native Task after lifecycle Delivery"
        plan = self._write_milestone_plan(objective)

        class PostCheckTaskDriftClient(MilestoneClient):
            exact_task: dict[str, object] | None = None

            def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
                response = super().run_json(*arguments, **keywords)
                if (
                    arguments[:2] == ("orchestration", "check")
                    and "--ack" not in arguments
                    and response["result"].get("deliveryId") == "delivery_2"  # type: ignore[index,union-attr]
                    and self.exact_task is None
                ):
                    self.exact_task = dict(self.tasks["task_2"])
                    self.tasks["task_2"]["task_title"] = "replacement title"
                return response

        client = PostCheckTaskDriftClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        client.settle_without_delivery("dispatch_2", outcome="succeeded")
        client.deliveries_paused = False
        with self.assertRaises(OrchestrateError) as caught:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "native_task_binding_mismatch")
        self.assertIsNotNone(client.exact_task)
        self.assertFalse(client.workers["dispatch_2"]["released"])
        self.assertFalse(
            any(
                call[:2] == ("orchestration", "check")
                and "--ack" in call
                and call[call.index("--ack") + 1] == "delivery_2"
                for call in client.calls
            )
        )
        with StateStore(self.root) as store:
            run = store.select_run(None)
            worker = store.connection.execute(
                "SELECT outcome, result_outcome, release_state FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            ).fetchone()
            delivery = store.connection.execute(
                "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = 'delivery_2'",
                (run.local_id,),
            ).fetchone()
            message = store.connection.execute(
                "SELECT effect_status FROM delivery_messages WHERE run_local_id = ? AND delivery_id = 'delivery_2'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual((worker["outcome"], worker["result_outcome"], worker["release_state"]), (None, None, "owned"))
            self.assertEqual(delivery["acked"], 0)
            self.assertEqual(message["effect_status"], "observed")

        for field, replacement in (
            ("spec", "replacement spec"),
            ("deps", ["task_replacement"]),
        ):
            with self.subTest(field=field):
                client.tasks["task_2"] = dict(client.exact_task or {})
                client.tasks["task_2"][field] = replacement
                with self.assertRaises(OrchestrateError) as later_drift:
                    resume(
                        self.root,
                        None,
                        client=client,  # type: ignore[arg-type]
                        milestone_plan=plan,
                        wait_timeout_ms=60_000,
                        require_context=False,
                    )
                self.assertEqual(later_drift.exception.code, "native_task_binding_mismatch")
                self.assertFalse(client.workers["dispatch_2"]["released"])

        client.tasks["task_2"] = dict(client.exact_task or {})
        recovered = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        self.assertEqual(recovered["status"], "worker_succeeded")
        self.assertEqual(recovered["verification"], "review_accepted")
        self.assertTrue(client.workers["dispatch_2"]["released"])

    @requires_native_windows_admission
    def test_delivery_revalidates_gate_identity_after_check_before_failed_effects(self) -> None:
        objective = "Revalidate the complete native gate after lifecycle Delivery"
        plan = self._write_milestone_plan(objective)

        class PostCheckGateDriftClient(MilestoneClient):
            exact_gate: dict[str, object] | None = None

            def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
                response = super().run_json(*arguments, **keywords)
                if (
                    arguments[:2] == ("orchestration", "check")
                    and "--ack" not in arguments
                    and response["result"].get("deliveryId") == "delivery_2"  # type: ignore[index,union-attr]
                    and self.exact_gate is None
                ):
                    gate_id, gate = next(
                        (key, value) for key, value in self.gates.items() if value["task_id"] == "task_2"
                    )
                    self.exact_gate = dict(gate)
                    gate["id"] = f"{gate_id}_replacement"
                return response

        client = PostCheckGateDriftClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        client.settle_without_delivery("dispatch_2", outcome="failed")
        client.deliveries_paused = False
        with self.assertRaises(OrchestrateError) as caught:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "native_gate_mismatch")
        self.assertIsNotNone(client.exact_gate)
        self.assertFalse(client.workers["dispatch_2"]["released"])
        self.assertFalse(
            any(
                call[:2] == ("orchestration", "check")
                and "--ack" in call
                and call[call.index("--ack") + 1] == "delivery_2"
                for call in client.calls
            )
        )
        with StateStore(self.root) as store:
            run = store.select_run(None)
            worker = store.connection.execute(
                "SELECT outcome, result_outcome, release_state FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            ).fetchone()
            delivery = store.connection.execute(
                "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = 'delivery_2'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual((worker["outcome"], worker["result_outcome"], worker["release_state"]), (None, None, "owned"))
            self.assertEqual(delivery["acked"], 0)

        gate_id = next(key for key, value in client.gates.items() if value["task_id"] == "task_2")
        for field, replacement in (("status", "pending"), ("resolution", "rejected")):
            with self.subTest(field=field):
                client.gates[gate_id] = dict(client.exact_gate or {})
                client.gates[gate_id][field] = replacement
                with self.assertRaises(OrchestrateError) as later_drift:
                    resume(
                        self.root,
                        None,
                        client=client,  # type: ignore[arg-type]
                        milestone_plan=plan,
                        wait_timeout_ms=60_000,
                        require_context=False,
                    )
                self.assertEqual(later_drift.exception.code, "native_gate_mismatch")
                self.assertFalse(client.workers["dispatch_2"]["released"])

        client.gates[gate_id] = dict(client.exact_gate or {})
        recovered = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        self.assertEqual(recovered["status"], "milestone_blocked")
        self.assertEqual(recovered["verification"], "not_run")
        self.assertTrue(client.workers["dispatch_2"]["released"])

    def _assert_post_check_worker_identity_hold(
        self,
        *,
        outcome: str,
        fault_path: tuple[str, ...],
        remove: bool,
    ) -> None:
        objective = f"Revalidate immutable worker identity after {outcome} Delivery"
        plan = self._write_milestone_plan(objective)

        class PostCheckWorkerDriftClient(MilestoneClient):
            inject_fault = False

            def _worker_show(self, dispatch_id: str) -> dict[str, object]:
                response = super()._worker_show(dispatch_id)
                if self.inject_fault and dispatch_id == "dispatch_2" and not self.workers[dispatch_id]["released"]:
                    target = response["result"]
                    for key in fault_path[:-1]:
                        target = target[key]  # type: ignore[index,assignment]
                    if remove:
                        del target[fault_path[-1]]  # type: ignore[index]
                    else:
                        target[fault_path[-1]] = "replacement"  # type: ignore[index]
                return response

            def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
                response = super().run_json(*arguments, **keywords)
                if (
                    arguments[:2] == ("orchestration", "check")
                    and "--ack" not in arguments
                    and response["result"].get("deliveryId") == "delivery_2"  # type: ignore[index,union-attr]
                ):
                    self.inject_fault = True
                return response

        client = PostCheckWorkerDriftClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        client.settle_without_delivery("dispatch_2", outcome=outcome)
        starts_before = len(
            [
                call
                for call in client.calls
                if call[:2] == ("orchestration", "worker-start")
                and call[call.index("--task") + 1] == "task_2"
            ]
        )
        client.deliveries_paused = False
        with self.assertRaises(OrchestrateError) as caught:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "settlement_mismatch")
        self.assertFalse(client.workers["dispatch_2"]["released"])
        self.assertFalse(
            any(
                call[:2] == ("orchestration", "worker-release")
                and call[call.index("--dispatch") + 1] == "dispatch_2"
                for call in client.calls
            )
        )
        with StateStore(self.root) as store:
            run = store.select_run(None)
            worker = store.connection.execute(
                "SELECT outcome, result_outcome, release_state FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            ).fetchone()
            delivery = store.connection.execute(
                "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = 'delivery_2'",
                (run.local_id,),
            ).fetchone()
            message = store.connection.execute(
                "SELECT effect_status FROM delivery_messages WHERE run_local_id = ? AND delivery_id = 'delivery_2'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual((worker["outcome"], worker["result_outcome"], worker["release_state"]), (None, None, "owned"))
            self.assertEqual(delivery["acked"], 0)
            self.assertEqual(message["effect_status"], "observed")

        client.inject_fault = False
        recovered = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        self.assertEqual(
            recovered["status"],
            "worker_succeeded" if outcome == "succeeded" else "milestone_blocked",
        )
        releases = [
            call
            for call in client.calls
            if call[:2] == ("orchestration", "worker-release")
            and call[call.index("--dispatch") + 1] == "dispatch_2"
        ]
        task_starts = [
            call
            for call in client.calls
            if call[:2] == ("orchestration", "worker-start")
            and call[call.index("--task") + 1] == "task_2"
        ]
        self.assertEqual(len(releases), 1)
        self.assertEqual(len(task_starts), starts_before)

    @requires_native_windows_admission
    def test_success_delivery_holds_changed_terminal_incarnation_until_exact_restoration(self) -> None:
        self._assert_post_check_worker_identity_hold(
            outcome="succeeded",
            fault_path=("terminal", "incarnationId"),
            remove=False,
        )

    @requires_native_windows_admission
    def test_failed_delivery_holds_missing_endpoint_incarnation_until_exact_restoration(self) -> None:
        self._assert_post_check_worker_identity_hold(
            outcome="failed",
            fault_path=("terminalResource", "endpointIncarnation"),
            remove=True,
        )

    @requires_native_windows_admission
    def test_grantless_historical_wide_run_reconciles_before_holding_new_launches(self) -> None:
        objective = "Reconcile a historical capacity-four worker before any replacement launch"
        plan = self._write_milestone_plan(objective, max_workers=4)
        client = MilestoneClient(self.root, pause_after_owner=True)
        first = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            allow_exceptional_capacity=True,
            capacity_reason="historical capacity-four fixture",
            wait_timeout_ms=500,
            require_context=False,
        )
        self.assertEqual(first["status"], "milestone_waiting")
        client.settle_without_delivery("dispatch_2", outcome="succeeded")
        with StateStore(self.root) as store:
            run = store.select_run(None)
            store.connection.execute(
                "DELETE FROM evidence WHERE run_local_id = ? AND kind = 'capacity-grant'",
                (run.local_id,),
            )
        client.deliveries_paused = False
        starts_before = len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")])
        checks_before = len(
            [call for call in client.calls if call[:2] == ("orchestration", "check") and "--ack" not in call]
        )
        with self.assertRaises(OrchestrateError) as held:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(held.exception.code, "exceptional_capacity_required")
        self.assertEqual(
            len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")]),
            starts_before,
        )
        self.assertGreater(
            len([call for call in client.calls if call[:2] == ("orchestration", "check") and "--ack" not in call]),
            checks_before,
        )
        self.assertTrue(client.workers["dispatch_2"]["released"])
        self.assertTrue(
            any(
                call[:2] == ("orchestration", "check")
                and "--ack" in call
                and call[call.index("--ack") + 1] == "delivery_2"
                for call in client.calls
            )
        )
        with StateStore(self.root) as store:
            run = store.select_run(None)
            worker = store.connection.execute(
                "SELECT outcome, result_outcome, release_state FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual(
                (worker["outcome"], worker["result_outcome"], worker["release_state"]),
                ("succeeded", "accepted", "released"),
            )

        calls_after_reconciliation = len(client.calls)
        with self.assertRaises(OrchestrateError) as restarted_hold:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(restarted_hold.exception.code, "exceptional_capacity_required")
        new_calls = client.calls[calls_after_reconciliation:]
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in new_calls))
        self.assertFalse(any(call[:2] == ("orchestration", "worker-release") for call in new_calls))
        self.assertFalse(any(call[:2] == ("orchestration", "check") for call in new_calls))

        client.deliveries_paused = True
        granted = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            allow_exceptional_capacity=True,
            capacity_reason="  authorize the remaining bounded review launch  ",
            wait_timeout_ms=500,
            require_context=False,
        )
        self.assertEqual(granted["status"], "milestone_waiting")
        starts_after_grant = [call for call in client.calls if call[:2] == ("orchestration", "worker-start")]
        self.assertEqual(len(starts_after_grant), starts_before + 1)
        client.deliveries_paused = False
        completed = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            allow_exceptional_capacity=True,
            capacity_reason="  authorize the remaining bounded review launch  ",
            wait_timeout_ms=60_000,
            require_context=False,
        )
        self.assertEqual(completed["status"], "worker_succeeded")
        self.assertEqual(completed["verification"], "review_accepted")
        starts_after = [call for call in client.calls if call[:2] == ("orchestration", "worker-start")]
        self.assertEqual(len(starts_after), starts_before + 1)
        self.assertEqual(starts_after[-1][starts_after[-1].index("--task") + 1], "task_3")

    @requires_native_windows_admission
    def test_settlement_restart_rejoins_an_applied_release_before_local_outcome(self) -> None:
        objective = "Resume exact milestone settlement after release readback loss"
        plan = self._write_milestone_plan(objective)

        class ReleaseReadbackLossClient(MilestoneClient):
            lost_release_readback = False

            def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
                if (
                    arguments[:2] == ("orchestration", "worker-show")
                    and arguments[arguments.index("--dispatch") + 1] == "dispatch_2"
                    and self.workers.get("dispatch_2", {}).get("released") is True
                    and not self.lost_release_readback
                ):
                    self.calls.append(arguments)
                    self.lost_release_readback = True
                    raise OrcaCommandError(
                        "synthetic release readback loss",
                        OrcaCommandResult(arguments, -1, "", "lost", None),
                    )
                return super().run_json(*arguments, **keywords)

        client = ReleaseReadbackLossClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        client.settle_without_delivery("dispatch_2", outcome="succeeded")
        client.deliveries_paused = False
        with self.assertRaises(OrcaCommandError):
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        with StateStore(self.root) as store:
            run = store.select_run(None)
            worker = store.connection.execute(
                "SELECT outcome, result_outcome, release_state FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = 'verify'",
                (run.local_id,),
            ).fetchone()
            release = store.connection.execute(
                "SELECT status FROM intentions WHERE run_local_id = ? AND operation = 'milestone-worker-release:verify'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual((worker["outcome"], worker["result_outcome"], worker["release_state"]), (None, None, "owned"))
            self.assertEqual(release["status"], "applied")

        client.workers["dispatch_2"]["launch"]["effective"]["effort"] = "replacement"  # type: ignore[index]
        with self.assertRaises(OrchestrateError) as drifted:
            resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=60_000,
                require_context=False,
            )
        self.assertEqual(drifted.exception.code, "settlement_mismatch")
        client.workers["dispatch_2"]["launch"]["effective"]["effort"] = "high"  # type: ignore[index]
        recovered = resume(
            self.root,
            None,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        self.assertEqual(recovered["status"], "worker_succeeded")
        self.assertEqual(recovered["verification"], "review_accepted")
        releases = [
            call
            for call in client.calls
            if call[:2] == ("orchestration", "worker-release")
            and call[call.index("--dispatch") + 1] == "dispatch_2"
        ]
        self.assertEqual(len(releases), 1)
        starts = [
            call
            for call in client.calls
            if call[:2] == ("orchestration", "worker-start")
            and call[call.index("--task") + 1] == "task_2"
        ]
        self.assertEqual(len(starts), 1)

    @requires_native_windows_admission
    def test_resume_repairs_missing_session_projection_idempotently(self) -> None:
        objective = "Integrate then repair interrupted telemetry projection"
        plan = self._write_milestone_plan(objective)
        client = MilestoneClient(self.root, pause_after_owner=True)
        implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=500,
            require_context=False,
        )
        with StateStore(self.root) as store:
            run = store.select_run(None)
            store.connection.execute(
                """DELETE FROM evidence WHERE run_local_id = ? AND kind = 'efficiency-event'
                   AND payload_json LIKE '%\"identity\":\"dispatch_2\"%'""",
                (run.local_id,),
            )
        starts_before = len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")])
        for _ in range(2):
            report = resume(
                self.root,
                None,
                client=client,  # type: ignore[arg-type]
                milestone_plan=plan,
                wait_timeout_ms=1,
                require_context=False,
            )
            self.assertEqual(report["status"], "milestone_waiting")
        with StateStore(self.root) as store:
            run = store.select_run(None)
            events = store.connection.execute(
                """SELECT payload_json FROM evidence WHERE run_local_id = ? AND kind = 'efficiency-event'
                   AND payload_json LIKE '%\"identity\":\"dispatch_2\"%'""",
                (run.local_id,),
            ).fetchall()
        self.assertEqual(len(events), 1)
        starts_after = len([call for call in client.calls if call[:2] == ("orchestration", "worker-start")])
        self.assertEqual(starts_after, starts_before)

    def test_single_worker_happy_path_releases_before_ack_and_preserves_wip(self) -> None:
        objective = "Make the bounded change"
        before = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        delivery_observed = threading.Event()
        client = FakeClient(
            completion_responses(
                self.root,
                objective,
                delivery_observed=delivery_observed,
            )
        )
        report = self._run_event_backed(
            lambda: implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="happy-path worker completion",
        )
        after = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")

        self.assertEqual(before, after)
        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "pending")
        release_index = next(i for i, call in enumerate(client.calls) if "worker-release" in call)
        ack_index = next(i for i, call in enumerate(client.calls) if "--ack" in call)
        self.assertLess(release_index, ack_index)

    def test_long_objective_creates_the_exact_bounded_orca_title(self) -> None:
        objective = (
            "Make the failing counter test pass by changing only counter.py. Preserve the existing README.md "
            "and owner-note.txt WIP. Run python -m unittest discover -s tests."
        )
        delivery_observed = threading.Event()
        client = FakeClient(
            completion_responses(
                self.root,
                objective,
                delivery_observed=delivery_observed,
            )
        )

        report = self._run_event_backed(
            lambda: implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="long-title worker completion",
        )

        task_call = next(call for call in client.calls if call[:2] == ("orchestration", "task-create"))
        requested_title = task_call[task_call.index("--task-title") + 1]
        self.assertEqual(
            requested_title,
            "Make the failing counter test pass by changing only counter.py. Preserve the...",
        )
        self.assertEqual(len(requested_title), 79)
        self.assertEqual(report["status"], "worker_succeeded")

    def test_resume_accepts_existing_orca_normalized_title_without_creating_another_task(self) -> None:
        objective = (
            "Make the failing counter test pass by changing only counter.py. Preserve the existing README.md "
            "and owner-note.txt WIP. Run python -m unittest discover -s tests."
        )
        profile = setup_project(self.root)
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        packet = make_packet(
            objective=objective,
            profile=profile,
            sources=sources,
            launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
            python_executable=str(Path(sys.executable).resolve()),
            run_id="run_1",
            reader=reader,
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1", phase="task_created")
            store.save_packet(run.local_id, "task_1", canonical_packet_json(packet))
            local_id = run.local_id

        client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                {
                    "result": {
                        "tasks": [
                            {
                                "id": "task_1",
                                "run_id": "run_1",
                                "task_title": (
                                    "Make the failing counter test pass by changing only counter.py. Preserve the..."
                                ),
                                "spec": packet_spec(packet),
                                "status": "ready",
                            }
                        ]
                    }
                },
                _worker_start(self.root),
                _worker_start_readback(self.root),
            ]
        )

        report = resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["taskId"], "task_1")
        self.assertFalse(any(call[:2] == ("orchestration", "task-create") for call in client.calls))
        worker_start = next(call for call in client.calls if call[:2] == ("orchestration", "worker-start"))
        self.assertEqual(worker_start[worker_start.index("--task") + 1], "task_1")

    def test_hook_credentials_do_not_make_an_ordinary_terminal_an_agent(self) -> None:
        client = FakeClient(identity_responses(self.root))
        identity = require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(identity.terminal_handle, "term_plain")

    def test_native_agent_identity_is_rejected_before_mutation(self) -> None:
        client = FakeClient(identity_responses(self.root, agent="codex")[:1])
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "agent_terminal_not_controller")

    def test_active_context_dispatch_is_rejected_by_dispatch_status(self) -> None:
        workers = [
            {
                "dispatchId": "dispatch_active",
                "agentTerminalHandle": "term_plain",
                "dispatchStatus": "dispatched",
                "terminalState": "retained",
            }
        ]
        client = FakeClient(identity_responses(self.root, workers=workers)[:3])
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "agent_terminal_not_controller")

    def test_controller_requires_exact_current_worktree_before_mutation(self) -> None:
        responses = identity_responses(self.root)
        terminal = responses[0]["result"]["terminal"]  # type: ignore[index]
        terminal["worktreePath"] = str(self.root.parent)  # type: ignore[index]
        client = FakeClient(responses[:1])
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "controller_worktree_mismatch")

    def test_conflicting_native_run_binding_is_rejected(self) -> None:
        client = FakeClient(identity_responses(self.root, run_id="run_other"))
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "controller_run_conflict")

    def test_failed_worker_is_not_reported_as_success_or_verification(self) -> None:
        objective = "Fail honestly"
        delivery_observed = threading.Event()
        client = FakeClient(
            completion_responses(
                self.root,
                objective,
                "failed",
                delivery_observed=delivery_observed,
            )
        )
        report = self._run_event_backed(
            lambda: implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="failed-worker completion",
        )
        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertEqual(report["verification"], "not_run")

    def test_opaque_release_text_is_not_a_structured_disposition(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="release", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            _record_fixture_binding(store, run.local_id)
            opaque_resource = {
                "releaseState": "retained",
                "detail": "not external_terminal; owned resource is still live",
            }
            responses: list[Response] = [
                mutation("request_release", dispatchId="dispatch_1", state="retained"),
                _release_worker_show(
                    opaque_resource,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                ),
            ]
            with self.assertRaises(OrchestrateError) as caught:
                _release_disposition(
                    FakeClient(responses),
                    store,
                    run,
                    expected_semantics="succeeded",
                )  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "release_unconfirmed")

    def test_exact_user_takeover_is_preserved_but_unproven_external_shape_holds(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="user_takeover", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_takeover",
                task_id="task_takeover",
                dispatch_id="dispatch_takeover",
                phase="waiting",
            )
            dispatch_id = str(run.dispatch_id)
            resource = {
                "id": "terminal-resource-takeover",
                "ownershipState": "user_owned",
                "releaseState": "retained",
                "retainedReason": "user_takeover",
                "originDispatchId": dispatch_id,
                "ownerDispatchId": dispatch_id,
                "terminalHandle": "term_exact",
                "worktreeId": "worktree_exact",
                "releaseRequestedAt": None,
                "releaseCompletedAt": None,
                "releaseError": None,
                "archive": {"source": None, "status": None},
            }
            _record_fixture_binding(
                store,
                run.local_id,
                dispatch_id=dispatch_id,
                resource_id="terminal-resource-takeover",
                terminal_handle="term_exact",
                worktree_id="worktree_exact",
            )
            _release_disposition(
                FakeClient(
                    [
                        mutation("request_release", dispatchId=dispatch_id, state="retained"),
                        _release_worker_show(
                            resource,
                            run_id="run_takeover",
                            task_id="task_takeover",
                            dispatch_id=dispatch_id,
                        ),
                    ]
                ),
                store,
                run,
                expected_semantics="succeeded",
            )  # type: ignore[arg-type]
            with self.assertRaises(OrchestrateError) as prompt_stall_hold:
                _release_disposition(
                    FakeClient(
                        [
                            _release_worker_show(
                                resource,
                                run_id="run_takeover",
                                task_id="task_takeover",
                                dispatch_id=dispatch_id,
                            )
                        ]
                    ),
                    store,
                    run,
                    expected_semantics="succeeded",
                    require_released=True,
                )  # type: ignore[arg-type]
            self.assertEqual(prompt_stall_hold.exception.code, "release_unconfirmed")

        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="external", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_external",
                task_id="task_external",
                dispatch_id="dispatch_external",
                phase="waiting",
            )
            dispatch_id = str(run.dispatch_id)
            external = {
                **resource,
                "id": "terminal-resource-external",
                "ownershipState": "external",
                "releaseState": "not_requested",
                "retainedReason": "external_terminal",
                "originDispatchId": dispatch_id,
                "ownerDispatchId": dispatch_id,
            }
            _record_fixture_binding(
                store,
                run.local_id,
                dispatch_id=dispatch_id,
                resource_id="terminal-resource-external",
                terminal_handle="term_exact",
                worktree_id="worktree_exact",
            )
            with self.assertRaises(OrchestrateError) as caught:
                _release_disposition(
                    FakeClient(
                        [
                            mutation("request_external", dispatchId=dispatch_id, state="retained"),
                            _release_worker_show(
                                external,
                                run_id="run_external",
                                task_id="task_external",
                                dispatch_id=dispatch_id,
                            ),
                        ]
                    ),
                    store,
                    run,
                    expected_semantics="succeeded",
                )  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "release_unconfirmed")

    def test_release_readback_binds_exact_resource_identity_and_disposition_for_both_orca_shapes(self) -> None:
        for current_shape in (False, True):
            for receipt_state in ("released", "already_released"):
                with self.subTest(current_shape=current_shape, receipt_state=receipt_state):
                    with StateStore(self.root, home=Path(self.state_temp.name)) as store:
                        run = store.create_run(
                            objective=f"release-{current_shape}-{receipt_state}",
                            profile_digest="p",
                            source_digest="s",
                        )
                        run = store.update_run(
                            run.local_id,
                            native_run_id=f"run-{current_shape}-{receipt_state}",
                            task_id=f"task-{current_shape}-{receipt_state}",
                            dispatch_id=f"dispatch-{current_shape}-{receipt_state}",
                            phase="launch_cleanup_pending",
                        )
                        dispatch_id = str(run.dispatch_id)
                        resource = _released_resource(dispatch_id)
                        _record_fixture_binding(store, run.local_id, dispatch_id=dispatch_id)
                        _release_disposition(
                            FakeClient(
                                [
                                    mutation(
                                        f"request-{current_shape}-{receipt_state}",
                                        dispatchId=dispatch_id,
                                        state=receipt_state,
                                    ),
                                    _release_worker_show(
                                        resource,
                                        run_id=str(run.native_run_id),
                                        task_id=str(run.task_id),
                                        dispatch_id=dispatch_id,
                                        status="failed",
                                        current_shape=current_shape,
                                    ),
                                ]
                            ),
                            store,
                            run,
                            expected_semantics="failed",
                            require_released=True,
                        )  # type: ignore[arg-type]

        for changed_field, changed_value in (
            ("id", "terminal-resource-other"),
            ("terminalHandle", "term_other"),
            ("worktreeId", "worktree_other"),
            ("ownershipState", "owned"),
            ("releaseState", "releasing"),
            ("releaseCompletedAt", None),
        ):
            with self.subTest(changed_field=changed_field):
                with StateStore(self.root, home=Path(self.state_temp.name)) as store:
                    run = store.create_run(
                        objective=f"mismatch-{changed_field}",
                        profile_digest="p",
                        source_digest="s",
                    )
                    run = store.update_run(
                        run.local_id,
                        native_run_id=f"run-{changed_field}",
                        task_id=f"task-{changed_field}",
                        dispatch_id=f"dispatch-{changed_field}",
                        phase="launch_cleanup_pending",
                    )
                    dispatch_id = str(run.dispatch_id)
                    resource = _released_resource(dispatch_id)
                    resource[changed_field] = changed_value
                    _record_fixture_binding(store, run.local_id, dispatch_id=dispatch_id)
                    with self.assertRaises(OrchestrateError) as caught:
                        _release_disposition(
                            FakeClient(
                                [
                                    mutation(
                                        f"request-{changed_field}",
                                        dispatchId=dispatch_id,
                                        state="released",
                                    ),
                                    _release_worker_show(
                                        resource,
                                        run_id=str(run.native_run_id),
                                        task_id=str(run.task_id),
                                        dispatch_id=dispatch_id,
                                        status="failed",
                                        current_shape=True,
                                    ),
                                ]
                            ),
                            store,
                            run,
                            expected_semantics="failed",
                            require_released=True,
                        )  # type: ignore[arg-type]
                    self.assertEqual(caught.exception.code, "release_unconfirmed")

    def test_ordinary_post_release_semantics_reject_each_contradiction_for_both_shapes(self) -> None:
        contradiction_fields = (
            "dispatch_status",
            "dispatch_last_failure",
            "worker_state",
            "worker_stage",
            "worker_last_error",
            "worker_terminal_handle",
            "attached_terminal",
        )
        for current_shape in (False, True):
            for outcome in ("succeeded", "failed"):
                for field in contradiction_fields:
                    with self.subTest(current_shape=current_shape, outcome=outcome, field=field):
                        with tempfile.TemporaryDirectory() as home, StateStore(
                            self.root,
                            home=Path(home),
                        ) as store:
                            run = store.create_run(
                                objective=f"ordinary-{current_shape}-{outcome}-{field}",
                                profile_digest="p",
                                source_digest="s",
                            )
                            run = store.update_run(
                                run.local_id,
                                native_run_id="run_1",
                                task_id="task_1",
                                dispatch_id="dispatch_1",
                                phase="waiting",
                            )
                            _record_fixture_binding(store, run.local_id)
                            release = store.prepare_intention(
                                run.local_id,
                                "worker-release",
                                ["orchestration", "worker-release", "--dispatch", "dispatch_1"],
                            )
                            store.mark_intention(
                                release,
                                "applied",
                                request_id="request_release",
                                response=mutation(
                                    "request_release",
                                    dispatchId="dispatch_1",
                                    state="released",
                                ),
                            )
                            responses = settlement_responses(outcome, current_shape=current_shape)
                            latest = deepcopy(responses[3])
                            assert isinstance(latest, dict)
                            _contradict_release_semantics(
                                latest,
                                current_shape=current_shape,
                                field=field,
                            )
                            client = FakeClient([responses[0], responses[1], latest])
                            message = lifecycle_message(
                                "worker_done",
                                {
                                    "taskId": "task_1",
                                    "dispatchId": "dispatch_1",
                                    "outcome": outcome,
                                },
                                message_id="message_done",
                                subject="done",
                            )
                            delivery = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
                            store.journal_delivery(run.local_id, "delivery_1", delivery, [message])

                            with self.assertRaises(OrchestrateError) as caught:
                                _process_delivery(
                                    client,  # type: ignore[arg-type]
                                    store,
                                    run,
                                    "delivery_1",
                                    [message],
                                )

                            self.assertEqual(caught.exception.code, "release_unconfirmed")
                            persisted = store.get_run(run.local_id)
                            self.assertEqual(persisted.phase, "waiting")
                            self.assertIsNone(persisted.worker_outcome)
                            self.assertFalse(any("worker-start" in call for call in client.calls))
                            self.assertFalse(any("worker-release" in call for call in client.calls))
                            self.assertFalse(any(call[:2] == ("terminal", "close") for call in client.calls))
                            self.assertFalse(any("--ack" in call for call in client.calls))

    def test_prompt_stall_release_pending_converges_with_exact_semantics_for_both_shapes(self) -> None:
        worktree_id = f"repo::{self.root.resolve()}"
        for current_shape in (False, True):
            for recovery_mode in ("same_runtime", "restart"):
                with self.subTest(current_shape=current_shape, recovery_mode=recovery_mode):
                    with tempfile.TemporaryDirectory() as home, StateStore(
                        self.root,
                        home=Path(home),
                    ) as store:
                        run = store.create_run(
                            objective=f"prompt-stall-{current_shape}-{recovery_mode}",
                            profile_digest="p",
                            source_digest="s",
                        )
                        run = store.update_run(
                            run.local_id,
                            native_run_id="run_1",
                            task_id="task_1",
                            dispatch_id="dispatch_1",
                            phase="launch_cleanup_pending",
                        )
                        _record_fixture_binding(
                            store,
                            run.local_id,
                            worktree_id=worktree_id,
                        )
                        release_response = mutation(
                            "request_release_pending",
                            dispatchId="dispatch_1",
                            state="release_pending",
                            processAction="none",
                            recovery="Retry after endpoint recovery without another coordinator decision.",
                        )
                        responses: list[Response]
                        if recovery_mode == "restart":
                            release = store.prepare_intention(
                                run.local_id,
                                "worker-release",
                                ["orchestration", "worker-release", "--dispatch", "dispatch_1"],
                            )
                            store.mark_intention(
                                release,
                                "applied",
                                request_id="request_release_pending",
                                response=release_response,
                            )
                            responses = [_released_prompt_stall_readback(self.root, current_shape=current_shape)]
                        else:
                            responses = [
                                release_response,
                                _released_prompt_stall_readback(self.root, current_shape=current_shape),
                            ]
                        client = FakeClient(responses)

                        finished = _finish_prompt_stall_cleanup(
                            client,  # type: ignore[arg-type]
                            store,
                            run,
                        )

                        self.assertEqual(finished.phase, "worker_failed")
                        expected_release_calls = 0 if recovery_mode == "restart" else 1
                        self.assertEqual(
                            sum(call[:2] == ("orchestration", "worker-release") for call in client.calls),
                            expected_release_calls,
                        )
                        self.assertFalse(any("worker-start" in call for call in client.calls))
                        self.assertFalse(any(call[:2] == ("terminal", "close") for call in client.calls))

    def test_prompt_stall_release_pending_rejects_each_semantic_contradiction_without_replay(self) -> None:
        contradiction_fields = (
            "dispatch_status",
            "dispatch_last_failure",
            "worker_state",
            "worker_stage",
            "worker_last_error",
            "worker_terminal_handle",
            "attached_terminal",
        )
        worktree_id = f"repo::{self.root.resolve()}"
        for current_shape in (False, True):
            for recovery_mode in ("same_runtime", "restart"):
                for field in contradiction_fields:
                    with self.subTest(
                        current_shape=current_shape,
                        recovery_mode=recovery_mode,
                        field=field,
                    ):
                        with tempfile.TemporaryDirectory() as home, StateStore(
                            self.root,
                            home=Path(home),
                        ) as store:
                            run = store.create_run(
                                objective=f"prompt-stall-{current_shape}-{recovery_mode}-{field}",
                                profile_digest="p",
                                source_digest="s",
                            )
                            run = store.update_run(
                                run.local_id,
                                native_run_id="run_1",
                                task_id="task_1",
                                dispatch_id="dispatch_1",
                                phase="launch_cleanup_pending",
                            )
                            _record_fixture_binding(
                                store,
                                run.local_id,
                                worktree_id=worktree_id,
                            )
                            release_response = mutation(
                                "request_release_pending",
                                dispatchId="dispatch_1",
                                state="release_pending",
                                processAction="none",
                                recovery="Retry after endpoint recovery without another coordinator decision.",
                            )
                            latest = _released_prompt_stall_readback(
                                self.root,
                                current_shape=current_shape,
                            )
                            _contradict_release_semantics(
                                latest,
                                current_shape=current_shape,
                                field=field,
                            )
                            responses: list[Response]
                            if recovery_mode == "restart":
                                release = store.prepare_intention(
                                    run.local_id,
                                    "worker-release",
                                    ["orchestration", "worker-release", "--dispatch", "dispatch_1"],
                                )
                                store.mark_intention(
                                    release,
                                    "applied",
                                    request_id="request_release_pending",
                                    response=release_response,
                                )
                                responses = [latest]
                            else:
                                responses = [release_response, latest]
                            client = FakeClient(responses)

                            with self.assertRaises(OrchestrateError) as caught:
                                _finish_prompt_stall_cleanup(
                                    client,  # type: ignore[arg-type]
                                    store,
                                    run,
                                )

                            self.assertEqual(caught.exception.code, "release_unconfirmed")
                            persisted = store.get_run(run.local_id)
                            self.assertEqual(persisted.phase, "launch_cleanup_pending")
                            self.assertIsNone(persisted.worker_outcome)
                            expected_release_calls = 0 if recovery_mode == "restart" else 1
                            self.assertEqual(
                                sum(
                                    call[:2] == ("orchestration", "worker-release")
                                    for call in client.calls
                                ),
                                expected_release_calls,
                            )
                            self.assertFalse(any("worker-start" in call for call in client.calls))
                            self.assertFalse(any(call[:2] == ("terminal", "close") for call in client.calls))
                            self.assertFalse(any("--ack" in call for call in client.calls))

    def _start_pending_question(self, objective: str) -> dict[str, object]:
        delivery_observed = threading.Event()
        responses = completion_responses(self.root, objective)[:4]
        responses.extend([
            _worker_start_with_real_preflight(self.root, current_shape=True),
            _worker_start_readback(self.root, current_shape=True),
        ])
        def pending_delivery(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            delivery_observed.set()
            return {
                "result": {
                    "deliveryId": "delivery_q",
                    "messages": [
                        lifecycle_message(
                            "question",
                            {"taskId": "task_1", "dispatchId": "dispatch_1"},
                            message_id="question_1",
                            body="Choose A or B",
                        )
                    ],
                }
            }
        responses.append(pending_delivery)
        client = FakeClient(responses)
        completed = threading.Event()
        outcome: dict[str, object] = {}

        def run_implementation() -> None:
            try:
                outcome["report"] = implement(
                    self.root,
                    objective,
                    client=client,  # type: ignore[arg-type]
                    wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                    require_context=False,
                )
            except BaseException as exc:
                outcome["error"] = exc
            finally:
                completed.set()

        worker = threading.Thread(target=run_implementation, daemon=True)
        worker.start()
        self.assertTrue(completed.wait(LIVENESS_TIMEOUT), "pending-question fixture exceeded its outer liveness bound")
        if "error" in outcome:
            raise outcome["error"]  # type: ignore[misc]
        report = outcome["report"]
        self.assertTrue(delivery_observed.is_set())
        self.assertEqual(report["status"], "waiting")
        self.assertEqual(report["admission"], "admitted")
        self.assertEqual(report["pendingQuestions"], ["question_1"])
        return report

    def _answer_guard_snapshot(self, local_id: str) -> dict[str, object]:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            return {
                "questions": [
                    dict(row)
                    for row in store.connection.execute(
                        "SELECT * FROM questions WHERE run_local_id = ? ORDER BY message_id",
                        (local_id,),
                    ).fetchall()
                ],
                "deliveries": [
                    dict(row)
                    for row in store.connection.execute(
                        "SELECT * FROM deliveries WHERE run_local_id = ? ORDER BY delivery_id",
                        (local_id,),
                    ).fetchall()
                ],
                "messages": [
                    dict(row)
                    for row in store.connection.execute(
                        """SELECT * FROM delivery_messages
                           WHERE run_local_id = ? ORDER BY delivery_id, ordinal""",
                        (local_id,),
                    ).fetchall()
                ],
                "preflights": [
                    (row["dispatch_id"], row["outcome"], row["observation_json"])
                    for row in store.connection.execute(
                        """SELECT dispatch_id, outcome, observation_json FROM preflight_observations
                           WHERE run_local_id = ? ORDER BY dispatch_id""",
                        (local_id,),
                    ).fetchall()
                ],
            }

    @requires_native_windows_admission
    def test_question_delivery_remains_unacknowledged_then_answers_exactly(self) -> None:
        objective = "Ask when blocked"
        report = self._start_pending_question(objective)

        answer_client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                mutation("request_reply", message={"id": "reply_1"}),
                acknowledgement("delivery_q"),
            ]
        )
        answered = answer(
            self.root,
            str(report["runId"]),
            "question_1",
            "Choose A",
            client=answer_client,  # type: ignore[arg-type]
            require_context=False,
        )
        self.assertEqual(answered["status"], "waiting")
        self.assertEqual(answered["admission"], "admitted")
        self.assertEqual(answered["pendingQuestions"], [])
        self.assertTrue(any(call[:2] == ("orchestration", "reply") for call in answer_client.calls))
        self.assertTrue(any("--ack" in call for call in answer_client.calls))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            question = store.connection.execute(
                "SELECT status, answer FROM questions WHERE message_id = 'question_1'",
            ).fetchone()
            delivery = store.connection.execute(
                "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = 'delivery_q'",
                (str(report["localRunId"]),),
            ).fetchone()
            self.assertEqual((question["status"], question["answer"]), ("answered", "Choose A"))
            self.assertEqual(delivery["acked"], 1)

    @requires_native_windows_admission
    def test_concurrent_public_preflight_cannot_cross_complete_answer_effect_interval(self) -> None:
        objective = "Fence a concurrent answer and second preflight"
        report = self._start_pending_question(objective)
        local_id = str(report["localRunId"])
        before = self._answer_guard_snapshot(local_id)
        answer_inside_fence = threading.Event()
        release_answer = threading.Event()
        answer_exited_fence = threading.Event()
        preflight_at_fence = threading.Event()
        answer_thread: dict[str, int | None] = {"identity": None}
        preflight_thread: dict[str, int | None] = {"identity": None}

        class BlockingAnswerClient(FakeClient):
            def run_json(self, *arguments: str, **kwargs: object) -> dict[str, object]:
                if arguments[:2] == ("orchestration", "run-use"):
                    answer_inside_fence.set()
                    if not release_answer.wait(LIVENESS_TIMEOUT):
                        raise AssertionError("answer effect interval was not released")
                return super().run_json(*arguments, **kwargs)

        answer_client = BlockingAnswerClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                mutation("request_reply", message={"id": "reply_1"}),
                acknowledgement("delivery_q"),
            ]
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            packet = store.get_packet(local_id, "task_1")
        second_client = RealPreflightClient(self.root, packet, current_shape=True)
        original_enter = AdmissionEffectFence.__enter__
        original_exit = AdmissionEffectFence.__exit__

        def observed_enter(fence: AdmissionEffectFence) -> AdmissionEffectFence:
            if threading.get_ident() == preflight_thread["identity"]:
                fence.timeout_seconds = LIVENESS_TIMEOUT
                preflight_at_fence.set()
            return original_enter(fence)  # type: ignore[return-value]

        def observed_exit(
            fence: AdmissionEffectFence,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: object,
        ) -> None:
            original_exit(fence, exc_type, exc, traceback)  # type: ignore[arg-type]
            if threading.get_ident() == answer_thread["identity"]:
                answer_exited_fence.set()

        def run_answer() -> dict[str, object]:
            answer_thread["identity"] = threading.get_ident()
            return answer(
                self.root,
                "run_1",
                "question_1",
                "Choose A",
                client=answer_client,  # type: ignore[arg-type]
                require_context=False,
            )

        def run_second_preflight() -> dict[str, object]:
            preflight_thread["identity"] = threading.get_ident()
            return worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_2",
                packet_id=str(packet["packetId"]),
                client=second_client,  # type: ignore[arg-type]
                environment={"ORCA_TERMINAL_HANDLE": "term_other"},
                platform="win32",
            )

        with patch.object(AdmissionEffectFence, "__enter__", observed_enter), patch.object(
            AdmissionEffectFence,
            "__exit__",
            observed_exit,
        ), ThreadPoolExecutor(
            max_workers=2,
        ) as executor:
            answer_future = executor.submit(run_answer)
            self.assertTrue(answer_inside_fence.wait(LIVENESS_TIMEOUT))
            preflight_future = executor.submit(run_second_preflight)
            self.assertTrue(preflight_at_fence.wait(LIVENESS_TIMEOUT))
            self.assertFalse(preflight_future.done())
            self.assertEqual(self._answer_guard_snapshot(local_id), before)
            release_answer.set()

            self.assertTrue(answer_exited_fence.wait(LIVENESS_TIMEOUT))
            answered = answer_future.result(timeout=LIVENESS_TIMEOUT)
            with self.assertRaises(OrchestrateError) as rejected:
                preflight_future.result(timeout=LIVENESS_TIMEOUT)

        self.assertEqual(answered["admission"], "admitted")
        self.assertEqual(answered["pendingQuestions"], [])
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(second_client.calls, [])
        self.assertTrue(any(call[:2] == ("orchestration", "reply") for call in answer_client.calls))
        self.assertTrue(any("--ack" in call for call in answer_client.calls))
        after = self._answer_guard_snapshot(local_id)
        self.assertEqual(after["preflights"][0], before["preflights"][0])  # type: ignore[index]
        self.assertEqual(
            [(row[0], row[1]) for row in after["preflights"]],  # type: ignore[index]
            [("dispatch_1", "passed"), ("dispatch_2", "rejected")],
        )

        # Once the conflicting row exists, a later public answer performs no
        # native reply/ack and changes none of the immutable or message effects.
        conflict_snapshot = self._answer_guard_snapshot(local_id)
        idle = FakeClient([])
        held = answer(
            self.root,
            "run_1",
            "question_1",
            "Choose A",
            client=idle,  # type: ignore[arg-type]
            require_context=False,
        )
        self.assertEqual(held["status"], "preflight_held")
        self.assertEqual(held["admission"], "conflicting")
        self.assertEqual(idle.calls, [])
        self.assertEqual(self._answer_guard_snapshot(local_id), conflict_snapshot)

    @requires_native_windows_admission
    def test_answer_holds_second_dispatch_observation_before_orca_or_local_effects(self) -> None:
        report = self._start_pending_question("Hold answer after a second Dispatch observation")
        local_id = str(report["localRunId"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(local_id)
            packet = store.get_packet(run.local_id, "task_1")

        second_client = RealPreflightClient(self.root, packet, current_shape=True)
        with self.assertRaises(OrchestrateError) as caught:
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_2",
                packet_id=str(packet["packetId"]),
                client=second_client,  # type: ignore[arg-type]
                environment={"ORCA_TERMINAL_HANDLE": "term_other"},
                platform="win32",
            )
        self.assertEqual(caught.exception.code, "preflight_rejected")
        self.assertEqual(second_client.calls, [])
        before = self._answer_guard_snapshot(local_id)
        self.assertEqual(
            [(row[0], row[1]) for row in before["preflights"]],  # type: ignore[index]
            [("dispatch_1", "passed"), ("dispatch_2", "rejected")],
        )

        idle = FakeClient([])
        held = answer(
            self.root,
            "run_1",
            "question_1",
            "Choose A",
            client=idle,  # type: ignore[arg-type]
        )

        self.assertEqual(held["status"], "preflight_held")
        self.assertEqual(held["admission"], "conflicting")
        self.assertEqual(held["pendingQuestions"], ["question_1"])
        self.assertIn("separately authorized cleanup", held["nextObligation"])
        self.assertEqual(idle.calls, [])
        self.assertEqual(self._answer_guard_snapshot(local_id), before)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(local_id)
            self.assertEqual(run.phase, "preflight_held")
            self.assertEqual(joined_preflight_status(store, run), "conflicting")

    @requires_native_windows_admission
    def test_answer_holds_malformed_observation_before_orca_or_local_effects(self) -> None:
        report = self._start_pending_question("Hold answer after malformed immutable evidence")
        local_id = str(report["localRunId"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            store.connection.execute(
                """UPDATE preflight_observations SET observation_json = ?
                   WHERE run_local_id = ? AND dispatch_id = 'dispatch_1'""",
                ("{malformed", local_id),
            )
        before = self._answer_guard_snapshot(local_id)

        idle = FakeClient([])
        held = answer(
            self.root,
            "run_1",
            "question_1",
            "Choose A",
            client=idle,  # type: ignore[arg-type]
        )

        self.assertEqual(held["status"], "preflight_held")
        self.assertEqual(held["admission"], "conflicting")
        self.assertEqual(held["admissionDetail"]["code"], "preflight_identity_conflict")  # type: ignore[index]
        self.assertEqual(held["pendingQuestions"], ["question_1"])
        self.assertIn("separately authorized cleanup", held["nextObligation"])
        self.assertEqual(idle.calls, [])
        self.assertEqual(self._answer_guard_snapshot(local_id), before)
        self.assertEqual(before["preflights"], [("dispatch_1", "passed", "{malformed")])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            self.assertEqual(store.get_run(local_id).phase, "preflight_held")

    @requires_native_windows_admission
    def test_conflicting_blocked_question_transitions_to_preflight_hold_and_keeps_escalation(self) -> None:
        objective = "Hold a blocked question after conflicting preflight"
        question = lifecycle_message(
            "question",
            {"taskId": "task_1", "dispatchId": "dispatch_1"},
            message_id="question_blocked",
            body="Choose A or B",
        )
        escalation = lifecycle_message(
            "escalation",
            {"taskId": "task_1", "dispatchId": "dispatch_1"},
            message_id="escalation_blocked",
            subject="Blocked: preserve this evidence",
            body="The worker is still blocked.",
        )
        delivery_observed = threading.Event()

        def blocked_delivery(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            delivery_observed.set()
            return {"result": {"deliveryId": "delivery_blocked", "messages": [question, escalation]}}

        responses = completion_responses(self.root, objective)[:4]
        responses.extend([
            _worker_start_with_real_preflight(self.root, current_shape=True),
            _worker_start_readback(self.root, current_shape=True),
            blocked_delivery,
        ])
        client = FakeClient(responses)
        initial = self._run_event_backed(
            lambda: implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="blocked question and escalation",
        )
        local_id = str(initial["localRunId"])
        self.assertEqual(initial["status"], "blocked")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            packet = store.get_packet(local_id, "task_1")
        with self.assertRaises(OrchestrateError):
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_2",
                packet_id=str(packet["packetId"]),
                client=RealPreflightClient(self.root, packet, current_shape=True),  # type: ignore[arg-type]
                environment={"ORCA_TERMINAL_HANDLE": "term_other"},
                platform="win32",
            )
        before = self._answer_guard_snapshot(local_id)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            escalation_evidence = store.evidence(local_id)

        idle = FakeClient([])
        held = answer(
            self.root,
            "run_1",
            "question_blocked",
            "Choose A",
            client=idle,  # type: ignore[arg-type]
            require_context=False,
        )

        self.assertEqual(held["status"], "preflight_held")
        self.assertEqual(held["admission"], "conflicting")
        self.assertEqual(held["pendingQuestions"], ["question_blocked"])
        self.assertIn("immutable worker preflight is held", held["nextObligation"])
        self.assertNotIn("answer question", held["nextObligation"])
        self.assertEqual(idle.calls, [])
        self.assertEqual(self._answer_guard_snapshot(local_id), before)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            self.assertEqual(store.get_run(local_id).phase, "preflight_held")
            self.assertEqual(store.evidence(local_id), escalation_evidence)
        self.assertTrue(any(item["kind"] == "worker-escalation" for item in escalation_evidence))

    @requires_native_windows_admission
    def test_pending_admission_preflight_obligation_precedes_question_instruction(self) -> None:
        report = self._start_pending_question("Report pending admission before a question")
        local_id = str(report["localRunId"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            store.connection.execute(
                "DELETE FROM preflight_observations WHERE run_local_id = ?",
                (local_id,),
            )
        before = self._answer_guard_snapshot(local_id)
        idle = FakeClient([])

        pending = answer(
            self.root,
            "run_1",
            "question_1",
            "Choose A",
            client=idle,  # type: ignore[arg-type]
            require_context=False,
        )

        self.assertEqual(pending["status"], "waiting")
        self.assertEqual(pending["admission"], "pending")
        self.assertEqual(pending["pendingQuestions"], ["question_1"])
        self.assertIn("immutable worker preflight is pending", pending["nextObligation"])
        self.assertIn("recover", pending["nextObligation"])
        self.assertNotIn("answer question", pending["nextObligation"])
        self.assertEqual(idle.calls, [])
        self.assertEqual(self._answer_guard_snapshot(local_id), before)

    def test_realistic_raw_wire_question_and_escalation_are_journaled_before_effects(self) -> None:
        objective = "Hold a real FIFO Delivery"
        question = lifecycle_message(
            "question",
            {
                "taskId": "task_1",
                "dispatchId": "dispatch_1",
                "question": "Should I continue?",
                "options": ["continue", "stop"],
            },
            message_id="question_1",
            body="Should I continue?",
        )
        escalation = lifecycle_message(
            "escalation",
            {"taskId": "task_1", "dispatchId": "dispatch_1"},
            message_id="escalation_1",
            subject="Blocked: exact fixture",
            body="The worker is blocked.",
        )
        delivery_observed = threading.Event()

        def raw_delivery(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            delivery_observed.set()
            return {
                "result": {
                    "deliveryId": "delivery_raw",
                    "messages": [question, escalation],
                }
            }

        client = FakeClient([*completion_responses(self.root, objective)[:6], raw_delivery])

        report = self._run_event_backed(
            lambda: implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="raw question and escalation",
        )

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["pendingQuestions"], ["question_1"])
        self.assertFalse(any("--ack" in call for call in client.calls))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(str(report["localRunId"]))
            persisted = store.delivery_messages(run.local_id, "delivery_raw")
            rows = store.messages(run.local_id, "delivery_raw")
        self.assertEqual(persisted, [question, escalation])
        self.assertEqual([row["effect_status"] for row in rows], ["pending-answer", "unresolved"])
        self.assertIsInstance(persisted[0]["payload"], str)

    def test_raw_lifecycle_payload_rejects_malformed_nonobject_duplicate_and_alias_shapes(self) -> None:
        payloads = {
            "predecoded": {"taskId": "task_1", "dispatchId": "dispatch_1"},
            "malformed": "{",
            "nonobject": "[]",
            "duplicate": '{"taskId":"task_1","taskId":"task_1","dispatchId":"dispatch_1"}',
            "mixed-alias": '{"taskId":"task_1","task_id":"task_1","dispatchId":"dispatch_1"}',
        }
        for label, raw_payload in payloads.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as home, StateStore(
                self.root,
                home=Path(home),
            ) as store:
                run = store.create_run(objective=label, profile_digest="p", source_digest="s")
                run = store.update_run(
                    run.local_id,
                    native_run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    phase="waiting",
                )
                _record_fixture_binding(store, run.local_id)
                message = lifecycle_message(
                    "heartbeat",
                    {"taskId": "task_1", "dispatchId": "dispatch_1"},
                    message_id="message_bad",
                )
                message["payload"] = raw_payload
                delivery = {"result": {"deliveryId": "delivery_bad", "messages": [message]}}
                store.journal_delivery(run.local_id, "delivery_bad", delivery, [message])
                client = FakeClient([])

                with self.assertRaises(OrchestrateError) as caught:
                    _process_delivery(
                        client,  # type: ignore[arg-type]
                        store,
                        run,
                        "delivery_bad",
                        [message],
                    )

                self.assertEqual(caught.exception.code, "delivery_unsupported")
                self.assertEqual(client.calls, [])
                persisted = store.connection.execute(
                    "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = ?",
                    (run.local_id, "delivery_bad"),
                ).fetchone()
                self.assertEqual(persisted["acked"], 0)

    def test_raw_lifecycle_message_rejects_wrong_run_task_dispatch_and_sender(self) -> None:
        cases = {
            "run": lifecycle_message(
                "heartbeat",
                {"taskId": "task_1", "dispatchId": "dispatch_1"},
                message_id="message_bad",
                run_id="run_other",
            ),
            "task": lifecycle_message(
                "heartbeat",
                {"taskId": "task_other", "dispatchId": "dispatch_1"},
                message_id="message_bad",
            ),
            "dispatch": lifecycle_message(
                "heartbeat",
                {"taskId": "task_1", "dispatchId": "dispatch_other"},
                message_id="message_bad",
            ),
            "question-sender": lifecycle_message(
                "question",
                {"taskId": "task_1", "dispatchId": "dispatch_1"},
                message_id="message_bad",
                from_handle="term_worker",
            ),
            "terminal-sender": lifecycle_message(
                "escalation",
                {"taskId": "task_1", "dispatchId": "dispatch_1"},
                message_id="message_bad",
                from_handle="dispatch:dispatch_1",
            ),
        }
        for label, message in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as home, StateStore(
                self.root,
                home=Path(home),
            ) as store:
                run = store.create_run(objective=label, profile_digest="p", source_digest="s")
                run = store.update_run(
                    run.local_id,
                    native_run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    phase="waiting",
                )
                _record_fixture_binding(store, run.local_id)
                delivery = {"result": {"deliveryId": "delivery_bad", "messages": [message]}}
                store.journal_delivery(run.local_id, "delivery_bad", delivery, [message])
                client = FakeClient([])

                with self.assertRaises(OrchestrateError) as caught:
                    _process_delivery(
                        client,  # type: ignore[arg-type]
                        store,
                        run,
                        "delivery_bad",
                        [message],
                    )

                self.assertIn(caught.exception.code, {"delivery_binding_mismatch", "delivery_sender_untrusted"})
                self.assertEqual(client.calls, [])

    def test_delivery_effects_require_the_immutable_whole_delivery_journal(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="journal first", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            _record_fixture_binding(store, run.local_id)
            message = lifecycle_message(
                "heartbeat",
                {"taskId": "task_1", "dispatchId": "dispatch_1"},
                message_id="heartbeat_1",
            )
            client = FakeClient([])

            with self.assertRaises(OrchestrateError) as caught:
                _process_delivery(
                    client,  # type: ignore[arg-type]
                    store,
                    run,
                    "delivery_missing",
                    [message],
                )

            self.assertEqual(caught.exception.code, "delivery_journal_missing")
            self.assertIsNone(store.get_run(run.local_id).delivery_id)
            self.assertEqual(store.evidence(run.local_id), [])

    def test_mixed_fifo_stops_on_stale_row_and_keeps_the_whole_delivery_unacknowledged(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="mixed FIFO", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            _record_fixture_binding(store, run.local_id)
            first = lifecycle_message(
                "heartbeat",
                {"taskId": "task_1", "dispatchId": "dispatch_1"},
                message_id="heartbeat_1",
            )
            stale = lifecycle_message(
                "escalation",
                {"taskId": "task_other", "dispatchId": "dispatch_1"},
                message_id="escalation_stale",
            )
            messages = [first, stale]
            delivery = {"result": {"deliveryId": "delivery_mixed", "messages": messages}}
            store.journal_delivery(run.local_id, "delivery_mixed", delivery, messages)
            client = FakeClient([])

            with self.assertRaises(OrchestrateError) as caught:
                _process_delivery(
                    client,  # type: ignore[arg-type]
                    store,
                    run,
                    "delivery_mixed",
                    messages,
                )

            self.assertEqual(caught.exception.code, "delivery_binding_mismatch")
            rows = store.messages(run.local_id, "delivery_mixed")
            self.assertEqual([row["effect_status"] for row in rows], ["processed", "observed"])
            persisted = store.connection.execute(
                "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = ?",
                (run.local_id, "delivery_mixed"),
            ).fetchone()
            self.assertEqual(persisted["acked"], 0)
            self.assertEqual(client.calls, [])

    def test_unproven_worker_turn_records_one_read_only_diagnostic_without_resending(self) -> None:
        objective = "Diagnose accepted input without a managed preflight"
        client = FakeClient(
            [
                *completion_responses(self.root, objective)[:4],
                _worker_start(self.root),
                _worker_start_readback(self.root),
                {"result": {"deliveryId": None, "messages": [], "timedOut": True}},
                _worker_start_readback(self.root),
            ]
        )

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=300_000,
            require_context=False,
        )

        self.assertEqual(report["status"], "awaiting_preflight")
        self.assertIn("turn start is unproven", report["nextObligation"])
        diagnostics = [
            item for item in report["evidence"]
            if item["kind"] == "compatibility" and item["subject"] == "worker input submission unproven"
        ]
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["status"], "unresolved")

        self.assertFalse(diagnostics[0]["payload"]["taskInputResent"])
        self.assertEqual(sum("worker-start" in call for call in client.calls), 1)
        self.assertEqual(sum(call[:2] == ("orchestration", "worker-show") for call in client.calls), 2)
        self.assertEqual(sum(call[:2] == ("orchestration", "check") for call in client.calls), 1)
        self.assertFalse(any(call[:2] == ("terminal", "send") for call in client.calls))

        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(str(report["localRunId"]))
            repeated = FakeClient([])
            _record_input_submission_diagnostic(repeated, store, run)  # type: ignore[arg-type]
            self.assertEqual(repeated.calls, [])
            self.assertEqual(
                sum(item["subject"] == "worker input submission unproven" for item in store.evidence(run.local_id)),
                1,
            )

    def test_thirty_minute_healthy_wait_uses_native_checks_without_coordination_model_launches(self) -> None:
        objective = "Wait efficiently for the already-running implementation owner"
        responses = completion_responses(self.root, objective)[:6]
        responses.extend(
            {"result": {"deliveryId": None, "messages": [], "timedOut": True}}
            for _ in range(30)
        )
        client = FakeClient(responses)
        clock = (0.0, *(float(index * 60) for index in range(30)), 1800.0)

        with patch("orchestrate.controller._monotonic", side_effect=clock) as controlled_clock:
            report = implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=1_800_000,
                require_context=False,
            )

        checks = [call for call in client.calls if call[:2] == ("orchestration", "check")]
        starts = [call for call in client.calls if call[:2] == ("orchestration", "worker-start")]
        self.assertEqual(report["status"], "waiting")
        self.assertEqual(controlled_clock.call_count, 32)
        self.assertEqual(len(checks), 30)
        self.assertTrue(all(call[call.index("--timeout-ms") + 1] == "60000" for call in checks))
        self.assertEqual(len(starts), 1)
        self.assertEqual(
            report["efficiency"]["usage"]["controllerModelCalls"],
            {"value": 0, "scope": "instrumented deterministic controller only"},
        )
        self.assertEqual(client.responses, [])

    def test_controller_rejects_both_versioned_input_accepted_failure_fields(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="validate accepted worker", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="awaiting_preflight",
            )
            for current_shape in (False, True):
                for field in ("dispatch_last_failure", "worker_last_error"):
                    with self.subTest(current_shape=current_shape, field=field):
                        arguments: dict[str, object] = {field: "contradiction"}
                        payload = _worker_start_readback(
                            self.root,
                            current_shape=current_shape,
                            **arguments,
                        )
                        with self.assertRaises(OrchestrateError) as caught:
                            _validate_worker_start_readback(
                                payload,
                                run=run,
                                dispatch_id="dispatch_1",
                                worktree_id=f"repo::{self.root.resolve()}",
                                worktree_selector=f"path:{self.root.resolve()}",
                                terminal_id="term_worker",
                                agent="codex",
                                model="gpt-5.6-sol",
                                effort="high",
                            )
                        self.assertEqual(caught.exception.code, "orca_contract_error")

    def test_bounded_submission_diagnostic_rejects_contradictory_ready_identity(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="diagnose contradiction", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="awaiting_preflight",
            )
            _record_fixture_binding(
                store,
                run.local_id,
                worktree_id=f"repo::{self.root.resolve()}",
            )
            payload = _worker_start_readback(
                self.root,
                current_shape=True,
                worker_last_error="contradiction",
            )
            client = FakeClient([payload])

            self.assertTrue(_record_input_submission_diagnostic(client, store, run))  # type: ignore[arg-type]

            evidence = store.evidence(run.local_id)
            self.assertEqual(evidence[-1]["status"], "conflicting")
            self.assertEqual(evidence[-1]["payload"]["workerReadback"]["status"], "unsupported-identity-shape")
            self.assertEqual(len(client.calls), 1)
            self.assertFalse(any(call[:2] == ("terminal", "send") for call in client.calls))

    def test_run_summary_surfaces_precise_rejected_preflight_mismatch(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="show rejection", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="awaiting_preflight",
            )
            store.record_preflight(
                run.local_id,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                observation={
                    "schema": "orchestrate-worker-preflight/v1",
                    "outcome": "rejected",
                    "mismatches": [
                        {
                            "code": "preflight_identity_conflict",
                            "message": "terminal.executionHostId did not prove local execution",
                        }
                    ],
                },
            )

            report = _run_summary(store, run)

        self.assertEqual(report["admission"], "rejected")
        self.assertIn("immutable worker preflight is held", report["nextObligation"])
        self.assertEqual(
            report["admissionDetail"]["mismatches"][0]["code"],
            "preflight_identity_conflict",
        )

    def _seed_ackable_delivery(
        self,
        store: StateStore,
        *,
        delivery_id: str,
        objective: str,
    ) -> object:
        run = store.create_run(objective=objective, profile_digest="p", source_digest="s")
        run = store.update_run(
            run.local_id,
            native_run_id=f"run_{delivery_id}",
            phase="waiting",
            delivery_id=delivery_id,
        )
        message = {"id": f"message_{delivery_id}", "type": "heartbeat"}
        payload = {"result": {"deliveryId": delivery_id, "messages": [message]}}
        store.journal_delivery(run.local_id, delivery_id, payload, [message])
        store.mark_message(run.local_id, delivery_id, str(message["id"]), "processed")
        return store.get_run(run.local_id)

    def test_delivery_ack_rejects_missing_null_wrong_conflicting_and_malformed_identity(self) -> None:
        cases = {
            "missing": mutation("request_ack", deliveryId=None, messages=[]),
            "null": mutation("request_ack", acknowledged=None, deliveryId=None, messages=[]),
            "wrong": mutation("request_ack", acknowledged="delivery_other", deliveryId=None, messages=[]),
            "conflicting": mutation(
                "request_ack",
                acknowledged="delivery_conflicting",
                acknowledgedDeliveryId="delivery_other",
                deliveryId=None,
                messages=[],
            ),
            "malformed": mutation("request_ack", acknowledged={"id": "delivery_malformed"}, messages=[]),
        }
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            for label, response in cases.items():
                with self.subTest(label=label):
                    delivery_id = f"delivery_{label}"
                    run = self._seed_ackable_delivery(
                        store,
                        delivery_id=delivery_id,
                        objective=f"reject {label} acknowledgement",
                    )
                    with self.assertRaises(OrchestrateError) as caught:
                        _ack_if_resolved(FakeClient([response]), store, run)  # type: ignore[arg-type]
                    self.assertEqual(caught.exception.code, "mutation_outcome_uncertain")
                    persisted = store.get_run(run.local_id)
                    self.assertEqual(persisted.delivery_id, delivery_id)
                    delivery = store.connection.execute(
                        "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = ?",
                        (run.local_id, delivery_id),
                    ).fetchone()
                    self.assertEqual(delivery["acked"], 0)
                    intention = store.connection.execute(
                        "SELECT status FROM intentions WHERE run_local_id = ? AND operation = 'delivery-ack'",
                        (run.local_id,),
                    ).fetchone()
                    self.assertEqual(intention["status"], "uncertain")

    def test_applied_ack_receipt_replays_after_crash_before_local_commit(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = self._seed_ackable_delivery(
                store,
                delivery_id="delivery_crash",
                objective="recover exact acknowledgement receipt",
            )
            arguments = [
                "orchestration",
                "check",
                "--run",
                str(run.native_run_id),
                "--ack",
                "delivery_crash",
            ]
            intention = store.prepare_intention(run.local_id, "delivery-ack", arguments)
            store.mark_intention(
                intention,
                "applied",
                request_id="request_ack",
                returncode=0,
                response=acknowledgement("delivery_crash"),
            )

            recovered = reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]

            self.assertIsNone(recovered.delivery_id)
            delivery = store.connection.execute(
                "SELECT acked FROM deliveries WHERE run_local_id = ? AND delivery_id = 'delivery_crash'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual(delivery["acked"], 1)

    def test_applied_ack_without_exact_identity_is_never_inferred_from_argv(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = self._seed_ackable_delivery(
                store,
                delivery_id="delivery_legacy",
                objective="reject argument-only acknowledgement",
            )
            intention = store.prepare_intention(
                run.local_id,
                "delivery-ack",
                ["orchestration", "check", "--run", str(run.native_run_id), "--ack", "delivery_legacy"],
            )
            store.mark_intention(
                intention,
                "applied",
                request_id="request_ack",
                returncode=0,
                response=mutation("request_ack", messages=[]),
            )

            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]

            self.assertEqual(caught.exception.code, "delivery_ack_unconfirmed")
            self.assertEqual(store.get_run(run.local_id).delivery_id, "delivery_legacy")

    def test_uncertain_ack_exact_retry_requires_same_acknowledged_delivery(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = self._seed_ackable_delivery(
                store,
                delivery_id="delivery_retry",
                objective="retry exact acknowledgement",
            )
            intention = store.prepare_intention(
                run.local_id,
                "delivery-ack",
                ["orchestration", "check", "--run", str(run.native_run_id), "--ack", "delivery_retry"],
            )
            store.mark_intention(intention, "uncertain", request_id="request_ack")
            client = FakeClient(
                [
                    request_show("request_ack", method="orchestration.check"),
                    acknowledgement("delivery_retry"),
                ]
            )

            recovered = reconcile_intentions(client, store, run)  # type: ignore[arg-type]

            self.assertIsNone(recovered.delivery_id)
            retry = client.calls[-1]
            self.assertEqual(retry[retry.index("--retry-request") + 1], "request_ack")

    def test_uncertain_ack_retry_with_wrong_acknowledged_delivery_remains_uncommitted(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = self._seed_ackable_delivery(
                store,
                delivery_id="delivery_retry_wrong",
                objective="reject wrong recovered acknowledgement",
            )
            intention = store.prepare_intention(
                run.local_id,
                "delivery-ack",
                [
                    "orchestration",
                    "check",
                    "--run",
                    str(run.native_run_id),
                    "--ack",
                    "delivery_retry_wrong",
                ],
            )
            store.mark_intention(intention, "uncertain", request_id="request_ack")
            client = FakeClient(
                [
                    request_show("request_ack", method="orchestration.check"),
                    acknowledgement("delivery_other"),
                ]
            )

            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(client, store, run)  # type: ignore[arg-type]

            self.assertEqual(caught.exception.code, "mutation_outcome_uncertain")
            self.assertEqual(store.get_run(run.local_id).delivery_id, "delivery_retry_wrong")
            row = store.connection.execute(
                "SELECT status FROM intentions WHERE id = ?",
                (intention,),
            ).fetchone()
            self.assertEqual(row["status"], "uncertain")

    def test_ack_response_journals_a_distinct_valid_next_fifo_delivery(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = self._seed_ackable_delivery(
                store,
                delivery_id="delivery_first",
                objective="preserve next FIFO delivery",
            )
            next_message = {"id": "message_next", "type": "heartbeat"}
            response = acknowledgement(
                "delivery_first",
                deliveryId="delivery_next",
                messages=[next_message],
            )

            self.assertTrue(_ack_if_resolved(FakeClient([response]), store, run))  # type: ignore[arg-type]

            current = store.get_run(run.local_id)
            self.assertEqual(current.delivery_id, "delivery_next")
            rows = store.messages(run.local_id, "delivery_next")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["message_id"], "message_next")
            self.assertEqual(rows[0]["effect_status"], "observed")

    def _seed_rejected_preflight(self) -> tuple[str, dict[str, str]]:
        mismatch = {
            "code": "preflight_identity_conflict",
            "message": "terminal.executionHostId did not prove local execution",
        }
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="held objective", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_held",
                task_id="task_held",
                dispatch_id="dispatch_held",
                phase="awaiting_preflight",
            )
            store.record_worker_resource_binding(
                run.local_id,
                dispatch_id="dispatch_held",
                resource_id="resource_held",
                terminal_handle="term_held",
                worktree_id="worktree_held",
                readback={"immutable": True},
            )
            store.record_preflight(
                run.local_id,
                run_id="run_held",
                task_id="task_held",
                dispatch_id="dispatch_held",
                observation={
                    "schema": "orchestrate-worker-preflight/v1",
                    "outcome": "rejected",
                    "runId": "run_held",
                    "taskId": "task_held",
                    "dispatchId": "dispatch_held",
                    "mismatches": [mismatch],
                },
            )
            return run.local_id, mismatch

    def test_implement_surfaces_rejected_preflight_hold_with_zero_orca_effects(self) -> None:
        local_id, mismatch = self._seed_rejected_preflight()
        client = FakeClient([])

        report = implement(
            self.root,
            "a different objective must not replace the held attempt",
            client=client,  # type: ignore[arg-type]
            require_context=False,
        )

        self.assertEqual(report["localRunId"], local_id)
        self.assertEqual(report["status"], "preflight_held")
        self.assertEqual(report["admission"], "rejected")
        self.assertEqual(report["admissionDetail"]["mismatches"], [mismatch])
        self.assertEqual(client.calls, [])

    def test_resume_surfaces_rejected_preflight_hold_with_zero_orca_effects(self) -> None:
        local_id, mismatch = self._seed_rejected_preflight()
        client = FakeClient([])

        report = resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            require_context=False,
        )

        self.assertEqual(report["status"], "preflight_held")
        self.assertEqual(report["admissionDetail"]["mismatches"], [mismatch])
        self.assertIn("separately authorized cleanup", report["nextObligation"])
        self.assertEqual(client.calls, [])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            binding = store.get_worker_resource_binding(local_id)
            self.assertEqual(binding.resource_id, "resource_held")  # type: ignore[union-attr]

    def test_resume_holds_malformed_immutable_preflight_with_zero_orca_effects(self) -> None:
        local_id, _ = self._seed_rejected_preflight()
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            store.connection.execute(
                "UPDATE preflight_observations SET observation_json = ? WHERE run_local_id = ?",
                ("{malformed", local_id),
            )
        client = FakeClient([])

        report = resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            require_context=False,
        )

        self.assertEqual(report["status"], "preflight_held")
        self.assertEqual(report["admission"], "conflicting")
        self.assertEqual(report["admissionDetail"]["code"], "preflight_identity_conflict")
        self.assertIn("malformed", report["admissionDetail"]["message"].lower())
        self.assertEqual(client.calls, [])

    def test_resume_holds_conflicting_immutable_preflight_with_zero_orca_effects(self) -> None:
        local_id, _ = self._seed_rejected_preflight()
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            store.connection.execute(
                "UPDATE preflight_observations SET observation_json = ? WHERE run_local_id = ?",
                (
                    json.dumps(
                        {
                            "schema": "orchestrate-worker-preflight/v1",
                            "outcome": "indeterminate",
                            "mismatches": [{"code": "conflicting_outcome"}],
                        }
                    ),
                    local_id,
                ),
            )
        client = FakeClient([])

        report = resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            require_context=False,
        )

        self.assertEqual(report["status"], "preflight_held")
        self.assertEqual(report["admission"], "conflicting")
        self.assertEqual(report["admissionDetail"]["code"], "preflight_identity_conflict")
        self.assertEqual(report["admissionDetail"]["mismatches"], [{"code": "conflicting_outcome"}])
        self.assertEqual(client.calls, [])

    def test_uncertain_request_replays_only_exact_native_shape(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create", "--objective", "recover"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            client = FakeClient(
                [
                    request_show("request_1"),
                    {
                        "result": {
                            "run": {"id": "run_recovered"},
                            "mutation": {"requestId": "request_1", "replayed": True},
                        }
                    },
                ]
            )
            recovered = reconcile_intentions(client, store, run)  # type: ignore[arg-type]
            self.assertEqual(recovered.native_run_id, "run_recovered")
            self.assertIn("--retry-request", client.calls[1])

    def test_generic_request_status_is_rejected(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([{"result": {"status": "completed"}}]), store, run)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "orca_contract_error")

    def test_request_receipt_for_a_different_method_is_rejected(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(
                    FakeClient([request_show("request_1", method="orchestration.task-create")]),  # type: ignore[arg-type]
                    store,
                    run,
                )
            self.assertEqual(caught.exception.code, "orca_contract_error")

    def test_native_mutation_request_id_wins_over_transport_correlation(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            response = {
                "id": "transport-correlation",
                "result": {
                    "run": {"id": "run_recovered"},
                    "mutation": {"requestId": "native-mutation", "replayed": False},
                },
            }
            _mutation(FakeClient([response]), store, run, "run-create", ["orchestration", "run-create"])  # type: ignore[arg-type]
            row = store.connection.execute("SELECT * FROM intentions").fetchone()
            self.assertEqual(row["request_id"], "native-mutation")

    def test_error_data_mutation_receipt_is_recoverable_but_generic_id_is_not(self) -> None:
        payload = {
            "id": "transport-correlation",
            "error": {"data": {"mutation": {"requestId": "native-error-request"}, "requestId": "generic"}},
        }
        self.assertEqual(_request_id(payload), "native-error-request")
        self.assertIsNone(_request_id({"error": {"data": {"requestId": "generic"}}}))

    def test_prepared_but_never_invoked_intention_is_safe_to_start(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create", "--objective", "recover"])
            recovered = reconcile_intentions(
                FakeClient([mutation("request_1", run={"id": "run_recovered"})]),  # type: ignore[arg-type]
                store,
                run,
            )
            self.assertEqual(recovered.native_run_id, "run_recovered")

    def test_missing_request_history_preserves_uncertainty(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create"])
            store.mark_intention(intention, "uncertain")
            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "unknown_external_effect")

    def test_replayed_processed_delivery_does_not_release_twice(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="replay", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            message = lifecycle_message(
                "worker_done",
                {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
                message_id="message_done",
                subject="done",
            )
            payload = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
            store.journal_delivery(run.local_id, "delivery_1", payload, [message])
            _record_fixture_binding(store, run.local_id)
            completed = _process_delivery(FakeClient(settlement_responses()), store, run, "delivery_1", [message])  # type: ignore[arg-type]
            replay_client = FakeClient([])
            replayed = _process_delivery(replay_client, store, completed, "delivery_1", [message])  # type: ignore[arg-type]
            self.assertEqual(replayed.phase, "worker_unadmitted")
            self.assertEqual(replay_client.calls, [])

    def test_release_receipt_survives_failed_readback_without_reissuing_release(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="release recovery", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            message = lifecycle_message(
                "worker_done",
                {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
                message_id="message_done",
                subject="done",
            )
            delivery = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
            store.journal_delivery(run.local_id, "delivery_1", delivery, [message])
            _record_fixture_binding(store, run.local_id)
            first_responses = settlement_responses()
            pending_readback = settlement_responses()[-1]
            pending_readback["result"]["terminalResource"]["releaseState"] = "releasing"  # type: ignore[index]
            pending_readback["result"]["terminalResource"]["releaseCompletedAt"] = None  # type: ignore[index]
            first_responses[-1] = pending_readback
            with self.assertRaises(OrchestrateError) as initial:
                _process_delivery(FakeClient(first_responses), store, run, "delivery_1", [message])  # type: ignore[arg-type]
            self.assertEqual(initial.exception.code, "release_unconfirmed")

            exact = settlement_responses()
            recovery_client = FakeClient([exact[0], exact[1], exact[-1]])
            recovered = _process_delivery(
                recovery_client,
                store,
                store.get_run(run.local_id),
                "delivery_1",
                [message],
            )  # type: ignore[arg-type]

            self.assertEqual(recovered.phase, "worker_unadmitted")
            self.assertFalse(any("worker-release" in call for call in recovery_client.calls))
            self.assertEqual(len(store.evidence(run.local_id)), 3)

    def test_resume_reprocesses_terminal_phase_observed_delivery_and_acks(self) -> None:
        profile = setup_project(self.root)
        objective = "Complete"
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        message = lifecycle_message(
            "worker_done",
            {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
            message_id="message_done",
            subject="done",
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="worker_succeeded",
                worker_outcome="succeeded",
                delivery_id="delivery_1",
            )
            store.journal_delivery(
                run.local_id,
                "delivery_1",
                {"result": {"deliveryId": "delivery_1", "messages": [message]}},
                [message],
            )
            release = store.prepare_intention(
                run.local_id,
                "worker-release",
                ["orchestration", "worker-release", "--dispatch", "dispatch_1"],
            )
            store.mark_intention(
                release,
                "applied",
                request_id="request_release",
                response=mutation("request_release", dispatchId="dispatch_1", state="released"),
            )
            _record_fixture_binding(store, run.local_id)
            local_id = run.local_id
        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            *settlement_responses()[:2],
            settlement_responses()[3],
            acknowledgement("delivery_1"),
        ]
        report = resume(
            self.root,
            local_id,
            client=FakeClient(responses),  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )
        self.assertEqual(report["status"], "worker_unadmitted")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            persisted = store.get_run(local_id)
            self.assertIsNone(persisted.delivery_id)
            self.assertEqual(store.messages(local_id, "delivery_1")[0]["effect_status"], "processed")

    def test_dispatched_and_stored_packet_have_one_truthful_identity(self) -> None:
        objective = "Inspect packet identity"
        delivery_observed = threading.Event()

        def pending_delivery(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            delivery_observed.set()
            return {
                "result": {
                    "deliveryId": "delivery_q",
                    "messages": [
                        lifecycle_message(
                            "question",
                            {"taskId": "task_1", "dispatchId": "dispatch_1"},
                            message_id="question_1",
                            body="pause",
                        )
                    ],
                }
            }

        responses = completion_responses(self.root, objective)[:6]
        responses.append(pending_delivery)
        client = FakeClient(responses)
        report = self._run_event_backed(
            lambda: implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="stored-packet question",
        )
        task_call = next(call for call in client.calls if "task-create" in call)
        spec = task_call[task_call.index("--spec") + 1]
        embedded_json = spec[spec.index("{"):]
        dispatched = json.loads(embedded_json)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            stored = store.get_packet(str(report["localRunId"]), "task_1")
            stored_bytes = store.connection.execute(
                "SELECT CAST(packet_json AS BLOB) AS packet_bytes FROM packets WHERE run_local_id = ? AND task_id = ?",
                (str(report["localRunId"]), "task_1"),
            ).fetchone()["packet_bytes"]
        self.assertEqual(stored_bytes, embedded_json.encode("utf-8"))
        self.assertEqual(dispatched, stored)
        self.assertEqual(dispatched["schema"], "orchestrate-worker-packet/v3")
        self.assertEqual(dispatched["admission"]["commandTemplate"][1:4], ["-I", "-m", "orchestrate"])
        self.assertEqual(
            Path(dispatched["admission"]["commandTemplate"][0]).resolve(),
            Path(sys.executable).resolve(),
        )
        self.assertEqual(dispatched["native"]["taskIdSource"], "orca-injected-task-and-dispatch-preamble")
        self.assertNotIn("taskId", dispatched["native"])
        self.assertEqual(dispatched["operationalProfile"]["selectedDigest"], dispatched["profileDigest"])
        self.assertEqual(dispatched["operationalProfile"]["selectionSource"], "initial-setup-selection")

    def test_legacy_packet_json_is_not_rewritten_and_still_requires_task_readback(self) -> None:
        objective = "Resume a legacy packet row"
        profile = setup_project(self.root)
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
        ]
        client = FakeClient(responses)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1", phase="task_created")
            packet = make_packet(
                objective=objective,
                profile=profile,
                sources=sources,
                launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                python_executable=str(Path(sys.executable).resolve()),
                run_id="run_1",
                reader=reader,
            )
            legacy_json = json.dumps(packet, sort_keys=True)
            store.connection.execute(
                "INSERT INTO packets VALUES (?, ?, ?, datetime('now'))",
                (run.local_id, "task_1", legacy_json),
            )
            local_id = run.local_id

        responses.append(
            {
                "result": {
                    "tasks": [
                        {
                            "id": "task_1",
                            "run_id": "run_1",
                            "task_title": objective,
                            "spec": packet_spec(packet),
                            "status": "ready",
                        }
                    ]
                }
            }
        )
        responses.extend(
            [
                _worker_start(self.root),
                _worker_start_readback(self.root),
            ]
        )
        resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            self.assertEqual(store.get_packet_json(local_id, "task_1"), legacy_json)
            self.assertNotEqual(legacy_json, canonical_packet_json(packet))

    def test_worker_launch_substitution_holds_before_waiting(self) -> None:
        objective = "Reject substituted model"
        responses = completion_responses(self.root, objective)[:4]
        substituted = _worker_start(self.root)
        substituted["result"]["launch"]["effective"]["model"] = "different"  # type: ignore[index]
        responses.append(substituted)
        with self.assertRaises(OrchestrateError) as caught:
            implement(
                self.root,
                objective,
                client=FakeClient(responses),  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "worker_launch_mismatch")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.select_run(None)
            intention = store.connection.execute(
                "SELECT status FROM intentions WHERE run_local_id = ? AND operation = 'worker-start'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual(intention["status"], "receipt_confirmed")
            with self.assertRaises(OrchestrateError) as replay:
                reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]
            self.assertEqual(replay.exception.code, "worker_launch_mismatch")
            self.assertEqual(store.get_run(run.local_id).phase, "task_created")

    def test_worker_start_exit_contract_is_enforced_for_fresh_and_recovery_callers(self) -> None:
        objective = "Reject mismatched worker-start exit status"
        ready_nonzero = OrcaJsonResponse(_worker_start(self.root), returncode=1)
        with self.assertRaises(OrchestrateError) as fresh:
            implement(
                self.root,
                objective,
                client=FakeClient([*completion_responses(self.root, objective)[:4], ready_nonzero]),  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(fresh.exception.code, "worker_start_exit_mismatch")

        arguments = [
            "orchestration",
            "worker-start",
            "--run",
            "run_1",
            "--task",
            "task_1",
            "--worktree",
            f"path:{self.root.resolve()}",
            "--agent",
            "codex",
            "--model",
            "gpt-5.6-sol",
            "--effort",
            "high",
        ]
        for state, returncode in (("ready", 1), ("failed", 37)):
            with self.subTest(state=state, returncode=returncode):
                with StateStore(self.root, home=Path(self.state_temp.name)) as store:
                    run = store.create_run(
                        objective=f"recover-{state}-{returncode}",
                        profile_digest="p",
                        source_digest="s",
                    )
                    run = store.update_run(
                        run.local_id,
                        native_run_id=f"run-{state}-{returncode}",
                        task_id="task_1",
                        phase="task_created",
                    )
                    response = _worker_start(self.root) if state == "ready" else _prompt_stall_start(self.root)
                    response["result"]["runId"] = str(run.native_run_id)  # type: ignore[index]
                    intention = store.prepare_intention(run.local_id, "worker-start", arguments)
                    store.mark_intention(
                        intention,
                        "receipt_confirmed",
                        request_id="request_worker",
                        returncode=returncode,
                        response=response,
                    )
                    with self.assertRaises(OrchestrateError) as recovered:
                        reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]
                    self.assertEqual(recovered.exception.code, "worker_start_exit_mismatch")
                    self.assertEqual(store.get_run(run.local_id).phase, "task_created")

    def test_worker_start_client_seam_without_returncode_fails_closed(self) -> None:
        objective = "Reject a worker-start response without process status"
        response_without_returncode = dict(_worker_start(self.root))
        client = FakeClient(
            [
                *completion_responses(self.root, objective)[:4],
                response_without_returncode,
            ],
            default_returncode=None,
        )

        with self.assertRaises(OrchestrateError) as caught:
            implement(
                self.root,
                objective,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )

        self.assertEqual(caught.exception.code, "orca_contract_error")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-show") for call in client.calls))

    def test_prompt_stall_binds_failed_dispatch_and_releases_terminal_before_return(self) -> None:
        objective = "Contain a stalled prompt"
        responses = completion_responses(self.root, objective)[:4]
        responses.extend(
            [
                _prompt_stall_start(self.root),
                _prompt_stall_readback(self.root, current_shape=True),
                mutation(
                    "request_release",
                    dispatchId="dispatch_1",
                    state="released",
                    processAction="closed_agent_terminal",
                    archive={"source": "transcript", "status": "captured"},
                ),
                _released_prompt_stall_readback(self.root),
            ]
        )
        client = FakeClient(responses)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["dispatchId"], "dispatch_1")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertEqual(report["verification"], "not_run")
        self.assertFalse(any(call[:2] == ("orchestration", "check") for call in client.calls))
        start_index = next(i for i, call in enumerate(client.calls) if "worker-start" in call)
        release_index = next(i for i, call in enumerate(client.calls) if "worker-release" in call)
        self.assertLess(start_index, release_index)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            persisted = store.get_run(str(report["localRunId"]))
            self.assertEqual(persisted.phase, "worker_failed")
            statuses = {
                row["operation"]: row["status"]
                for row in store.connection.execute(
                    "SELECT operation, status FROM intentions WHERE run_local_id = ?",
                    (persisted.local_id,),
                ).fetchall()
            }
            self.assertEqual(statuses["worker-start"], "applied")
            self.assertEqual(statuses["worker-release"], "applied")

    def test_release_pending_restart_converges_by_exact_readback_without_release_replay(self) -> None:
        objective = "Recover a pending prompt-stall release"
        pending_readback = _prompt_stall_readback(self.root, current_shape=True)
        pending_resource = pending_readback["result"]["terminalResource"]  # type: ignore[index]
        pending_resource.update(  # type: ignore[union-attr]
            releaseState="releasing",
            releaseRequestedAt="2026-01-01T00:00:00Z",
            releaseCompletedAt=None,
            releaseError=None,
            archive={"source": "transcript", "status": "captured"},
        )
        recovery_text = (
            "The owning endpoint is temporarily unavailable; recovery will retry this release "
            "after reconnect without another coordinator decision."
        )
        first_responses = completion_responses(self.root, objective)[:4]
        first_responses.extend(
            [
                _prompt_stall_start(self.root),
                _prompt_stall_readback(self.root, current_shape=True),
                mutation(
                    "request_release_pending",
                    dispatchId="dispatch_1",
                    state="release_pending",
                    processAction="none",
                    archive={"source": "transcript", "status": "captured"},
                    lastError="endpoint unavailable",
                    recovery=recovery_text,
                ),
                pending_readback,
            ]
        )
        with self.assertRaises(OrchestrateError) as initial:
            implement(
                self.root,
                objective,
                client=FakeClient(first_responses),  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(initial.exception.code, "release_pending")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            pending_run = store.select_run(None)
            self.assertEqual(pending_run.phase, "launch_cleanup_pending")
            stored_release = store.connection.execute(
                "SELECT response_json FROM intentions WHERE run_local_id = ? AND operation = 'worker-release'",
                (pending_run.local_id,),
            ).fetchone()
            self.assertEqual(
                json.loads(stored_release["response_json"])["result"]["recovery"],
                recovery_text,
            )
            local_id = pending_run.local_id

        still_pending_client = FakeClient(
            [
                mutation("request_use_still_pending", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                pending_readback,
            ]
        )
        with self.assertRaises(OrchestrateError) as still_pending:
            resume(
                self.root,
                local_id,
                client=still_pending_client,  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(still_pending.exception.code, "release_pending")
        self.assertFalse(
            any(call[:2] == ("orchestration", "worker-start") for call in still_pending_client.calls)
        )
        self.assertFalse(
            any(call[:2] == ("orchestration", "worker-release") for call in still_pending_client.calls)
        )
        self.assertFalse(any(call[:2] == ("terminal", "close") for call in still_pending_client.calls))
        self.assertEqual(
            sum(call[:2] == ("orchestration", "worker-show") for call in still_pending_client.calls),
            1,
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            self.assertEqual(store.get_run(local_id).phase, "launch_cleanup_pending")

        recovery_client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                _released_prompt_stall_readback(self.root, current_shape=True),
            ]
        )
        report = resume(
            self.root,
            local_id,
            client=recovery_client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )
        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertEqual(report["verification"], "not_run")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in recovery_client.calls))
        self.assertFalse(any(call[:2] == ("orchestration", "worker-release") for call in recovery_client.calls))
        self.assertFalse(any(call[:2] == ("terminal", "close") for call in recovery_client.calls))
        self.assertEqual(
            sum(call[:2] == ("orchestration", "worker-show") for call in recovery_client.calls),
            1,
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            recovered = store.get_run(local_id)
            self.assertEqual(recovered.phase, "worker_failed")
            self.assertEqual(recovered.verification_status, "not_run")

    def test_resume_converges_from_live_released_prompt_stall_shape_without_lifecycle_replay(self) -> None:
        objective = "Recover the already released prompt-stall worker"
        worktree_id = f"repo::{self.root.resolve()}"
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="launch_cleanup_pending",
            )
            _record_fixture_binding(store, run.local_id, worktree_id=worktree_id)
            release = store.prepare_intention(
                run.local_id,
                "worker-release",
                ["orchestration", "worker-release", "--dispatch", "dispatch_1"],
            )
            store.mark_intention(
                release,
                "applied",
                request_id="request_release",
                response=mutation(
                    "request_release",
                    dispatchId="dispatch_1",
                    state="already_released",
                ),
            )
            local_id = run.local_id

        live_readback = _released_prompt_stall_readback(self.root, current_shape=True)
        client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                live_readback,
            ]
        )

        report = resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertEqual(report["verification"], "not_run")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in client.calls))
        self.assertFalse(any(call[:2] == ("orchestration", "worker-release") for call in client.calls))
        self.assertFalse(any(call[:2] == ("terminal", "close") for call in client.calls))
        self.assertFalse(any("--ack" in call for call in client.calls))
        self.assertEqual(
            sum(call[:2] == ("orchestration", "worker-show") for call in client.calls),
            1,
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            persisted = store.get_run(local_id)
            self.assertEqual(persisted.phase, "worker_failed")
            self.assertEqual(persisted.verification_status, "not_run")

    def test_resume_recovers_fea_prompt_stall_receipt_without_replaying_worker_start(self) -> None:
        objective = "Recover the stored failed start"
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                phase="task_created",
            )
            arguments = [
                "orchestration",
                "worker-start",
                "--run",
                "run_1",
                "--task",
                "task_1",
                "--worktree",
                f"path:{self.root.resolve()}",
                "--agent",
                "codex",
                "--model",
                "gpt-5.6-sol",
                "--effort",
                "high",
            ]
            intention = store.prepare_intention(run.local_id, "worker-start", arguments)
            store.mark_intention(
                intention,
                "uncertain",
                request_id="request_worker",
                error=_prompt_stall_start(self.root, include_ok=True),
            )
            local_id = run.local_id

        responses: list[Response] = [
            _prompt_stall_readback(self.root),
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            mutation(
                "request_release",
                dispatchId="dispatch_1",
                state="released",
                processAction="closed_agent_terminal",
                archive={"source": "transcript", "status": "captured"},
            ),
            _released_prompt_stall_readback(self.root),
        ]
        client = FakeClient(responses)

        report = resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["dispatchId"], "dispatch_1")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in client.calls))
        self.assertFalse(any(call[:2] == ("orchestration", "request-show") for call in client.calls))
        self.assertEqual(sum(call[:2] == ("orchestration", "worker-release") for call in client.calls), 1)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            persisted = store.get_run(local_id)
            self.assertEqual(persisted.phase, "worker_failed")
            stored_start = store.connection.execute(
                "SELECT status, response_json FROM intentions WHERE id = ?",
                (intention,),
            ).fetchone()
            self.assertEqual(stored_start["status"], "applied")
            self.assertEqual(json.loads(stored_start["response_json"])["result"]["lastError"], "agent_prompt_stalled")

    def test_last_prelaunch_source_validation_stops_before_worker_start(self) -> None:
        objective = "Reject prelaunch drift"
        responses = completion_responses(self.root, objective)[:3]

        def drift_after_task_readback(client: FakeClient, arguments: tuple[str, ...]) -> dict[str, object]:
            response = _task_readback(client, arguments)
            (self.root / "AGENTS.md").write_text("Changed before worker-start.\n", encoding="utf-8")
            return response

        responses.append(drift_after_task_readback)
        client = FakeClient(responses)
        with self.assertRaises(OrchestrateError) as caught:
            implement(self.root, objective, client=client, wait_timeout_ms=1, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "source_binding_changed")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in client.calls))

    def _assert_real_pre_receipt_preflight_joins(self, *, current_shape: bool) -> None:
        objective = "Join the real early worker preflight"
        responses = completion_responses(self.root, objective)[:4]
        responses.extend([
            _worker_start_with_real_preflight(self.root, current_shape=current_shape),
            _worker_start_readback(self.root, current_shape=current_shape),
        ])
        client = FakeClient(responses)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["status"], "waiting")
        self.assertEqual(report["admission"], "admitted")
        self.assertEqual(report["dispatchId"], "dispatch_1")
        self.assertEqual(report["admissionDetail"]["outcome"], "passed")
        self.assertNotIn("otherDispatchObservations", report["admissionDetail"])
        self.assertNotIn("status", report["admissionDetail"])
        self.assertFalse(any(call[:2] == ("orchestration", "check") for call in client.calls))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(str(report["localRunId"]))
            observation = store.get_preflight(run.local_id, "task_1", "dispatch_1")
            binding = store.get_worker_resource_binding(run.local_id)
            self.assertEqual(joined_preflight_status(store, run), "admitted")
            self.assertEqual(
                [item["dispatchId"] for item in store.list_preflights(run.local_id, "task_1")],
                ["dispatch_1"],
            )
            self.assertEqual(observation["outcome"], "passed")  # type: ignore[index]
            self.assertEqual(observation["native"]["terminalResourceId"], binding.resource_id)  # type: ignore[index,union-attr]
            self.assertEqual(observation["native"]["terminalHandle"], binding.terminal_handle)  # type: ignore[index,union-attr]
            self.assertEqual(observation["native"]["worktreeId"], binding.worktree_id)  # type: ignore[index,union-attr]

    @requires_native_windows_admission
    def test_orca_1_4_198_real_preflight_before_controller_receipt_joins_to_admitted(self) -> None:
        self._assert_real_pre_receipt_preflight_joins(current_shape=False)

    @requires_native_windows_admission
    def test_orca_1_4_199_real_preflight_before_controller_receipt_joins_to_admitted(self) -> None:
        self._assert_real_pre_receipt_preflight_joins(current_shape=True)

    def _assert_later_controller_binding_mismatch_holds(self, field: str) -> None:
        objective = f"Hold a later {field} mismatch"
        later = {
            "resource_id": "terminal-resource-1",
            "terminal_handle": "term_worker",
            "worktree_id": f"repo::{self.root.resolve()}",
        }
        later[field] = f"different-{field}"
        responses = completion_responses(self.root, objective)[:4]
        responses.extend([
            _worker_start_with_real_preflight(
                self.root,
                current_shape=True,
                later_resource_id=later["resource_id"],
                later_terminal_handle=later["terminal_handle"],
                later_worktree_id=later["worktree_id"],
            ),
            _worker_start_readback(
                self.root,
                current_shape=True,
                resource_id=later["resource_id"],
                terminal_handle=later["terminal_handle"],
                worktree_id=later["worktree_id"],
            ),
        ])
        client = FakeClient(responses)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["status"], "preflight_held")
        self.assertEqual(report["admission"], "conflicting")
        self.assertIn("separately authorized cleanup", report["nextObligation"])
        self.assertFalse(any(call[:2] == ("orchestration", "check") for call in client.calls))
        self.assertFalse(any(call[:2] == ("terminal", "send") for call in client.calls))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(str(report["localRunId"]))
            observation = store.get_preflight(run.local_id, "task_1", "dispatch_1")
            binding = store.get_worker_resource_binding(run.local_id)
            native_field = {
                "resource_id": "terminalResourceId",
                "terminal_handle": "terminalHandle",
                "worktree_id": "worktreeId",
            }[field]
            self.assertEqual(run.phase, "preflight_held")
            self.assertNotEqual(observation["native"][native_field], getattr(binding, field))  # type: ignore[index,arg-type]

    @requires_native_windows_admission
    def test_later_controller_resource_id_mismatch_holds(self) -> None:
        self._assert_later_controller_binding_mismatch_holds("resource_id")

    @requires_native_windows_admission
    def test_later_controller_terminal_handle_mismatch_holds(self) -> None:
        self._assert_later_controller_binding_mismatch_holds("terminal_handle")

    @requires_native_windows_admission
    def test_later_controller_worktree_id_mismatch_holds(self) -> None:
        self._assert_later_controller_binding_mismatch_holds("worktree_id")

    def _ordinary_effect_calls(self, client: FakeClient) -> list[tuple[str, ...]]:
        """Every Orca call after the worker-start receipt other than its own binding readback."""

        start_index = next(
            index for index, call in enumerate(client.calls) if call[:2] == ("orchestration", "worker-start")
        )
        later = client.calls[start_index + 1 :]
        self.assertEqual(later[:1], [("orchestration", "worker-show", "--dispatch", "dispatch_2", "--json")])
        return later[1:]

    def _preflight_rows(self, local_id: str) -> list[tuple[str, str, str]]:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            rows = store.connection.execute(
                """SELECT dispatch_id, outcome, observation_json FROM preflight_observations
                   WHERE run_local_id = ? ORDER BY dispatch_id""",
                (local_id,),
            ).fetchall()
        return [(row["dispatch_id"], row["outcome"], row["observation_json"]) for row in rows]

    def _assert_split_dispatch_detail(self, report: dict[str, object], *, other: list[tuple[str, str]]) -> None:
        self.assertEqual(report["status"], "preflight_held")
        self.assertEqual(report["admission"], "conflicting")
        self.assertIn("separately authorized cleanup", str(report["nextObligation"]))
        detail = report["admissionDetail"]
        self.assertIsInstance(detail, dict)
        self.assertEqual(detail["status"], "conflicting")  # type: ignore[index]
        self.assertEqual(detail["code"], "preflight_dispatch_conflict")  # type: ignore[index]
        self.assertIn("different Dispatch", detail["message"])  # type: ignore[index]
        self.assertEqual(detail["boundDispatchId"], report["dispatchId"])  # type: ignore[index]
        observed = [
            (item["dispatchId"], item["outcome"])
            for item in detail["otherDispatchObservations"]  # type: ignore[index]
        ]
        self.assertEqual(observed, other)
        for item in detail["otherDispatchObservations"]:  # type: ignore[index]
            self.assertIsInstance(item["observedAt"], str)

    def _assert_later_controller_dispatch_mismatch_holds(self, *, current_shape: bool) -> None:
        objective = "Hold a later Dispatch identity split"
        responses = completion_responses(self.root, objective)[:4]
        responses.extend([
            _worker_start_with_real_preflight(
                self.root,
                current_shape=current_shape,
                later_dispatch_id="dispatch_2",
            ),
            _worker_start_readback(self.root, current_shape=current_shape, dispatch_id="dispatch_2"),
        ])
        client = FakeClient(responses)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        self.assertEqual(report["dispatchId"], "dispatch_2")
        self._assert_split_dispatch_detail(report, other=[("dispatch_1", "passed")])
        self.assertIsNone(report["admissionDetail"]["outcome"])  # type: ignore[index]
        self.assertEqual(self._ordinary_effect_calls(client), [])
        self.assertFalse(any(call[:2] == ("orchestration", "check") for call in client.calls))
        self.assertFalse(any(call[:2] == ("terminal", "send") for call in client.calls))
        local_id = str(report["localRunId"])
        rows = self._preflight_rows(local_id)
        self.assertEqual([(row[0], row[1]) for row in rows], [("dispatch_1", "passed")])
        passed_bytes = rows[0][2]
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(local_id)
            binding = store.get_worker_resource_binding(run.local_id)
            self.assertEqual(run.phase, "preflight_held")
            self.assertEqual(run.dispatch_id, "dispatch_2")
            self.assertEqual(binding.dispatch_id, "dispatch_2")  # type: ignore[union-attr]
            self.assertEqual(joined_preflight_status(store, run), "conflicting")
            self.assertIsNone(store.get_preflight(run.local_id, "task_1", "dispatch_2"))
            self.assertEqual(
                store.get_preflight(run.local_id, "task_1", "dispatch_1")["dispatchId"],  # type: ignore[index]
                "dispatch_1",
            )
            self.assertEqual(
                [row["status"] for row in store.connection.execute(
                    "SELECT status FROM intentions WHERE run_local_id = ? AND operation = 'worker-start'",
                    (run.local_id,),
                ).fetchall()],
                ["applied"],
            )

        # Restart-shaped entry points keep the hold visible with zero Orca effects.
        idle = FakeClient([])
        resumed = resume(self.root, local_id, client=idle, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(idle.calls, [])
        self.assertEqual(resumed["dispatchId"], "dispatch_2")
        self._assert_split_dispatch_detail(resumed, other=[("dispatch_1", "passed")])
        repeated = implement(
            self.root,
            "A new objective must not start while the split attempt is held",
            client=idle,  # type: ignore[arg-type]
            require_context=False,
        )
        self.assertEqual(idle.calls, [])
        self.assertEqual(repeated["localRunId"], local_id)
        self._assert_split_dispatch_detail(repeated, other=[("dispatch_1", "passed")])
        explained = explain(self.root, local_id)
        self._assert_split_dispatch_detail(explained, other=[("dispatch_1", "passed")])
        self.assertEqual(self._preflight_rows(local_id), [("dispatch_1", "passed", passed_bytes)])

    @requires_native_windows_admission
    def test_orca_1_4_198_later_controller_dispatch_id_mismatch_holds(self) -> None:
        self._assert_later_controller_dispatch_mismatch_holds(current_shape=False)

    @requires_native_windows_admission
    def test_orca_1_4_199_later_controller_dispatch_id_mismatch_holds(self) -> None:
        self._assert_later_controller_dispatch_mismatch_holds(current_shape=True)

    @requires_native_windows_admission
    def test_second_dispatch_preflight_is_rejected_and_exact_later_binding_still_holds(self) -> None:
        objective = "Hold an ambiguous pre-receipt double preflight"
        responses = completion_responses(self.root, objective)[:4]
        responses.extend([
            _worker_start_with_real_preflight(
                self.root,
                current_shape=True,
                second_preflight_dispatch_id="dispatch_2",
            ),
            _worker_start_readback(self.root, current_shape=True),
        ])
        client = FakeClient(responses)

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )

        # The controller bound the very Dispatch that passed, yet a second immutable
        # observation exists for the same Run/Task: ambiguity fails closed.
        self.assertEqual(report["dispatchId"], "dispatch_1")
        self._assert_split_dispatch_detail(report, other=[("dispatch_2", "rejected")])
        self.assertEqual(report["admissionDetail"]["outcome"], "passed")  # type: ignore[index]
        self.assertFalse(any(call[:2] == ("orchestration", "check") for call in client.calls))
        self.assertFalse(any(call[:2] == ("terminal", "send") for call in client.calls))
        self.assertEqual(client.responses, [])
        local_id = str(report["localRunId"])
        rows = self._preflight_rows(local_id)
        self.assertEqual([(row[0], row[1]) for row in rows], [("dispatch_1", "passed"), ("dispatch_2", "rejected")])
        rejected = json.loads(rows[1][2])
        self.assertEqual(rejected["mismatches"][0]["code"], "preflight_dispatch_conflict")
        self.assertEqual(
            rejected["mismatches"][0]["details"]["otherObservations"][0]["dispatchId"],
            "dispatch_1",
        )
        self.assertIsNone(rejected["native"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.get_run(local_id)
            self.assertEqual(run.phase, "preflight_held")
            self.assertEqual(joined_preflight_status(store, run), "conflicting")
        idle = FakeClient([])
        resumed = resume(self.root, local_id, client=idle, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(idle.calls, [])
        self._assert_split_dispatch_detail(resumed, other=[("dispatch_2", "rejected")])
        self.assertEqual(self._preflight_rows(local_id), rows)

    def test_admitted_output_before_launch_receipt_is_preserved_through_resume(self) -> None:
        objective = "Preserve post-admission output"
        responses = completion_responses(self.root, objective)[:4]
        admitted_start = _worker_start_with_preflight(self.root)

        def start_then_output(client: FakeClient, arguments: tuple[str, ...]) -> dict[str, object]:
            response = admitted_start(client, arguments)  # type: ignore[operator]
            (self.root / "output.txt").write_text("legitimate worker output\n", encoding="utf-8")
            return response

        responses.extend([start_then_output, _worker_start_readback(self.root)])
        initial = implement(
            self.root, objective, client=FakeClient(responses), wait_timeout_ms=1, require_context=False,  # type: ignore[arg-type]
        )
        self.assertEqual(initial["status"], "waiting")
        delivery_observed = threading.Event()
        resume_responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            completion_responses(
                self.root,
                objective,
                delivery_observed=delivery_observed,
            )[6],
            *settlement_responses(worktree_id=f"repo::{self.root.resolve()}"),
            acknowledgement("delivery_1"),
        ]
        client = FakeClient(resume_responses)
        final = self._run_event_backed(
            lambda: resume(
                self.root,
                str(initial["localRunId"]),
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="admitted-output resume completion",
        )
        self.assertEqual(final["status"], "worker_succeeded")
        self.assertEqual((self.root / "output.txt").read_text(encoding="utf-8").strip(), "legitimate worker output")

    def test_prepared_worker_start_crash_recovery_still_validates_effective_launch(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover prepared start", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                phase="task_created",
            )
            arguments = [
                "orchestration",
                "worker-start",
                "--run",
                "run_1",
                "--task",
                "task_1",
                "--worktree",
                f"path:{self.root.resolve()}",
                "--agent",
                "codex",
                "--model",
                "gpt-5.6-sol",
                "--effort",
                "high",
            ]
            intention = store.prepare_intention(run.local_id, "worker-start", arguments)
            substituted = _worker_start(self.root)
            substituted["result"]["launch"]["effective"]["effort"] = "low"  # type: ignore[index]

            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([substituted]), store, run)  # type: ignore[arg-type]

            self.assertEqual(caught.exception.code, "worker_launch_mismatch")
            persisted = store.connection.execute(
                "SELECT status FROM intentions WHERE id = ?",
                (intention,),
            ).fetchone()
            self.assertEqual(persisted["status"], "receipt_confirmed")
            self.assertEqual(store.get_run(run.local_id).phase, "task_created")

    def test_resume_revalidates_exact_task_packet_before_worker_start(self) -> None:
        objective = "Reject substituted Task spec"
        responses = completion_responses(self.root, objective)[:3]
        responses.append(
            {
                "result": {
                    "tasks": [
                        {
                            "id": "task_1",
                            "run_id": "run_1",
                            "task_title": objective,
                            "spec": "substituted",
                            "status": "ready",
                        }
                    ]
                }
            }
        )
        with self.assertRaises(OrchestrateError) as initial:
            implement(
                self.root,
                objective,
                client=FakeClient(responses),  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(initial.exception.code, "orca_contract_error")
        self.assertEqual(initial.exception.data, {"mismatches": ["spec"]})
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.select_run(None)
            local_id = run.local_id
            store.get_packet(local_id, "task_1")
        replay_client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                {
                    "result": {
                        "tasks": [
                            {
                                "id": "task_1",
                                "run_id": "run_1",
                                "task_title": objective,
                                "spec": "persistently substituted",
                                "status": "ready",
                            }
                        ]
                    }
                },
            ]
        )
        with self.assertRaises(OrchestrateError) as replay:
            resume(
                self.root,
                local_id,
                client=replay_client,  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(replay.exception.code, "orca_contract_error")
        self.assertEqual(replay.exception.data, {"mismatches": ["spec"]})
        self.assertFalse(any("worker-start" in call for call in replay_client.calls))

    def test_resume_recovers_missing_packet_before_worker_start(self) -> None:
        profile = setup_project(self.root)
        objective = "Recover the exact task packet"
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1", phase="task_created")
            local_id = run.local_id
        recovered_packet = make_packet(
            objective=objective,
            profile=profile,
            sources=sources,
            launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
            python_executable=str(Path(sys.executable).resolve()),
            run_id="run_1",
            reader=reader,
        )

        def recovered_task_readback(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            with StateStore(self.root, home=Path(self.state_temp.name)) as recovered_store:
                with self.assertRaises(OrchestrateError) as missing:
                    recovered_store.get_packet(local_id, "task_1")
                self.assertEqual(missing.exception.code, "packet_not_found")
            return {
                "result": {
                    "tasks": [
                        {
                            "id": "task_1",
                            "run_id": "run_1",
                            "task_title": objective,
                            "spec": packet_spec(recovered_packet),
                            "status": "ready",
                        }
                    ]
                }
            }

        delivery_observed = threading.Event()

        def recovered_question(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            delivery_observed.set()
            return {
                "result": {
                    "deliveryId": "delivery_q",
                    "messages": [
                        lifecycle_message(
                            "question",
                            {"taskId": "task_1", "dispatchId": "dispatch_1"},
                            message_id="question_1",
                            body="Continue?",
                        )
                    ],
                }
            }

        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            recovered_task_readback,
            _worker_start(self.root),
            _worker_start_readback(self.root),
            recovered_question,
        ]
        client = FakeClient(responses)
        result = self._run_event_backed(
            lambda: resume(
                self.root,
                local_id,
                client=client,  # type: ignore[arg-type]
                wait_timeout_ms=int(LIVENESS_TIMEOUT * 500),
                require_context=False,
            ),
            delivery_observed,
            description="recovered-packet question",
        )
        self.assertEqual(result["pendingQuestions"], ["question_1"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            recovered = store.get_packet(local_id, "task_1")
        self.assertEqual(recovered["native"]["taskIdSource"], "orca-injected-task-and-dispatch-preamble")


if __name__ == "__main__":
    unittest.main()
