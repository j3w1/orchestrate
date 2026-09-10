from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.admission import joined_preflight_status, worker_preflight
from orchestrate.controller import (
    _ack_if_resolved,
    _finish_prompt_stall_cleanup,
    _mutation,
    _process_delivery,
    _record_input_submission_diagnostic,
    _release_disposition,
    _request_id,
    _run_summary,
    _validate_worker_start_readback,
    answer,
    explain,
    implement,
    reconcile_intentions,
    resume,
)
from orchestrate.errors import OrchestrateError
from orchestrate.identity import require_plain_controller
from orchestrate.orca import OrcaJsonResponse
from orchestrate.packets import canonical_packet_json, make_packet, packet_spec
from orchestrate.profile import setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from orchestrate.state import StateStore


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


def completion_responses(root: Path, objective: str, outcome: str = "succeeded") -> list[Response]:
    delivery = {
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


class ControllerTests(unittest.TestCase):
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

    def test_single_worker_happy_path_releases_before_ack_and_preserves_wip(self) -> None:
        objective = "Make the bounded change"
        before = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        client = FakeClient(completion_responses(self.root, objective))
        report = implement(self.root, objective, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
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
        client = FakeClient(completion_responses(self.root, objective))

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=100,
            require_context=False,
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
        report = implement(
            self.root,
            objective,
            client=FakeClient(completion_responses(self.root, objective, "failed")),  # type: ignore[arg-type]
            wait_timeout_ms=100,
            require_context=False,
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

    def test_question_delivery_remains_unacknowledged_then_answers_exactly(self) -> None:
        objective = "Ask when blocked"
        responses = completion_responses(self.root, objective)[:6]
        responses.append(
            {
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
        )
        client = FakeClient(responses)
        report = implement(self.root, objective, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(report["pendingQuestions"], ["question_1"])

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
        self.assertEqual(answered["pendingQuestions"], [])

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
        delivery = {
            "result": {
                "deliveryId": "delivery_raw",
                "messages": [question, escalation],
            }
        }
        client = FakeClient([*completion_responses(self.root, objective)[:6], delivery])

        report = implement(
            self.root,
            objective,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=100,
            require_context=False,
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
            self.assertEqual(len(store.evidence(run.local_id)), 2)

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
        responses = completion_responses(self.root, objective)[:6]
        responses.append(
            {
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
        )
        client = FakeClient(responses)
        report = implement(self.root, objective, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
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

    def test_orca_1_4_198_real_preflight_before_controller_receipt_joins_to_admitted(self) -> None:
        self._assert_real_pre_receipt_preflight_joins(current_shape=False)

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

    def test_later_controller_resource_id_mismatch_holds(self) -> None:
        self._assert_later_controller_binding_mismatch_holds("resource_id")

    def test_later_controller_terminal_handle_mismatch_holds(self) -> None:
        self._assert_later_controller_binding_mismatch_holds("terminal_handle")

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

    def test_orca_1_4_198_later_controller_dispatch_id_mismatch_holds(self) -> None:
        self._assert_later_controller_dispatch_mismatch_holds(current_shape=False)

    def test_orca_1_4_199_later_controller_dispatch_id_mismatch_holds(self) -> None:
        self._assert_later_controller_dispatch_mismatch_holds(current_shape=True)

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
        resume_responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            completion_responses(self.root, objective)[6],
            *settlement_responses(worktree_id=f"repo::{self.root.resolve()}"),
            acknowledgement("delivery_1"),
        ]
        final = resume(
            self.root, str(initial["localRunId"]), client=FakeClient(resume_responses),
            wait_timeout_ms=100, require_context=False,  # type: ignore[arg-type]
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

        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            recovered_task_readback,
            _worker_start(self.root),
            _worker_start_readback(self.root),
            {
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
            },
        ]
        result = resume(
            self.root,
            local_id,
            client=FakeClient(responses),  # type: ignore[arg-type]
            wait_timeout_ms=100,
            require_context=False,
        )
        self.assertEqual(result["pendingQuestions"], ["question_1"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            recovered = store.get_packet(local_id, "task_1")
        self.assertEqual(recovered["native"]["taskIdSource"], "orca-injected-task-and-dispatch-preamble")


if __name__ == "__main__":
    unittest.main()
