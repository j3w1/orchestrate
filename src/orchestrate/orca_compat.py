"""Strict public readback shapes for the supported Orca 1.4.198/1.4.199 range."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Any, Literal


class WorkerShowShapeError(ValueError):
    """The readback is neither one exact supported shape nor a consistent alias pair."""


class LifecycleMessageShapeError(ValueError):
    """A current Delivery row has an unsupported shape, binding, or sender."""

    def __init__(self, message: str, *, category: Literal["shape", "binding", "sender"] = "shape") -> None:
        super().__init__(message)
        self.category = category


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


WorkerExecutionSemantics = Literal["succeeded", "failed", "prompt_stall"]


@dataclass(frozen=True, slots=True)
class WorkerExecutionShape:
    dispatch_status: str
    last_failure: str | None
    worker_state: str
    worker_stage: str
    last_error: str | None


_CURRENT_LIFECYCLE_TYPES = frozenset({"heartbeat", "question", "escalation", "worker_done"})
_MESSAGE_IDENTITY_ALIASES = frozenset({"runId", "deliveryContract", "fromHandle", "toHandle"})
_PAYLOAD_IDENTITY_ALIASES = frozenset({"task_id", "dispatch_id", "runId", "run_id"})


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-JSON numeric constant {value}")


def _strict_json_object(raw: object) -> dict[str, Any]:
    if not isinstance(raw, str):
        raise LifecycleMessageShapeError("current lifecycle payload is not a JSON string")

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        decoded: dict[str, Any] = {}
        for key, value in pairs:
            if key in decoded:
                raise ValueError(f"duplicate JSON key {key}")
            decoded[key] = value
        return decoded

    try:
        decoded = json.loads(
            raw,
            object_pairs_hook=object_pairs,
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise LifecycleMessageShapeError(f"current lifecycle payload is invalid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise LifecycleMessageShapeError("current lifecycle payload JSON is not an object")
    return decoded


def current_lifecycle_message_payload(
    message: Mapping[str, Any],
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    terminal_handle: str,
) -> dict[str, Any]:
    """Validate and decode the exact Orca 1.4.199 current Delivery wire row."""

    message_id = message.get("id")
    message_type = message.get("type")
    if not isinstance(message_id, str) or not message_id:
        raise LifecycleMessageShapeError("current lifecycle row omitted its message id")
    if not isinstance(message_type, str) or message_type not in _CURRENT_LIFECYCLE_TYPES:
        raise LifecycleMessageShapeError("current Delivery contains unsupported or malformed lifecycle mail")
    present_message_aliases = sorted(_MESSAGE_IDENTITY_ALIASES.intersection(message))
    if present_message_aliases:
        raise LifecycleMessageShapeError(
            f"current lifecycle row used unsupported identity aliases: {', '.join(present_message_aliases)}"
        )
    if message.get("delivery_contract") != "current_delivery":
        raise LifecycleMessageShapeError("lifecycle row omitted delivery_contract=current_delivery")
    if message.get("run_id") != run_id or message.get("to_handle") != f"run:{run_id}":
        raise LifecycleMessageShapeError(
            "lifecycle row is stale or belongs to another Run",
            category="binding",
        )

    payload = _strict_json_object(message.get("payload"))
    present_payload_aliases = sorted(_PAYLOAD_IDENTITY_ALIASES.intersection(payload))
    if present_payload_aliases:
        raise LifecycleMessageShapeError(
            f"current lifecycle payload used unsupported identity aliases: {', '.join(present_payload_aliases)}"
        )
    if payload.get("taskId") != task_id or payload.get("dispatchId") != dispatch_id:
        raise LifecycleMessageShapeError(
            "lifecycle payload is stale or belongs to another Task or Dispatch",
            category="binding",
        )
    if message_type == "question":
        question = payload.get("question")
        options = payload.get("options")
        if not isinstance(question, str) or not question or message.get("body") != question:
            raise LifecycleMessageShapeError(
                "current question payload does not match its exact message body"
            )
        if not isinstance(options, list) or any(not isinstance(item, str) for item in options):
            raise LifecycleMessageShapeError("current question payload has malformed options")

    expected_sender = f"dispatch:{dispatch_id}" if message_type == "question" else terminal_handle
    if message.get("from_handle") != expected_sender:
        raise LifecycleMessageShapeError(
            f"{message_type} sender is not the exact bound {'Dispatch' if message_type == 'question' else 'worker terminal'}",
            category="sender",
        )
    return payload


# Orca's worker stage records execution progress; terminalResource records the
# independently changing terminal disposition.  Keep the two installed response
# versions explicit even where their execution semantics are equal, so a future
# version cannot silently inherit either contract.
_WORKER_EXECUTION_SHAPES: dict[str, dict[WorkerExecutionSemantics, WorkerExecutionShape]] = {
    "1.4.198": {
        "succeeded": WorkerExecutionShape("completed", None, "succeeded", "settled", None),
        "failed": WorkerExecutionShape("failed", "worker_failed", "failed", "settled", "worker_failed"),
        "prompt_stall": WorkerExecutionShape(
            "failed",
            "agent_prompt_stalled",
            "failed",
            "dispatch_input",
            "agent_prompt_stalled",
        ),
    },
    "1.4.199": {
        "succeeded": WorkerExecutionShape("completed", None, "succeeded", "settled", None),
        "failed": WorkerExecutionShape("failed", "worker_failed", "failed", "settled", "worker_failed"),
        "prompt_stall": WorkerExecutionShape(
            "failed",
            "agent_prompt_stalled",
            "failed",
            "dispatch_input",
            "agent_prompt_stalled",
        ),
    },
}


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


def worker_execution_identity(
    dispatch: Mapping[str, Any],
    worker: Mapping[str, Any],
    *,
    semantics: WorkerExecutionSemantics,
    terminal_handle: str,
) -> WorkerIdentity:
    """Validate one version-bound execution shape independently of resource release."""

    identity = worker_show_identity(dispatch, worker)
    expected = _WORKER_EXECUTION_SHAPES[identity.dispatch.version][semantics]
    if (
        dispatch.get("status") != expected.dispatch_status
        or identity.dispatch.last_failure != expected.last_failure
        or worker.get("state") != expected.worker_state
        or worker.get("stage") != expected.worker_stage
        or identity.last_error != expected.last_error
        or identity.terminal_handle != terminal_handle
    ):
        raise WorkerShowShapeError(
            f"Orca {identity.dispatch.version} worker execution does not match {semantics} semantics"
        )
    return identity


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
