"""Compatibility diagnostics for the first Orca vertical milestone."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any
import uuid

from .errors import OrchestrateError
from .identity import require_plain_controller
from .orca import JsonObject, OrcaClient, OrcaCommandError
from .orca_compat import (
    DeliveryAcknowledgementShapeError,
    LifecycleMessageShapeError,
    TerminalResourceIdentity,
    WorkerShowShapeError,
    current_lifecycle_message_payload,
    exact_delivery_acknowledgement,
    worker_execution_identity,
    worker_input_accepted_readback,
    worker_show_dispatch_identity,
    worker_terminal_resource_identity,
)
from .state import make_private_state_directory, require_private_state_target, state_home


DOCTOR_SCHEMA = "orchestrate-doctor/v1"
REQUIRED_CAPABILITIES = (
    "orchestration.contract.v1",
    "orchestration.worker-launch-preferences.v1",
)
PROBE_OBJECTIVE = "Disposable orchestrate public CLI compatibility probe"
PROBE_MARKER = "orchestrate-disposable/v1\n"
PROBE_TASK_SPEC = (
    "Compatibility probe only. Do not inspect or edit files. Report worker_done with "
    "outcome succeeded, files-modified empty, and exactly three short sentences confirming "
    "the task was received, no files were changed, and no work remains."
)


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    status: str
    detail: str

    def as_dict(self) -> JsonObject:
        return {"name": self.name, "status": self.status, "detail": self.detail}


class ProbeContractError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "probe_contract_error",
        data: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.data = dict(data) if data is not None else None


def _result_object(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise ProbeContractError("Orca response omitted the result object")
    return result


def _entity_id(payload: Mapping[str, Any], entity: str) -> str:
    result = _result_object(payload)
    nested = result.get(entity)
    if isinstance(nested, Mapping) and isinstance(nested.get("id"), str):
        return nested["id"]
    raise ProbeContractError(f"Orca response omitted the {entity} identifier")


def _mutation_request(payload: Mapping[str, Any]) -> str:
    mutation = _result_object(payload).get("mutation")
    if (
        not isinstance(mutation, Mapping)
        or not isinstance(mutation.get("requestId"), str)
        or not isinstance(mutation.get("replayed"), bool)
    ):
        raise ProbeContractError("Orca mutation omitted its exact native request receipt")
    return mutation["requestId"]


def collect_doctor_report(client: OrcaClient) -> JsonObject:
    checks: list[Check] = []
    checks.append(
        Check(
            "orca-executable",
            "pass" if client.executable_available() else "fail",
            f"resolved command: {client.command[0]}",
        )
    )

    version: str | None = None
    capabilities: list[str] = []
    try:
        version_result = client.run_text("--version")
        version = version_result.stdout.strip()
        checks.append(Check("orca-version", "pass", version or "version output was empty"))
    except OrcaCommandError as exc:
        checks.append(Check("orca-version", "fail", str(exc)))

    try:
        status = client.run_json("status", "--json")
        runtime = _result_object(status).get("runtime")
        if not isinstance(runtime, Mapping):
            raise ProbeContractError("status response omitted runtime")
        raw_capabilities = runtime.get("capabilities")
        if isinstance(raw_capabilities, list):
            capabilities = sorted(item for item in raw_capabilities if isinstance(item, str))
        state = runtime.get("state")
        reachable = runtime.get("reachable")
        runtime_ok = state == "ready" and reachable is True
        checks.append(
            Check(
                "orca-runtime",
                "pass" if runtime_ok else "fail",
                f"state={state!s}, reachable={reachable!s}",
            )
        )
        missing = [item for item in REQUIRED_CAPABILITIES if item not in capabilities]
        checks.append(
            Check(
                "orchestration-contract",
                "pass" if not missing else "fail",
                "required public capabilities present" if not missing else f"missing: {', '.join(missing)}",
            )
        )
    except (OrcaCommandError, ProbeContractError) as exc:
        checks.append(Check("orca-runtime", "fail", str(exc)))
        checks.append(Check("orchestration-contract", "unavailable", "runtime status was unavailable"))

    overall = "pass" if all(check.status == "pass" for check in checks) else "blocked"
    return {
        "schema": DOCTOR_SCHEMA,
        "status": overall,
        "orcaCommand": list(client.command),
        "orcaVersion": version,
        "checks": [check.as_dict() for check in checks],
        "capabilities": capabilities,
        "activeProbe": "NOT_RUN",
    }


def _probe_git(project: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", os.fspath(project), *arguments),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=False,
    )
    if completed.returncode:
        raise ProbeContractError(completed.stderr.strip() or "Disposable project Git inspection failed")
    return completed.stdout


def _probe_baseline(project: Path) -> dict[str, str]:
    root = Path(_probe_git(project, "rev-parse", "--show-toplevel").strip()).resolve()
    if root != project.resolve():
        raise ProbeContractError("The probe project must be the exact disposable Git root")
    marker = project / ".orchestrate-disposable"
    if not marker.is_file() or marker.read_text(encoding="utf-8") != PROBE_MARKER:
        raise ProbeContractError(
            "The active probe requires a committed .orchestrate-disposable marker"
        )
    status = _probe_git(project, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    if status:
        raise ProbeContractError("The disposable probe project must start clean")
    return {
        "root": os.fspath(project.resolve()),
        "head": _probe_git(project, "rev-parse", "HEAD").strip(),
        "tree": _probe_git(project, "rev-parse", "HEAD^{tree}").strip(),
        "statusSha256": hashlib.sha256(status.encode("utf-8")).hexdigest(),
    }


def _worktree_path(payload: Mapping[str, Any]) -> str:
    worktree = _result_object(payload).get("worktree")
    if not isinstance(worktree, Mapping) or not isinstance(worktree.get("path"), str):
        raise ProbeContractError("worktree current omitted the exact path")
    return os.fspath(Path(worktree["path"]).resolve())


def _probe_path(token: str) -> Path:
    return state_home() / "probes" / f"{token}.json"


def _write_probe_receipt(token: str, receipt: Mapping[str, Any]) -> None:
    path = _probe_path(token)
    baseline = receipt.get("baseline")
    root = baseline.get("root") if isinstance(baseline, Mapping) else None
    if not isinstance(root, str):
        raise ProbeContractError("The probe receipt omitted its project storage boundary")
    make_private_state_directory(Path(root), path.parent)
    temporary = path.with_suffix(".tmp")
    try:
        temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
        temporary.replace(path)
    except OSError as exc:
        raise ProbeContractError("The host-local probe receipt could not be persisted") from exc


def _read_probe_receipt(token: str) -> dict[str, Any]:
    if not token.startswith("probe_"):
        raise ProbeContractError("The active worker probe requires a minted probe token")
    try:
        value = json.loads(_probe_path(token).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProbeContractError("The probe token has no readable host-local receipt") from exc
    if not isinstance(value, dict) or value.get("schema") != "orchestrate-active-probe/v1":
        raise ProbeContractError("The probe receipt is malformed")
    return value


def _preflight_project(
    client: OrcaClient,
    project: Path,
    *,
    expected_run_id: str | None = None,
) -> tuple[dict[str, str], JsonObject]:
    try:
        require_private_state_target(project, state_home() / "probes")
        require_plain_controller(
            client,
            project,
            expected_run_id=expected_run_id,
            allow_expected_unbound=expected_run_id is not None,
        )
    except OrchestrateError as exc:
        raise ProbeContractError(str(exc), code=exc.code, data=exc.data) from exc
    baseline = _probe_baseline(project)
    worktree = client.run_json("worktree", "current", "--json")
    if _worktree_path(worktree) != baseline["root"]:
        raise ProbeContractError("The caller is not in the exact disposable Orca worktree")
    return baseline, worktree


def create_run_probe(client: OrcaClient, *, project: Path) -> JsonObject:
    baseline, worktree = _preflight_project(client, project)
    current = client.run_json("orchestration", "run-current", "--json")
    if _result_object(current).get("run") is not None:
        raise ProbeContractError("The disposable controller terminal is already bound to a Run")
    token = f"probe_{uuid.uuid4().hex}"
    objective = f"{PROBE_OBJECTIVE} [nonce:{token}]"
    receipts: dict[str, JsonObject] = {"worktreeCurrent": worktree, "runCurrent": current}
    payload = _probe_call(
        client,
        receipts,
        "runCreate",
        "orchestration",
        "run-create",
        "--objective",
        objective,
        "--json",
    )
    _contract_step(receipts, "runMutation", lambda: _mutation_request(payload))
    try:
        run_id = _entity_id(payload, "run")
    except ProbeContractError as exc:
        raise ProbeContractError(
            str(exc),
            data={"stage": "runIdentity", "receipts": receipts},
        ) from exc
    receipt = {
        "schema": "orchestrate-active-probe/v1",
        "token": token,
        "state": "ready",
        "runId": run_id,
        "objective": objective,
        "baseline": baseline,
        "worktreeId": _result_object(worktree)["worktree"].get("id"),
        "receipts": receipts,
    }
    _write_probe_receipt(token, receipt)
    return {
        "schema": DOCTOR_SCHEMA,
        "status": "created",
        "activeProbe": "run-created-and-controller-exited",
        "runId": run_id,
        "probeToken": token,
        "receipts": receipts,
        "next": "Run the active worker probe with this one-use token from a fresh process in the same ordinary terminal.",
    }


def _delivery(payload: Mapping[str, Any]) -> tuple[str, list[Mapping[str, Any]]]:
    result = _result_object(payload)
    delivery_id = result.get("deliveryId")
    messages = result.get("messages")
    if (
        not isinstance(delivery_id, str)
        or not delivery_id
        or not isinstance(messages, list)
        or not all(isinstance(item, Mapping) for item in messages)
    ):
        raise ProbeContractError("check response omitted the delivery id or messages")
    return delivery_id, list(messages)


def _message_type(message: Mapping[str, Any]) -> str | None:
    direct = message.get("type")
    return direct if isinstance(direct, str) else None


def _probe_call(
    client: OrcaClient,
    receipts: dict[str, JsonObject],
    stage: str,
    *arguments: str,
    timeout_seconds: float | None = None,
) -> JsonObject:
    try:
        payload = client.run_json(*arguments, timeout_seconds=timeout_seconds)
    except OrcaCommandError as exc:
        failure: JsonObject = {
            "stage": stage,
            "receipts": dict(receipts),
        }
        if exc.result is not None:
            failure["failedCommand"] = {
                "argv": list(exc.result.argv),
                "returncode": exc.result.returncode,
                "payload": exc.result.payload,
                "stdout": exc.result.stdout[:8192],
                "stderr": exc.result.stderr,
            }
        raise ProbeContractError(str(exc), code=exc.code, data=failure) from exc
    receipts[stage] = payload
    return payload


def _contract_step(
    receipts: Mapping[str, JsonObject],
    stage: str,
    operation: Callable[[], Any],
) -> Any:
    try:
        return operation()
    except ProbeContractError as exc:
        if exc.data is not None:
            raise
        raise ProbeContractError(
            str(exc),
            code=exc.code,
            data={"stage": stage, "receipts": dict(receipts)},
        ) from exc


def _validate_delivery_messages(
    messages: list[Mapping[str, Any]],
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    terminal_handle: str,
) -> tuple[str, str | None]:
    message_types = [_message_type(message) for message in messages]
    interventions = sorted({item for item in message_types if item in {"escalation", "question"}})
    if interventions:
        raise ProbeContractError(
            "The probe received coordinator intervention mail "
            f"({', '.join(interventions)}); the Delivery remains unacknowledged for coordinator inspection"
        )
    unexpected = sorted({str(item) for item in message_types if item not in {"heartbeat", "worker_done"}})
    if unexpected:
        raise ProbeContractError(
            "The probe received unsupported or malformed FIFO mail "
            f"({', '.join(unexpected)}); the Delivery remains unacknowledged for coordinator inspection"
        )
    done_messages = [message for message in messages if _message_type(message) == "worker_done"]
    if len(done_messages) != 1:
        raise ProbeContractError(
            "The probe requires exactly one worker_done; "
            "the Delivery remains unacknowledged for coordinator inspection"
        )
    decoded_payloads: dict[str, dict[str, Any]] = {}
    for ordinal, message in enumerate(messages):
        try:
            payload = current_lifecycle_message_payload(
                message,
                run_id=run_id,
                task_id=task_id,
                dispatch_id=dispatch_id,
                terminal_handle=terminal_handle,
            )
        except LifecycleMessageShapeError as exc:
            raise ProbeContractError(
                f"Invalid current lifecycle wire row: {exc}; "
                "the Delivery remains unacknowledged for coordinator inspection"
            ) from exc
        message_id = message.get("id")
        decoded_payloads[message_id if isinstance(message_id, str) else f"ordinal-{ordinal}"] = payload
    done_id = done_messages[0].get("id")
    done_payload = decoded_payloads.get(done_id) if isinstance(done_id, str) else None
    worker_outcome = done_payload.get("outcome") if isinstance(done_payload, Mapping) else None
    if worker_outcome not in {"succeeded", "failed"}:
        raise ProbeContractError(
            "The worker_done omitted a recognized outcome; "
            "the Delivery remains unacknowledged for coordinator inspection"
        )
    files_modified = done_payload.get("filesModified") if isinstance(done_payload, Mapping) else None
    no_edit_problem = None
    if files_modified != []:
        no_edit_problem = "worker_done did not prove an empty filesModified list"
    return worker_outcome, no_edit_problem


def _validate_native_settlement(
    worker_payload: Mapping[str, Any],
    task_payload: Mapping[str, Any],
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    worker_outcome: str,
) -> None:
    worker_result = _result_object(worker_payload)
    dispatch = worker_result.get("dispatch")
    if not isinstance(dispatch, Mapping):
        raise ProbeContractError("worker-show omitted the native Dispatch record")
    try:
        identity = worker_show_dispatch_identity(dispatch)
    except WorkerShowShapeError as exc:
        raise ProbeContractError(str(exc)) from exc
    expected_status = "completed" if worker_outcome == "succeeded" else "failed"
    if (
        dispatch.get("id") != dispatch_id
        or identity.run_id != run_id
        or identity.task_id != task_id
        or dispatch.get("status") != expected_status
    ):
        raise ProbeContractError("Native Dispatch state does not match the exact worker_done result")

    task_result = _result_object(task_payload)
    tasks = task_result.get("tasks")
    if not isinstance(tasks, list):
        raise ProbeContractError("task-list omitted native Task records")
    matching = [task for task in tasks if isinstance(task, Mapping) and task.get("id") == task_id]
    if len(matching) != 1:
        raise ProbeContractError("task-list did not return exactly the launched Task")
    task = matching[0]
    if task.get("run_id") != run_id or task.get("status") != expected_status:
        raise ProbeContractError("Native Task state does not match the exact worker_done result")


def _validate_release(
    worker_payload: Mapping[str, Any],
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    worker_outcome: str,
    expected_resource: TerminalResourceIdentity,
) -> None:
    result = _result_object(worker_payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    resource = result.get("terminalResource")
    if not isinstance(dispatch, Mapping) or not isinstance(worker, Mapping):
        raise ProbeContractError("Post-release worker-show omitted the exact worker identity")
    try:
        identity = worker_execution_identity(
            dispatch,
            worker,
            semantics=worker_outcome,
            terminal_handle=expected_resource.terminal_handle,
        )
    except WorkerShowShapeError as exc:
        raise ProbeContractError(str(exc)) from exc
    if (
        dispatch.get("id") != dispatch_id
        or identity.dispatch.run_id != run_id
        or identity.dispatch.task_id != task_id
        or identity.dispatch_id != dispatch_id
        or identity.worktree_id != expected_resource.worktree_id
    ):
        raise ProbeContractError(
            "Post-release worker-show did not preserve the exact settled worker semantics"
        )
    if not isinstance(resource, Mapping):
        raise ProbeContractError("Post-release worker-show omitted the terminal resource")
    try:
        resource_identity = worker_terminal_resource_identity(resource, dispatch_id=dispatch_id)
    except WorkerShowShapeError as exc:
        raise ProbeContractError(str(exc)) from exc
    if (
        resource_identity != expected_resource
        or resource.get("ownershipState") != "released"
        or resource.get("releaseState") != "released"
        or resource.get("retainedReason") is not None
        or not isinstance(resource.get("releaseRequestedAt"), str)
        or not isinstance(resource.get("releaseCompletedAt"), str)
        or resource.get("releaseError") is not None
        or resource.get("archive") != {"source": "transcript", "status": "captured"}
        or result.get("terminal") is not None
    ):
        raise ProbeContractError(
            "Worker release is not confirmed as released; the Delivery remains unacknowledged"
        )


def _validate_worker_start(
    payload: Mapping[str, Any],
    *,
    run_id: str,
    task_id: str,
    worktree_id: str,
) -> tuple[str, str]:
    result = _result_object(payload)
    dispatch_id = result.get("dispatchId")
    if (
        result.get("runId") != run_id
        or result.get("taskId") != task_id
        or not isinstance(dispatch_id, str)
        or result.get("state") != "ready"
        or result.get("stage") != "input_accepted"
        or not isinstance(result.get("setup"), Mapping)
        or result["setup"].get("state") != "not_applicable"
    ):
        raise ProbeContractError("worker-start did not accept the exact Run and Task")
    launch = result.get("launch")
    expected = {"agent": "codex", "model": None, "effort": None}
    if not isinstance(launch, Mapping) or launch.get("requested") != expected or launch.get("effective") != expected:
        raise ProbeContractError("worker-start substituted the requested agent launch")
    effects = result.get("effects")
    if (
        not isinstance(effects, list)
        or len(effects) != 4
        or not all(isinstance(item, Mapping) for item in effects)
    ):
        raise ProbeContractError("worker-start omitted its exact effects")
    worktrees = [item for item in effects if item.get("kind") == "worktree"]
    terminals = [item for item in effects if item.get("kind") == "terminal" and item.get("role") == "agent"]
    inputs = [item for item in effects if item.get("kind") == "dispatch_input" and item.get("role") == "agent"]
    setups = [item for item in effects if item.get("kind") == "setup"]
    if (
        len(worktrees) != 1
        or worktrees[0].get("id") != worktree_id
        or worktrees[0].get("action") != "reused"
        or len(terminals) != 1
        or terminals[0].get("action") != "created"
        or not isinstance(terminals[0].get("id"), str)
        or len(inputs) != 1
        or inputs[0].get("id") != terminals[0].get("id")
        or inputs[0].get("state") != "accepted"
        or len(setups) != 1
        or setups[0].get("action") != "not_applicable"
        or setups[0].get("state") != "not_applicable"
    ):
        raise ProbeContractError("worker-start effects do not prove exact request receipt")
    resources = result.get("residualResources")
    if not isinstance(resources, list) or not all(isinstance(item, Mapping) for item in resources):
        raise ProbeContractError("worker-start residualResources has an unknown shape")
    allowed = {("worktree", worktree_id), ("terminal", terminals[0]["id"])}
    if any((item.get("kind"), item.get("id")) not in allowed for item in resources):
        raise ProbeContractError("worker-start reported an unexpected residual resource")
    return dispatch_id, str(terminals[0]["id"])


def _validate_started_worker(
    worker_payload: Mapping[str, Any],
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    worktree_id: str,
    terminal_handle: str,
) -> TerminalResourceIdentity:
    result = _result_object(worker_payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    resource = result.get("terminalResource")
    if (
        not isinstance(dispatch, Mapping)
        or not isinstance(worker, Mapping)
        or not isinstance(resource, Mapping)
    ):
        raise ProbeContractError("Initial worker-show omitted the accepted worker identity")
    try:
        accepted = worker_input_accepted_readback(
            dispatch,
            worker,
            resource,
            run_id=run_id,
            task_id=task_id,
            dispatch_id=dispatch_id,
            worktree_id=worktree_id,
            terminal_handle=terminal_handle,
        )
    except WorkerShowShapeError as exc:
        raise ProbeContractError(str(exc)) from exc
    return accepted.resource


def _validate_delivery_ack(payload: Mapping[str, Any], *, delivery_id: str) -> None:
    try:
        exact_delivery_acknowledgement(_result_object(payload), delivery_id=delivery_id)
    except DeliveryAcknowledgementShapeError as exc:
        raise ProbeContractError(str(exc)) from exc


def run_worker_probe(client: OrcaClient, probe_token: str, *, project: Path, wait_timeout_ms: int) -> JsonObject:
    """Resume a Run and exercise worker completion/release/ack as a root coordinator."""

    try:
        require_private_state_target(project, state_home() / "probes")
    except OrchestrateError as exc:
        raise ProbeContractError(str(exc), code=exc.code, data=exc.data) from exc
    receipt = _read_probe_receipt(probe_token)
    if receipt.get("state") != "ready" or receipt.get("token") != probe_token:
        raise ProbeContractError("The probe token is not ready for one-use execution")
    run_id = receipt.get("runId")
    if not isinstance(run_id, str):
        raise ProbeContractError("The probe receipt omitted its Run")
    baseline, worktree = _preflight_project(client, project, expected_run_id=run_id)
    if baseline != receipt.get("baseline"):
        raise ProbeContractError("The disposable project changed after Run creation")
    inspected_run = client.run_json("orchestration", "run-show", "--id", run_id, "--json")
    native_run = _result_object(inspected_run).get("run")
    if not isinstance(native_run, Mapping) or native_run.get("objective") != receipt.get("objective"):
        raise ProbeContractError("The native Run does not match the minted probe nonce")
    inspected_tasks = client.run_json("orchestration", "task-list", "--run", run_id, "--json")
    if _result_object(inspected_tasks).get("tasks") not in ([], None):
        raise ProbeContractError("The probe Run already has Tasks")
    current = client.run_json("orchestration", "run-current", "--json")
    bound = _result_object(current).get("run")
    if isinstance(bound, Mapping) and bound.get("id") != run_id:
        raise ProbeContractError("The caller terminal is bound to a conflicting Run")
    receipt["state"] = "consumed"
    _write_probe_receipt(probe_token, receipt)

    receipts: dict[str, JsonObject] = {
        "worktreeCurrent": worktree,
        "runShow": inspected_run,
        "taskListBefore": inspected_tasks,
        "runCurrent": current,
    }
    run_use = _probe_call(client, receipts, "runUse", "orchestration", "run-use", "--id", run_id, "--json")
    _contract_step(receipts, "runUseMutation", lambda: _mutation_request(run_use))
    task_payload = _probe_call(
        client,
        receipts,
        "taskCreate",
        "orchestration",
        "task-create",
        "--run",
        run_id,
        "--task-title",
        "Public CLI completion probe",
        "--spec",
        PROBE_TASK_SPEC,
        "--json",
    )
    task_id = _contract_step(receipts, "taskIdentity", lambda: _entity_id(task_payload, "task"))
    _contract_step(receipts, "taskMutation", lambda: _mutation_request(task_payload))
    worker_payload = _probe_call(
        client,
        receipts,
        "workerStart",
        "orchestration",
        "worker-start",
        "--run",
        run_id,
        "--task",
        task_id,
        "--worktree",
        f"path:{project.resolve()}",
        "--agent",
        "codex",
        "--timeout-ms",
        "60000",
        "--json",
        timeout_seconds=90,
    )
    _contract_step(receipts, "workerMutation", lambda: _mutation_request(worker_payload))
    worktree_id = receipt.get("worktreeId")
    if not isinstance(worktree_id, str):
        raise ProbeContractError("The probe receipt omitted its exact worktree identity")
    dispatch_id, terminal_handle = _contract_step(
        receipts,
        "dispatchIdentity",
        lambda: _validate_worker_start(
            worker_payload,
            run_id=run_id,
            task_id=task_id,
            worktree_id=worktree_id,
        ),
    )
    started_worker = _probe_call(
        client,
        receipts,
        "startedWorker",
        "orchestration",
        "worker-show",
        "--dispatch",
        dispatch_id,
        "--json",
    )
    expected_resource = _contract_step(
        receipts,
        "workerResourceIdentity",
        lambda: _validate_started_worker(
            started_worker,
            run_id=run_id,
            task_id=task_id,
            dispatch_id=dispatch_id,
            worktree_id=worktree_id,
            terminal_handle=terminal_handle,
        ),
    )
    delivery_payload = _probe_call(
        client,
        receipts,
        "delivery",
        "orchestration",
        "check",
        "--run",
        run_id,
        "--wait",
        "--types",
        "worker_done,escalation,question",
        "--timeout-ms",
        str(wait_timeout_ms),
        "--json",
        timeout_seconds=(wait_timeout_ms / 1000) + 30,
    )
    receipt["state"] = "delivery_observed"
    receipt["delivery"] = delivery_payload
    _write_probe_receipt(probe_token, receipt)
    delivery_id, messages = _contract_step(
        receipts,
        "deliveryContract",
        lambda: _delivery(delivery_payload),
    )
    worker_outcome, no_edit_problem = _contract_step(
        receipts,
        "deliveryValidation",
        lambda: _validate_delivery_messages(
            messages,
            run_id=run_id,
            task_id=task_id,
            dispatch_id=dispatch_id,
            terminal_handle=terminal_handle,
        ),
    )
    settled_worker = _probe_call(
        client,
        receipts,
        "settledWorker",
        "orchestration",
        "worker-show",
        "--dispatch",
        dispatch_id,
        "--json",
    )
    settled_tasks = _probe_call(
        client,
        receipts,
        "settledTasks",
        "orchestration",
        "task-list",
        "--run",
        run_id,
        "--json",
    )
    _contract_step(
        receipts,
        "nativeSettlement",
        lambda: _validate_native_settlement(
            settled_worker,
            settled_tasks,
            run_id=run_id,
            task_id=task_id,
            dispatch_id=dispatch_id,
            worker_outcome=worker_outcome,
        ),
    )
    release = _probe_call(
        client,
        receipts,
        "workerRelease",
        "orchestration",
        "worker-release",
        "--dispatch",
        dispatch_id,
        "--json",
    )
    _contract_step(receipts, "releaseMutation", lambda: _mutation_request(release))
    released_worker = _probe_call(
        client,
        receipts,
        "releasedWorker",
        "orchestration",
        "worker-show",
        "--dispatch",
        dispatch_id,
        "--json",
    )
    _contract_step(
        receipts,
        "releaseDisposition",
        lambda: _validate_release(
            released_worker,
            run_id=run_id,
            task_id=task_id,
            dispatch_id=dispatch_id,
            worker_outcome=worker_outcome,
            expected_resource=expected_resource,
        ),
    )
    readback = _probe_baseline(project)
    if readback != baseline:
        no_edit_problem = "the disposable workspace changed from its exact baseline"
    ack = _probe_call(
        client,
        receipts,
        "deliveryAck",
        "orchestration",
        "check",
        "--run",
        run_id,
        "--ack",
        delivery_id,
        "--json",
    )
    _contract_step(receipts, "ackMutation", lambda: _mutation_request(ack))
    _contract_step(
        receipts,
        "ackIdentity",
        lambda: _validate_delivery_ack(ack, delivery_id=delivery_id),
    )
    return {
        "schema": DOCTOR_SCHEMA,
        "status": "pass" if worker_outcome == "succeeded" and no_edit_problem is None else "blocked",
        "activeProbe": "run-resume-worker-completion-release-ack",
        "runId": run_id,
        "taskId": task_id,
        "dispatchId": dispatch_id,
        "deliveryId": delivery_id,
        "workerOutcome": worker_outcome,
        "noEditValidation": "pass" if no_edit_problem is None else no_edit_problem,
        "receipts": receipts,
    }


def error_report(exc: Exception, *, operation: str) -> JsonObject:
    if isinstance(exc, (OrcaCommandError, ProbeContractError)):
        code = exc.code
    else:
        code = "probe_contract_error"
    error: JsonObject = {"code": code, "message": str(exc)}
    if isinstance(exc, OrcaCommandError) and exc.result and exc.result.payload:
        response_error = exc.result.payload.get("error")
        if isinstance(response_error, Mapping) and "data" in response_error:
            error["data"] = response_error["data"]
    if isinstance(exc, ProbeContractError) and exc.data is not None:
        error["data"] = exc.data
    return {
        "schema": DOCTOR_SCHEMA,
        "status": "blocked",
        "activeProbe": operation,
        "error": error,
    }
