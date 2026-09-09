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

from .orca import JsonObject, OrcaClient, OrcaCommandError
from .state import state_home


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


def _ordinary_probe_caller() -> None:
    if any(os.environ.get(key) for key in ("ORCA_AGENT_HOOK_TOKEN", "ORCA_AGENT_LAUNCH_TOKEN")):
        raise ProbeContractError("A dispatched or reasoning-agent terminal cannot run the active probe")
    if not os.environ.get("ORCA_TERMINAL_HANDLE"):
        raise ProbeContractError("The active probe requires its own ordinary Orca terminal")


def _probe_path(token: str) -> Path:
    return state_home() / "probes" / f"{token}.json"


def _write_probe_receipt(token: str, receipt: Mapping[str, Any]) -> None:
    path = _probe_path(token)
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _preflight_project(client: OrcaClient, project: Path) -> tuple[dict[str, str], JsonObject]:
    _ordinary_probe_caller()
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
    task_id: str,
    dispatch_id: str,
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
    for message in messages:
        payload = message.get("payload")
        if not isinstance(payload, Mapping):
            raise ProbeContractError(
                "Lifecycle mail omitted its payload; the Delivery remains unacknowledged"
            )
        bound_task = payload.get("taskId")
        bound_dispatch = payload.get("dispatchId")
        if bound_task != task_id or bound_dispatch != dispatch_id:
            raise ProbeContractError(
                "The FIFO Delivery contains mail not bound to the launched Task and Dispatch; "
                "it remains unacknowledged for coordinator inspection"
            )
    done_payload = done_messages[0].get("payload")
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
    expected_status = "completed" if worker_outcome == "succeeded" else "failed"
    if (
        dispatch.get("id") != dispatch_id
        or dispatch.get("run_id") != run_id
        or dispatch.get("task_id") != task_id
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


def _validate_release(worker_payload: Mapping[str, Any], dispatch_id: str) -> None:
    result = _result_object(worker_payload)
    dispatch = result.get("dispatch")
    resource = result.get("terminalResource")
    if not isinstance(dispatch, Mapping) or dispatch.get("id") != dispatch_id:
        raise ProbeContractError("Post-release worker-show did not return the exact Dispatch")
    if not isinstance(resource, Mapping) or resource.get("releaseState") != "released":
        raise ProbeContractError(
            "Worker release is not confirmed as released; the Delivery remains unacknowledged"
        )


def run_worker_probe(client: OrcaClient, probe_token: str, *, project: Path, wait_timeout_ms: int) -> JsonObject:
    """Resume a Run and exercise worker completion/release/ack as a root coordinator."""

    receipt = _read_probe_receipt(probe_token)
    if receipt.get("state") != "ready" or receipt.get("token") != probe_token:
        raise ProbeContractError("The probe token is not ready for one-use execution")
    baseline, worktree = _preflight_project(client, project)
    if baseline != receipt.get("baseline"):
        raise ProbeContractError("The disposable project changed after Run creation")
    run_id = receipt.get("runId")
    if not isinstance(run_id, str):
        raise ProbeContractError("The probe receipt omitted its Run")
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
    _probe_call(client, receipts, "runUse", "orchestration", "run-use", "--id", run_id, "--json")
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
    dispatch_id = _contract_step(
        receipts,
        "dispatchIdentity",
        lambda: _entity_id(worker_payload, "dispatch"),
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
            task_id=task_id,
            dispatch_id=dispatch_id,
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
    _probe_call(
        client,
        receipts,
        "workerRelease",
        "orchestration",
        "worker-release",
        "--dispatch",
        dispatch_id,
        "--json",
    )
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
        lambda: _validate_release(released_worker, dispatch_id),
    )
    readback = _probe_baseline(project)
    if readback != baseline:
        no_edit_problem = "the disposable workspace changed from its exact baseline"
    _probe_call(
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
