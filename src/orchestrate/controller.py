"""Deterministic one-worker controller over public Orca commands."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

from .config import load_owner_model
from .admission import joined_preflight_status, validate_packet_sources
from .errors import OrchestrateError
from .identity import ControllerIdentity, require_plain_controller
from .orca import JsonObject, OrcaClient, OrcaCommandError, orca_task_title
from .packets import canonical_packet_json, make_packet, packet_spec_from_json
from .profile import ProjectProfile
from .readers import read_project
from .sources import SourceIndex, build_source_index
from .state import RunRecord, StateStore


REPORT_SCHEMA = "orchestrate-report/v1"
TERMINAL_PHASES = {"worker_succeeded", "worker_failed", "worker_unadmitted", "blocked", "completed"}


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


def _entity_id(payload: Mapping[str, Any], entity: str) -> str:
    nested = _result(payload).get(entity)
    if not isinstance(nested, Mapping) or not isinstance(nested.get("id"), str):
        raise OrchestrateError(f"Orca response omitted {entity}.id", code="orca_contract_error")
    return nested["id"]


def _request_id(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    result = payload.get("result")
    if isinstance(result, Mapping):
        mutation = result.get("mutation")
        if isinstance(mutation, Mapping) and isinstance(mutation.get("requestId"), str):
            return mutation["requestId"]
    error = payload.get("error")
    data = error.get("data") if isinstance(error, Mapping) else None
    if isinstance(data, Mapping):
        mutation = data.get("mutation")
        if isinstance(mutation, Mapping) and isinstance(mutation.get("requestId"), str):
            return mutation["requestId"]
    return None


def _mutation_request_id(payload: Mapping[str, Any], *, expected: str | None = None) -> str:
    request_id = _request_id(payload)
    mutation = _result(payload).get("mutation")
    if (
        not isinstance(mutation, Mapping)
        or not isinstance(request_id, str)
        or not request_id
        or not isinstance(mutation.get("replayed"), bool)
        or (expected is not None and request_id != expected)
    ):
        raise OrchestrateError(
            "Orca mutation response omitted its exact native request receipt",
            code="orca_contract_error",
        )
    return request_id


def _mutation(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    operation: str,
    arguments: list[str],
    *,
    timeout_seconds: float | None = None,
) -> JsonObject:
    intention_id = store.prepare_intention(run.local_id, operation, arguments)
    store.mark_intention(intention_id, "invoking")
    try:
        response = client.run_json(*arguments, "--json", timeout_seconds=timeout_seconds)
    except OrcaCommandError as exc:
        payload = exc.result.payload if exc.result is not None else None
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(payload),
            error=payload or {"message": str(exc), "code": exc.code},
        )
        raise OrchestrateError(
            f"Orca mutation {operation} did not produce a confirmed receipt: {exc}",
            code="mutation_outcome_uncertain",
            data={"operation": operation, "requestId": _request_id(payload), "orcaCode": exc.code},
        ) from exc
    try:
        request_id = _mutation_request_id(response)
    except OrchestrateError as exc:
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(response),
            error=response,
        )
        raise OrchestrateError(
            f"Orca mutation {operation} succeeded without a recoverable native request receipt",
            code="mutation_outcome_uncertain",
            data={"operation": operation, "requestId": _request_id(response)},
        ) from exc
    store.mark_intention(
        intention_id,
        "applied",
        request_id=request_id,
        response=response,
    )
    return response


def _apply_intention(store: StateStore, run: RunRecord, operation: str, response: Mapping[str, Any]) -> RunRecord:
    if operation == "run-create":
        identity = _entity_id(response, "run")
        if run.native_run_id not in {None, identity}:
            raise OrchestrateError("Run receipt conflicts with the local binding", code="intention_binding_mismatch")
        fields: dict[str, object] = {"native_run_id": identity}
        if run.phase == "preparing":
            fields["phase"] = "run_created"
        return store.update_run(run.local_id, **fields)
    if operation == "task-create":
        identity = _entity_id(response, "task")
        if run.task_id not in {None, identity}:
            raise OrchestrateError("Task receipt conflicts with the local binding", code="intention_binding_mismatch")
        fields = {"task_id": identity}
        if run.phase == "run_created":
            fields["phase"] = "task_created"
        return store.update_run(run.local_id, **fields)
    if operation == "worker-start":
        identity = _result(response).get("dispatchId")
        if not isinstance(identity, str):
            raise OrchestrateError("worker-start response omitted dispatchId", code="orca_contract_error")
        if run.dispatch_id not in {None, identity}:
            raise OrchestrateError("Dispatch receipt conflicts with the local binding", code="intention_binding_mismatch")
        fields = {"dispatch_id": identity}
        if run.phase == "task_created":
            fields["phase"] = "awaiting_preflight"
        return store.update_run(run.local_id, **fields)
    return store.get_run(run.local_id)


def _replay_applied_intentions(store: StateStore, run: RunRecord) -> RunRecord:
    rows = store.connection.execute(
        "SELECT operation, arguments_json, response_json FROM intentions WHERE run_local_id = ? AND status = 'applied' ORDER BY created_at",
        (run.local_id,),
    ).fetchall()
    current = run
    for row in rows:
        if row["response_json"]:
            current = _apply_intention(store, current, row["operation"], json.loads(row["response_json"]))
            arguments = json.loads(row["arguments_json"])
            if row["operation"] == "delivery-ack" and "--ack" in arguments:
                delivery_id = arguments[arguments.index("--ack") + 1]
                store.mark_delivery_acked(run.local_id, delivery_id)
                if current.delivery_id == delivery_id:
                    current = store.update_run(run.local_id, delivery_id=None)
            if row["operation"] == "reply" and "--id" in arguments and "--body" in arguments:
                message_id = arguments[arguments.index("--id") + 1]
                body = arguments[arguments.index("--body") + 1]
                question = store.connection.execute(
                    "SELECT delivery_id FROM questions WHERE message_id = ?",
                    (message_id,),
                ).fetchone()
                if question is not None:
                    store.answer_question(message_id, body)
                    store.mark_message(run.local_id, question["delivery_id"], message_id, "answered")
    return current


def _request_state(
    payload: Mapping[str, Any],
    *,
    expected_request_id: str,
    expected_method: str | None = None,
) -> str:
    result = _result(payload)
    required = {
        "requestId": str,
        "state": str,
        "method": str,
        "createdAt": str,
        "updatedAt": str,
    }
    if any(not isinstance(result.get(key), kind) for key, kind in required.items()):
        raise OrchestrateError("request-show returned an unknown public shape", code="orca_contract_error")
    if (
        result["requestId"] != expected_request_id
        or (expected_method is not None and result["method"] != expected_method)
        or "receipt" not in result
        or "interpretation" not in result
    ):
        raise OrchestrateError("request-show did not bind the exact native request", code="orca_contract_error")
    return result["state"]


def _stored_argument(arguments: list[object], name: str) -> str:
    if name not in arguments or arguments.index(name) + 1 >= len(arguments):
        raise OrchestrateError("Stored worker-start arguments are incomplete", code="orca_contract_error")
    value = arguments[arguments.index(name) + 1]
    if not isinstance(value, str):
        raise OrchestrateError("Stored worker-start arguments are malformed", code="orca_contract_error")
    return value


def _reconcile_confirmed_worker_start(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    *,
    intention_id: str,
    arguments: list[object],
    response: Mapping[str, Any],
    request_id: str,
) -> RunRecord:
    agent = _stored_argument(arguments, "--agent")
    model = _stored_argument(arguments, "--model")
    effort = _stored_argument(arguments, "--effort")
    worktree_selector = _stored_argument(arguments, "--worktree")
    worktree_id, terminal_id = _validate_worker_start(
        response,
        run=run,
        worktree_id=None,
        agent=agent,
        model=model,
        effort=effort,
    )
    dispatch_id = _result(response)["dispatchId"]
    readback = client.run_json(
        "orchestration",
        "worker-show",
        "--dispatch",
        str(dispatch_id),
        "--json",
    )
    _validate_worker_start_readback(
        readback,
        run=run,
        dispatch_id=dispatch_id,
        worktree_id=worktree_id,
        worktree_selector=worktree_selector,
        terminal_id=terminal_id,
        agent=agent,
        model=model,
        effort=effort,
    )
    store.mark_intention(
        intention_id,
        "applied",
        request_id=request_id,
        response=response,
    )
    return _apply_intention(store, run, "worker-start", response)


def reconcile_intentions(client: OrcaClient, store: StateStore, run: RunRecord) -> RunRecord:
    current = _replay_applied_intentions(store, run)
    for row in store.unsettled_intentions(run.local_id):
        if row["status"] == "receipt_confirmed":
            if row["operation"] != "worker-start" or not row["response_json"]:
                raise OrchestrateError(
                    "A confirmed mutation receipt has no bounded reconciliation path",
                    code="unknown_external_effect",
                    data={"operation": row["operation"], "intentionId": row["id"]},
                )
            arguments = json.loads(row["arguments_json"])
            response = json.loads(row["response_json"])
            current = _reconcile_confirmed_worker_start(
                client,
                store,
                current,
                intention_id=row["id"],
                arguments=arguments,
                response=response,
                request_id=row["request_id"],
            )
            continue
        if row["status"] == "prepared":
            arguments = json.loads(row["arguments_json"])
            store.mark_intention(row["id"], "invoking")
            try:
                replay = client.run_json(*arguments, "--json")
            except OrcaCommandError as exc:
                payload = exc.result.payload if exc.result is not None else None
                store.mark_intention(
                    row["id"],
                    "uncertain",
                    request_id=_request_id(payload),
                    error=payload or {"message": str(exc), "code": exc.code},
                )
                raise OrchestrateError(
                    "A prepared Orca mutation did not produce a confirmed receipt",
                    code="mutation_outcome_uncertain",
                    data={"operation": row["operation"], "requestId": _request_id(payload)},
                ) from exc
            try:
                replay_request_id = _mutation_request_id(replay)
            except OrchestrateError as exc:
                store.mark_intention(
                    row["id"],
                    "uncertain",
                    request_id=_request_id(replay),
                    error=replay,
                )
                raise OrchestrateError(
                    "A prepared Orca mutation succeeded without a recoverable native request receipt",
                    code="mutation_outcome_uncertain",
                    data={"operation": row["operation"], "requestId": _request_id(replay)},
                ) from exc
            if row["operation"] == "worker-start":
                store.mark_intention(
                    row["id"],
                    "receipt_confirmed",
                    request_id=replay_request_id,
                    response=replay,
                )
                current = _reconcile_confirmed_worker_start(
                    client,
                    store,
                    current,
                    intention_id=row["id"],
                    arguments=arguments,
                    response=replay,
                    request_id=replay_request_id,
                )
            else:
                store.mark_intention(
                    row["id"],
                    "applied",
                    request_id=replay_request_id,
                    response=replay,
                )
                current = _apply_intention(store, current, row["operation"], replay)
            continue
        request_id = row["request_id"]
        if not request_id:
            raise OrchestrateError(
                "A prior Orca mutation may have taken effect but has no recovery identity",
                code="unknown_external_effect",
                data={"operation": row["operation"], "intentionId": row["id"]},
            )
        arguments = json.loads(row["arguments_json"])
        expected_method = (
            f"{arguments[0]}.{arguments[1]}"
            if isinstance(arguments, list)
            and len(arguments) >= 2
            and all(isinstance(item, str) for item in arguments[:2])
            else None
        )
        if expected_method is None:
            raise OrchestrateError("Stored mutation arguments are malformed", code="orca_contract_error")
        receipt = client.run_json("orchestration", "request-show", "--request", request_id, "--json")
        state = _request_state(
            receipt,
            expected_request_id=request_id,
            expected_method=expected_method,
        )
        if state == "absent":
            raise OrchestrateError(
                "Orca has no receipt for an uncertain mutation; absence is not proof of no effect",
                code="unknown_external_effect",
                data={"operation": row["operation"], "requestId": request_id},
            )
        if state not in {"completed", "pending"}:
            raise OrchestrateError(
                f"Unrecognized request recovery state: {state}",
                code="orca_contract_error",
            )
        replay = client.run_json(*arguments, "--retry-request", request_id, "--json")
        _mutation_request_id(replay, expected=request_id)
        if row["operation"] == "worker-start":
            store.mark_intention(
                row["id"],
                "receipt_confirmed",
                request_id=request_id,
                response=replay,
            )
            current = _reconcile_confirmed_worker_start(
                client,
                store,
                current,
                intention_id=row["id"],
                arguments=arguments,
                response=replay,
                request_id=request_id,
            )
        else:
            store.mark_intention(row["id"], "applied", request_id=request_id, response=replay)
            current = _apply_intention(store, current, row["operation"], replay)
    return _replay_applied_intentions(store, current)


def _run_summary(store: StateStore, run: RunRecord, *, live: object = None) -> JsonObject:
    pending = store.pending_questions(run.local_id)
    if pending:
        next_obligation = f"answer question {pending[0]['message_id']}"
    elif run.phase == "worker_succeeded" and run.verification_status == "pending":
        next_obligation = "independent verification and project acceptance remain unresolved"
    elif run.phase == "awaiting_preflight":
        next_obligation = "worker must complete the exact managed preflight before edits or checks"
    elif run.phase == "waiting":
        next_obligation = "resume foreground supervision"
    elif run.phase == "worker_unadmitted":
        next_obligation = "worker claim is preserved but cannot satisfy verification without joined admission"
    elif run.phase == "worker_failed":
        next_obligation = "inspect the failed attempt before an explicit retry decision"
    else:
        next_obligation = "continue the deterministic Run state machine"
    return {
        "schema": REPORT_SCHEMA,
        "status": run.phase,
        "runId": run.native_run_id,
        "localRunId": run.local_id,
        "taskId": run.task_id,
        "dispatchId": run.dispatch_id,
        "workerOutcome": run.worker_outcome,
        "verification": run.verification_status,
        "admission": joined_preflight_status(store, run),
        "pendingQuestions": [row["message_id"] for row in pending],
        "nextObligation": next_obligation,
        "live": live,
        "evidence": store.evidence(run.local_id),
    }


def _create_native_run(client: OrcaClient, store: StateStore, run: RunRecord) -> RunRecord:
    response = _mutation(
        client,
        store,
        run,
        "run-create",
        ["orchestration", "run-create", "--objective", run.objective],
    )
    current = _apply_intention(store, run, "run-create", response)
    readback = client.run_json("orchestration", "run-show", "--id", str(current.native_run_id), "--json")
    native = _result(readback).get("run")
    if (
        not isinstance(native, Mapping)
        or native.get("id") != current.native_run_id
        or native.get("objective") != current.objective
    ):
        raise OrchestrateError("run-create did not bind the exact native Run", code="orca_contract_error")
    return current


def _require_profile_selection(profile: ProjectProfile) -> None:
    if profile.candidate_changed:
        raise OrchestrateError(
            "The candidate profile differs from the selected host-local operational configuration",
            code="profile_selection_changed",
            data={"selectedDigest": profile.digest, "candidateDigest": profile.candidate_digest},
        )


def _require_candidate_coverage(sources: SourceIndex) -> None:
    candidate = sources.value.get("candidate")
    if not isinstance(candidate, Mapping) or candidate.get("coverageComplete") is not True:
        uncovered = candidate.get("uncoveredChanges", []) if isinstance(candidate, Mapping) else []
        raise OrchestrateError(
            "Dirty candidate coverage is incomplete; select only reviewed relevant paths in candidateSources",
            code="candidate_coverage_incomplete",
            data={"uncoveredChanges": uncovered},
        )


def _create_task_and_packet(client: OrcaClient, store: StateStore, run: RunRecord, profile: ProjectProfile) -> RunRecord:
    reader = read_project(profile, run.objective)
    sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
    _require_candidate_coverage(sources)
    if sources.digest != run.source_digest:
        raise OrchestrateError(
            "Project sources changed after Run preparation; no worker was launched",
            code="source_binding_changed",
        )
    choice = load_owner_model()
    draft = make_packet(
        objective=run.objective,
        profile=profile,
        sources=sources,
        launch={"agent": choice.agent, "model": choice.model, "effort": choice.effort},
        python_executable=os.fspath(Path(sys.executable).resolve()),
        run_id=run.native_run_id,
        reader=reader,
    )
    draft_json = canonical_packet_json(draft)
    response = _mutation(
        client,
        store,
        run,
        "task-create",
        [
            "orchestration",
            "task-create",
            "--run",
            str(run.native_run_id),
            "--task-title",
            orca_task_title(run.objective),
            "--spec",
            packet_spec_from_json(draft_json),
        ],
    )
    current = _apply_intention(store, run, "task-create", response)
    store.save_packet(current.local_id, str(current.task_id), draft_json)
    _validate_task_binding(client, current, draft_json)
    return current


def _validate_task_binding(client: OrcaClient, run: RunRecord, packet_json: str) -> None:
    if not run.native_run_id or not run.task_id:
        raise OrchestrateError("Local Task binding is incomplete", code="orca_contract_error")
    tasks = client.run_json("orchestration", "task-list", "--run", str(run.native_run_id), "--json")
    task_rows = _result(tasks).get("tasks")
    matching = [
        item
        for item in task_rows if isinstance(item, Mapping) and item.get("id") == run.task_id
    ] if isinstance(task_rows, list) else []
    expected_spec = packet_spec_from_json(packet_json)
    mismatches: list[str] = []
    if len(matching) != 1:
        mismatches.append("task_identity_count")
    else:
        task = matching[0]
        if task.get("run_id") != run.native_run_id:
            mismatches.append("run_id")
        if task.get("task_title") != orca_task_title(run.objective):
            mismatches.append("task_title")
        if task.get("spec") != expected_spec:
            mismatches.append("spec")
        if task.get("status") not in {"ready", "pending"}:
            mismatches.append("status")
    if mismatches:
        raise OrchestrateError(
            "Native Task readback did not preserve the exact binding: " + ", ".join(mismatches),
            code="orca_contract_error",
            data={"mismatches": mismatches},
        )


def _ensure_packet(store: StateStore, run: RunRecord, profile: ProjectProfile) -> str:
    try:
        stored_json = store.get_packet_json(run.local_id, str(run.task_id))
        packet = store.get_packet(run.local_id, str(run.task_id))
        canonical_json = canonical_packet_json(packet)
        legacy_json = json.dumps(packet, sort_keys=True)
        if stored_json not in {canonical_json, legacy_json}:
            raise OrchestrateError(
                "The stored immutable Task packet uses an unrecognized byte representation",
                code="packet_identity_conflict",
            )
        return canonical_json
    except OrchestrateError as exc:
        if exc.code != "packet_not_found":
            raise
    reader = read_project(profile, run.objective)
    sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
    _require_candidate_coverage(sources)
    if sources.digest != run.source_digest:
        raise OrchestrateError(
            "Project sources changed before the durable Task packet was recovered",
            code="source_binding_changed",
        )
    choice = load_owner_model()
    recovered = make_packet(
        objective=run.objective,
        profile=profile,
        sources=sources,
        launch={"agent": choice.agent, "model": choice.model, "effort": choice.effort},
        python_executable=os.fspath(Path(sys.executable).resolve()),
        run_id=run.native_run_id,
        reader=reader,
    )
    recovered_json = canonical_packet_json(recovered)
    return recovered_json


def _effect(
    effects: list[object],
    *,
    kind: str,
    role: str | None = None,
) -> Mapping[str, Any]:
    matching = [
        item
        for item in effects
        if isinstance(item, Mapping) and item.get("kind") == kind and (role is None or item.get("role") == role)
    ]
    if len(matching) != 1:
        raise OrchestrateError(f"worker-start omitted its exact {kind} effect", code="orca_contract_error")
    return matching[0]


def _validate_residual_resources(
    resources: object,
    *,
    worktree_id: str,
    terminal_id: str,
) -> None:
    if not isinstance(resources, list) or not all(isinstance(item, Mapping) for item in resources):
        raise OrchestrateError("worker-start residualResources has an unknown shape", code="orca_contract_error")
    for item in resources:
        if item.get("kind") == "terminal" and item.get("id") == terminal_id:
            continue
        if item.get("kind") == "worktree" and item.get("id") == worktree_id:
            continue
        raise OrchestrateError("worker-start reported an unexpected residual resource", code="orca_contract_error")


def _validate_worker_start(
    payload: Mapping[str, Any],
    *,
    run: RunRecord,
    worktree_id: str | None,
    agent: str,
    model: str,
    effort: str,
) -> tuple[str, str]:
    result = _result(payload)
    if (
        result.get("runId") != run.native_run_id
        or result.get("taskId") != run.task_id
        or not isinstance(result.get("dispatchId"), str)
        or result.get("state") != "ready"
        or result.get("stage") != "input_accepted"
        or not isinstance(result.get("setup"), Mapping)
        or result["setup"].get("state") != "not_applicable"
    ):
        raise OrchestrateError("worker-start did not accept the exact Run and Task", code="orca_contract_error")
    launch = result.get("launch")
    expected_launch = {"agent": agent, "model": model, "effort": effort}
    if (
        not isinstance(launch, Mapping)
        or launch.get("requested") != expected_launch
        or launch.get("effective") != expected_launch
    ):
        raise OrchestrateError("worker-start substituted the requested launch", code="worker_launch_mismatch")
    effects = result.get("effects")
    if not isinstance(effects, list) or len(effects) != 4:
        raise OrchestrateError("worker-start omitted its exact effects", code="orca_contract_error")
    worktree = _effect(effects, kind="worktree")
    actual_worktree_id = worktree.get("id")
    if (
        worktree.get("action") != "reused"
        or not isinstance(actual_worktree_id, str)
        or (worktree_id is not None and actual_worktree_id != worktree_id)
    ):
        raise OrchestrateError("worker-start used a different workspace", code="worker_workspace_mismatch")
    setup = _effect(effects, kind="setup")
    if setup.get("action") != "not_applicable" or setup.get("state") != "not_applicable":
        raise OrchestrateError("worker-start attempted unexpected setup", code="worker_workspace_mismatch")
    terminal = _effect(effects, kind="terminal", role="agent")
    terminal_id = terminal.get("id")
    if terminal.get("action") != "created" or not isinstance(terminal_id, str):
        raise OrchestrateError("worker-start did not create the expected agent terminal", code="orca_contract_error")
    dispatch_input = _effect(effects, kind="dispatch_input", role="agent")
    if dispatch_input.get("id") != terminal_id or dispatch_input.get("state") != "accepted":
        raise OrchestrateError("worker-start did not prove exact request receipt", code="worker_input_unproven")
    _validate_residual_resources(
        result.get("residualResources"),
        worktree_id=actual_worktree_id,
        terminal_id=terminal_id,
    )
    return actual_worktree_id, terminal_id


def _validate_worker_start_readback(
    payload: Mapping[str, Any],
    *,
    run: RunRecord,
    dispatch_id: str,
    worktree_id: str,
    worktree_selector: str,
    terminal_id: str,
    agent: str,
    model: str,
    effort: str,
) -> None:
    result = _result(payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    if (
        not isinstance(dispatch, Mapping)
        or dispatch.get("id") != dispatch_id
        or dispatch.get("run_id") != run.native_run_id
        or dispatch.get("task_id") != run.task_id
        or dispatch.get("status") != "dispatched"
        or not isinstance(worker, Mapping)
        or worker.get("state") != "ready"
        or worker.get("stage") != "input_accepted"
        or worker.get("worktree_id") != worktree_id
        or worker.get("agent_terminal_handle") != terminal_id
    ):
        raise OrchestrateError("worker-show did not confirm the exact accepted worker", code="orca_contract_error")
    options = worker.get("startOptions")
    expected_launch = {"agent": agent, "model": model, "effort": effort}
    if (
        not isinstance(options, Mapping)
        or options.get("worktree") != worktree_selector
        or options.get("resolvedWorktreeId") != worktree_id
        or options.get("terminal") is not None
        or options.get("agent") != agent
        or options.get("setup") != "not_applicable"
        or options.get("setupSource") != "existing_worktree"
        or not isinstance(options.get("launch"), Mapping)
        or options["launch"].get("requested") != expected_launch
        or options["launch"].get("effective") != expected_launch
    ):
        raise OrchestrateError("worker-show launch readback does not match the request", code="worker_launch_mismatch")
    _validate_residual_resources(
        worker.get("residualResources"),
        worktree_id=worktree_id,
        terminal_id=terminal_id,
    )


def _start_worker(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    profile: ProjectProfile,
    packet_json: str,
    *,
    worktree_id: str | None,
) -> RunRecord:
    validation = validate_packet_sources(profile, run, packet_json)
    launch = validation.packet["admission"]["launch"]
    arguments = [
        "orchestration",
        "worker-start",
        "--run",
        str(run.native_run_id),
        "--task",
        str(run.task_id),
        "--worktree",
        f"path:{profile.root.resolve()}",
        "--agent",
        launch["agent"],
        "--model",
        launch["model"],
        "--effort",
        launch["effort"],
        "--timeout-ms",
        "60000",
    ]
    intention_id = store.prepare_intention(run.local_id, "worker-start", arguments)
    store.mark_intention(intention_id, "invoking")
    try:
        response = client.run_json(*arguments, "--json", timeout_seconds=90)
    except OrcaCommandError as exc:
        payload = exc.result.payload if exc.result is not None else None
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(payload),
            error=payload or {"message": str(exc), "code": exc.code},
        )
        raise OrchestrateError(
            f"Orca mutation worker-start did not produce a confirmed receipt: {exc}",
            code="mutation_outcome_uncertain",
            data={"operation": "worker-start", "requestId": _request_id(payload), "orcaCode": exc.code},
        ) from exc
    try:
        request_id = _mutation_request_id(response)
    except OrchestrateError as exc:
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(response),
            error=response,
        )
        raise OrchestrateError(
            "Orca mutation worker-start succeeded without a recoverable native request receipt",
            code="mutation_outcome_uncertain",
            data={"operation": "worker-start", "requestId": _request_id(response)},
        ) from exc
    store.mark_intention(
        intention_id,
        "receipt_confirmed",
        request_id=request_id,
        response=response,
    )
    exact_worktree_id, terminal_id = _validate_worker_start(
        response,
        run=run,
        worktree_id=worktree_id,
        agent=launch["agent"],
        model=launch["model"],
        effort=launch["effort"],
    )
    dispatch_id = _result(response)["dispatchId"]
    readback = client.run_json("orchestration", "worker-show", "--dispatch", str(dispatch_id), "--json")
    _validate_worker_start_readback(
        readback,
        run=run,
        dispatch_id=dispatch_id,
        worktree_id=exact_worktree_id,
        worktree_selector=arguments[arguments.index("--worktree") + 1],
        terminal_id=terminal_id,
        agent=launch["agent"],
        model=launch["model"],
        effort=launch["effort"],
    )
    store.mark_intention(intention_id, "applied", request_id=request_id, response=response)
    return _apply_intention(store, run, "worker-start", response)


def _delivery(payload: Mapping[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    result = _result(payload)
    delivery_id = result.get("deliveryId")
    messages = result.get("messages")
    if delivery_id is None and messages in (None, []):
        return None, []
    if not isinstance(delivery_id, str) or not isinstance(messages, list) or not all(isinstance(item, dict) for item in messages):
        raise OrchestrateError("Orca check returned a malformed Delivery", code="orca_contract_error")
    return delivery_id, messages


def _join_admission(store: StateStore, run: RunRecord) -> tuple[RunRecord, str]:
    status = joined_preflight_status(store, run)
    if status == "admitted" and run.phase == "awaiting_preflight":
        run = store.update_run(run.local_id, phase="waiting")
    return run, status


def _validate_native_settlement(client: OrcaClient, run: RunRecord, outcome: str) -> None:
    worker = client.run_json("orchestration", "worker-show", "--dispatch", str(run.dispatch_id), "--json")
    tasks = client.run_json("orchestration", "task-list", "--run", str(run.native_run_id), "--json")
    dispatch = _result(worker).get("dispatch")
    task_rows = _result(tasks).get("tasks")
    expected = "completed" if outcome == "succeeded" else "failed"
    if not isinstance(dispatch, Mapping) or any(
        (
            dispatch.get("id") != run.dispatch_id,
            dispatch.get("task_id") != run.task_id,
            dispatch.get("run_id") != run.native_run_id,
            dispatch.get("status") != expected,
        )
    ):
        raise OrchestrateError("worker_done does not match native Dispatch settlement", code="settlement_mismatch")
    matching = [item for item in task_rows or [] if isinstance(item, Mapping) and item.get("id") == run.task_id]
    if len(matching) != 1 or matching[0].get("status") != expected or matching[0].get("run_id") != run.native_run_id:
        raise OrchestrateError("worker_done does not match native Task settlement", code="settlement_mismatch")


def _release_disposition(client: OrcaClient, store: StateStore, run: RunRecord) -> None:
    rows = store.connection.execute(
        """SELECT arguments_json, response_json FROM intentions
           WHERE run_local_id = ? AND operation = 'worker-release' AND status = 'applied'""",
        (run.local_id,),
    ).fetchall()
    released = False
    release_response: Mapping[str, Any] | None = None
    for row in rows:
        arguments = json.loads(row["arguments_json"])
        if isinstance(arguments, list) and "--dispatch" in arguments:
            index = arguments.index("--dispatch")
            released = index + 1 < len(arguments) and arguments[index + 1] == run.dispatch_id
        if released:
            if row["response_json"]:
                decoded = json.loads(row["response_json"])
                release_response = decoded if isinstance(decoded, Mapping) else None
            break
    if not released:
        release_response = _mutation(
            client,
            store,
            run,
            "worker-release",
            ["orchestration", "worker-release", "--dispatch", str(run.dispatch_id)],
        )
    if release_response is None:
        raise OrchestrateError("Worker release receipt is unavailable", code="release_unconfirmed")
    release_result = _result(release_response)
    if (
        release_result.get("dispatchId") != run.dispatch_id
        or release_result.get("state") not in {"released", "retained"}
    ):
        raise OrchestrateError("Worker release receipt does not bind the exact Dispatch", code="release_unconfirmed")
    worker = client.run_json("orchestration", "worker-show", "--dispatch", str(run.dispatch_id), "--json")
    result = _result(worker)
    dispatch = result.get("dispatch")
    if not isinstance(dispatch, Mapping) or dispatch.get("id") != run.dispatch_id:
        raise OrchestrateError("worker-show release readback belongs to another Dispatch", code="release_unconfirmed")
    resource = result.get("terminalResource")
    if not isinstance(resource, Mapping):
        raise OrchestrateError("worker-show omitted terminal release state", code="release_unconfirmed")
    ownership = resource.get("ownershipState")
    state = resource.get("releaseState")
    reason = resource.get("retainedReason")
    archive = resource.get("archive")
    exact_owner = (
        isinstance(resource.get("id"), str)
        and resource.get("originDispatchId") == run.dispatch_id
        and resource.get("ownerDispatchId") == run.dispatch_id
        and isinstance(resource.get("terminalHandle"), str)
        and isinstance(resource.get("worktreeId"), str)
        and resource.get("releaseError") is None
        and isinstance(archive, Mapping)
    )
    if (
        exact_owner
        and release_result.get("state") == "released"
        and ownership == "released"
        and state == "released"
        and reason is None
        and isinstance(resource.get("releaseRequestedAt"), str)
        and isinstance(resource.get("releaseCompletedAt"), str)
        and archive.get("source") == "transcript"
        and archive.get("status") == "captured"
    ):
        return
    if (
        exact_owner
        and release_result.get("state") == "retained"
        and ownership == "user_owned"
        and state == "retained"
        and reason == "user_takeover"
        and resource.get("releaseRequestedAt") is None
        and resource.get("releaseCompletedAt") is None
        and archive.get("source") is None
        and archive.get("status") is None
    ):
        return
    raise OrchestrateError(
        "Worker terminal release is not in a confirmed terminal disposition",
        code="release_unconfirmed",
        data={"terminalResource": dict(resource)},
    )


def _message_identity(message: Mapping[str, Any], ordinal: int) -> str:
    value = message.get("id")
    return value if isinstance(value, str) else f"ordinal-{ordinal}"


def _process_delivery(client: OrcaClient, store: StateStore, run: RunRecord, delivery_id: str, messages: list[dict[str, Any]]) -> RunRecord:
    current = store.update_run(run.local_id, delivery_id=delivery_id)
    if sum(message.get("type") == "worker_done" for message in messages) > 1:
        raise OrchestrateError("Delivery contains more than one worker_done", code="delivery_unsupported")
    existing = {row["message_id"]: row["effect_status"] for row in store.messages(run.local_id, delivery_id)}
    escalation_unresolved = False
    for ordinal, message in enumerate(messages):
        message_id = _message_identity(message, ordinal)
        message_type = message.get("type")
        payload = message.get("payload")
        if message_type in {"heartbeat", "question", "escalation", "worker_done"}:
            if not isinstance(payload, Mapping):
                raise OrchestrateError("Lifecycle mail omitted its payload", code="delivery_unsupported")
            if payload.get("taskId") != current.task_id or payload.get("dispatchId") != current.dispatch_id:
                raise OrchestrateError("Lifecycle mail is stale or belongs to another attempt", code="delivery_binding_mismatch")
        prior_status = existing.get(message_id)
        if prior_status in {"processed", "answered", "pending-answer", "unresolved"}:
            escalation_unresolved = escalation_unresolved or prior_status == "unresolved"
            continue
        if message_type == "heartbeat":
            store.add_evidence(current.local_id, kind="worker-signal", status="observed", subject="heartbeat", payload=message)
            store.mark_message(current.local_id, delivery_id, message_id, "processed")
            continue
        if message_type == "question":
            body = message.get("body") if isinstance(message.get("body"), str) else ""
            store.save_question(message_id=message_id, run_local_id=current.local_id, delivery_id=delivery_id, body=body)
            store.mark_message(current.local_id, delivery_id, message_id, "pending-answer")
            continue
        if message_type == "escalation":
            store.add_evidence(current.local_id, kind="worker-escalation", status="unresolved", subject=str(message.get("subject", "escalation")), payload=message)
            store.mark_message(current.local_id, delivery_id, message_id, "unresolved")
            escalation_unresolved = True
            continue
        if message_type != "worker_done" or not isinstance(payload, Mapping):
            raise OrchestrateError("Delivery contains unsupported or malformed mail", code="delivery_unsupported")
        if payload.get("taskId") != current.task_id or payload.get("dispatchId") != current.dispatch_id:
            raise OrchestrateError("worker_done is stale or belongs to another attempt", code="delivery_binding_mismatch")
        outcome = payload.get("outcome")
        if outcome not in {"succeeded", "failed"}:
            raise OrchestrateError("worker_done has no recognized outcome", code="delivery_unsupported")
        current, admission = _join_admission(store, current)
        _validate_native_settlement(client, current, outcome)
        _release_disposition(client, store, current)
        current = store.finalize_worker_message(
            run_local_id=current.local_id,
            delivery_id=delivery_id,
            message_id=message_id,
            outcome=outcome,
            subject=str(message.get("subject", "worker_done")),
            payload=message,
            admitted=admission == "admitted",
            admission_detail={"status": admission, "dispatchId": current.dispatch_id},
        )
    if escalation_unresolved:
        current = store.update_run(current.local_id, phase="blocked")
    return current


def _ack_if_resolved(client: OrcaClient, store: StateStore, run: RunRecord) -> bool:
    if not run.delivery_id:
        return False
    rows = store.messages(run.local_id, run.delivery_id)
    if not rows or any(row["effect_status"] not in {"processed", "answered"} for row in rows):
        return False
    _mutation(
        client,
        store,
        run,
        "delivery-ack",
        [
            "orchestration",
            "check",
            "--run",
            str(run.native_run_id),
            "--ack",
            run.delivery_id,
        ],
    )
    store.mark_delivery_acked(run.local_id, run.delivery_id)
    return True


def _reprocess_bound_delivery(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
) -> RunRecord:
    if not run.delivery_id:
        return run
    messages = store.delivery_messages(run.local_id, run.delivery_id)
    if not messages:
        raise OrchestrateError(
            "The bound FIFO Delivery is missing its immutable journal",
            code="delivery_journal_missing",
        )
    return _process_delivery(client, store, run, run.delivery_id, messages)


def _bind_native_run(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    identity: ControllerIdentity | None,
) -> RunRecord:
    if not run.native_run_id:
        return run
    if identity is None or identity.current_run_id != run.native_run_id:
        response = _mutation(
            client,
            store,
            run,
            "run-use",
            ["orchestration", "run-use", "--id", run.native_run_id],
        )
        if _entity_id(response, "run") != run.native_run_id:
            raise OrchestrateError("run-use bound a different native Run", code="controller_run_conflict")
    current = client.run_json("orchestration", "run-current", "--json")
    native = _result(current).get("run")
    if not isinstance(native, Mapping) or native.get("id") != run.native_run_id:
        raise OrchestrateError("run-current did not confirm the exact selected Run", code="controller_run_conflict")
    shown = client.run_json("orchestration", "run-show", "--id", run.native_run_id, "--json")
    record = _result(shown).get("run")
    if (
        not isinstance(record, Mapping)
        or record.get("id") != run.native_run_id
        or record.get("objective") != run.objective
    ):
        raise OrchestrateError("The selected native Run no longer matches local provenance", code="controller_run_conflict")
    return store.get_run(run.local_id)


def _supervise(client: OrcaClient, store: StateStore, run: RunRecord, *, wait_timeout_ms: int) -> JsonObject:
    deadline = time.monotonic() + (wait_timeout_ms / 1000)
    current, _ = _join_admission(store, run)
    while current.phase in {"awaiting_preflight", "waiting"}:
        remaining = max(1, int((deadline - time.monotonic()) * 1000))
        if remaining <= 1:
            break
        window = min(remaining, 60_000)
        payload = client.run_json(
            "orchestration",
            "check",
            "--run",
            str(current.native_run_id),
            "--wait",
            "--types",
            "worker_done,escalation,question",
            "--timeout-ms",
            str(window),
            "--json",
            timeout_seconds=(window / 1000) + 30,
        )
        delivery_id, messages = _delivery(payload)
        if not delivery_id:
            current, _ = _join_admission(store, current)
            continue
        store.journal_delivery(current.local_id, delivery_id, payload, messages)
        current, _ = _join_admission(store, current)
        current = _process_delivery(client, store, current, delivery_id, messages)
        if store.pending_questions(current.local_id) or current.phase == "blocked":
            break
        _ack_if_resolved(client, store, current)
    return _run_summary(store, store.get_run(current.local_id))


def implement(
    root: Path,
    objective: str | None,
    *,
    client: OrcaClient,
    wait_timeout_ms: int = 300_000,
    require_context: bool = True,
) -> JsonObject:
    profile = ProjectProfile.load(root)
    if objective is None:
        return resume(
            profile.root,
            None,
            client=client,
            wait_timeout_ms=wait_timeout_ms,
            require_context=require_context,
        )
    _require_profile_selection(profile)
    identity = require_plain_controller(client, profile.root) if require_context else None
    with StateStore(profile.root) as store:
        normalized = objective.strip()
        if not normalized:
            raise OrchestrateError("Objective must not be empty", code="objective_empty")
        if len(normalized) > 8000:
            raise OrchestrateError("Objective exceeds the bounded 8000-character limit", code="objective_too_large")
        reader = read_project(profile, normalized)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        _require_candidate_coverage(sources)
        with store.lock("project-create"):
            if store.active_runs():
                raise OrchestrateError(
                    "An active local Run already exists; resume it before starting another objective",
                    code="active_run_exists",
                )
            run = store.create_run(objective=normalized, profile_digest=profile.digest, source_digest=sources.digest)
            with store.lock(run.local_id):
                run = _create_native_run(client, store, run)
                run = _create_task_and_packet(client, store, run, profile)
                packet_json = _ensure_packet(store, run, profile)
                run = _start_worker(
                    client,
                    store,
                    run,
                    profile,
                    packet_json,
                    worktree_id=identity.worktree_id if identity else None,
                )
                return _supervise(client, store, run, wait_timeout_ms=wait_timeout_ms)


def resume(
    root: Path,
    run_id: str | None,
    *,
    client: OrcaClient,
    wait_timeout_ms: int = 300_000,
    require_context: bool = True,
) -> JsonObject:
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.select_run(run_id)
        identity = (
            require_plain_controller(
                client,
                profile.root,
                expected_run_id=run.native_run_id,
                allow_expected_unbound=True,
            )
            if require_context and run.native_run_id
            else (require_plain_controller(client, profile.root) if require_context else None)
        )
        with store.lock(run.local_id):
            prepared_start = any(
                row["operation"] == "worker-start" and row["status"] == "prepared"
                for row in store.unsettled_intentions(run.local_id)
            )
            if prepared_start:
                _require_profile_selection(profile)
                validate_packet_sources(profile, run, _ensure_packet(store, run, profile))
            run = reconcile_intentions(client, store, run)
            run = _bind_native_run(client, store, run, identity)
            if run.phase in {"preparing", "run_created", "task_created"}:
                _require_profile_selection(profile)
                reader = read_project(profile, run.objective)
                current_sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
                _require_candidate_coverage(current_sources)
                if current_sources.digest != run.source_digest:
                    raise OrchestrateError(
                        "Bound project sources changed; resume will not launch or repeat external effects",
                        code="source_binding_changed",
                        data={"expected": run.source_digest, "actual": current_sources.digest},
                    )
            if run.delivery_id:
                run = _reprocess_bound_delivery(client, store, run)
            if run.delivery_id and not store.pending_questions(run.local_id) and run.phase != "blocked":
                _ack_if_resolved(client, store, run)
                run = store.get_run(run.local_id)
            if store.pending_questions(run.local_id) or run.phase in TERMINAL_PHASES:
                return _run_summary(store, run)
            if run.phase == "preparing":
                run = _create_native_run(client, store, run)
            if run.phase == "run_created":
                run = _create_task_and_packet(client, store, run, profile)
            if run.phase == "task_created":
                packet_json = _ensure_packet(store, run, profile)
                _validate_task_binding(
                    client,
                    run,
                    packet_json,
                )
                # Missing local packet history is restored only after native Orca
                # proves the exact immutable Task spec; reconstruction is not proof.
                store.save_packet(run.local_id, str(run.task_id), packet_json)
                run = _start_worker(
                    client,
                    store,
                    run,
                    profile,
                    packet_json,
                    worktree_id=identity.worktree_id if identity else None,
                )
            return _supervise(client, store, run, wait_timeout_ms=wait_timeout_ms)


def status(root: Path, run_id: str | None, *, client: OrcaClient) -> JsonObject:
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.select_for_read(run_id)
        live: object
        if not run.native_run_id:
            live = {"status": "not-created"}
        else:
            try:
                live = {
                    "run": client.run_json("orchestration", "run-show", "--id", run.native_run_id, "--json"),
                    "tasks": client.run_json("orchestration", "task-list", "--run", run.native_run_id, "--json"),
                    "worker": client.run_json("orchestration", "worker-show", "--dispatch", run.dispatch_id, "--json") if run.dispatch_id else None,
                }
            except OrcaCommandError as exc:
                live = {"status": "unavailable", "code": exc.code, "detail": str(exc)}
        return _run_summary(store, run, live=live)


def explain(root: Path, run_id: str | None) -> JsonObject:
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.select_for_read(run_id)
        reader = read_project(profile, run.objective)
        current = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        report = _run_summary(store, run)
        report["sourceBinding"] = {
            "expected": run.source_digest,
            "current": current.digest,
            "unchanged": (
                run.source_digest == current.digest
                if current.value["candidate"].get("coverageComplete") is True
                else None
            ),
            "coverageComplete": current.value["candidate"].get("coverageComplete"),
        }
        report["explanationSource"] = "host-local records and exact source identities; no model call"
        return report


def packet(root: Path, run_id: str, task_id: str) -> JsonObject:
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.get_run(run_id)
        return store.get_packet(run.local_id, task_id)


def answer(
    root: Path,
    run_id: str,
    question_id: str,
    text: str,
    *,
    client: OrcaClient,
    require_context: bool = True,
) -> JsonObject:
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.get_run(run_id)
        identity = (
            require_plain_controller(
                client,
                profile.root,
                expected_run_id=run.native_run_id,
                allow_expected_unbound=True,
            )
            if require_context and run.native_run_id
            else (require_plain_controller(client, profile.root) if require_context else None)
        )
        with store.lock(run.local_id):
            run = reconcile_intentions(client, store, run)
            run = _bind_native_run(client, store, run, identity)
            row = store.connection.execute("SELECT * FROM questions WHERE message_id = ?", (question_id,)).fetchone()
            if row is None or row["run_local_id"] != run.local_id:
                raise OrchestrateError("Question does not belong to the selected Run", code="question_not_found")
            if row["status"] == "answered":
                if row["answer"] != text:
                    raise OrchestrateError("Question already has a different answer", code="question_already_answered")
            else:
                _mutation(
                    client,
                    store,
                    run,
                    "reply",
                    [
                        "orchestration",
                        "reply",
                        "--run",
                        str(run.native_run_id),
                        "--id",
                        question_id,
                        "--body",
                        text,
                    ],
                )
                store.answer_question(question_id, text)
                store.mark_message(run.local_id, row["delivery_id"], question_id, "answered")
            _ack_if_resolved(client, store, store.get_run(run.local_id))
            return _run_summary(store, store.get_run(run.local_id))
