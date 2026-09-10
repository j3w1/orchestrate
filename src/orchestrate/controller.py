"""Deterministic one-worker controller over public Orca commands."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Literal

from .config import RoleChoice, load_owner_model, load_role_roster
from .coordination import (
    LoadedMilestonePlan,
    MilestonePlan,
    MilestoneTask,
    NativeDagScheduler,
    NativeGateBinding,
    NativeTaskBinding,
    ReviewEvidence,
    WorkerSession,
    load_milestone_plan,
    milestone_plan_relative_path,
    require_current_review,
    validate_release_receipt,
)
from .admission import joined_preflight_status, other_dispatch_observations, validate_packet_sources
from .errors import OrchestrateError
from .identity import ControllerIdentity, require_plain_controller
from .orca import JsonObject, OrcaClient, OrcaCommandError, orca_task_title
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
    worker_show_identity,
    worker_terminal_resource_identity,
)
from .packets import (
    canonical_packet_json,
    make_milestone_packet,
    make_packet,
    packet_spec_from_json,
)
from .profile import ProjectProfile
from .readers import ReaderResult, read_project
from .safeio import read_project_bytes
from .sources import SourceIndex, build_source_index
from .state import RunRecord, StateStore, WorkerResourceBinding, utc_now


REPORT_SCHEMA = "orchestrate-report/v1"
TERMINAL_PHASES = {"worker_succeeded", "worker_failed", "worker_unadmitted", "blocked", "completed"}
PROMPT_STALL_ERROR = "agent_prompt_stalled"
PROMPT_STALL_STAGE = "dispatch_input"
PROMPT_STALL_CLEANUP_PHASE = "launch_cleanup_pending"
INPUT_SUBMISSION_DIAGNOSTIC_SUBJECT = "worker input submission unproven"
PREFLIGHT_HELD_PHASE = "preflight_held"


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


def _response_returncode(payload: Mapping[str, Any]) -> int:
    returncode = getattr(payload, "returncode", None)
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise OrchestrateError("Orca response lost its subprocess exit status", code="orca_contract_error")
    return returncode


def _validate_worker_start_returncode(payload: Mapping[str, Any], returncode: int) -> None:
    state = _result(payload).get("state")
    if (returncode == 0 and state == "ready") or (
        returncode == 1 and state in {"failed", "outcome_unknown"}
    ):
        return
    raise OrchestrateError(
        "worker-start exit status does not match its documented semantic result",
        code="worker_start_exit_mismatch",
        data={"returncode": returncode, "state": state},
    )


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


def _delivery_ack_argument(arguments: list[object]) -> str:
    if arguments.count("--ack") != 1:
        raise OrchestrateError(
            "Stored Delivery acknowledgement arguments are incomplete or conflicting",
            code="delivery_ack_unconfirmed",
        )
    index = arguments.index("--ack")
    if index + 1 >= len(arguments):
        raise OrchestrateError(
            "Stored Delivery acknowledgement omitted its Delivery identity",
            code="delivery_ack_unconfirmed",
        )
    delivery_id = arguments[index + 1]
    if not isinstance(delivery_id, str) or not delivery_id:
        raise OrchestrateError(
            "Stored Delivery acknowledgement identity is malformed",
            code="delivery_ack_unconfirmed",
        )
    return delivery_id


def _validate_operation_response(
    operation: str,
    arguments: list[object],
    response: Mapping[str, Any],
) -> None:
    if operation != "delivery-ack":
        return
    delivery_id = _delivery_ack_argument(arguments)
    try:
        exact_delivery_acknowledgement(_result(response), delivery_id=delivery_id)
    except DeliveryAcknowledgementShapeError as exc:
        raise OrchestrateError(str(exc), code="delivery_ack_unconfirmed") from exc


def _apply_delivery_ack_receipt(
    store: StateStore,
    run: RunRecord,
    arguments: list[object],
    response: Mapping[str, Any],
) -> RunRecord:
    delivery_id = _delivery_ack_argument(arguments)
    try:
        next_delivery_id, messages = exact_delivery_acknowledgement(
            _result(response),
            delivery_id=delivery_id,
        )
    except DeliveryAcknowledgementShapeError as exc:
        raise OrchestrateError(str(exc), code="delivery_ack_unconfirmed") from exc
    if next_delivery_id is not None:
        store.journal_delivery(run.local_id, next_delivery_id, dict(response), messages)
    store.mark_delivery_acked(run.local_id, delivery_id)
    current = store.get_run(run.local_id)
    if next_delivery_id is not None:
        current = store.update_run(run.local_id, delivery_id=next_delivery_id)
    return current


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
            returncode=exc.result.returncode if exc.result is not None else None,
            error=payload or {"message": str(exc), "code": exc.code},
        )
        raise OrchestrateError(
            f"Orca mutation {operation} did not produce a confirmed receipt: {exc}",
            code="mutation_outcome_uncertain",
            data={"operation": operation, "requestId": _request_id(payload), "orcaCode": exc.code},
        ) from exc
    try:
        request_id = _mutation_request_id(response)
        _validate_operation_response(operation, list(arguments), response)
    except OrchestrateError as exc:
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(response),
            error=response,
        )
        raise OrchestrateError(
            f"Orca mutation {operation} did not confirm its exact requested effect",
            code="mutation_outcome_uncertain",
            data={"operation": operation, "requestId": _request_id(response)},
        ) from exc
    store.mark_intention(
        intention_id,
        "applied",
        request_id=request_id,
        returncode=_response_returncode(response),
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
        result = _result(response)
        identity = result.get("dispatchId")
        if not isinstance(identity, str):
            raise OrchestrateError("worker-start response omitted dispatchId", code="orca_contract_error")
        if run.dispatch_id not in {None, identity}:
            raise OrchestrateError("Dispatch receipt conflicts with the local binding", code="intention_binding_mismatch")
        fields = {"dispatch_id": identity}
        if run.phase == "task_created":
            if result.get("state") == "ready":
                fields["phase"] = "awaiting_preflight"
            elif _is_prompt_stall_result(result):
                fields.update(
                    phase=PROMPT_STALL_CLEANUP_PHASE,
                    worker_outcome="failed",
                    verification_status="not_run",
                )
            else:
                raise OrchestrateError(
                    "worker-start response has no supported local state transition",
                    code="orca_contract_error",
                )
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
            arguments = json.loads(row["arguments_json"])
            response = json.loads(row["response_json"])
            _validate_operation_response(row["operation"], arguments, response)
            if row["operation"] == "delivery-ack":
                current = _apply_delivery_ack_receipt(store, current, arguments, response)
            else:
                current = _apply_intention(store, current, row["operation"], response)
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
    returncode: int,
    request_id: str,
    expected_worktree_id: str | None = None,
) -> RunRecord:
    agent = _stored_argument(arguments, "--agent")
    model = _stored_argument(arguments, "--model")
    effort = _stored_argument(arguments, "--effort")
    worktree_selector = _stored_argument(arguments, "--worktree")
    _validate_worker_start_returncode(response, returncode)
    result = _result(response)
    if _is_prompt_stall_result(result):
        worktree_id, terminal_id = _validate_prompt_stall_start(
            response,
            run=run,
            worktree_id=expected_worktree_id,
            agent=agent,
            model=model,
            effort=effort,
        )
    elif result.get("state") == "ready":
        worktree_id, terminal_id = _validate_worker_start(
            response,
            run=run,
            worktree_id=expected_worktree_id,
            agent=agent,
            model=model,
            effort=effort,
        )
    else:
        raise OrchestrateError(
            "worker-start produced a non-ready outcome without a bounded reconciliation contract",
            code="unknown_external_effect",
            data={
                "dispatchId": result.get("dispatchId"),
                "state": result.get("state"),
                "stage": result.get("stage"),
            },
        )
    dispatch_id = _result(response)["dispatchId"]
    readback = client.run_json(
        "orchestration",
        "worker-show",
        "--dispatch",
        str(dispatch_id),
        "--json",
    )
    if _is_prompt_stall_result(result):
        resource_identity = _validate_prompt_stall_readback(
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
    else:
        resource_identity = _validate_worker_start_readback(
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
    store.record_worker_resource_binding(
        run.local_id,
        dispatch_id=str(dispatch_id),
        resource_id=resource_identity.resource_id,
        terminal_handle=resource_identity.terminal_handle,
        worktree_id=resource_identity.worktree_id,
        readback=readback,
    )
    store.mark_intention(
        intention_id,
        "applied",
        request_id=request_id,
        returncode=returncode,
        response=response,
    )
    return _apply_intention(store, run, "worker-start", response)


def _stored_prompt_stall_receipt(row: Mapping[str, Any]) -> tuple[Mapping[str, Any], int] | None:
    """Recover fea035d's misclassified nonzero ok=true worker-start receipt."""

    if row["operation"] != "worker-start" or row["status"] not in {"invoking", "uncertain"}:
        return None
    encoded = row["error_json"]
    if not isinstance(encoded, str):
        return None
    try:
        payload = json.loads(encoded)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, Mapping) or payload.get("ok") is not True:
        return None
    try:
        result = _result(payload)
        request_id = _mutation_request_id(payload)
    except OrchestrateError:
        return None
    if not _is_prompt_stall_result(result):
        return None
    stored_request_id = row["request_id"]
    if stored_request_id not in {None, request_id}:
        raise OrchestrateError(
            "Stored worker-start failure conflicts with its native mutation receipt",
            code="intention_binding_mismatch",
        )
    returncode = row["returncode"]
    if returncode is None:
        # The frozen predecessor stored only this exact error envelope. It reached
        # error_json because its strict client rejected the documented nonzero.
        returncode = 1
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise OrchestrateError("Stored worker-start exit status is malformed", code="orca_contract_error")
    return payload, returncode


def reconcile_intentions(client: OrcaClient, store: StateStore, run: RunRecord) -> RunRecord:
    current = _replay_applied_intentions(store, run)
    for row in store.unsettled_intentions(run.local_id):
        stored_prompt_stall = _stored_prompt_stall_receipt(row)
        if stored_prompt_stall is not None:
            stored_response, stored_returncode = stored_prompt_stall
            request_id = _mutation_request_id(stored_response)
            store.mark_intention(
                row["id"],
                "receipt_confirmed",
                request_id=request_id,
                returncode=stored_returncode,
                response=stored_response,
            )
            current = _reconcile_confirmed_worker_start(
                client,
                store,
                current,
                intention_id=row["id"],
                arguments=json.loads(row["arguments_json"]),
                response=stored_response,
                returncode=stored_returncode,
                request_id=request_id,
            )
            continue
        if row["status"] == "receipt_confirmed":
            if row["operation"] != "worker-start" or not row["response_json"]:
                raise OrchestrateError(
                    "A confirmed mutation receipt has no bounded reconciliation path",
                    code="unknown_external_effect",
                    data={"operation": row["operation"], "intentionId": row["id"]},
                )
            arguments = json.loads(row["arguments_json"])
            response = json.loads(row["response_json"])
            returncode = row["returncode"]
            if returncode is None and _is_prompt_stall_result(_result(response)):
                returncode = 1
            if isinstance(returncode, bool) or not isinstance(returncode, int):
                raise OrchestrateError(
                    "A confirmed worker-start receipt lacks its exact subprocess exit status",
                    code="unknown_external_effect",
                    data={"operation": row["operation"], "intentionId": row["id"]},
                )
            current = _reconcile_confirmed_worker_start(
                client,
                store,
                current,
                intention_id=row["id"],
                arguments=arguments,
                response=response,
                returncode=returncode,
                request_id=row["request_id"],
            )
            continue
        if row["status"] == "prepared":
            arguments = json.loads(row["arguments_json"])
            store.mark_intention(row["id"], "invoking")
            try:
                replay = client.run_json(
                    *arguments,
                    "--json",
                    allow_worker_start_nonzero=row["operation"] == "worker-start",
                )
            except OrcaCommandError as exc:
                payload = exc.result.payload if exc.result is not None else None
                store.mark_intention(
                    row["id"],
                    "uncertain",
                    request_id=_request_id(payload),
                    returncode=exc.result.returncode if exc.result is not None else None,
                    error=payload or {"message": str(exc), "code": exc.code},
                )
                raise OrchestrateError(
                    "A prepared Orca mutation did not produce a confirmed receipt",
                    code="mutation_outcome_uncertain",
                    data={"operation": row["operation"], "requestId": _request_id(payload)},
                ) from exc
            try:
                replay_request_id = _mutation_request_id(replay)
                _validate_operation_response(row["operation"], arguments, replay)
            except OrchestrateError as exc:
                store.mark_intention(
                    row["id"],
                    "uncertain",
                    request_id=_request_id(replay),
                    error=replay,
                )
                raise OrchestrateError(
                    "A prepared Orca mutation did not confirm its exact requested effect",
                    code="mutation_outcome_uncertain",
                    data={"operation": row["operation"], "requestId": _request_id(replay)},
                ) from exc
            if row["operation"] == "worker-start":
                returncode = _response_returncode(replay)
                store.mark_intention(
                    row["id"],
                    "receipt_confirmed",
                    request_id=replay_request_id,
                    returncode=returncode,
                    response=replay,
                )
                current = _reconcile_confirmed_worker_start(
                    client,
                    store,
                    current,
                    intention_id=row["id"],
                    arguments=arguments,
                    response=replay,
                    returncode=returncode,
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
        replay = client.run_json(
            *arguments,
            "--retry-request",
            request_id,
            "--json",
            allow_worker_start_nonzero=row["operation"] == "worker-start",
        )
        try:
            _mutation_request_id(replay, expected=request_id)
            _validate_operation_response(row["operation"], arguments, replay)
        except OrchestrateError as exc:
            store.mark_intention(
                row["id"],
                "uncertain",
                request_id=_request_id(replay),
                error=replay,
            )
            raise OrchestrateError(
                "Recovered Orca mutation did not confirm its exact requested effect",
                code="mutation_outcome_uncertain",
                data={"operation": row["operation"], "requestId": _request_id(replay)},
            ) from exc
        if row["operation"] == "worker-start":
            returncode = _response_returncode(replay)
            store.mark_intention(
                row["id"],
                "receipt_confirmed",
                request_id=request_id,
                returncode=returncode,
                response=replay,
            )
            current = _reconcile_confirmed_worker_start(
                client,
                store,
                current,
                intention_id=row["id"],
                arguments=arguments,
                response=replay,
                returncode=returncode,
                request_id=request_id,
            )
        else:
            store.mark_intention(
                row["id"],
                "applied",
                request_id=request_id,
                response=replay,
            )
            current = _apply_intention(store, current, row["operation"], replay)
    return _replay_applied_intentions(store, current)


def _run_summary(store: StateStore, run: RunRecord, *, live: object = None) -> JsonObject:
    pending = store.pending_questions(run.local_id)
    admission = joined_preflight_status(store, run)
    admission_detail: object = None
    if run.task_id and run.dispatch_id:
        try:
            observation = store.get_preflight(run.local_id, run.task_id, run.dispatch_id)
            other_observations = other_dispatch_observations(store, run.local_id, run.task_id, run.dispatch_id)
        except OrchestrateError as exc:
            admission_detail = {"status": "conflicting", "code": exc.code, "message": str(exc)}
        else:
            if isinstance(observation, Mapping):
                admission_detail = {
                    "outcome": observation.get("outcome"),
                    "mismatches": observation.get("mismatches", []),
                    "limitations": observation.get("limitations", []),
                    "observedAt": observation.get("observedAt"),
                }
            elif other_observations:
                admission_detail = {
                    "outcome": None,
                    "mismatches": [],
                    "limitations": ["No immutable preflight observation exists for the bound Dispatch."],
                    "observedAt": None,
                }
            if admission_detail is not None and other_observations:
                # Immutable evidence recorded under another Dispatch stays visible;
                # a later controller binding never hides or clears it.
                admission_detail.update(
                    boundDispatchId=run.dispatch_id,
                    otherDispatchObservations=other_observations,
                )
            if admission == "conflicting" and admission_detail is not None:
                if other_observations:
                    admission_detail.update(
                        status="conflicting",
                        code="preflight_dispatch_conflict",
                        message=(
                            "An immutable preflight observation exists for a different Dispatch "
                            "of this Run/Task than the controller bound"
                        ),
                    )
                else:
                    admission_detail.update(
                        status="conflicting",
                        code="preflight_identity_conflict",
                        message="The immutable preflight observation conflicts with its bound launch",
                    )
    input_unproven = any(
        item["kind"] == "compatibility" and item["subject"] == INPUT_SUBMISSION_DIAGNOSTIC_SUBJECT
        for item in store.evidence(run.local_id)
    )
    if admission in {"rejected", "conflicting"} and (
        run.phase in {"awaiting_preflight", "waiting", "blocked", PREFLIGHT_HELD_PHASE}
        or bool(pending)
    ):
        next_obligation = (
            "the immutable worker preflight is held; inspect admissionDetail and use a separately "
            "authorized cleanup path to settle the exact bound attempt"
        )
    elif admission == "pending" and pending:
        next_obligation = (
            "the immutable worker preflight is pending; complete or recover the exact managed "
            "preflight before answering any question"
        )
    elif pending:
        next_obligation = f"answer question {pending[0]['message_id']}"
    elif run.phase == "worker_succeeded" and run.verification_status == "pending":
        next_obligation = "independent verification and project acceptance remain unresolved"
    elif run.phase == "awaiting_preflight" and input_unproven:
        next_obligation = (
            "worker input was accepted but turn start is unproven; inspect the bound Dispatch "
            "without resending task input"
        )
    elif run.phase == "awaiting_preflight":
        next_obligation = "worker must complete the exact managed preflight before edits or checks"
    elif run.phase == "waiting":
        next_obligation = "resume foreground supervision"
    elif run.phase == "worker_unadmitted":
        next_obligation = "worker claim is preserved but cannot satisfy verification without joined admission"
    elif run.phase == "worker_failed":
        next_obligation = "inspect the failed attempt before an explicit retry decision"
    elif run.phase == PROMPT_STALL_CLEANUP_PHASE:
        next_obligation = "reconcile and release the exact failed worker terminal before any retry"
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
        "admission": admission,
        "admissionDetail": admission_detail,
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


def _milestone_plan_path(store: StateStore, run: RunRecord) -> str | None:
    row = store.connection.execute(
        "SELECT relative_path FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    return str(row["relative_path"]) if row is not None else None


def _run_source_index(
    profile: ProjectProfile,
    reader: ReaderResult,
    *,
    milestone_path: str | None,
) -> SourceIndex:
    consulted = set(getattr(reader, "consulted_paths"))
    if milestone_path is not None:
        consulted.add(milestone_path)
    return build_source_index(profile, extra_sources=consulted)


def _create_task_and_packet(client: OrcaClient, store: StateStore, run: RunRecord, profile: ProjectProfile) -> RunRecord:
    reader = read_project(profile, run.objective)
    sources = _run_source_index(
        profile,
        reader,
        milestone_path=_milestone_plan_path(store, run),
    )
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
    sources = _run_source_index(
        profile,
        reader,
        milestone_path=_milestone_plan_path(store, run),
    )
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


def _is_prompt_stall_result(result: Mapping[str, Any]) -> bool:
    return (
        result.get("state") == "failed"
        and result.get("stage") == PROMPT_STALL_STAGE
        and result.get("failedStage") == PROMPT_STALL_STAGE
        and result.get("lastError") == PROMPT_STALL_ERROR
    )


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
    model: str | None,
    effort: str | None,
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
    expected_launch = {"agent": agent}
    if model is not None:
        expected_launch["model"] = model
    if effort is not None:
        expected_launch["effort"] = effort
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


def _validate_prompt_stall_start(
    payload: Mapping[str, Any],
    *,
    run: RunRecord,
    worktree_id: str | None,
    agent: str,
    model: str,
    effort: str,
) -> tuple[str, str]:
    """Validate Orca 1.4.198's exact failed-after-injection-attempt receipt."""

    result = _result(payload)
    if (
        result.get("runId") != run.native_run_id
        or result.get("taskId") != run.task_id
        or not isinstance(result.get("dispatchId"), str)
        or not _is_prompt_stall_result(result)
        or not isinstance(result.get("setup"), Mapping)
        or result["setup"].get("state") != "not_applicable"
    ):
        raise OrchestrateError(
            "worker-start did not prove the exact failed prompt-stall attempt",
            code="orca_contract_error",
        )
    launch = result.get("launch")
    expected_launch = {"agent": agent, "model": model, "effort": effort}
    if (
        not isinstance(launch, Mapping)
        or launch.get("requested") != expected_launch
        or launch.get("effective") != expected_launch
    ):
        raise OrchestrateError("failed worker-start substituted the requested launch", code="worker_launch_mismatch")
    effects = result.get("effects")
    if not isinstance(effects, list) or len(effects) != 3:
        raise OrchestrateError("failed worker-start omitted its exact effects", code="orca_contract_error")
    worktree = _effect(effects, kind="worktree")
    actual_worktree_id = worktree.get("id")
    if (
        worktree.get("action") != "reused"
        or not isinstance(actual_worktree_id, str)
        or (worktree_id is not None and actual_worktree_id != worktree_id)
    ):
        raise OrchestrateError("failed worker-start used a different workspace", code="worker_workspace_mismatch")
    setup = _effect(effects, kind="setup")
    if setup.get("action") != "not_applicable" or setup.get("state") != "not_applicable":
        raise OrchestrateError("failed worker-start attempted unexpected setup", code="worker_workspace_mismatch")
    terminal = _effect(effects, kind="terminal", role="agent")
    terminal_id = terminal.get("id")
    if terminal.get("action") != "created" or not isinstance(terminal_id, str):
        raise OrchestrateError("failed worker-start did not identify its created terminal", code="orca_contract_error")
    resources = result.get("residualResources")
    if (
        not isinstance(resources, list)
        or len(resources) != 1
        or not isinstance(resources[0], Mapping)
        or resources[0].get("kind") != "terminal"
        or resources[0].get("role") != "agent"
        or resources[0].get("action") != "created"
        or resources[0].get("id") != terminal_id
    ):
        raise OrchestrateError(
            "failed worker-start did not preserve one exact releasable terminal",
            code="orca_contract_error",
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
    model: str | None,
    effort: str | None,
) -> TerminalResourceIdentity:
    result = _result(payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    terminal_resource = result.get("terminalResource")
    if (
        not isinstance(dispatch, Mapping)
        or not isinstance(worker, Mapping)
        or not isinstance(terminal_resource, Mapping)
    ):
        raise OrchestrateError("worker-show omitted the accepted worker identity", code="orca_contract_error")
    try:
        accepted = worker_input_accepted_readback(
            dispatch,
            worker,
            terminal_resource,
            run_id=str(run.native_run_id),
            task_id=str(run.task_id),
            dispatch_id=dispatch_id,
            worktree_id=worktree_id,
            terminal_handle=terminal_id,
        )
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="orca_contract_error") from exc
    options = worker.get("startOptions")
    expected_launch = {"agent": agent}
    if model is not None:
        expected_launch["model"] = model
    if effort is not None:
        expected_launch["effort"] = effort
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
    return accepted.resource


def _validate_prompt_stall_readback(
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
    allow_released_terminal: bool = False,
) -> TerminalResourceIdentity:
    result = _result(payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    if not isinstance(dispatch, Mapping) or not isinstance(worker, Mapping):
        raise OrchestrateError("worker-show omitted the failed worker identity", code="orca_contract_error")
    try:
        identity = worker_execution_identity(
            dispatch,
            worker,
            semantics="prompt_stall",
            terminal_handle=terminal_id,
        )
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="orca_contract_error") from exc
    terminal_resource = result.get("terminalResource")
    if not isinstance(terminal_resource, Mapping):
        raise OrchestrateError(
            "worker-show omitted the failed worker terminal resource",
            code="orca_contract_error",
        )
    try:
        resource_identity = worker_terminal_resource_identity(
            terminal_resource,
            dispatch_id=dispatch_id,
        )
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="orca_contract_error") from exc
    if (
        dispatch.get("id") != dispatch_id
        or identity.dispatch.run_id != run.native_run_id
        or identity.dispatch.task_id != run.task_id
        or identity.dispatch_id != dispatch_id
        or identity.worktree_id != worktree_id
        or (terminal_resource.get("releaseState") == "released" and not allow_released_terminal)
    ):
        raise OrchestrateError(
            "worker-show did not confirm the exact failed prompt-stall worker",
            code="orca_contract_error",
        )
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
        raise OrchestrateError(
            "worker-show failed-start launch readback does not match the request",
            code="worker_launch_mismatch",
        )
    resources = worker.get("residualResources")
    if (
        not isinstance(resources, list)
        or len(resources) != 1
        or not isinstance(resources[0], Mapping)
        or resources[0].get("kind") != "terminal"
        or resources[0].get("id") != terminal_id
    ):
        raise OrchestrateError("worker-show lost the failed worker terminal identity", code="orca_contract_error")
    if (
        resource_identity.terminal_handle != terminal_id
        or resource_identity.worktree_id != worktree_id
    ):
        raise OrchestrateError(
            "worker-show did not bind the failed Dispatch to its exact terminal resource",
            code="orca_contract_error",
        )
    return resource_identity


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
        response = client.run_json(
            *arguments,
            "--json",
            timeout_seconds=90,
            allow_worker_start_nonzero=True,
        )
    except OrcaCommandError as exc:
        payload = exc.result.payload if exc.result is not None else None
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(payload),
            returncode=exc.result.returncode if exc.result is not None else None,
            error=payload or {"message": str(exc), "code": exc.code},
        )
        raise OrchestrateError(
            f"Orca mutation worker-start did not produce a confirmed receipt: {exc}",
            code="mutation_outcome_uncertain",
            data={"operation": "worker-start", "requestId": _request_id(payload), "orcaCode": exc.code},
        ) from exc
    returncode = _response_returncode(response)
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
        returncode=returncode,
        response=response,
    )
    current = _reconcile_confirmed_worker_start(
        client,
        store,
        run,
        intention_id=intention_id,
        arguments=arguments,
        response=response,
        returncode=returncode,
        request_id=request_id,
        expected_worktree_id=worktree_id,
    )
    return _finish_prompt_stall_cleanup(client, store, current)


def _delivery(payload: Mapping[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    result = _result(payload)
    delivery_id = result.get("deliveryId")
    messages = result.get("messages")
    if delivery_id is None and messages in (None, []):
        return None, []
    if not isinstance(delivery_id, str) or not delivery_id or not isinstance(messages, list) or not all(isinstance(item, dict) for item in messages):
        raise OrchestrateError("Orca check returned a malformed Delivery", code="orca_contract_error")
    return delivery_id, messages


def _join_admission(store: StateStore, run: RunRecord) -> tuple[RunRecord, str]:
    status = joined_preflight_status(store, run)
    if status == "admitted" and run.phase == "awaiting_preflight":
        run = store.update_run(run.local_id, phase="waiting")
    elif status in {"rejected", "conflicting"} and (
        run.phase in {"awaiting_preflight", "waiting", "blocked", PREFLIGHT_HELD_PHASE}
        or (
            bool(store.pending_questions(run.local_id))
            and run.phase not in {"worker_succeeded", "worker_failed", "worker_unadmitted", "completed"}
        )
    ):
        if run.phase != PREFLIGHT_HELD_PHASE:
            run = store.update_run(run.local_id, phase=PREFLIGHT_HELD_PHASE)
    return run, status


def _record_input_submission_diagnostic(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
) -> bool:
    """Record one read-only diagnostic when accepted input has not produced preflight."""

    if any(
        item["kind"] == "compatibility" and item["subject"] == INPUT_SUBMISSION_DIAGNOSTIC_SUBJECT
        for item in store.evidence(run.local_id)
    ):
        return True
    binding = store.get_worker_resource_binding(run.local_id)
    if binding is None or binding.dispatch_id != run.dispatch_id:
        return False

    def persist(status: str, readback: Mapping[str, Any]) -> bool:
        store.add_evidence(
            run.local_id,
            kind="compatibility",
            status=status,
            subject=INPUT_SUBMISSION_DIAGNOSTIC_SUBJECT,
            payload={
                "code": "worker_input_submission_unproven",
                "runId": run.native_run_id,
                "taskId": run.task_id,
                "dispatchId": run.dispatch_id,
                "terminalHandle": binding.terminal_handle,
                "turnStarted": "unproven",
                "taskInputResent": False,
                "externalEffect": "none; worker-show readback only",
                "workerReadback": dict(readback),
            },
        )
        return True

    try:
        payload = client.run_json(
            "orchestration",
            "worker-show",
            "--dispatch",
            str(run.dispatch_id),
            "--json",
        )
        result = _result(payload)
        dispatch = result.get("dispatch")
        worker = result.get("worker")
        resource = result.get("terminalResource")
        if not isinstance(dispatch, Mapping) or not isinstance(worker, Mapping) or not isinstance(resource, Mapping):
            return persist("conflicting", {"status": "malformed"})
        accepted = worker_input_accepted_readback(
            dispatch,
            worker,
            resource,
            run_id=str(run.native_run_id),
            task_id=str(run.task_id),
            dispatch_id=str(run.dispatch_id),
            worktree_id=binding.worktree_id,
            terminal_handle=binding.terminal_handle,
            resource_id=binding.resource_id,
        )
    except OrcaCommandError as exc:
        return persist("unavailable", {"status": "unavailable", "code": exc.code})
    except OrchestrateError as exc:
        return persist("conflicting", {"status": "malformed", "code": exc.code})
    except WorkerShowShapeError:
        return persist("conflicting", {"status": "unsupported-identity-shape"})
    if (
        accepted.worker.dispatch_id != run.dispatch_id
        or accepted.resource.resource_id != binding.resource_id
    ):
        return persist("conflicting", {"status": "identity-or-state-mismatch"})
    observation = result.get("observation")
    if isinstance(observation, Mapping) and "agentWait" in observation:
        agent_wait: object = observation.get("agentWait")
    elif isinstance(observation, Mapping):
        agent_wait = "not-reported"
    else:
        agent_wait = "observation-unavailable"
    return persist(
        "unresolved",
        {
            "status": "matched-input-accepted",
            "workerState": worker.get("state"),
            "workerStage": worker.get("stage"),
            "agentWait": agent_wait,
        },
    )


def _validate_native_settlement(client: OrcaClient, run: RunRecord, outcome: str) -> None:
    worker = client.run_json("orchestration", "worker-show", "--dispatch", str(run.dispatch_id), "--json")
    tasks = client.run_json("orchestration", "task-list", "--run", str(run.native_run_id), "--json")
    dispatch = _result(worker).get("dispatch")
    task_rows = _result(tasks).get("tasks")
    expected = "completed" if outcome == "succeeded" else "failed"
    if not isinstance(dispatch, Mapping):
        raise OrchestrateError("worker_done settlement omitted its native Dispatch", code="settlement_mismatch")
    try:
        identity = worker_show_dispatch_identity(dispatch)
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="settlement_mismatch") from exc
    if any(
        (
            dispatch.get("id") != run.dispatch_id,
            identity.task_id != run.task_id,
            identity.run_id != run.native_run_id,
            dispatch.get("status") != expected,
        )
    ):
        raise OrchestrateError("worker_done does not match native Dispatch settlement", code="settlement_mismatch")
    matching = [item for item in task_rows or [] if isinstance(item, Mapping) and item.get("id") == run.task_id]
    if len(matching) != 1 or matching[0].get("status") != expected or matching[0].get("run_id") != run.native_run_id:
        raise OrchestrateError("worker_done does not match native Task settlement", code="settlement_mismatch")


def _recover_prompt_stall_resource_binding(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
) -> tuple[WorkerResourceBinding, Mapping[str, Any]]:
    rows = store.connection.execute(
        """SELECT arguments_json, response_json, returncode FROM intentions
           WHERE run_local_id = ? AND operation = 'worker-start' AND status = 'applied'""",
        (run.local_id,),
    ).fetchall()
    if len(rows) != 1 or not rows[0]["response_json"]:
        raise OrchestrateError(
            "The exact pre-release worker resource binding is unavailable",
            code="release_unconfirmed",
        )
    arguments = json.loads(rows[0]["arguments_json"])
    response = json.loads(rows[0]["response_json"])
    if not isinstance(arguments, list) or not isinstance(response, Mapping):
        raise OrchestrateError("Stored worker-start recovery data is malformed", code="orca_contract_error")
    returncode = rows[0]["returncode"]
    if returncode is None and _is_prompt_stall_result(_result(response)):
        returncode = 1
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise OrchestrateError(
            "The pre-release worker-start exit status is unavailable",
            code="release_unconfirmed",
        )
    _validate_worker_start_returncode(response, returncode)
    worktree_id, terminal_id = _validate_prompt_stall_start(
        response,
        run=run,
        worktree_id=None,
        agent=_stored_argument(arguments, "--agent"),
        model=_stored_argument(arguments, "--model"),
        effort=_stored_argument(arguments, "--effort"),
    )
    readback = client.run_json(
        "orchestration",
        "worker-show",
        "--dispatch",
        str(run.dispatch_id),
        "--json",
    )
    resource = _validate_prompt_stall_readback(
        readback,
        run=run,
        dispatch_id=str(run.dispatch_id),
        worktree_id=worktree_id,
        worktree_selector=_stored_argument(arguments, "--worktree"),
        terminal_id=terminal_id,
        agent=_stored_argument(arguments, "--agent"),
        model=_stored_argument(arguments, "--model"),
        effort=_stored_argument(arguments, "--effort"),
        allow_released_terminal=True,
    )
    binding = store.record_worker_resource_binding(
        run.local_id,
        dispatch_id=str(run.dispatch_id),
        resource_id=resource.resource_id,
        terminal_handle=resource.terminal_handle,
        worktree_id=resource.worktree_id,
        readback=readback,
    )
    return binding, readback


def _release_disposition(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    *,
    expected_semantics: Literal["succeeded", "failed", "prompt_stall"],
    require_released: bool = False,
) -> None:
    rows = store.connection.execute(
        """SELECT arguments_json, response_json FROM intentions
           WHERE run_local_id = ? AND operation = 'worker-release' AND status = 'applied'""",
        (run.local_id,),
    ).fetchall()
    release_applied = False
    release_response: Mapping[str, Any] | None = None
    for row in rows:
        arguments = json.loads(row["arguments_json"])
        if isinstance(arguments, list) and "--dispatch" in arguments:
            index = arguments.index("--dispatch")
            release_applied = index + 1 < len(arguments) and arguments[index + 1] == run.dispatch_id
        if release_applied:
            if row["response_json"]:
                decoded = json.loads(row["response_json"])
                release_response = decoded if isinstance(decoded, Mapping) else None
            break
    binding = store.get_worker_resource_binding(run.local_id)
    recovered_readback: Mapping[str, Any] | None = None
    if binding is None:
        binding, recovered_readback = _recover_prompt_stall_resource_binding(client, store, run)
    if binding.dispatch_id != run.dispatch_id:
        raise OrchestrateError(
            "The stored worker resource belongs to another Dispatch",
            code="release_unconfirmed",
        )
    if not release_applied:
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
    release_state = release_result.get("state")
    if release_result.get("dispatchId") != run.dispatch_id or release_state not in {
        "released",
        "already_released",
        "retained",
        "release_pending",
    }:
        raise OrchestrateError("Worker release receipt does not bind the exact Dispatch", code="release_unconfirmed")
    if release_state == "release_pending":
        archive_metadata = release_result.get("archive")
        if (
            release_result.get("processAction") != "none"
            or not isinstance(release_result.get("recovery"), str)
            or not release_result["recovery"]
            or (archive_metadata is not None and not isinstance(archive_metadata, Mapping))
            or ("lastError" in release_result and not isinstance(release_result.get("lastError"), str))
        ):
            raise OrchestrateError(
                "Worker release_pending receipt omitted its exact recovery metadata",
                code="release_unconfirmed",
            )
    worker = recovered_readback if release_applied and recovered_readback is not None else client.run_json(
        "orchestration", "worker-show", "--dispatch", str(run.dispatch_id), "--json"
    )
    result = _result(worker)
    dispatch = result.get("dispatch")
    shown_worker = result.get("worker")
    if not isinstance(dispatch, Mapping) or not isinstance(shown_worker, Mapping):
        raise OrchestrateError("worker-show release readback omitted its exact worker", code="release_unconfirmed")
    try:
        shown_identity = worker_show_identity(dispatch, shown_worker)
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="release_unconfirmed") from exc
    if (
        dispatch.get("id") != run.dispatch_id
        or shown_identity.dispatch.run_id != run.native_run_id
        or shown_identity.dispatch.task_id != run.task_id
        or shown_identity.dispatch_id != run.dispatch_id
        or shown_identity.worktree_id != binding.worktree_id
    ):
        raise OrchestrateError("worker-show release readback belongs to another Dispatch", code="release_unconfirmed")
    resource = result.get("terminalResource")
    if not isinstance(resource, Mapping):
        raise OrchestrateError("worker-show omitted terminal release state", code="release_unconfirmed")
    try:
        resource_identity = worker_terminal_resource_identity(
            resource,
            dispatch_id=str(run.dispatch_id),
        )
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="release_unconfirmed") from exc
    if (
        resource_identity.resource_id != binding.resource_id
        or resource_identity.terminal_handle != binding.terminal_handle
        or resource_identity.worktree_id != binding.worktree_id
    ):
        raise OrchestrateError(
            "worker-show release readback changed the bound terminal resource identity",
            code="release_unconfirmed",
        )
    ownership = resource.get("ownershipState")
    state = resource.get("releaseState")
    reason = resource.get("retainedReason")
    archive = resource.get("archive")
    exact_owner = (
        resource.get("releaseError") is None
        and isinstance(archive, Mapping)
    )
    released_resource = (
        exact_owner
        and release_state in {"released", "already_released", "release_pending"}
        and ownership == "released"
        and state == "released"
        and reason is None
        and isinstance(resource.get("releaseRequestedAt"), str)
        and isinstance(resource.get("releaseCompletedAt"), str)
        and archive.get("source") == "transcript"
        and archive.get("status") == "captured"
    )
    retained_resource = (
        not require_released
        and exact_owner
        and release_state == "retained"
        and ownership == "user_owned"
        and state == "retained"
        and reason == "user_takeover"
        and resource.get("releaseRequestedAt") is None
        and resource.get("releaseCompletedAt") is None
        and archive.get("source") is None
        and archive.get("status") is None
    )
    if released_resource or retained_resource:
        try:
            worker_execution_identity(
                dispatch,
                shown_worker,
                semantics=expected_semantics,
                terminal_handle=binding.terminal_handle,
            )
        except WorkerShowShapeError as exc:
            raise OrchestrateError(
                "worker-show release readback does not preserve the exact settled worker semantics",
                code="release_unconfirmed",
            ) from exc
        if released_resource and result.get("terminal") is not None:
            raise OrchestrateError(
                "worker-show release readback still exposes an attached terminal",
                code="release_unconfirmed",
            )
        return
    if release_state == "release_pending":
        raise OrchestrateError(
            "Worker terminal release remains pending exact Orca recovery",
            code="release_pending",
            data={
                "release": dict(release_result),
                "terminalResource": dict(resource),
            },
        )
    raise OrchestrateError(
        "Worker terminal release is not in a confirmed terminal disposition",
        code="release_unconfirmed",
        data={"terminalResource": dict(resource)},
    )


def _finish_prompt_stall_cleanup(client: OrcaClient, store: StateStore, run: RunRecord) -> RunRecord:
    if run.phase != PROMPT_STALL_CLEANUP_PHASE:
        return run
    _release_disposition(
        client,
        store,
        run,
        expected_semantics="prompt_stall",
        require_released=True,
    )
    return store.update_run(
        run.local_id,
        phase="worker_failed",
        worker_outcome="failed",
        verification_status="not_run",
    )


def _message_identity(message: Mapping[str, Any], ordinal: int) -> str:
    value = message.get("id")
    return value if isinstance(value, str) else f"ordinal-{ordinal}"


def _require_delivery_journal(
    store: StateStore,
    run: RunRecord,
    delivery_id: str,
    messages: list[dict[str, Any]],
) -> None:
    row = store.connection.execute(
        "SELECT response_json FROM deliveries WHERE run_local_id = ? AND delivery_id = ?",
        (run.local_id, delivery_id),
    ).fetchone()
    if row is None:
        raise OrchestrateError(
            "Delivery effects require the immutable whole FIFO journal",
            code="delivery_journal_missing",
        )
    try:
        response = json.loads(row["response_json"])
        stored_delivery_id, stored_messages = _delivery(response)
    except (json.JSONDecodeError, TypeError, OrchestrateError) as exc:
        raise OrchestrateError(
            "The immutable FIFO Delivery journal is malformed",
            code="delivery_identity_conflict",
        ) from exc
    if stored_delivery_id != delivery_id or stored_messages != messages:
        raise OrchestrateError(
            "Delivery effects do not match the immutable whole FIFO journal",
            code="delivery_identity_conflict",
        )


def _process_delivery(client: OrcaClient, store: StateStore, run: RunRecord, delivery_id: str, messages: list[dict[str, Any]]) -> RunRecord:
    _require_delivery_journal(store, run, delivery_id, messages)
    current = store.update_run(run.local_id, delivery_id=delivery_id)
    current, _ = _join_admission(store, current)
    if current.phase == PREFLIGHT_HELD_PHASE:
        return current
    if sum(message.get("type") == "worker_done" for message in messages) > 1:
        raise OrchestrateError("Delivery contains more than one worker_done", code="delivery_unsupported")
    existing = {row["message_id"]: row["effect_status"] for row in store.messages(run.local_id, delivery_id)}
    binding = store.get_worker_resource_binding(current.local_id)
    if binding is None or binding.dispatch_id != current.dispatch_id:
        raise OrchestrateError(
            "Lifecycle mail cannot be bound to the exact worker terminal",
            code="delivery_sender_untrusted",
        )
    escalation_unresolved = False
    for ordinal, message in enumerate(messages):
        message_id = _message_identity(message, ordinal)
        message_type = message.get("type")
        try:
            payload = current_lifecycle_message_payload(
                message,
                run_id=str(current.native_run_id),
                task_id=str(current.task_id),
                dispatch_id=str(current.dispatch_id),
                terminal_handle=binding.terminal_handle,
            )
        except LifecycleMessageShapeError as exc:
            code = {
                "shape": "delivery_unsupported",
                "binding": "delivery_binding_mismatch",
                "sender": "delivery_sender_untrusted",
            }[exc.category]
            raise OrchestrateError(str(exc), code=code) from exc
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
        if message_type != "worker_done":
            raise OrchestrateError("Delivery contains unsupported or malformed mail", code="delivery_unsupported")
        outcome = payload.get("outcome")
        if outcome not in {"succeeded", "failed"}:
            raise OrchestrateError("worker_done has no recognized outcome", code="delivery_unsupported")
        current, admission = _join_admission(store, current)
        _validate_native_settlement(client, current, outcome)
        _release_disposition(client, store, current, expected_semantics=outcome)
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
    arguments = [
        "orchestration",
        "check",
        "--run",
        str(run.native_run_id),
        "--ack",
        run.delivery_id,
    ]
    response = _mutation(
        client,
        store,
        run,
        "delivery-ack",
        arguments,
    )
    _apply_delivery_ack_receipt(store, run, arguments, response)
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
    if current.phase == PREFLIGHT_HELD_PHASE:
        return _run_summary(store, current)
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
        # A pending first preflight must remain able to run while check waits.
        # Once check returns, the dedicated fence orders every admission join,
        # local Delivery effect, native lifecycle effect, retry, and ack as one
        # interval against any later public preflight observation.
        with store.admission_effect_fence(current.local_id):
            current, admission = _join_admission(store, store.get_run(current.local_id))
            if current.phase == PREFLIGHT_HELD_PHASE:
                break
            delivery_id, messages = _delivery(payload)
            if not delivery_id:
                if (
                    current.phase == "awaiting_preflight"
                    and admission == "pending"
                    and _result(payload).get("timedOut") is True
                    and _record_input_submission_diagnostic(client, store, current)
                ):
                    break
                continue
            store.journal_delivery(current.local_id, delivery_id, payload, messages)
            current = store.update_run(current.local_id, delivery_id=delivery_id)
            current, _ = _join_admission(store, current)
            if current.phase == PREFLIGHT_HELD_PHASE:
                break
            current = _process_delivery(client, store, current, delivery_id, messages)
            if store.pending_questions(current.local_id) or current.phase == "blocked":
                break
            _ack_if_resolved(client, store, current)
    current, _ = _join_admission(store, store.get_run(current.local_id))
    return _run_summary(store, store.get_run(current.local_id))


def _record_milestone_plan(store: StateStore, run: RunRecord, loaded: LoadedMilestonePlan) -> None:
    existing = store.connection.execute(
        "SELECT * FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    identity = (loaded.relative_path, loaded.digest, loaded.canonical_json, loaded.plan.contract.digest)
    if existing is not None:
        observed = (
            existing["relative_path"],
            existing["plan_digest"],
            existing["plan_json"],
            existing["contract_digest"],
        )
        if observed != identity:
            raise OrchestrateError("The selected milestone plan changed identity", code="milestone_plan_changed")
        return
    now = utc_now()
    store.connection.execute(
        """INSERT INTO milestone_plan_bindings
           VALUES (?, ?, ?, ?, NULL, ?, 'owner_pending', ?, ?)""",
        (run.local_id, *identity, now, now),
    )


def _milestone_result_path(store: StateStore, run_local_id: str, task_key: str) -> Path:
    digest = hashlib.sha256(f"{run_local_id}\0{task_key}".encode("utf-8")).hexdigest()
    directory = store.directory / "milestone-results"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{digest}.json"


def _reload_milestone_plan(
    profile: ProjectProfile,
    store: StateStore,
    run: RunRecord,
) -> tuple[LoadedMilestonePlan, SourceIndex, ReaderResult]:
    row = store.connection.execute(
        "SELECT * FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    if row is None:
        raise OrchestrateError("The Run has no bound milestone plan", code="milestone_plan_missing")
    reader = read_project(profile, run.objective)
    sources = _run_source_index(profile, reader, milestone_path=row["relative_path"])
    _require_candidate_coverage(sources)
    loaded = load_milestone_plan(
        profile.root,
        row["relative_path"],
        objective=run.objective,
        candidate_digest=sources.digest,
        sources=sources,
    )
    expected = (row["relative_path"], row["plan_digest"], row["plan_json"], row["contract_digest"])
    actual = (loaded.relative_path, loaded.digest, loaded.canonical_json, loaded.plan.contract.digest)
    if actual != expected:
        raise OrchestrateError("The tracked milestone plan changed after owner launch", code="milestone_plan_changed")
    selected_candidate = row["candidate_digest"]
    if selected_candidate is not None and selected_candidate != sources.digest:
        raise OrchestrateError(
            "The candidate changed after the milestone verification snapshot",
            code="stale_review",
            data={"expected": selected_candidate, "actual": sources.digest},
        )
    return loaded, sources, reader


def _runtime_milestone_plan(
    profile: ProjectProfile,
    store: StateStore,
    run: RunRecord,
    loaded: LoadedMilestonePlan,
    sources: SourceIndex,
    reader: ReaderResult,
) -> tuple[MilestonePlan, dict[str, str]]:
    roster = load_role_roster()
    runtime_tasks: list[MilestoneTask] = []
    packets: dict[str, str] = {}
    for task in loaded.plan.tasks:
        if task.integration_owner:
            runtime_tasks.append(task)
            continue
        choice = roster.choice(task.role)
        result_path = _milestone_result_path(store, run.local_id, task.key)
        packet_value = make_milestone_packet(
            objective=run.objective,
            task_key=task.key,
            task_spec=task.spec,
            role=task.role,
            candidate_digest=loaded.plan.candidate_digest,
            contract_digest=loaded.plan.contract.digest,
            contract=json.loads(loaded.plan.contract.canonical_json),
            result_path=os.fspath(result_path),
            max_workers=loaded.plan.max_workers,
            profile=profile,
            sources=sources,
            launch=choice.requested(),
            python_executable=os.fspath(Path(sys.executable).resolve()),
            run_id=str(run.native_run_id),
            reader=reader,
        )
        packet_json = canonical_packet_json(packet_value)
        packets[task.key] = packet_json
        runtime_tasks.append(replace(task, spec=packet_spec_from_json(packet_json)))
    return replace(loaded.plan, tasks=tuple(runtime_tasks)).validated(), packets


def _milestone_gate_rows(store: StateStore, run: RunRecord) -> dict[str, NativeGateBinding]:
    rows = store.connection.execute(
        "SELECT * FROM milestone_gate_bindings WHERE run_local_id = ? ORDER BY created_at",
        (run.local_id,),
    ).fetchall()
    return {
        row["task_key"]: NativeGateBinding(
            row["task_key"],
            row["task_id"],
            row["gate_id"],
            row["gate_kind"],
            row["question"],
            row["status"],
            row["resolution"],
        )
        for row in rows
    }


def _start_milestone_worker(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    profile: ProjectProfile,
    task: MilestoneTask,
    binding: NativeTaskBinding,
    packet_json: str,
    choice: RoleChoice,
    *,
    worktree_id: str | None,
) -> None:
    existing = store.connection.execute(
        "SELECT * FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = ?",
        (run.local_id, task.key),
    ).fetchone()
    if existing is not None:
        if existing["task_id"] != binding.task_id or existing["role"] != task.role or existing["agent"] != choice.agent:
            raise OrchestrateError("Stored milestone worker conflicts with its planned Task", code="native_worker_binding_mismatch")
        return
    operation = f"milestone-worker-start:{task.key}"
    unresolved = store.connection.execute(
        "SELECT id, request_id, status FROM intentions WHERE run_local_id = ? AND operation = ? AND status != 'applied'",
        (run.local_id, operation),
    ).fetchone()
    if unresolved is not None:
        raise OrchestrateError(
            "A prior milestone worker launch is unresolved; no duplicate was issued",
            code="unknown_external_effect",
            data={"taskKey": task.key, "requestId": unresolved["request_id"], "status": unresolved["status"]},
        )
    task_run = replace(run, task_id=binding.task_id, dispatch_id=None)
    validate_packet_sources(profile, task_run, packet_json)
    selector = f"path:{profile.root.resolve()}"
    arguments = [
        "orchestration",
        "worker-start",
        "--run",
        str(run.native_run_id),
        "--task",
        binding.task_id,
        "--worktree",
        selector,
        "--agent",
        choice.agent,
    ]
    if choice.model is not None:
        arguments.extend(("--model", choice.model))
    if choice.effort is not None:
        arguments.extend(("--effort", choice.effort))
    arguments.extend(("--timeout-ms", "60000"))
    intention_id = store.prepare_intention(run.local_id, operation, arguments)
    store.mark_intention(intention_id, "invoking")
    try:
        response = client.run_json(
            *arguments,
            "--json",
            timeout_seconds=90,
            allow_worker_start_nonzero=True,
        )
    except OrcaCommandError as exc:
        payload = exc.result.payload if exc.result is not None else None
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=_request_id(payload),
            returncode=exc.result.returncode if exc.result is not None else None,
            error=payload or {"message": str(exc), "code": exc.code},
        )
        raise OrchestrateError(
            "Milestone worker launch is uncertain; no duplicate will be issued",
            code="mutation_outcome_uncertain",
            data={"taskKey": task.key, "requestId": _request_id(payload)},
        ) from exc
    returncode = _response_returncode(response)
    request_id = _mutation_request_id(response)
    try:
        _validate_worker_start_returncode(response, returncode)
        if _result(response).get("state") != "ready":
            raise OrchestrateError(
                "Milestone worker did not reach the exact ready state; its resources require explicit recovery",
                code="milestone_worker_start_failed",
            )
        actual_worktree, terminal = _validate_worker_start(
            response,
            run=task_run,
            worktree_id=worktree_id,
            agent=choice.agent,
            model=choice.model,
            effort=choice.effort,
        )
        dispatch_id = _result(response).get("dispatchId")
        if not isinstance(dispatch_id, str):
            raise OrchestrateError("worker-start omitted Dispatch identity", code="orca_contract_error")
        readback = client.run_json("orchestration", "worker-show", "--dispatch", dispatch_id, "--json")
        resource = _validate_worker_start_readback(
            readback,
            run=task_run,
            dispatch_id=dispatch_id,
            worktree_id=actual_worktree,
            worktree_selector=selector,
            terminal_id=terminal,
            agent=choice.agent,
            model=choice.model,
            effort=choice.effort,
        )
    except OrchestrateError as exc:
        store.mark_intention(
            intention_id,
            "uncertain",
            request_id=request_id,
            returncode=returncode,
            error={"code": exc.code, "message": str(exc), "response": response},
        )
        raise
    store.mark_intention(
        intention_id,
        "applied",
        request_id=request_id,
        returncode=returncode,
        response=response,
    )
    now = utc_now()
    store.connection.execute(
        """INSERT INTO milestone_worker_bindings
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'owned', ?, ?, ?)""",
        (
            run.local_id,
            task.key,
            binding.task_id,
            dispatch_id,
            task.role,
            choice.agent,
            resource.resource_id,
            resource.terminal_handle,
            resource.worktree_id,
            json.dumps(readback, sort_keys=True),
            now,
            now,
        ),
    )


def _read_milestone_result(
    store: StateStore,
    run: RunRecord,
    plan: MilestonePlan,
    task: MilestoneTask,
    worker: object,
    payload: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    expected_path = _milestone_result_path(store, run.local_id, task.key)
    if payload.get("reportPath") != os.fspath(expected_path):
        return None, None
    try:
        raw = read_project_bytes(
            store.directory,
            expected_path.relative_to(store.directory).as_posix(),
        )
        if len(raw) > 64 * 1024:
            raise ValueError("result exceeds limit")
        result = json.loads(raw.decode("utf-8"))
    except (OSError, OrchestrateError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None, None
    expected_fields = {
        "schema",
        "taskKey",
        "taskId",
        "dispatchId",
        "candidateDigest",
        "contractDigest",
        "outcome",
    }
    if not isinstance(result, Mapping) or set(result) != expected_fields:
        return None, None
    outcome = result.get("outcome")
    if (
        result.get("schema") != "orchestrate-milestone-result/v1"
        or result.get("taskKey") != task.key
        or result.get("taskId") != worker["task_id"]  # type: ignore[index]
        or result.get("dispatchId") != worker["dispatch_id"]  # type: ignore[index]
        or result.get("candidateDigest") != plan.candidate_digest
        or result.get("contractDigest") != plan.contract.digest
        or outcome not in {"accepted", "rejected"}
    ):
        return None, None
    digest = "result_sha256_" + hashlib.sha256(raw).hexdigest()
    return str(outcome), digest


def _release_milestone_worker(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    worker: object,
    *,
    outcome: Literal["succeeded", "failed"],
) -> str:
    operation = f"milestone-worker-release:{worker['task_key']}"  # type: ignore[index]
    rows = store.connection.execute(
        "SELECT * FROM intentions WHERE run_local_id = ? AND operation = ? ORDER BY created_at",
        (run.local_id, operation),
    ).fetchall()
    if len(rows) > 1:
        raise OrchestrateError("Milestone worker has conflicting release intentions", code="release_unconfirmed")
    if rows:
        row = rows[0]
        if row["status"] != "applied" or not row["response_json"]:
            raise OrchestrateError(
                "Milestone worker release remains uncertain; no duplicate was issued",
                code="unknown_external_effect",
                data={"taskKey": worker["task_key"], "requestId": row["request_id"], "status": row["status"]},
            )
        release = json.loads(row["response_json"])
    else:
        release = _mutation(
            client,
            store,
            run,
            operation,
            ["orchestration", "worker-release", "--dispatch", worker["dispatch_id"]],  # type: ignore[index]
        )
    readback = client.run_json(
        "orchestration",
        "worker-show",
        "--dispatch",
        worker["dispatch_id"],  # type: ignore[index]
        "--json",
    )
    session = WorkerSession(
        worker["dispatch_id"],  # type: ignore[index]
        worker["task_id"],  # type: ignore[index]
        worker["terminal_handle"],  # type: ignore[index]
        worker["resource_id"],  # type: ignore[index]
        worker["worktree_id"],  # type: ignore[index]
        worker["agent"],  # type: ignore[index]
        str(run.native_run_id),
        outcome,
    )
    decision = validate_release_receipt(session, release, worker_readback=readback)
    if decision.state == "uncertain":
        store.connection.execute(
            "UPDATE milestone_worker_bindings SET release_state = 'uncertain', updated_at = ? WHERE run_local_id = ? AND task_key = ?",
            (utc_now(), run.local_id, worker["task_key"]),  # type: ignore[index]
        )
        raise OrchestrateError(
            "Milestone worker release remains uncertain; follow only its recorded recovery action",
            code="release_pending",
            data={"taskKey": worker["task_key"], "recovery": dict(decision.recovery_metadata or {})},  # type: ignore[index]
        )
    return decision.state


def _finalize_milestone_worker(
    store: StateStore,
    run: RunRecord,
    worker: object,
    *,
    delivery_id: str,
    message_id: str,
    message: Mapping[str, Any],
    outcome: str,
    result_outcome: str | None,
    result_digest: str | None,
    release_state: str,
) -> None:
    now = utc_now()
    evidence_id = "evidence_" + hashlib.sha256(
        f"{run.local_id}\0{delivery_id}\0{message_id}\0milestone-result".encode("utf-8")
    ).hexdigest()
    with store.transaction():
        store.connection.execute(
            """UPDATE milestone_worker_bindings
               SET outcome = ?, result_outcome = ?, result_digest = ?, release_state = ?, updated_at = ?
               WHERE run_local_id = ? AND task_key = ? AND dispatch_id = ? AND outcome IS NULL""",
            (
                outcome,
                result_outcome,
                result_digest,
                release_state,
                now,
                run.local_id,
                worker["task_key"],  # type: ignore[index]
                worker["dispatch_id"],  # type: ignore[index]
            ),
        )
        store.connection.execute(
            "INSERT OR IGNORE INTO evidence VALUES (?, ?, 'milestone-result', ?, ?, ?, ?)",
            (
                evidence_id,
                run.local_id,
                result_outcome or "missing",
                f"planned Task {worker['task_key']}",  # type: ignore[index]
                json.dumps(dict(message), sort_keys=True),
                now,
            ),
        )
        store.connection.execute(
            """UPDATE delivery_messages SET effect_status = 'processed'
               WHERE run_local_id = ? AND delivery_id = ? AND message_id = ?""",
            (run.local_id, delivery_id, message_id),
        )


def _process_milestone_delivery(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    plan: MilestonePlan,
    delivery_id: str,
    messages: list[dict[str, Any]],
) -> RunRecord:
    _require_delivery_journal(store, run, delivery_id, messages)
    task_map = {task.key: task for task in plan.tasks}
    existing = {row["message_id"]: row["effect_status"] for row in store.messages(run.local_id, delivery_id)}
    for ordinal, message in enumerate(messages):
        message_id = _message_identity(message, ordinal)
        if existing.get(message_id) in {"processed", "answered", "pending-answer", "unresolved"}:
            continue
        message_type = message.get("type")
        sender = message.get("from_handle")
        if message_type == "question" and isinstance(sender, str) and sender.startswith("dispatch:"):
            worker = store.connection.execute(
                "SELECT * FROM milestone_worker_bindings WHERE run_local_id = ? AND dispatch_id = ?",
                (run.local_id, sender.removeprefix("dispatch:")),
            ).fetchone()
        else:
            worker = store.connection.execute(
                "SELECT * FROM milestone_worker_bindings WHERE run_local_id = ? AND terminal_handle = ?",
                (run.local_id, sender),
            ).fetchone()
        if worker is None:
            raise OrchestrateError("Milestone lifecycle mail has no exact planned worker", code="delivery_sender_untrusted")
        payload = current_lifecycle_message_payload(
            message,
            run_id=str(run.native_run_id),
            task_id=worker["task_id"],
            dispatch_id=worker["dispatch_id"],
            terminal_handle=worker["terminal_handle"],
        )
        admitted = joined_preflight_status(
            store,
            replace(run, task_id=worker["task_id"], dispatch_id=worker["dispatch_id"]),
        ) == "admitted"
        if message_type == "heartbeat":
            store.add_evidence(run.local_id, kind="worker-signal", status="observed", subject="heartbeat", payload=message)
            store.mark_message(run.local_id, delivery_id, message_id, "processed")
            continue
        if message_type == "question":
            if not admitted:
                raise OrchestrateError(
                    "Milestone worker question has no exact joined preflight admission",
                    code="worker_unadmitted",
                )
            body = message.get("body") if isinstance(message.get("body"), str) else ""
            store.save_question(message_id=message_id, run_local_id=run.local_id, delivery_id=delivery_id, body=body)
            store.mark_message(run.local_id, delivery_id, message_id, "pending-answer")
            continue
        if message_type == "escalation":
            if not admitted:
                raise OrchestrateError(
                    "Milestone worker escalation has no exact joined preflight admission",
                    code="worker_unadmitted",
                )
            store.add_evidence(run.local_id, kind="worker-escalation", status="unresolved", subject=str(message.get("subject", "escalation")), payload=message)
            store.mark_message(run.local_id, delivery_id, message_id, "unresolved")
            return store.update_run(run.local_id, phase="milestone_blocked", verification_status="not_run")
        if message_type != "worker_done" or payload.get("outcome") not in {"succeeded", "failed"}:
            raise OrchestrateError("Milestone Delivery contains unsupported mail", code="delivery_unsupported")
        outcome = str(payload["outcome"])
        task = task_map[worker["task_key"]]
        _validate_native_settlement(
            client,
            replace(run, task_id=worker["task_id"], dispatch_id=worker["dispatch_id"]),
            outcome,
        )
        result_outcome, result_digest = _read_milestone_result(store, run, plan, task, worker, payload)
        release_state = _release_milestone_worker(
            client,
            store,
            run,
            worker,
            outcome=outcome,  # type: ignore[arg-type]
        )
        if not admitted:
            result_outcome = None
            result_digest = None
        _finalize_milestone_worker(
            store,
            run,
            worker,
            delivery_id=delivery_id,
            message_id=message_id,
            message=message,
            outcome=outcome,
            result_outcome=result_outcome,
            result_digest=result_digest,
            release_state=release_state,
        )
        if outcome != "succeeded" or result_outcome != "accepted":
            run = store.update_run(run.local_id, phase="milestone_blocked", verification_status="not_run")
    return store.get_run(run.local_id)


def _milestone_source_binding(
    profile: ProjectProfile,
    store: StateStore,
    run: RunRecord,
) -> JsonObject:
    plan = store.connection.execute(
        "SELECT relative_path, candidate_digest FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    expected = (
        plan["candidate_digest"]
        if plan is not None and plan["candidate_digest"] is not None
        else run.source_digest
    )
    try:
        packet_rows = store.connection.execute(
            "SELECT packet_json FROM packets WHERE run_local_id = ? ORDER BY created_at, task_id",
            (run.local_id,),
        ).fetchall()
        if not packet_rows:
            raise OrchestrateError(
                "No immutable packet source inventory is available for this milestone",
                code="packet_not_found",
            )
        candidate_inventories: list[frozenset[str]] = []
        for row in packet_rows:
            try:
                packet_value = json.loads(row["packet_json"])
            except json.JSONDecodeError as exc:
                raise OrchestrateError(
                    "A stored immutable Task packet is not valid JSON",
                    code="packet_identity_conflict",
                ) from exc
            sources = packet_value.get("sources") if isinstance(packet_value, Mapping) else None
            if not isinstance(sources, list):
                raise OrchestrateError(
                    "A stored immutable Task packet lost its source inventory",
                    code="packet_identity_conflict",
                )
            packet_paths: set[str] = set()
            for source in sources:
                path = source.get("path") if isinstance(source, Mapping) else None
                if not isinstance(path, str) or not path:
                    raise OrchestrateError(
                        "A stored immutable Task packet has a malformed source identity",
                        code="packet_identity_conflict",
                    )
                packet_paths.add(path)
            if packet_value.get("sourceDigest") == expected:
                candidate_inventories.append(frozenset(packet_paths))
        if not candidate_inventories or any(
            inventory != candidate_inventories[0]
            for inventory in candidate_inventories[1:]
        ):
            raise OrchestrateError(
                "No single immutable packet source inventory binds the selected milestone candidate",
                code="packet_identity_conflict",
            )
        bound_paths = set(candidate_inventories[0])
        if plan is not None:
            bound_paths.add(str(plan["relative_path"]))
        current = build_source_index(profile, extra_sources=bound_paths)
        candidate = current.value.get("candidate")
        coverage = candidate.get("coverageComplete") if isinstance(candidate, Mapping) else None
        return {
            "expected": expected,
            "current": current.digest,
            "unchanged": expected == current.digest if coverage is True else None,
            "coverageComplete": coverage,
        }
    except OrchestrateError as exc:
        return {
            "expected": expected,
            "current": None,
            "unchanged": None,
            "coverageComplete": None,
            "error": {"code": exc.code, "message": str(exc)},
        }


def _milestone_summary(
    store: StateStore,
    run: RunRecord,
    *,
    live: object = None,
    source_binding: Mapping[str, Any] | None = None,
) -> JsonObject:
    report = _run_summary(store, run, live=live)
    plan = store.connection.execute(
        "SELECT relative_path, plan_digest, candidate_digest, contract_digest, status FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    tasks = store.connection.execute(
        """SELECT b.task_key, b.task_id, w.dispatch_id, w.outcome, w.result_outcome, w.release_state
           FROM milestone_task_bindings b
           LEFT JOIN milestone_worker_bindings w
             ON w.run_local_id = b.run_local_id AND w.task_key = b.task_key
           WHERE b.run_local_id = ? ORDER BY b.created_at""",
        (run.local_id,),
    ).fetchall()
    gates = store.connection.execute(
        "SELECT task_key, task_id, gate_id, gate_kind, status, resolution FROM milestone_gate_bindings WHERE run_local_id = ? ORDER BY created_at",
        (run.local_id,),
    ).fetchall()
    task_reports: list[dict[str, Any]] = []
    for row in tasks:
        task_report = dict(row)
        task_report["admission"] = (
            joined_preflight_status(
                store,
                replace(run, task_id=row["task_id"], dispatch_id=row["dispatch_id"]),
            )
            if row["dispatch_id"] is not None
            else "pending"
        )
        task_reports.append(task_report)
    plan_report = dict(plan) if plan is not None else None
    report["milestone"] = {
        "plan": plan_report,
        "tasks": task_reports,
        "gates": [dict(row) for row in gates],
    }
    if source_binding is not None:
        report["sourceBinding"] = dict(source_binding)
    accepted_review_is_stale = (
        run.phase == "worker_succeeded"
        and run.verification_status == "review_accepted"
        and source_binding is not None
        and source_binding.get("unchanged") is not True
    )
    if accepted_review_is_stale:
        stored_plan_status = plan_report.get("status") if plan_report is not None else None
        if plan_report is not None:
            plan_report["status"] = "review_stale"
        if source_binding.get("coverageComplete") is False:
            reason = "candidate_coverage_incomplete"
        elif source_binding.get("current") is not None:
            reason = "candidate_digest_changed"
        else:
            reason = "source_binding_unavailable"
        report["status"] = "stale_review"
        report["verification"] = "review_stale"
        report["staleEvidence"] = {
            "kind": "independent_review",
            "reason": reason,
            "storedVerification": run.verification_status,
            "storedPlanStatus": stored_plan_status,
            "required": "a new independent review bound to the current candidate and settled contract",
        }
        report["nextObligation"] = (
            "accepted review evidence is stale; a new independent review of the current candidate is required"
        )
    elif run.phase == "worker_succeeded" and run.verification_status == "review_accepted":
        report["nextObligation"] = "independent review is accepted; external project acceptance remains unresolved"
    elif run.phase == "milestone_blocked":
        report["nextObligation"] = "planned verification or review did not produce accepted current evidence"
    else:
        report["nextObligation"] = "continue the bounded native milestone and its unresolved gates"
    return report


def _advance_milestone(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    profile: ProjectProfile,
    *,
    worktree_id: str | None,
) -> tuple[RunRecord, MilestonePlan]:
    loaded, sources, reader = _reload_milestone_plan(profile, store, run)
    plan_row = store.connection.execute(
        "SELECT * FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    if plan_row["candidate_digest"] is None:
        if run.phase != "worker_succeeded" or run.worker_outcome != "succeeded":
            raise OrchestrateError("The integration owner has not settled successfully", code="integration_owner_unsettled")
        now = utc_now()
        store.connection.execute(
            """UPDATE milestone_plan_bindings
               SET candidate_digest = ?, status = 'active', updated_at = ?
               WHERE run_local_id = ? AND candidate_digest IS NULL""",
            (sources.digest, now, run.local_id),
        )
        run = store.update_run(
            run.local_id,
            phase="milestone_preparing",
            verification_status="pending",
        )
    runtime_plan, packets = _runtime_milestone_plan(profile, store, run, loaded, sources, reader)
    if run.phase == "milestone_blocked":
        return run, runtime_plan
    owner = next(task for task in runtime_plan.tasks if task.integration_owner)
    if not run.task_id:
        raise OrchestrateError("Integration owner lost its native Task identity", code="integration_owner_invalid")
    scheduler = NativeDagScheduler(client, store, run.local_id)
    bindings = scheduler.create_followups(
        str(run.native_run_id),
        runtime_plan,
        integration_owner=NativeTaskBinding(owner.key, run.task_id, (), "first-increment-owner"),
    )
    for key, packet_json in packets.items():
        store.save_packet(run.local_id, bindings[key].task_id, packet_json)
    gates = scheduler.create_gates(runtime_plan, bindings)
    for task in runtime_plan.tasks:
        gate = gates.get(task.key)
        if gate is None or gate.status == "resolved":
            continue
        if task.gate == "verification":
            gates[task.key] = scheduler.resolve_gate(gate, resolution="accepted")
            continue
        verification_dependencies = [
            dependency
            for dependency in task.dependencies
            if next(item for item in runtime_plan.tasks if item.key == dependency).gate == "verification"
        ]
        accepted = all(
            (
                row := store.connection.execute(
                    "SELECT outcome, result_outcome FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = ?",
                    (run.local_id, dependency),
                ).fetchone()
            ) is not None
            and row["outcome"] == "succeeded"
            and row["result_outcome"] == "accepted"
            for dependency in verification_dependencies
        )
        if accepted:
            gates[task.key] = scheduler.resolve_gate(gate, resolution="accepted")

    workers = store.connection.execute(
        "SELECT * FROM milestone_worker_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchall()
    reviewer = next(task for task in runtime_plan.tasks if task.role == "reviewer")
    review_worker = next((row for row in workers if row["task_key"] == reviewer.key), None)
    if review_worker is not None and review_worker["outcome"] is not None:
        evidence = ReviewEvidence(
            review_worker["task_id"],
            runtime_plan.candidate_digest,
            runtime_plan.contract.digest,
            review_worker["result_outcome"] if review_worker["result_outcome"] in {"accepted", "rejected"} else "rejected",
        )
        if evidence.task_id != bindings[reviewer.key].task_id or review_worker["outcome"] != "succeeded":
            raise OrchestrateError("Review result does not bind the exact settled planned Task", code="review_gate_invalid")
        require_current_review(
            evidence,
            candidate_digest=runtime_plan.candidate_digest,
            contract_digest=runtime_plan.contract.digest,
            task_id=bindings[reviewer.key].task_id,
            settled=True,
        )
        run = store.update_run(run.local_id, phase="worker_succeeded", verification_status="review_accepted")
        store.connection.execute(
            "UPDATE milestone_plan_bindings SET status = 'review_accepted', updated_at = ? WHERE run_local_id = ?",
            (utc_now(), run.local_id),
        )
        return run, runtime_plan

    active = {row["task_key"] for row in workers if row["outcome"] is None}
    finished = {owner.key, *(row["task_key"] for row in workers if row["outcome"] is not None)}
    gate_rows = _milestone_gate_rows(store, run)
    roster = load_role_roster()
    remaining_capacity = runtime_plan.max_workers - len(active)
    for binding in scheduler.ready_wave(
        str(run.native_run_id),
        runtime_plan,
        bindings,
        remaining_capacity=remaining_capacity,
    ):
        task = next(item for item in runtime_plan.tasks if item.key == binding.key)
        if task.key in active or task.key in finished:
            continue
        gate = gate_rows.get(task.key)
        if task.gate in {"verification", "review"} and (
            gate is None or gate.status != "resolved" or gate.resolution != "accepted"
        ):
            raise OrchestrateError("Native ready view bypassed an unresolved planned gate", code="native_ready_gate_mismatch")
        if len(active) >= runtime_plan.max_workers:
            break
        _start_milestone_worker(
            client,
            store,
            run,
            profile,
            task,
            binding,
            packets[task.key],
            roster.choice(task.role),
            worktree_id=worktree_id,
        )
        active.add(task.key)
    run = store.update_run(run.local_id, phase="milestone_waiting", verification_status="pending")
    return run, runtime_plan


def _supervise_milestone(
    client: OrcaClient,
    store: StateStore,
    run: RunRecord,
    profile: ProjectProfile,
    *,
    worktree_id: str | None,
    wait_timeout_ms: int,
) -> JsonObject:
    if run.delivery_id:
        loaded, sources, reader = _reload_milestone_plan(profile, store, run)
        plan, _ = _runtime_milestone_plan(profile, store, run, loaded, sources, reader)
        messages = store.delivery_messages(run.local_id, run.delivery_id)
        if not messages:
            raise OrchestrateError(
                "The bound milestone Delivery is missing its immutable journal",
                code="delivery_journal_missing",
            )
        run = _process_milestone_delivery(client, store, run, plan, run.delivery_id, messages)
        if not store.pending_questions(run.local_id):
            _ack_if_resolved(client, store, run)
            run = store.get_run(run.local_id)
        if store.pending_questions(run.local_id) or run.phase == "milestone_blocked":
            return _milestone_summary(store, run)
    current, plan = _advance_milestone(client, store, run, profile, worktree_id=worktree_id)
    deadline = time.monotonic() + (wait_timeout_ms / 1000)
    while current.phase == "milestone_waiting":
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
        with store.admission_effect_fence(current.local_id):
            delivery_id, messages = _delivery(payload)
            if not delivery_id:
                continue
            store.journal_delivery(current.local_id, delivery_id, payload, messages)
            current = store.update_run(current.local_id, delivery_id=delivery_id)
            current = _process_milestone_delivery(client, store, current, plan, delivery_id, messages)
            if store.pending_questions(current.local_id):
                break
            _ack_if_resolved(client, store, current)
            current = store.get_run(current.local_id)
            if current.phase == "milestone_blocked":
                break
        current, plan = _advance_milestone(client, store, current, profile, worktree_id=worktree_id)
    return _milestone_summary(store, store.get_run(current.local_id))


def implement(
    root: Path,
    objective: str | None,
    *,
    client: OrcaClient,
    milestone_plan: str | None = None,
    wait_timeout_ms: int = 300_000,
    require_context: bool = True,
) -> JsonObject:
    profile = ProjectProfile.load(root)
    if objective is None:
        return resume(
            profile.root,
            None,
            client=client,
            milestone_plan=milestone_plan,
            wait_timeout_ms=wait_timeout_ms,
            require_context=require_context,
        )
    with StateStore(profile.root) as store:
        normalized = objective.strip()
        if not normalized:
            raise OrchestrateError("Objective must not be empty", code="objective_empty")
        if len(normalized) > 8000:
            raise OrchestrateError("Objective exceeds the bounded 8000-character limit", code="objective_too_large")
        with store.lock("project-create"):
            active = store.active_runs()
            for existing in active:
                held, _ = _join_admission(store, existing)
                if held.phase == PREFLIGHT_HELD_PHASE:
                    return _run_summary(store, held)
            if active:
                raise OrchestrateError(
                    "An active local Run already exists; resume it before starting another objective",
                    code="active_run_exists",
                )
            _require_profile_selection(profile)
            identity = require_plain_controller(client, profile.root) if require_context else None
            reader = read_project(profile, normalized)
            selected_plan_path = (
                milestone_plan_relative_path(profile.root, milestone_plan)
                if milestone_plan is not None
                else None
            )
            sources = _run_source_index(profile, reader, milestone_path=selected_plan_path)
            _require_candidate_coverage(sources)
            loaded_plan = (
                load_milestone_plan(
                    profile.root,
                    selected_plan_path,
                    objective=normalized,
                    candidate_digest=sources.digest,
                    sources=sources,
                )
                if selected_plan_path is not None
                else None
            )
            run = store.create_run(objective=normalized, profile_digest=profile.digest, source_digest=sources.digest)
            if loaded_plan is not None:
                _record_milestone_plan(store, run, loaded_plan)
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
                report = _supervise(client, store, run, wait_timeout_ms=wait_timeout_ms)
                current = store.get_run(run.local_id)
                if loaded_plan is not None and current.phase == "worker_succeeded" and current.delivery_id is None:
                    return _supervise_milestone(
                        client,
                        store,
                        current,
                        profile,
                        worktree_id=identity.worktree_id if identity else None,
                        wait_timeout_ms=wait_timeout_ms,
                    )
                return _milestone_summary(store, current) if loaded_plan is not None else report


def resume(
    root: Path,
    run_id: str | None,
    *,
    client: OrcaClient,
    milestone_plan: str | None = None,
    wait_timeout_ms: int = 300_000,
    require_context: bool = True,
) -> JsonObject:
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.select_run(run_id)
        with store.lock(run.local_id):
            bound_plan_path = _milestone_plan_path(store, run)
            if milestone_plan is not None:
                requested_plan_path = milestone_plan_relative_path(profile.root, milestone_plan)
                if bound_plan_path is None or requested_plan_path != bound_plan_path:
                    raise OrchestrateError(
                        "Resume plan does not match the Run's immutable selected plan",
                        code="milestone_plan_changed",
                    )
            if bound_plan_path is not None and (
                run.phase.startswith("milestone_")
                or (run.phase == "worker_succeeded" and run.delivery_id is None)
            ):
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
                run = _bind_native_run(client, store, run, identity)
                return _supervise_milestone(
                    client,
                    store,
                    run,
                    profile,
                    worktree_id=identity.worktree_id if identity else None,
                    wait_timeout_ms=wait_timeout_ms,
                )
            run, _ = _join_admission(store, run)
            if run.phase == PREFLIGHT_HELD_PHASE:
                return _run_summary(store, run)
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
            unsettled = store.unsettled_intentions(run.local_id)
            prepared_start = any(
                row["operation"] == "worker-start" and row["status"] == "prepared"
                for row in unsettled
            )
            recovering_worker_start = any(row["operation"] == "worker-start" for row in unsettled)
            if prepared_start:
                _require_profile_selection(profile)
                validate_packet_sources(profile, run, _ensure_packet(store, run, profile))
            if run.dispatch_id and not recovering_worker_start:
                # Retry/replay can itself issue native effects or apply a stored
                # reply/ack locally. Keep that complete interval ordered against
                # every later managed preflight observation.
                with store.admission_effect_fence(run.local_id):
                    run, status = _join_admission(store, store.get_run(run.local_id))
                    if status in {"rejected", "conflicting"}:
                        return _run_summary(store, run)
                    run = reconcile_intentions(client, store, run)
                    run, status = _join_admission(store, run)
                    if status in {"rejected", "conflicting"}:
                        return _run_summary(store, run)
            else:
                # worker-start recovery must not hold the fence: the public Orca
                # call may be waiting for this exact worker's first preflight.
                run = reconcile_intentions(client, store, run)
                run, _ = _join_admission(store, run)
                if run.phase == PREFLIGHT_HELD_PHASE:
                    return _run_summary(store, run)
            if run.phase in {"preparing", "run_created", "task_created"}:
                _require_profile_selection(profile)
                reader = read_project(profile, run.objective)
                current_sources = _run_source_index(
                    profile,
                    reader,
                    milestone_path=bound_plan_path,
                )
                _require_candidate_coverage(current_sources)
                if current_sources.digest != run.source_digest:
                    raise OrchestrateError(
                        "Bound project sources changed; resume will not launch or repeat external effects",
                        code="source_binding_changed",
                        data={"expected": run.source_digest, "actual": current_sources.digest},
                    )
            if run.dispatch_id:
                # Native binding, Delivery replay/effects, and acknowledgement
                # form one admission-ordered interval. A conflicting preflight
                # is either visible at this join or cannot be inserted until all
                # of these effects have ended.
                with store.admission_effect_fence(run.local_id):
                    run, status = _join_admission(store, store.get_run(run.local_id))
                    if status in {"rejected", "conflicting"}:
                        return _run_summary(store, run)
                    run = _bind_native_run(client, store, run, identity)
                    run = _finish_prompt_stall_cleanup(client, store, run)
                    if run.delivery_id:
                        run = _reprocess_bound_delivery(client, store, run)
                    if run.delivery_id and not store.pending_questions(run.local_id) and run.phase != "blocked":
                        _ack_if_resolved(client, store, run)
                        run = store.get_run(run.local_id)
                    if store.pending_questions(run.local_id) or run.phase in TERMINAL_PHASES:
                        if (
                            bound_plan_path is not None
                            and run.phase == "worker_succeeded"
                            and run.delivery_id is None
                            and not store.pending_questions(run.local_id)
                        ):
                            return _supervise_milestone(
                                client,
                                store,
                                run,
                                profile,
                                worktree_id=identity.worktree_id if identity else None,
                                wait_timeout_ms=wait_timeout_ms,
                            )
                        return _milestone_summary(store, run) if bound_plan_path is not None else _run_summary(store, run)
            else:
                run = _bind_native_run(client, store, run, identity)
                run = _finish_prompt_stall_cleanup(client, store, run)
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
            report = _supervise(client, store, run, wait_timeout_ms=wait_timeout_ms)
            current = store.get_run(run.local_id)
            if bound_plan_path is not None and current.phase == "worker_succeeded" and current.delivery_id is None:
                return _supervise_milestone(
                    client,
                    store,
                    current,
                    profile,
                    worktree_id=identity.worktree_id if identity else None,
                    wait_timeout_ms=wait_timeout_ms,
                )
            return _milestone_summary(store, current) if bound_plan_path is not None else report


def status(root: Path, run_id: str | None, *, client: OrcaClient) -> JsonObject:
    profile = ProjectProfile.load(root, require_sources=False)
    with StateStore(profile.root) as store:
        run = store.select_for_read(run_id)
        live: object
        if not run.native_run_id:
            live = {"status": "not-created"}
        else:
            try:
                milestone_dispatches = [
                    row["dispatch_id"]
                    for row in store.connection.execute(
                        "SELECT dispatch_id FROM milestone_worker_bindings WHERE run_local_id = ? ORDER BY created_at",
                        (run.local_id,),
                    ).fetchall()
                ]
                live = {
                    "run": client.run_json("orchestration", "run-show", "--id", run.native_run_id, "--json"),
                    "tasks": client.run_json("orchestration", "task-list", "--run", run.native_run_id, "--json"),
                    "worker": client.run_json("orchestration", "worker-show", "--dispatch", run.dispatch_id, "--json") if run.dispatch_id else None,
                    "milestoneWorkers": [
                        client.run_json("orchestration", "worker-show", "--dispatch", dispatch_id, "--json")
                        for dispatch_id in milestone_dispatches
                    ],
                }
            except OrcaCommandError as exc:
                live = {"status": "unavailable", "code": exc.code, "detail": str(exc)}
        if _milestone_plan_path(store, run) is not None:
            source_binding = _milestone_source_binding(profile, store, run)
            return _milestone_summary(store, run, live=live, source_binding=source_binding)
        return _run_summary(store, run, live=live)


def explain(root: Path, run_id: str | None) -> JsonObject:
    profile = ProjectProfile.load(root, require_sources=False)
    with StateStore(profile.root) as store:
        run = store.select_for_read(run_id)
        milestone_path = _milestone_plan_path(store, run)
        if milestone_path is not None:
            source_binding = _milestone_source_binding(profile, store, run)
            report = _milestone_summary(store, run, source_binding=source_binding)
            report["explanationSource"] = "host-local records and exact source identities; no model call"
            return report
        reader = read_project(profile, run.objective)
        current = _run_source_index(
            profile,
            reader,
            milestone_path=None,
        )
        expected_source_digest = run.source_digest
        report = _run_summary(store, run)
        report["sourceBinding"] = {
            "expected": expected_source_digest,
            "current": current.digest,
            "unchanged": (
                expected_source_digest == current.digest
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
        with store.lock(run.local_id):
            with store.admission_effect_fence(run.local_id):
                run, admission = _join_admission(store, store.get_run(run.local_id))
                if admission != "admitted":
                    return _run_summary(store, run)
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
                run, admission = _join_admission(store, store.get_run(run.local_id))
                if admission != "admitted":
                    return _run_summary(store, run)
                run = reconcile_intentions(client, store, run)
                run, admission = _join_admission(store, run)
                if admission != "admitted":
                    return _run_summary(store, run)
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
