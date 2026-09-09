"""Deterministic one-worker controller over public Orca commands."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from pathlib import Path
import time
from typing import Any

from .config import load_owner_model
from .errors import OrchestrateError
from .orca import JsonObject, OrcaClient, OrcaCommandError
from .packets import make_packet, packet_spec
from .profile import ProjectProfile
from .readers import read_project
from .sources import SourceIndex, build_source_index
from .state import RunRecord, StateStore


REPORT_SCHEMA = "orchestrate-report/v1"
TERMINAL_PHASES = {"worker_succeeded", "worker_failed", "blocked", "completed"}


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


def require_plain_controller(environment: Mapping[str, str] | None = None) -> None:
    env = os.environ if environment is None else environment
    if not env.get("ORCA_TERMINAL_HANDLE"):
        raise OrchestrateError(
            "No owned Orca terminal is present; use the bootstrap command",
            code="controller_terminal_missing",
        )
    agent_markers = (
        "ORCA_AGENT_HOOK_TOKEN",
        "ORCA_AGENT_HOOK_ENDPOINT",
        "ORCA_AGENT_LAUNCH_TOKEN",
    )
    if any(env.get(key) for key in agent_markers):
        raise OrchestrateError(
            "A reasoning-agent terminal cannot act as the persistent controller; use a dedicated ordinary Orca terminal",
            code="agent_terminal_not_controller",
        )


def _verify_workspace(client: OrcaClient, root: Path) -> None:
    payload = client.run_json("worktree", "current", "--json")
    worktree = _result(payload).get("worktree")
    if not isinstance(worktree, Mapping) or not isinstance(worktree.get("path"), str):
        raise OrchestrateError("worktree current omitted the exact path", code="orca_contract_error")
    if Path(worktree["path"]).resolve() != root.resolve():
        raise OrchestrateError(
            "The ordinary controller terminal is not in the exact project worktree",
            code="controller_worktree_mismatch",
        )


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
        if isinstance(data.get("requestId"), str):
            return data["requestId"]
    return None


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
    store.mark_intention(
        intention_id,
        "applied",
        request_id=_request_id(response),
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
        identity = _entity_id(response, "dispatch")
        if run.dispatch_id not in {None, identity}:
            raise OrchestrateError("Dispatch receipt conflicts with the local binding", code="intention_binding_mismatch")
        fields = {"dispatch_id": identity}
        if run.phase == "task_created":
            fields["phase"] = "waiting"
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


def _request_state(payload: Mapping[str, Any]) -> str | None:
    result = _result(payload)
    for key in ("status", "state"):
        if isinstance(result.get(key), str):
            return result[key]
    request = result.get("request")
    if isinstance(request, Mapping):
        for key in ("status", "state"):
            if isinstance(request.get(key), str):
                return request[key]
    return None


def reconcile_intentions(client: OrcaClient, store: StateStore, run: RunRecord) -> RunRecord:
    current = _replay_applied_intentions(store, run)
    for row in store.unsettled_intentions(run.local_id):
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
            store.mark_intention(
                row["id"],
                "applied",
                request_id=_request_id(replay),
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
        receipt = client.run_json("orchestration", "request-show", "--request", request_id, "--json")
        state = _request_state(receipt)
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
        arguments = json.loads(row["arguments_json"])
        replay = client.run_json(*arguments, "--retry-request", request_id, "--json")
        store.mark_intention(row["id"], "applied", request_id=request_id, response=replay)
        current = _apply_intention(store, current, row["operation"], replay)
    return _replay_applied_intentions(store, current)


def _run_summary(store: StateStore, run: RunRecord, *, live: object = None) -> JsonObject:
    pending = store.pending_questions(run.local_id)
    if pending:
        next_obligation = f"answer question {pending[0]['message_id']}"
    elif run.phase == "worker_succeeded" and run.verification_status == "pending":
        next_obligation = "independent verification and project acceptance remain unresolved"
    elif run.phase == "waiting":
        next_obligation = "resume foreground supervision"
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
    return _apply_intention(store, run, "run-create", response)


def _require_candidate_coverage(sources: SourceIndex) -> None:
    candidate = sources.value.get("candidate")
    if not isinstance(candidate, Mapping) or candidate.get("coverageComplete") is not True:
        uncovered = candidate.get("uncoveredChanges", []) if isinstance(candidate, Mapping) else []
        raise OrchestrateError(
            "Dirty candidate coverage is incomplete; add only approved relevant paths to candidateSources",
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
    draft = make_packet(
        objective=run.objective,
        profile=profile,
        sources=sources,
        run_id=run.native_run_id,
        reader=reader,
    )
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
            run.objective[:120],
            "--spec",
            packet_spec(draft),
        ],
    )
    current = _apply_intention(store, run, "task-create", response)
    packet = make_packet(
        objective=run.objective,
        profile=profile,
        sources=sources,
        run_id=current.native_run_id,
        task_id=current.task_id,
        reader=reader,
    )
    store.save_packet(current.local_id, str(current.task_id), packet)
    return current


def _ensure_packet(store: StateStore, run: RunRecord, profile: ProjectProfile) -> None:
    try:
        store.get_packet(run.local_id, str(run.task_id))
        return
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
    recovered = make_packet(
        objective=run.objective,
        profile=profile,
        sources=sources,
        run_id=run.native_run_id,
        task_id=run.task_id,
        reader=reader,
    )
    store.save_packet(run.local_id, str(run.task_id), recovered)


def _start_worker(client: OrcaClient, store: StateStore, run: RunRecord, profile: ProjectProfile) -> RunRecord:
    choice = load_owner_model()
    response = _mutation(
        client,
        store,
        run,
        "worker-start",
        [
            "orchestration",
            "worker-start",
            "--run",
            str(run.native_run_id),
            "--task",
            str(run.task_id),
            "--worktree",
            f"path:{profile.root.resolve()}",
            "--agent",
            choice.agent,
            "--model",
            choice.model,
            "--effort",
            choice.effort,
            "--timeout-ms",
            "60000",
        ],
        timeout_seconds=90,
    )
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
        """SELECT arguments_json FROM intentions
           WHERE run_local_id = ? AND operation = 'worker-release' AND status = 'applied'""",
        (run.local_id,),
    ).fetchall()
    released = False
    for row in rows:
        arguments = json.loads(row["arguments_json"])
        if isinstance(arguments, list) and "--dispatch" in arguments:
            index = arguments.index("--dispatch")
            released = index + 1 < len(arguments) and arguments[index + 1] == run.dispatch_id
        if released:
            break
    if not released:
        _mutation(
            client,
            store,
            run,
            "worker-release",
            ["orchestration", "worker-release", "--dispatch", str(run.dispatch_id)],
        )
    worker = client.run_json("orchestration", "worker-show", "--dispatch", str(run.dispatch_id), "--json")
    result = _result(worker)
    resource = result.get("terminalResource")
    if not isinstance(resource, Mapping):
        raise OrchestrateError("worker-show omitted terminal release state", code="release_unconfirmed")
    state = resource.get("releaseState")
    if state in {"released", "already_released"}:
        return
    encoded = json.dumps(resource, sort_keys=True).lower()
    if state == "retained" and ("external_terminal" in encoded or "no_owned_resource" in encoded):
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
        _validate_native_settlement(client, current, outcome)
        store.add_evidence(
            current.local_id,
            kind="worker-claim",
            status=outcome,
            subject=str(message.get("subject", "worker_done")),
            payload=message,
        )
        _release_disposition(client, store, current)
        current = store.update_run(
            current.local_id,
            worker_outcome=outcome,
            phase="worker_succeeded" if outcome == "succeeded" else "worker_failed",
            verification_status="pending" if outcome == "succeeded" else "not_run",
        )
        store.mark_message(current.local_id, delivery_id, message_id, "processed")
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
    store.update_run(run.local_id, delivery_id=None)
    return True


def _supervise(client: OrcaClient, store: StateStore, run: RunRecord, *, wait_timeout_ms: int) -> JsonObject:
    deadline = time.monotonic() + (wait_timeout_ms / 1000)
    current = run
    while current.phase == "waiting":
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
            continue
        store.journal_delivery(current.local_id, delivery_id, payload, messages)
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
    if require_context:
        require_plain_controller()
        _verify_workspace(client, root)
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        if objective is None:
            return resume(profile.root, None, client=client, wait_timeout_ms=wait_timeout_ms, require_context=False)
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
                _ensure_packet(store, run, profile)
                run = _start_worker(client, store, run, profile)
                return _supervise(client, store, run, wait_timeout_ms=wait_timeout_ms)


def resume(
    root: Path,
    run_id: str | None,
    *,
    client: OrcaClient,
    wait_timeout_ms: int = 300_000,
    require_context: bool = True,
) -> JsonObject:
    if require_context:
        require_plain_controller()
        _verify_workspace(client, root)
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.select_run(run_id)
        with store.lock(run.local_id):
            run = reconcile_intentions(client, store, run)
            if run.native_run_id:
                _mutation(
                    client,
                    store,
                    run,
                    "run-use",
                    ["orchestration", "run-use", "--id", run.native_run_id],
                )
            reader = read_project(profile, run.objective)
            current_sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
            if run.phase in {"preparing", "run_created", "task_created"}:
                _require_candidate_coverage(current_sources)
            if (
                current_sources.digest != run.source_digest
                and run.phase not in TERMINAL_PHASES
                and run.phase != "waiting"
            ):
                raise OrchestrateError(
                    "Bound project sources changed; resume will not launch or repeat external effects",
                    code="source_binding_changed",
                    data={"expected": run.source_digest, "actual": current_sources.digest},
                )
            if run.delivery_id and not store.pending_questions(run.local_id):
                _ack_if_resolved(client, store, run)
                run = store.get_run(run.local_id)
            if store.pending_questions(run.local_id) or run.phase in TERMINAL_PHASES:
                return _run_summary(store, run)
            if run.phase == "preparing":
                run = _create_native_run(client, store, run)
            if run.phase == "run_created":
                run = _create_task_and_packet(client, store, run, profile)
            if run.phase == "task_created":
                _ensure_packet(store, run, profile)
                run = _start_worker(client, store, run, profile)
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
    if require_context:
        require_plain_controller()
        _verify_workspace(client, root)
    profile = ProjectProfile.load(root)
    with StateStore(profile.root) as store:
        run = store.get_run(run_id)
        with store.lock(run.local_id):
            run = reconcile_intentions(client, store, run)
            if run.native_run_id:
                _mutation(
                    client,
                    store,
                    run,
                    "run-use",
                    ["orchestration", "run-use", "--id", run.native_run_id],
                )
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
