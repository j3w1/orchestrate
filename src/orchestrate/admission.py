"""Exact worker preflight and durable managed-attempt admission evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

from .errors import OrchestrateError
from .orca import JsonObject, OrcaClient, OrcaCommandError
from .packets import (
    PACKET_SCHEMA,
    PREFLIGHT_SCHEMA,
    canonical_packet_json,
    expected_packet_id,
    packet_spec_from_json,
)
from .profile import ProjectProfile
from .readers import CE_QUERY_SCRIPT, ReaderResult, read_project
from .safeio import approved_project_path, read_project_bytes
from .sources import SourceIndex, build_source_index
from .state import RunRecord, StateStore, utc_now


@dataclass(frozen=True, slots=True)
class PacketValidation:
    packet: dict[str, Any]
    reader: ReaderResult
    sources: SourceIndex
    routing_digest: str
    packet_json_sha256: str


def _digest(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


def _runtime_id(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("_meta")
    runtime_id = metadata.get("runtimeId") if isinstance(metadata, Mapping) else None
    if not isinstance(runtime_id, str) or not runtime_id:
        raise OrchestrateError("Orca readback omitted its runtime identity", code="preflight_identity_conflict")
    return runtime_id


def _operational_profile(profile: ProjectProfile) -> dict[str, Any]:
    return {
        "selectedDigest": profile.digest,
        "candidateDigest": profile.candidate_digest,
        "selectionSource": profile.selection_source,
        "selectionHistoryDigest": profile.selection_history_digest,
        "candidateChanged": profile.candidate_changed,
    }


def _verify_packet_source_bytes(
    profile: ProjectProfile,
    packet: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Compare packet-bound profile/source bytes before invoking any subprocess."""

    records = packet.get("sources")
    if (
        packet.get("profileDigest") != profile.digest
        or packet.get("operationalProfile") != _operational_profile(profile)
        or not isinstance(records, list)
    ):
        raise OrchestrateError("Bound operational profile changed", code="source_binding_changed")
    identities: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise OrchestrateError("Worker packet source identity is malformed", code="packet_identity_conflict")
        path = record.get("path")
        expected_sha = record.get("sha256")
        expected_bytes = record.get("bytes")
        if (
            not isinstance(path, str)
            or not path
            or path in identities
            or (expected_sha is not None and not isinstance(expected_sha, str))
            or type(expected_bytes) is not int
            or expected_bytes < 0
        ):
            raise OrchestrateError("Worker packet source identity is malformed", code="packet_identity_conflict")
        candidate = approved_project_path(profile.root, path, require_file=False)
        if expected_sha is None:
            if candidate.exists() or expected_bytes != 0:
                raise OrchestrateError("Bound project source bytes changed", code="source_binding_changed")
            identity = {"sha256": None, "bytes": 0}
        else:
            try:
                raw = read_project_bytes(profile.root, path)
            except OrchestrateError as exc:
                raise OrchestrateError("Bound project source bytes changed", code="source_binding_changed") from exc
            identity = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
            if identity != {"sha256": expected_sha, "bytes": expected_bytes}:
                raise OrchestrateError("Bound project source bytes changed", code="source_binding_changed")
        identities[path] = identity
    return identities


def _verify_source_index(
    profile: ProjectProfile,
    packet: Mapping[str, Any],
    sources: SourceIndex,
) -> None:
    candidate = sources.value.get("candidate")
    if not isinstance(candidate, Mapping) or candidate.get("coverageComplete") is not True:
        raise OrchestrateError(
            "Dirty candidate coverage is incomplete",
            code="candidate_coverage_incomplete",
            data={"uncoveredChanges": candidate.get("uncoveredChanges", []) if isinstance(candidate, Mapping) else []},
        )
    if (
        profile.candidate_changed
        or packet.get("profileDigest") != profile.digest
        or packet.get("sourceDigest") != sources.digest
        or packet.get("operationalProfile") != sources.value.get("operationalProfile")
        or packet.get("candidate") != sources.value.get("candidate")
        or packet.get("sources") != sources.value.get("sources")
    ):
        raise OrchestrateError("Bound project sources or routing changed", code="source_binding_changed")


def validate_packet_sources(
    profile: ProjectProfile,
    run: RunRecord,
    packet_json: str,
) -> PacketValidation:
    """Rebuild every bound source/routing interpretation and compare it to v3."""

    try:
        packet = json.loads(packet_json)
    except json.JSONDecodeError as exc:
        raise OrchestrateError("Stored worker packet is invalid JSON", code="packet_identity_conflict") from exc
    if not isinstance(packet, dict) or packet.get("schema") != PACKET_SCHEMA:
        raise OrchestrateError("A legacy packet cannot grant a new worker admission", code="preflight_packet_unsupported")
    if canonical_packet_json(packet) != packet_json or packet.get("packetId") != expected_packet_id(packet):
        raise OrchestrateError("Worker packet canonical identity is invalid", code="packet_identity_conflict")
    admission = packet.get("admission")
    if not isinstance(admission, Mapping) or admission.get("schema") != PREFLIGHT_SCHEMA:
        raise OrchestrateError("Worker packet omits the mandatory preflight contract", code="preflight_packet_unsupported")
    native_packet = packet.get("native")
    if (
        packet.get("objective") != run.objective
        or not isinstance(native_packet, Mapping)
        or native_packet.get("runId") != run.native_run_id
    ):
        raise OrchestrateError("Worker packet does not bind the selected native Run", code="packet_identity_conflict")
    identities = _verify_packet_source_bytes(profile, packet)
    pre_sources = build_source_index(profile, extra_sources=set(identities))
    _verify_source_index(profile, packet, pre_sources)
    packet_reader = packet.get("reader")
    if (
        not isinstance(packet_reader, Mapping)
        or packet_reader.get("kind") != profile.value["reader"]["kind"]
    ):
        raise OrchestrateError("Worker packet reader identity is malformed", code="packet_identity_conflict")
    query_paths = {"package.json", CE_QUERY_SCRIPT, "docs/project-log/manifest.json"}
    expected_query_sources = (
        {path: identities[path] for path in query_paths}
        if packet_reader.get("kind") == "ce-gd" and query_paths.issubset(identities)
        else None
    )
    if packet_reader.get("kind") == "ce-gd" and expected_query_sources is None:
        raise OrchestrateError("Worker packet omits the CE query source identity", code="packet_identity_conflict")
    reader = read_project(
        profile,
        run.objective,
        expected_ce_query_sources=expected_query_sources,
    )
    sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
    _verify_source_index(profile, packet, sources)
    actual_reader = {"kind": reader.kind, "routing": reader.routing}
    if (
        packet.get("reader") != actual_reader
    ):
        raise OrchestrateError("Bound project sources or routing changed", code="source_binding_changed")
    return PacketValidation(
        packet,
        reader,
        sources,
        _digest(actual_reader),
        hashlib.sha256(packet_json.encode("utf-8")).hexdigest(),
    )


def _native_identity(
    client: OrcaClient,
    *,
    profile: ProjectProfile,
    run: RunRecord,
    task_id: str,
    dispatch_id: str,
    packet_json: str,
    packet: Mapping[str, Any],
    environment: Mapping[str, str],
    platform: str,
) -> dict[str, Any]:
    handle = environment.get("ORCA_TERMINAL_HANDLE", "").strip()
    if not handle:
        raise OrchestrateError("Worker preflight requires its invoking Orca terminal", code="preflight_terminal_missing")
    shown = client.run_json("terminal", "show", "--terminal", handle, "--json")
    current = client.run_json("worktree", "current", "--json")
    worker_payload = client.run_json("orchestration", "worker-show", "--dispatch", dispatch_id, "--json")
    tasks_payload = client.run_json("orchestration", "task-list", "--run", str(run.native_run_id), "--json")
    runtime_ids = {_runtime_id(item) for item in (shown, current, worker_payload, tasks_payload)}
    if len(runtime_ids) != 1:
        raise OrchestrateError("Preflight readbacks crossed Orca runtime identities", code="preflight_identity_conflict")

    terminal = _result(shown).get("terminal")
    worktree = _result(current).get("worktree")
    worker_result = _result(worker_payload)
    dispatch = worker_result.get("dispatch")
    worker = worker_result.get("worker")
    tasks = _result(tasks_payload).get("tasks")
    admission = packet.get("admission")
    launch = admission.get("launch") if isinstance(admission, Mapping) else None
    command = admission.get("commandTemplate") if isinstance(admission, Mapping) else None
    if not isinstance(launch, Mapping):
        raise OrchestrateError("Worker packet omits its exact launch identity", code="preflight_packet_unsupported")
    expected_python = os.path.normcase(os.path.abspath(sys.executable))
    command_python = (
        os.path.normcase(os.path.abspath(command[0]))
        if isinstance(command, list) and command and isinstance(command[0], str)
        else None
    )
    expected_command = [
        command[0] if command_python == expected_python else sys.executable,
        "-I",
        "-m",
        "orchestrate",
        "worker-preflight",
        "--project",
        ".",
        "--run",
        "{injectedRunId}",
        "--task",
        "{injectedTaskId}",
        "--dispatch",
        "{injectedDispatchId}",
        "--packet-id",
        "{packetId}",
        "--json",
    ]
    if platform != "win32" or command_python != expected_python or command != expected_command:
        raise OrchestrateError("Worker preflight supports only its exact controller Python on native Windows", code="preflight_host_unsupported")
    if not isinstance(terminal, Mapping) or (
        terminal.get("handle") != handle
        or terminal.get("connected") is not True
        or terminal.get("writable") is not True
        or terminal.get("hostPlatform") != "win32"
        or terminal.get("agentIdentity") != launch.get("agent")
    ):
        raise OrchestrateError("terminal show did not prove the exact worker actor", code="preflight_identity_conflict")
    root = terminal.get("worktreePath")
    worktree_id = terminal.get("worktreeId")
    if (
        not isinstance(root, str)
        or Path(root).resolve() != profile.root.resolve()
        or not isinstance(worktree_id, str)
        or not isinstance(worktree, Mapping)
        or worktree.get("id") != worktree_id
        or not isinstance(worktree.get("path"), str)
        or Path(worktree["path"]).resolve() != profile.root.resolve()
    ):
        raise OrchestrateError("Worker preflight is in a different workspace", code="worker_workspace_mismatch")
    options = worker.get("startOptions") if isinstance(worker, Mapping) else None
    option_launch = options.get("launch") if isinstance(options, Mapping) else None
    if (
        not isinstance(dispatch, Mapping)
        or dispatch.get("id") != dispatch_id
        or dispatch.get("run_id") != run.native_run_id
        or dispatch.get("task_id") != task_id
        or dispatch.get("status") != "dispatched"
        or not isinstance(worker, Mapping)
        or worker.get("state") != "ready"
        or worker.get("stage") != "input_accepted"
        or worker.get("worktree_id") != worktree_id
        or worker.get("agent_terminal_handle") != handle
        or not isinstance(options, Mapping)
        or options.get("resolvedWorktreeId") != worktree_id
        or options.get("agent") != launch.get("agent")
        or not isinstance(option_launch, Mapping)
        or option_launch.get("requested") != launch
        or option_launch.get("effective") != launch
    ):
        raise OrchestrateError("worker-show did not prove the exact preflight actor and launch", code="preflight_identity_conflict")
    matching = [
        item for item in tasks if isinstance(item, Mapping) and item.get("id") == task_id
    ] if isinstance(tasks, list) else []
    if (
        len(matching) != 1
        or matching[0].get("run_id") != run.native_run_id
        or matching[0].get("spec") != packet_spec_from_json(packet_json)
        or matching[0].get("status") != "dispatched"
    ):
        raise OrchestrateError("Native Task does not bind the exact preflight packet", code="preflight_identity_conflict")
    return {
        "runtimeId": next(iter(runtime_ids)),
        "actor": terminal.get("agentIdentity"),
        "terminalHandle": handle,
        "executionHostId": terminal.get("executionHostId"),
        "hostPlatform": terminal.get("hostPlatform"),
        "worktreeId": worktree_id,
        "worktreeRoot": os.fspath(profile.root.resolve()),
        "launch": dict(launch),
    }


def worker_preflight(
    root: Path,
    *,
    run_id: str,
    task_id: str,
    dispatch_id: str,
    packet_id: str,
    client: OrcaClient,
    environment: Mapping[str, str] | None = None,
    platform: str | None = None,
) -> JsonObject:
    profile = ProjectProfile.load(root)
    env = os.environ if environment is None else environment
    selected_platform = sys.platform if platform is None else platform
    with StateStore(profile.root) as store:
        run = store.get_run(run_id)
        existing = store.get_preflight(run.local_id, task_id, dispatch_id)
        if existing is not None:
            code = "preflight_already_passed" if existing.get("outcome") == "passed" else "preflight_already_rejected"
            raise OrchestrateError("This Dispatch already has an immutable preflight observation; no fresh editing grant was issued", code=code)
        native: dict[str, Any] | None = None
        validation: PacketValidation | None = None
        try:
            if run.task_id != task_id or run.native_run_id != run_id or run.dispatch_id not in {None, dispatch_id}:
                raise OrchestrateError("Preflight selectors do not match the local Run", code="preflight_identity_conflict")
            packet_json = store.get_packet_json(run.local_id, task_id)
            packet = store.get_packet(run.local_id, task_id)
            if packet.get("packetId") != packet_id:
                raise OrchestrateError("Supplied packet ID does not match the immutable Task packet", code="packet_identity_conflict")
            _verify_packet_source_bytes(profile, packet)
            native = _native_identity(
                client,
                profile=profile,
                run=run,
                task_id=task_id,
                dispatch_id=dispatch_id,
                packet_json=packet_json,
                packet=packet,
                environment=env,
                platform=selected_platform,
            )
            validation = validate_packet_sources(profile, run, packet_json)
            observation = {
                "schema": PREFLIGHT_SCHEMA,
                "outcome": "passed",
                "runId": run_id,
                "taskId": task_id,
                "dispatchId": dispatch_id,
                "packetId": packet_id,
                "packetJsonSha256": validation.packet_json_sha256,
                "expectedSourceDigest": run.source_digest,
                "observedSourceDigest": validation.sources.digest,
                "profileDigest": profile.digest,
                "routingDigest": validation.routing_digest,
                "candidate": validation.sources.value["candidate"],
                "native": native,
                "observedAt": utc_now(),
                "limitations": [
                    "This is a managed observation, not enforcement against hostile preflight bypass.",
                    "It does not exclude concurrent external writers before or after observation.",
                ],
                "mismatches": [],
            }
            _, created = store.record_preflight(
                run.local_id,
                run_id=run_id,
                task_id=task_id,
                dispatch_id=dispatch_id,
                observation=observation,
            )
            if not created:
                raise OrchestrateError("Preflight observation already exists; no fresh editing grant was issued", code="preflight_already_recorded")
            return {"schema": PREFLIGHT_SCHEMA, "status": "admitted", "editingGrant": "fresh", "observation": observation}
        except (KeyError, TypeError, ValueError, OrchestrateError, OrcaCommandError) as exc:
            code = (
                exc.code
                if isinstance(exc, (OrchestrateError, OrcaCommandError))
                else "preflight_contract_invalid"
            )
            rejected = {
                "schema": PREFLIGHT_SCHEMA,
                "outcome": "rejected",
                "runId": run_id,
                "taskId": task_id,
                "dispatchId": dispatch_id,
                "packetId": packet_id,
                "native": native,
                "observedAt": utc_now(),
                "limitations": ["No managed editing admission was established."],
                "mismatches": [{"code": code, "message": str(exc)}],
            }
            store.record_preflight(
                run.local_id,
                run_id=run_id,
                task_id=task_id,
                dispatch_id=dispatch_id,
                observation=rejected,
            )
            raise OrchestrateError("Worker preflight was rejected", code="preflight_rejected", data={"cause": code}) from exc


def joined_preflight_status(store: StateStore, run: RunRecord) -> str:
    """Join immutable preflight evidence to the validated applied launch receipt."""

    if not run.task_id or not run.dispatch_id:
        return "pending"
    try:
        observation = store.get_preflight(run.local_id, run.task_id, run.dispatch_id)
    except OrchestrateError:
        return "conflicting"
    if observation is None:
        return "pending"
    if observation.get("outcome") == "rejected":
        return "rejected"
    rows = store.connection.execute(
        """SELECT arguments_json, response_json FROM intentions
           WHERE run_local_id = ? AND operation = 'worker-start' AND status = 'applied'
           ORDER BY created_at""",
        (run.local_id,),
    ).fetchall()
    if len(rows) != 1 or not rows[0]["response_json"]:
        return "pending"
    try:
        arguments = json.loads(rows[0]["arguments_json"])
        response = json.loads(rows[0]["response_json"])
        result = _result(response)
        effects = result.get("effects")
        packet_json = store.get_packet_json(run.local_id, run.task_id)
        packet = store.get_packet(run.local_id, run.task_id)
    except (json.JSONDecodeError, OrchestrateError, TypeError):
        return "conflicting"
    if not isinstance(effects, list):
        return "conflicting"
    worktrees = [item for item in effects if isinstance(item, Mapping) and item.get("kind") == "worktree"]
    terminals = [
        item
        for item in effects
        if isinstance(item, Mapping) and item.get("kind") == "terminal" and item.get("role") == "agent"
    ]
    launch = result.get("launch")
    admission = packet.get("admission")
    expected_launch = admission.get("launch") if isinstance(admission, Mapping) else None
    try:
        runtime_id = _runtime_id(response)
    except OrchestrateError:
        return "conflicting"
    expected = {
        "schema": PREFLIGHT_SCHEMA,
        "outcome": "passed",
        "runId": run.native_run_id,
        "taskId": run.task_id,
        "dispatchId": run.dispatch_id,
        "packetId": packet.get("packetId"),
        "packetJsonSha256": hashlib.sha256(packet_json.encode("utf-8")).hexdigest(),
        "expectedSourceDigest": run.source_digest,
        "observedSourceDigest": run.source_digest,
        "profileDigest": packet.get("profileDigest"),
        "routingDigest": _digest(packet.get("reader")),
        "candidate": packet.get("candidate"),
    }
    native = observation.get("native")
    expected_arguments = [
        "orchestration",
        "worker-start",
        "--run",
        str(run.native_run_id),
        "--task",
        str(run.task_id),
        "--worktree",
        f"path:{native.get('worktreeRoot')}" if isinstance(native, Mapping) else None,
        "--agent",
        expected_launch.get("agent") if isinstance(expected_launch, Mapping) else None,
        "--model",
        expected_launch.get("model") if isinstance(expected_launch, Mapping) else None,
        "--effort",
        expected_launch.get("effort") if isinstance(expected_launch, Mapping) else None,
        "--timeout-ms",
        "60000",
    ]
    if (
        any(observation.get(key) != value for key, value in expected.items())
        or result.get("dispatchId") != run.dispatch_id
        or len(worktrees) != 1
        or len(terminals) != 1
        or not isinstance(launch, Mapping)
        or not isinstance(expected_launch, Mapping)
        or launch.get("requested") != expected_launch
        or launch.get("effective") != expected_launch
        or not isinstance(native, Mapping)
        or native.get("runtimeId") != runtime_id
        or native.get("worktreeId") != worktrees[0].get("id")
        or native.get("terminalHandle") != terminals[0].get("id")
        or native.get("launch") != expected_launch
        or native.get("actor") != expected_launch.get("agent")
        or arguments != expected_arguments
    ):
        return "conflicting"
    return "admitted"
