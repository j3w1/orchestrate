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
from .orca_compat import WorkerShowShapeError, worker_input_accepted_readback
from .packets import (
    DecodedPacket,
    PREFLIGHT_SCHEMA,
    decode_packet,
    packet_spec_from_json,
)
from .profile import ProjectProfile
from .readers import CE_QUERY_SCRIPT, ReaderResult, read_project
from .safeio import approved_project_path, read_project_bytes
from .sources import (
    PreparedSourceSet,
    SourceAccess,
    SourceIndex,
    SourceReference,
    build_source_index,
    classify_source_reference,
)
from .state import RunRecord, StateStore, utc_now


@dataclass(frozen=True, slots=True)
class PacketValidation:
    packet: dict[str, Any]
    reader: ReaderResult
    sources: SourceIndex
    routing_digest: str
    packet_json_sha256: str


@dataclass(frozen=True, slots=True)
class AdmissionAttemptIdentity:
    run_id: str
    task_id: str
    dispatch_id: str
    packet_id: str


@dataclass(frozen=True, slots=True)
class PreparedPacketSources:
    decoded: DecodedPacket
    prepared: PreparedSourceSet
    sources: SourceIndex
    attempt: AdmissionAttemptIdentity | None


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


def _verify_reference_bytes(
    profile: ProjectProfile,
    reference: SourceReference,
    raw_by_path: dict[str, bytes | None],
) -> None:
    record = reference.packet_record
    if record is None:
        raise OrchestrateError("Worker packet source identity is malformed", code="packet_identity_conflict")
    candidate = approved_project_path(profile.root, reference.path, require_file=False)
    if reference.access == SourceAccess.REFERENCE_ONLY:
        if candidate.exists():
            raise OrchestrateError("Bound project source bytes changed", code="source_binding_changed")
        raw_by_path[reference.path] = None
        return
    try:
        raw = read_project_bytes(profile.root, reference.path)
    except OrchestrateError as exc:
        raise OrchestrateError("Bound project source bytes changed", code="source_binding_changed") from exc
    identity = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    if identity != {"sha256": record.sha256, "bytes": record.byte_count}:
        raise OrchestrateError("Bound project source bytes changed", code="source_binding_changed")
    raw_by_path[reference.path] = raw


def _prepare_packet_sources(
    profile: ProjectProfile,
    decoded: DecodedPacket,
    *,
    attempt: AdmissionAttemptIdentity | None = None,
) -> PreparedPacketSources:
    """Complete source eligibility, bytes, inventory, and index exactly once."""

    packet = decoded.value
    if (
        packet.get("profileDigest") != profile.digest
        or packet.get("operationalProfile") != _operational_profile(profile)
        or (
            attempt is not None
            and (
                packet.get("packetId") != attempt.packet_id
                or packet.get("native", {}).get("runId") != attempt.run_id
            )
        )
    ):
        raise OrchestrateError("Bound operational profile changed", code="source_binding_changed")
    packet_reader = packet["reader"]
    query_paths = {"package.json", CE_QUERY_SCRIPT, "docs/project-log/manifest.json"}
    record_paths = {record.path for record in decoded.source_records}
    if packet_reader.get("kind") == "ce-gd" and not query_paths.issubset(record_paths):
        raise OrchestrateError("Worker packet omits the CE query source identity", code="packet_identity_conflict")

    routing = packet_reader["routing"]
    reader_routed = {
        *profile.value["instructions"],
        *profile.value["taskEntrypoints"],
        *profile.value["commandManifests"],
    }
    for field in ("authority", "tasks", "contextPackets"):
        values = routing.get(field)
        if isinstance(values, list):
            reader_routed.update(item for item in values if isinstance(item, str))
    for field in ("registry", "task", "logManifest"):
        value = routing.get(field)
        if isinstance(value, str):
            reader_routed.add(value)
    if packet_reader.get("kind") == "ce-gd":
        reader_routed.add(CE_QUERY_SCRIPT)

    references = tuple(
        classify_source_reference(
            profile,
            record.path,
            packet_record=record,
            reader_routed=record.path in reader_routed,
            packet_bound=True,
        )
        for record in decoded.source_records
    )
    raw_by_path: dict[str, bytes | None] = {}
    guarded: list[SourceReference] = []
    for reference in references:
        if reference.access == SourceAccess.INVENTORY_GUARDED_BYTES:
            guarded.append(reference)
        else:
            _verify_reference_bytes(profile, reference, raw_by_path)

    # Directly eligible bytes are checked first, preserving the pre-subprocess
    # rejection guarantee for acknowledged selected sources. The one bounded
    # inventory is then completed for the source-index wire identity and every
    # inventory-guarded source.
    current_instructions = tuple(profile.require_instruction_acknowledgment())
    current_set = set(current_instructions)
    unacknowledged: list[str] = []
    for reference in guarded:
        if reference.path not in current_set:
            candidate = approved_project_path(profile.root, reference.path, require_file=False)
            if candidate.exists():
                unacknowledged.append(reference.path)
                continue
        _verify_reference_bytes(profile, reference, raw_by_path)
    if unacknowledged:
        raise OrchestrateError(
            "Packet-bound instructions outside the current conventional inventory require explicit selection and acknowledgment; review .orchestrate.json and rerun 'orchestrate setup --acknowledge-profile'",
            code="instruction_acknowledgment_required",
            data={"packetInstructionPaths": sorted(unacknowledged)},
        )

    prepared = PreparedSourceSet.create(references, current_instructions, raw_by_path)
    sources = build_source_index(
        profile,
        extra_sources=set(record_paths),
        prepared=prepared,
    )
    _verify_source_index(profile, packet, sources)
    return PreparedPacketSources(decoded, prepared, sources, attempt)


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
    *,
    decoded: DecodedPacket | None = None,
    prepared_stage: PreparedPacketSources | None = None,
) -> PacketValidation:
    """Consume the completed source stage and validate reader routing."""

    decoded = decode_packet(packet_json) if decoded is None else decoded
    packet = decoded.value
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
    milestone = packet.get("milestone")
    scope = packet.get("scope")
    if milestone is not None:
        task_key = scope.get("taskKey") if isinstance(scope, Mapping) else None
        task_spec = scope.get("taskSpec") if isinstance(scope, Mapping) else None
        # The packet validator normally has no StateStore argument. The exact
        # native Task binding is therefore joined by worker_preflight below,
        # while this source-only pass rejects malformed milestone identity.
        if (
            not isinstance(milestone, Mapping)
            or not isinstance(task_key, str)
            or not task_key
            or not isinstance(task_spec, str)
            or not task_spec
            or not isinstance(milestone.get("candidateDigest"), str)
            or not isinstance(milestone.get("contractDigest"), str)
            or not isinstance(milestone.get("contract"), Mapping)
        ):
            raise OrchestrateError("Milestone packet identity is malformed", code="packet_identity_conflict")
    stage = _prepare_packet_sources(profile, decoded) if prepared_stage is None else prepared_stage
    packet_reader = packet.get("reader")
    if (
        not isinstance(packet_reader, Mapping)
        or packet_reader.get("kind") != profile.value["reader"]["kind"]
    ):
        raise OrchestrateError("Worker packet reader identity is malformed", code="packet_identity_conflict")
    query_paths = {"package.json", CE_QUERY_SCRIPT, "docs/project-log/manifest.json"}
    expected_query_sources = (
        stage.prepared.identities(query_paths)
        if packet_reader.get("kind") == "ce-gd" and query_paths.issubset(stage.prepared.paths)
        else None
    )
    if packet_reader.get("kind") == "ce-gd" and expected_query_sources is None:
        raise OrchestrateError("Worker packet omits the CE query source identity", code="packet_identity_conflict")
    reader = read_project(
        profile,
        run.objective,
        expected_ce_query_sources=expected_query_sources,
        prepared_sources=stage.prepared,
    )
    if not set(reader.consulted_paths).issubset(stage.prepared.paths):
        raise OrchestrateError(
            "Reader routing escaped the completed packet source stage",
            code="packet_identity_conflict",
        )
    actual_reader = {"kind": reader.kind, "routing": reader.routing}
    if (
        packet.get("reader") != actual_reader
    ):
        raise OrchestrateError("Bound project sources or routing changed", code="source_binding_changed")
    return PacketValidation(
        packet,
        reader,
        stage.sources,
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
    terminal_resource = worker_result.get("terminalResource")
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
    if not isinstance(terminal, Mapping):
        raise OrchestrateError("terminal show omitted the worker terminal object", code="preflight_identity_conflict")
    identity_mismatches: list[dict[str, Any]] = []
    expected_terminal_fields = (
        ("terminal.handle", handle, terminal.get("handle")),
        ("terminal.connected", True, terminal.get("connected")),
        ("terminal.writable", True, terminal.get("writable")),
        ("terminal.executionHostId", "local", terminal.get("executionHostId")),
        ("terminal.agentIdentity", launch.get("agent"), terminal.get("agentIdentity")),
    )
    for field, expected, actual in expected_terminal_fields:
        if actual != expected or type(actual) is not type(expected):
            identity_mismatches.append({"field": field, "expected": expected, "actual": actual})
    if "hostPlatform" in terminal and terminal.get("hostPlatform") != "win32":
        identity_mismatches.append(
            {
                "field": "terminal.hostPlatform",
                "expected": "win32 when reported",
                "actual": terminal.get("hostPlatform"),
            }
        )
    if identity_mismatches:
        fields = ", ".join(item["field"] for item in identity_mismatches)
        raise OrchestrateError(
            f"terminal show did not prove the exact local Windows worker actor: {fields}",
            code="preflight_identity_conflict",
            data={"fields": identity_mismatches},
        )
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
        or not isinstance(worker, Mapping)
        or not isinstance(terminal_resource, Mapping)
    ):
        raise OrchestrateError("worker-show omitted the preflight worker identity", code="preflight_identity_conflict")
    try:
        accepted = worker_input_accepted_readback(
            dispatch,
            worker,
            terminal_resource,
            run_id=str(run.native_run_id),
            task_id=task_id,
            dispatch_id=dispatch_id,
            worktree_id=worktree_id,
            terminal_handle=handle,
        )
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="preflight_identity_conflict") from exc
    if (
        not isinstance(options, Mapping)
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
        "terminalResourceId": accepted.resource.resource_id,
        "executionHostId": terminal.get("executionHostId"),
        "controllerPlatform": platform,
        "hostPlatform": terminal.get("hostPlatform"),
        "terminalHostPlatform": terminal.get("hostPlatform"),
        "hostPlatformEvidence": (
            "terminal-show"
            if terminal.get("hostPlatform") == "win32"
            else "native-controller-and-local-execution-host"
        ),
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
    env = os.environ if environment is None else environment
    selected_platform = sys.platform if platform is None else platform
    with StateStore(root) as store:
        run = store.get_run(run_id)
        # Controller ownership deliberately does not cover this fence: the first
        # worker preflight may run before worker-start returns to implement/resume.
        with store.admission_effect_fence(run.local_id):
            # Fence acquisition may have waited while worker-start bound the
            # authoritative Dispatch/resource state. Never validate against the
            # pre-lock Run snapshot.
            run = store.get_run(run.local_id)
            existing = store.get_preflight(run.local_id, task_id, dispatch_id)
            if existing is not None:
                code = "preflight_already_passed" if existing.get("outcome") == "passed" else "preflight_already_rejected"
                raise OrchestrateError("This Dispatch already has an immutable preflight observation; no fresh editing grant was issued", code=code)
            native: dict[str, Any] | None = None
            validation: PacketValidation | None = None
            profile: ProjectProfile | None = None
            try:
                milestone_task = store.connection.execute(
                    "SELECT task_key FROM milestone_task_bindings WHERE run_local_id = ? AND task_id = ?",
                    (run.local_id, task_id),
                ).fetchone()
                owner_attempt = run.task_id == task_id
                if (
                    run.native_run_id != run_id
                    or (not owner_attempt and milestone_task is None)
                    or (owner_attempt and run.dispatch_id not in {None, dispatch_id})
                ):
                    raise OrchestrateError("Preflight selectors do not match the local Run", code="preflight_identity_conflict")
                foreign = other_dispatch_observations(store, run.local_id, task_id, dispatch_id)
                if foreign:
                    raise OrchestrateError(
                        "Another immutable preflight observation already binds this Run/Task to a different Dispatch",
                        code="preflight_dispatch_conflict",
                        data={"boundDispatchId": run.dispatch_id, "otherObservations": foreign},
                    )
                packet_json = store.get_packet_json(run.local_id, task_id)
                decoded = decode_packet(packet_json)
                packet = decoded.value
                attempt = AdmissionAttemptIdentity(run_id, task_id, dispatch_id, packet_id)
                if packet.get("packetId") != attempt.packet_id:
                    raise OrchestrateError("Supplied packet ID does not match the immutable Task packet", code="packet_identity_conflict")
                profile = ProjectProfile._load_for_packet_source_preflight(root)
                prepared_stage = _prepare_packet_sources(profile, decoded, attempt=attempt)
                native = _native_identity(
                    client,
                    profile=profile,
                    run=run,
                    task_id=attempt.task_id,
                    dispatch_id=attempt.dispatch_id,
                    packet_json=packet_json,
                    packet=packet,
                    environment=env,
                    platform=selected_platform,
                )
                if owner_attempt:
                    binding = store.get_worker_resource_binding(run.local_id)
                    bound_identity = (
                        (
                            binding.dispatch_id,
                            binding.resource_id,
                            binding.terminal_handle,
                            binding.worktree_id,
                        )
                        if binding is not None
                        else None
                    )
                else:
                    binding = store.connection.execute(
                        """SELECT dispatch_id, resource_id, terminal_handle, worktree_id
                           FROM milestone_worker_bindings
                           WHERE run_local_id = ? AND task_key = ?""",
                        (run.local_id, milestone_task["task_key"]),
                    ).fetchone()
                    bound_identity = (
                        (
                            binding["dispatch_id"],
                            binding["resource_id"],
                            binding["terminal_handle"],
                            binding["worktree_id"],
                        )
                        if binding is not None
                        else None
                    )
                if bound_identity is not None and bound_identity != (
                    attempt.dispatch_id,
                    native.get("terminalResourceId"),
                    native.get("terminalHandle"),
                    native.get("worktreeId"),
                ):
                    raise OrchestrateError(
                        "Worker preflight live resource identity conflicts with its immutable controller binding",
                        code="preflight_identity_conflict",
                    )
                validation = validate_packet_sources(
                    profile,
                    run,
                    packet_json,
                    decoded=decoded,
                    prepared_stage=prepared_stage,
                )
                observation = {
                    "schema": PREFLIGHT_SCHEMA,
                    "outcome": "passed",
                    "runId": run_id,
                    "taskId": task_id,
                    "dispatchId": dispatch_id,
                    "packetId": packet_id,
                    "packetJsonSha256": validation.packet_json_sha256,
                    "expectedSourceDigest": validation.packet.get("sourceDigest"),
                    "observedSourceDigest": validation.sources.digest,
                    "profileDigest": profile.digest,
                    "routingDigest": validation.routing_digest,
                    "candidate": validation.sources.value["candidate"],
                    "native": native,
                    "observedAt": utc_now(),
                    "limitations": [
                        "This is a managed observation, not enforcement against hostile preflight bypass.",
                        "The host-local admission/effect fence orders managed preflights and controller effects only.",
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
                mismatch: dict[str, Any] = {"code": code, "message": str(exc)}
                if isinstance(exc, OrchestrateError) and exc.data is not None:
                    mismatch["details"] = exc.data
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
                    "mismatches": [mismatch],
                }
                store.record_preflight(
                    run.local_id,
                    run_id=run_id,
                    task_id=task_id,
                    dispatch_id=dispatch_id,
                    observation=rejected,
                )
                raise OrchestrateError(
                    "Worker preflight was rejected",
                    code="preflight_rejected",
                    data={"cause": code, "mismatches": rejected["mismatches"]},
                ) from exc


def other_dispatch_observations(
    store: StateStore,
    run_local_id: str,
    task_id: str,
    dispatch_id: str | None,
) -> list[dict[str, Any]]:
    """Immutable observations for this Run/Task recorded under any other Dispatch.

    Raises the store's malformed-observation error unchanged so callers fail closed.
    """

    return [
        {
            "dispatchId": item["dispatchId"],
            "outcome": item["outcome"],
            "observedAt": item["observation"].get("observedAt"),
        }
        for item in store.list_preflights(run_local_id, task_id)
        if item["dispatchId"] != dispatch_id
    ]


def joined_preflight_status(store: StateStore, run: RunRecord) -> str:
    """Join immutable preflight evidence to the validated applied launch receipt.

    Once the controller has bound a Dispatch, every observation for the same
    local Run/Task is enumerated: none is ``pending``; exactly one for the bound
    Dispatch continues into the exact launch join; an observation under any
    other Dispatch, or more than one observation, is ``conflicting`` so the
    controller holds before ordinary effects instead of treating the split
    attempt identity as absent preflight.
    """

    if not run.task_id or not run.dispatch_id:
        return "pending"
    try:
        recorded = store.list_preflights(run.local_id, run.task_id)
    except OrchestrateError:
        return "conflicting"
    if not recorded:
        return "pending"
    if len(recorded) != 1 or recorded[0]["dispatchId"] != run.dispatch_id:
        return "conflicting"
    observation = recorded[0]["observation"]
    if observation.get("schema") != PREFLIGHT_SCHEMA:
        return "conflicting"
    outcome = observation.get("outcome")
    if outcome == "rejected":
        return "rejected"
    if outcome != "passed":
        return "conflicting"
    milestone_task = store.connection.execute(
        "SELECT task_key FROM milestone_task_bindings WHERE run_local_id = ? AND task_id = ?",
        (run.local_id, run.task_id),
    ).fetchone()
    operation = (
        f"milestone-worker-start:{milestone_task['task_key']}"
        if milestone_task is not None
        else "worker-start"
    )
    rows = store.connection.execute(
        """SELECT arguments_json, response_json FROM intentions
           WHERE run_local_id = ? AND operation = ? AND status = 'applied'
           ORDER BY created_at""",
        (run.local_id, operation),
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
        "expectedSourceDigest": packet.get("sourceDigest"),
        "observedSourceDigest": packet.get("sourceDigest"),
        "profileDigest": packet.get("profileDigest"),
        "routingDigest": _digest(packet.get("reader")),
        "candidate": packet.get("candidate"),
    }
    native = observation.get("native")
    if milestone_task is None:
        resource_binding = store.get_worker_resource_binding(run.local_id)
        resource_dispatch_id = resource_binding.dispatch_id if resource_binding is not None else None
        resource_id = resource_binding.resource_id if resource_binding is not None else None
        resource_terminal = resource_binding.terminal_handle if resource_binding is not None else None
        resource_worktree = resource_binding.worktree_id if resource_binding is not None else None
    else:
        milestone_worker = store.connection.execute(
            """SELECT dispatch_id, resource_id, terminal_handle, worktree_id
               FROM milestone_worker_bindings WHERE run_local_id = ? AND task_key = ?""",
            (run.local_id, milestone_task["task_key"]),
        ).fetchone()
        resource_binding = milestone_worker
        resource_dispatch_id = milestone_worker["dispatch_id"] if milestone_worker is not None else None
        resource_id = milestone_worker["resource_id"] if milestone_worker is not None else None
        resource_terminal = milestone_worker["terminal_handle"] if milestone_worker is not None else None
        resource_worktree = milestone_worker["worktree_id"] if milestone_worker is not None else None
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
    ]
    if isinstance(expected_launch, Mapping) and expected_launch.get("model") is not None:
        expected_arguments.extend(("--model", expected_launch.get("model")))
    if isinstance(expected_launch, Mapping) and expected_launch.get("effort") is not None:
        expected_arguments.extend(("--effort", expected_launch.get("effort")))
    expected_arguments.extend(("--timeout-ms", "60000"))
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
        or resource_binding is None
        or resource_dispatch_id != run.dispatch_id
        or native.get("terminalResourceId") != resource_id
        or native.get("terminalHandle") != resource_terminal
        or native.get("worktreeId") != resource_worktree
        or native.get("runtimeId") != runtime_id
        or native.get("worktreeId") != worktrees[0].get("id")
        or native.get("terminalHandle") != terminals[0].get("id")
        or native.get("launch") != expected_launch
        or native.get("actor") != expected_launch.get("agent")
        or arguments != expected_arguments
    ):
        return "conflicting"
    return "admitted"
