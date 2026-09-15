"""Versioned compact packets for one supervised worker."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any

from .errors import OrchestrateError
from .profile import ProjectProfile
from .readers import ReaderResult
from .sources import SourceIndex, SourceRecord, decode_source_records


PACKET_SCHEMA = "orchestrate-worker-packet/v3"
PREFLIGHT_SCHEMA = "orchestrate-worker-preflight/v1"
MAX_PACKET_BYTES = 1024 * 1024
PACKET_FIELDS = frozenset(
    {
        "schema",
        "objective",
        "scope",
        "native",
        "operationalProfile",
        "profileDigest",
        "sourceDigest",
        "candidate",
        "sources",
        "reader",
        "admission",
        "authorityLimitations",
        "requiredChecks",
        "outputs",
        "unresolvedDecisions",
        "packetId",
    }
)


@dataclass(frozen=True, slots=True)
class DecodedPacket:
    value: dict[str, Any]
    source_records: tuple[SourceRecord, ...]


def _packet_id(value: dict[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "packet_sha256_" + hashlib.sha256(raw).hexdigest()


def expected_packet_id(value: Mapping[str, Any]) -> str:
    content = dict(value)
    content.pop("packetId", None)
    return _packet_id(content)


def _string(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _named_sha256(value: object, name: str) -> bool:
    prefix = f"{name}_sha256_"
    return isinstance(value, str) and value.startswith(prefix) and _sha256(value[len(prefix):])


def _git_oid(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) in {40, 64}
        and all(character in "0123456789abcdef" for character in value)
    )


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _require_finite_numbers(value: object) -> None:
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError("non-finite JSON number")
        if isinstance(item, dict):
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


def decode_packet(packet_json: str) -> DecodedPacket:
    """Strictly decode canonical v3 and every source record without I/O."""

    if len(packet_json.encode("utf-8")) > MAX_PACKET_BYTES:
        raise OrchestrateError(
            "Stored worker packet exceeds the bounded decode limit",
            code="packet_identity_conflict",
        )
    try:
        packet = json.loads(
            packet_json,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_json_constant,
        )
        _require_finite_numbers(packet)
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise OrchestrateError("Stored worker packet is invalid JSON", code="packet_identity_conflict") from exc
    if isinstance(packet, dict) and packet.get("schema") != PACKET_SCHEMA:
        raise OrchestrateError(
            "A legacy packet cannot grant a new worker admission",
            code="preflight_packet_unsupported",
        )
    allowed_fields = PACKET_FIELDS | ({"milestone"} if isinstance(packet, dict) and "milestone" in packet else set())
    if (
        not isinstance(packet, dict)
        or set(packet) != allowed_fields
        or packet.get("schema") != PACKET_SCHEMA
        or canonical_packet_json(packet) != packet_json
        or packet.get("packetId") != expected_packet_id(packet)
        or not _string(packet.get("objective"))
        or not _sha256(packet.get("profileDigest"))
        or not _sha256(packet.get("sourceDigest"))
    ):
        raise OrchestrateError("Worker packet canonical identity is invalid", code="packet_identity_conflict")

    scope = packet.get("scope")
    native = packet.get("native")
    operational = packet.get("operationalProfile")
    reader = packet.get("reader")
    admission = packet.get("admission")
    candidate = packet.get("candidate")
    milestone = packet.get("milestone")
    expected_scope_fields = (
        {"projectRoot", "strategy", "maxWorkers", "taskKey", "role", "taskSpec"}
        if milestone is not None
        else {"projectRoot", "strategy", "maxWorkers"}
    )
    expected_output_fields = (
        {"completion", "verification", "milestoneResult"}
        if milestone is not None
        else {"completion", "verification"}
    )
    launch = admission.get("launch") if isinstance(admission, Mapping) else None
    if (
        not isinstance(scope, Mapping)
        or set(scope) != expected_scope_fields
        or scope.get("projectRoot") != "."
        or scope.get("strategy") not in {"single-owner-first", "bounded-native-dag"}
        or type(scope.get("maxWorkers")) is not int
        or scope["maxWorkers"] < 1
        or not isinstance(native, Mapping)
        or set(native) != {"runId", "taskIdSource"}
        or (native.get("runId") is not None and not _string(native.get("runId")))
        or native.get("taskIdSource") != "orca-injected-task-and-dispatch-preamble"
        or not isinstance(operational, Mapping)
        or set(operational) != {
            "selectedDigest", "candidateDigest", "selectionSource",
            "selectionHistoryDigest", "candidateChanged",
        }
        or not all(_sha256(operational.get(field)) for field in ("selectedDigest", "candidateDigest", "selectionHistoryDigest"))
        or not _string(operational.get("selectionSource"))
        or type(operational.get("candidateChanged")) is not bool
        or not isinstance(reader, Mapping)
        or set(reader) != {"kind", "routing"}
        or reader.get("kind") not in {"repo", "ce-gd"}
        or not isinstance(reader.get("routing"), Mapping)
        or not isinstance(admission, Mapping)
        or set(admission) != {"schema", "requiredBefore", "launch", "commandTemplate"}
        or admission.get("schema") != PREFLIGHT_SCHEMA
        or admission.get("requiredBefore") != ["project-edits", "project-checks"]
        or not isinstance(admission.get("launch"), Mapping)
        or not isinstance(launch, Mapping)
        or "agent" not in launch
        or not set(launch).issubset({"agent", "model", "effort"})
        or not all(_string(value) for value in launch.values())
        or ("effort" in launch and "model" not in launch)
        or not isinstance(admission.get("commandTemplate"), list)
        or not all(isinstance(item, str) for item in admission["commandTemplate"])
        or not isinstance(candidate, Mapping)
        or set(candidate) != {
            "head", "headTree", "branch", "statusSha256", "coveredChangesSha256",
            "coveredChanges", "uncoveredChanges", "coverageComplete", "dirty",
        }
        or not all(_git_oid(candidate.get(field)) for field in ("head", "headTree"))
        or not all(_sha256(candidate.get(field)) for field in ("statusSha256", "coveredChangesSha256"))
        or (candidate.get("branch") is not None and not _string(candidate.get("branch")))
        or type(candidate.get("coverageComplete")) is not bool
        or type(candidate.get("dirty")) is not bool
        or not isinstance(candidate.get("coveredChanges"), list)
        or not isinstance(candidate.get("uncoveredChanges"), list)
        or not isinstance(packet.get("authorityLimitations"), list)
        or not all(isinstance(item, str) for item in packet["authorityLimitations"])
        or not isinstance(packet.get("requiredChecks"), list)
        or not all(isinstance(item, str) for item in packet["requiredChecks"])
        or not isinstance(packet.get("outputs"), Mapping)
        or set(packet["outputs"]) != expected_output_fields
        or not _string(packet["outputs"].get("completion"))
        or not _string(packet["outputs"].get("verification"))
        or not isinstance(packet.get("unresolvedDecisions"), list)
    ):
        raise OrchestrateError("Worker packet identity is malformed", code="packet_identity_conflict")

    if milestone is not None and (
        not isinstance(milestone, Mapping)
        or set(milestone) != {"candidateDigest", "contractDigest", "contract"}
        or not _sha256(milestone.get("candidateDigest"))
        or not _named_sha256(milestone.get("contractDigest"), "contract")
        or not isinstance(milestone.get("contract"), Mapping)
        or not _string(scope.get("taskKey"))
        or not _string(scope.get("role"))
        or not _string(scope.get("taskSpec"))
    ):
        raise OrchestrateError("Milestone packet identity is malformed", code="packet_identity_conflict")
    if milestone is not None:
        milestone_result = packet["outputs"].get("milestoneResult")
        if (
            not isinstance(milestone_result, Mapping)
            or set(milestone_result) != {"schema", "path", "required", "outcomes", "instruction"}
            or milestone_result.get("schema") != "orchestrate-milestone-result/v1"
            or not _string(milestone_result.get("path"))
            or milestone_result.get("required") is not True
            or milestone_result.get("outcomes") != ["accepted", "rejected"]
            or not _string(milestone_result.get("instruction"))
        ):
            raise OrchestrateError("Milestone packet output is malformed", code="packet_identity_conflict")

    for change in candidate["coveredChanges"]:
        if (
            not isinstance(change, Mapping)
            or set(change) != {"path", "state", "worktreeSha256", "indexSha256"}
            or not _string(change.get("path"))
            or not _string(change.get("state"))
            or (change.get("worktreeSha256") is not None and not _sha256(change.get("worktreeSha256")))
            or (change.get("indexSha256") is not None and not _sha256(change.get("indexSha256")))
        ):
            raise OrchestrateError("Worker packet candidate identity is malformed", code="packet_identity_conflict")
    for change in candidate["uncoveredChanges"]:
        if (
            not isinstance(change, Mapping)
            or set(change) != {"path", "state"}
            or not _string(change.get("path"))
            or not _string(change.get("state"))
        ):
            raise OrchestrateError("Worker packet candidate identity is malformed", code="packet_identity_conflict")

    records = decode_source_records(packet.get("sources"))
    return DecodedPacket(packet, records)


def make_packet(
    *,
    objective: str,
    profile: ProjectProfile,
    sources: SourceIndex,
    launch: Mapping[str, str],
    python_executable: str,
    run_id: str | None = None,
    reader: ReaderResult | None = None,
) -> dict[str, Any]:
    packet: dict[str, Any] = {
        "schema": PACKET_SCHEMA,
        "objective": objective,
        "scope": {
            "projectRoot": ".",
            "strategy": "single-owner-first",
            "maxWorkers": 1,
        },
        "native": {
            "runId": run_id,
            "taskIdSource": "orca-injected-task-and-dispatch-preamble",
        },
        "operationalProfile": sources.value["operationalProfile"],
        "profileDigest": profile.digest,
        "sourceDigest": sources.digest,
        "candidate": sources.value["candidate"],
        "sources": sources.value["sources"],
        "reader": {"kind": reader.kind, "routing": reader.routing} if reader else {"kind": sources.value["reader"]},
        "admission": {
            "schema": PREFLIGHT_SCHEMA,
            "requiredBefore": ["project-edits", "project-checks"],
            "launch": dict(launch),
            "commandTemplate": [
                python_executable,
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
            ],
        },
        "authorityLimitations": [
            *sources.value["limitations"],
            "Preflight is a durable managed observation, not enforcement against hostile bypass.",
            "Source reads are bounded observations and do not exclude concurrent external writers.",
            "Worker admission is limited to native Windows in this milestone; WSL is not admitted.",
        ],
        "requiredChecks": list(profile.value["checks"]),
        "outputs": {
            "completion": "exact worker_done with outcome and changed-file list",
            "verification": "worker claims remain distinct from independent evidence",
        },
        "unresolvedDecisions": [],
    }
    packet.pop("packetId", None)
    packet["packetId"] = _packet_id(packet)
    return packet


def make_milestone_packet(
    *,
    objective: str,
    task_key: str,
    task_spec: str,
    role: str,
    candidate_digest: str,
    contract_digest: str,
    contract: Mapping[str, Any],
    result_path: str,
    max_workers: int,
    profile: ProjectProfile,
    sources: SourceIndex,
    launch: Mapping[str, str],
    python_executable: str,
    run_id: str,
    reader: ReaderResult,
) -> dict[str, Any]:
    """Extend v3 without changing its source/preflight contract."""

    packet = make_packet(
        objective=objective,
        profile=profile,
        sources=sources,
        launch=launch,
        python_executable=python_executable,
        run_id=run_id,
        reader=reader,
    )
    packet["scope"] = {
        "projectRoot": ".",
        "strategy": "bounded-native-dag",
        "maxWorkers": max_workers,
        "taskKey": task_key,
        "role": role,
        "taskSpec": task_spec,
    }
    packet["milestone"] = {
        "candidateDigest": candidate_digest,
        "contractDigest": contract_digest,
        "contract": dict(contract),
    }
    packet["outputs"]["milestoneResult"] = {
        "schema": "orchestrate-milestone-result/v1",
        "path": result_path,
        "required": True,
        "outcomes": ["accepted", "rejected"],
        "instruction": (
            "Write the exact JSON result after the planned work and pass this same path as worker_done --report-path."
        ),
    }
    packet.pop("packetId", None)
    packet["packetId"] = _packet_id(packet)
    return packet


def canonical_packet_json(packet: Mapping[str, Any]) -> str:
    """Return the one canonical JSON representation persisted and embedded in a Task."""

    return json.dumps(packet, indent=2, sort_keys=True)


def packet_spec_from_json(packet_json: str) -> str:
    try:
        packet = json.loads(packet_json)
    except json.JSONDecodeError:
        packet = None
    if isinstance(packet, Mapping) and isinstance(packet.get("milestone"), Mapping):
        prefix = (
            "Execute this exact bounded milestone packet in its stated read-only or owner role. "
            "Preserve unrelated WIP, follow the indexed project authority, write the exact required result, "
            "and do not convert worker success into independent acceptance.\n\n"
        )
    else:
        prefix = (
            "Execute this exact orchestrate packet as the single implementation owner. "
            "Preserve unrelated WIP, follow the indexed project authority, run required checks, "
            "and report observed evidence without claiming independent acceptance.\n\n"
        )
    return prefix + packet_json


def packet_spec(packet: Mapping[str, Any]) -> str:
    return packet_spec_from_json(canonical_packet_json(packet))
