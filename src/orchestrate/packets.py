"""Versioned compact packets for one supervised worker."""

from __future__ import annotations

import json
import hashlib
from collections.abc import Mapping
from typing import Any

from .profile import ProjectProfile
from .readers import ReaderResult
from .sources import SourceIndex


PACKET_SCHEMA = "orchestrate-worker-packet/v3"
PREFLIGHT_SCHEMA = "orchestrate-worker-preflight/v1"


def _packet_id(value: dict[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "packet_sha256_" + hashlib.sha256(raw).hexdigest()


def expected_packet_id(value: Mapping[str, Any]) -> str:
    content = dict(value)
    content.pop("packetId", None)
    return _packet_id(content)


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
