"""Versioned compact packets for one supervised worker."""

from __future__ import annotations

import json
import hashlib
from typing import Any

from .profile import ProjectProfile
from .readers import ReaderResult
from .sources import SourceIndex


PACKET_SCHEMA = "orchestrate-worker-packet/v2"


def _packet_id(value: dict[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "packet_sha256_" + hashlib.sha256(raw).hexdigest()


def make_packet(
    *,
    objective: str,
    profile: ProjectProfile,
    sources: SourceIndex,
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
        "authorityLimitations": sources.value["limitations"],
        "requiredChecks": list(profile.value["checks"]),
        "outputs": {
            "completion": "exact worker_done with outcome and changed-file list",
            "verification": "worker claims remain distinct from independent evidence",
        },
        "unresolvedDecisions": [],
    }
    packet["packetId"] = _packet_id(packet)
    return packet


def packet_spec(packet: dict[str, Any]) -> str:
    prefix = (
        "Execute this exact orchestrate packet as the single implementation owner. "
        "Preserve unrelated WIP, follow the indexed project authority, run required checks, "
        "and report observed evidence without claiming independent acceptance.\n\n"
    )
    return prefix + json.dumps(packet, indent=2, sort_keys=True)
