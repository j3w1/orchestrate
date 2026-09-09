"""Bounded source readers and exact candidate identities."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any

from .errors import OrchestrateError
from .profile import PROFILE_NAME, ProjectProfile, instruction_inventory
from .safeio import approved_project_path, is_sensitive_source, read_project_bytes


SOURCE_INDEX_SCHEMA = "orchestrate-source-index/v1"
def _git(root: Path, *arguments: str, allow_failure: bool = False) -> str:
    completed = subprocess.run(("git", "-C", os.fspath(root), *arguments), capture_output=True, check=False)
    if completed.returncode and not allow_failure:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise OrchestrateError(f"Git inspection failed: {detail}", code="git_inspection_failed")
    try:
        return completed.stdout.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise OrchestrateError("Git output is not strict UTF-8", code="git_path_encoding") from exc


def _git_bytes(root: Path, *arguments: str) -> bytes:
    completed = subprocess.run(("git", "-C", os.fspath(root), *arguments), capture_output=True, check=False)
    if completed.returncode:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise OrchestrateError(f"Git inspection failed: {detail}", code="git_inspection_failed")
    return completed.stdout


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _status_map(root: Path) -> dict[str, str]:
    raw_output = _git_bytes(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    try:
        output = raw_output.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise OrchestrateError("Git status paths are not strict UTF-8", code="git_path_encoding") from exc
    result: dict[str, str] = {}
    rows = output.split("\0")
    index = 0
    while index < len(rows):
        row = rows[index]
        index += 1
        if not row:
            continue
        status = row[:2]
        path = row[3:]
        if status[0] in {"R", "C"} and index < len(rows):
            original = rows[index]
            index += 1
            if status[0] == "R":
                result[original.replace("\\", "/")] = "D "
        result[path.replace("\\", "/")] = status
    return result


def _assert_readable_source(root: Path, relative: str) -> tuple[Path, bytes]:
    return root / relative, read_project_bytes(root, relative)


def _head_blob(root: Path, relative: str) -> bytes | None:
    completed = subprocess.run(
        ("git", "-C", os.fspath(root), "show", f"HEAD:{relative}"),
        capture_output=True,
        check=False,
    )
    return completed.stdout if completed.returncode == 0 else None


@dataclass(frozen=True, slots=True)
class SourceIndex:
    value: dict[str, Any]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return self.value


def _index_bytes(root: Path, relative: str) -> bytes | None:
    completed = subprocess.run(
        ("git", "-C", os.fspath(root), "show", f":{relative}"),
        capture_output=True,
        check=False,
    )
    return completed.stdout if completed.returncode == 0 else None


def build_source_index(profile: ProjectProfile, *, extra_sources: set[str] | None = None) -> SourceIndex:
    root = profile.root
    status = _status_map(root)
    instructions = set(instruction_inventory(root))
    configured = {
        *profile.value["instructions"],
        *instructions,
        *profile.value["taskEntrypoints"],
        *profile.value["commandManifests"],
        PROFILE_NAME,
        *(extra_sources or set()),
        *profile.value.get("candidateSources", []),
    }
    records: list[dict[str, Any]] = []
    for relative in sorted(configured):
        path = approved_project_path(root, relative, require_file=False)
        if not path.exists():
            records.append(
                {
                    "path": relative,
                    "state": "deleted" if "D" in status.get(relative, "") else "missing",
                    "sha256": None,
                    "bytes": 0,
                    "headSha256": (
                        _sha256(head) if (head := _head_blob(root, relative)) is not None else None
                    ),
                    "indexSha256": (
                        _sha256(index) if (index := _index_bytes(root, relative)) is not None else None
                    ),
                    "authority": "unavailable",
                }
            )
            continue
        _, raw = _assert_readable_source(root, relative)
        head = _head_blob(root, relative)
        changed_authority = relative == PROFILE_NAME and head != raw
        changed_instruction = (
            relative in profile.value["instructions"]
            or Path(relative).name in {"AGENTS.md", "CLAUDE.md"}
        ) and (head is None or head != raw)
        records.append(
            {
                "path": relative,
                "state": status.get(relative, "  "),
                "sha256": _sha256(raw),
                "bytes": len(raw),
                "headSha256": _sha256(head) if head is not None else None,
                "indexSha256": (
                    _sha256(index) if (index := _index_bytes(root, relative)) is not None else None
                ),
                "authority": "candidate-restrict-only" if changed_authority or changed_instruction else "consulted",
            }
        )

    covered_changes: list[dict[str, Any]] = []
    uncovered_changes: list[dict[str, str]] = []
    records_by_path = {item["path"]: item for item in records}
    for relative, git_status in sorted(status.items()):
        record = records_by_path.get(relative)
        if record is None or is_sensitive_source(relative):
            uncovered_changes.append({"path": relative, "state": git_status})
            continue
        covered_changes.append(
            {
                "path": relative,
                "state": git_status,
                "worktreeSha256": record["sha256"],
                "indexSha256": record["indexSha256"],
            }
        )
    change_bytes = json.dumps(covered_changes, sort_keys=True, separators=(",", ":")).encode("utf-8")
    raw_status = _git_bytes(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    candidate = {
        "head": _git(root, "rev-parse", "HEAD").strip(),
        "headTree": _git(root, "rev-parse", "HEAD^{tree}").strip(),
        "branch": _git(root, "branch", "--show-current").strip() or None,
        "statusSha256": _sha256(raw_status),
        "coveredChangesSha256": _sha256(change_bytes),
        "coveredChanges": covered_changes,
        "uncoveredChanges": uncovered_changes,
        "coverageComplete": not uncovered_changes,
        "dirty": bool(status),
    }
    value: dict[str, Any] = {
        "schema": SOURCE_INDEX_SCHEMA,
        "reader": profile.value["reader"]["kind"],
        "operationalProfile": {
            "selectedDigest": profile.digest,
            "candidateDigest": profile.candidate_digest,
            "selectionSource": profile.selection_source,
            "selectionHistoryDigest": profile.selection_history_digest,
            "candidateChanged": profile.candidate_changed,
        },
        "instructionInventory": sorted(instructions),
        "candidate": candidate,
        "sources": records,
        "limitations": [
            "Candidate instruction changes can restrict but cannot grant execution authority."
        ] if any(item["authority"] == "candidate-restrict-only" for item in records) else [],
    }
    if uncovered_changes:
        value["limitations"].append(
            "Dirty candidate paths outside explicit consulted coverage were not read: "
            + ", ".join(item["path"] for item in uncovered_changes)
        )
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return SourceIndex(value, _sha256(raw))


def read_source_text(profile: ProjectProfile, relative: str) -> str:
    """Read a configured source as UTF-8 without exposing excluded file classes."""

    allowed = {
        *profile.value["instructions"],
        *profile.value["taskEntrypoints"],
        *profile.value["commandManifests"],
    }
    if relative not in allowed:
        raise OrchestrateError(f"Source is not in the project profile: {relative}", code="source_not_configured")
    _, raw = _assert_readable_source(profile.root, relative)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise OrchestrateError(f"Configured source is not UTF-8: {relative}", code="source_encoding") from exc


def read_project_text(profile: ProjectProfile, relative: str) -> str:
    """Read a reader-resolved in-project source through the same safe boundary."""

    _, raw = _assert_readable_source(profile.root, relative)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise OrchestrateError(f"Reader source is not UTF-8: {relative}", code="source_encoding") from exc
