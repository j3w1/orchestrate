"""Bounded source readers and exact candidate identities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import subprocess
from types import MappingProxyType
from typing import Any, Literal

from .errors import OrchestrateError
from .profile import (
    INSTRUCTION_NAMES,
    MAX_GIT_PATH_BYTES,
    PROFILE_NAME,
    PROFILE_SELECTION_ACKNOWLEDGMENT_SOURCE,
    ProjectProfile,
)
from .safeio import (
    MAX_SOURCE_BYTES,
    ProjectSourceState,
    approved_project_path,
    is_sensitive_source,
    project_source_error_state,
    project_source_failure_state,
    project_source_state,
    read_project_bytes,
)


SOURCE_INDEX_SCHEMA = "orchestrate-source-index/v1"
SOURCE_RECORD_FIELDS = frozenset(
    {
        "path",
        "state",
        "sha256",
        "bytes",
        "headSha256",
        "indexSha256",
        "authority",
    }
)
SOURCE_AUTHORITIES = frozenset({"consulted", "candidate-restrict-only", "unavailable"})
SHA256 = re.compile(r"[0-9a-f]{64}")


class SourceKind(str, Enum):
    """Why a path belongs to the bounded source snapshot."""

    PROFILE = "profile"
    INSTRUCTION = "instruction"
    TASK_ENTRYPOINT = "task-entrypoint"
    COMMAND_MANIFEST = "command-manifest"
    CANDIDATE = "candidate"
    READER_ROUTED = "reader-routed"
    MILESTONE_PLAN = "milestone-plan"
    PACKET_BOUND = "packet-bound"


class SourceAccess(str, Enum):
    """Whether a reference may be consumed, and which proof must precede it."""

    REFERENCE_ONLY = "reference-only"
    DIRECT_BYTES = "direct-bytes"
    INVENTORY_GUARDED_BYTES = "inventory-guarded-bytes"


@dataclass(frozen=True, slots=True)
class SourceRevalidation:
    """Source-local result for a completed identity recheck."""

    state: ProjectSourceState
    path: str | None = None


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """Strict in-memory decoding of the unchanged source-index/v1 wire record."""

    path: str
    state: str
    sha256: str | None
    byte_count: int
    head_sha256: str | None
    index_sha256: str | None
    authority: str

    @classmethod
    def from_wire(cls, value: object) -> "SourceRecord":
        if not isinstance(value, Mapping) or set(value) != SOURCE_RECORD_FIELDS:
            raise OrchestrateError(
                "Worker packet source identity is malformed",
                code="packet_identity_conflict",
            )
        path = value.get("path")
        state = value.get("state")
        sha256 = value.get("sha256")
        byte_count = value.get("bytes")
        head_sha256 = value.get("headSha256")
        index_sha256 = value.get("indexSha256")
        authority = value.get("authority")
        hash_values = (sha256, head_sha256, index_sha256)
        if (
            not isinstance(path, str)
            or not path
            or "\\" in path
            or PureWindowsPath(path).drive
            or Path(path).is_absolute()
            or path != Path(path).as_posix()
            or any(part in {"", ".", ".."} for part in Path(path).parts)
            or not isinstance(state, str)
            or not state
            or any(
                item is not None
                and (not isinstance(item, str) or SHA256.fullmatch(item) is None)
                for item in hash_values
            )
            or type(byte_count) is not int
            or byte_count < 0
            or authority not in SOURCE_AUTHORITIES
            or (sha256 is None and byte_count != 0)
            or (sha256 is None and authority != "unavailable")
            or (sha256 is not None and authority == "unavailable")
        ):
            raise OrchestrateError(
                "Worker packet source identity is malformed",
                code="packet_identity_conflict",
            )
        return cls(
            path,
            state,
            sha256,
            byte_count,
            head_sha256,
            index_sha256,
            authority,
        )

@dataclass(frozen=True, slots=True)
class SourceReference:
    """One path's membership and byte-read eligibility, derived once."""

    path: str
    kinds: frozenset[SourceKind]
    access: SourceAccess
    packet_record: SourceRecord | None

    @property
    def instruction_class(self) -> bool:
        return SourceKind.INSTRUCTION in self.kinds


@dataclass(frozen=True, slots=True)
class PreparedSourceSet:
    """Completed packet-source stage consumed by indexing and the reader."""

    references: tuple[SourceReference, ...]
    instruction_inventory: tuple[str, ...]
    raw_by_path: Mapping[str, bytes | None]

    @classmethod
    def create(
        cls,
        references: tuple[SourceReference, ...],
        instruction_inventory: tuple[str, ...],
        raw_by_path: Mapping[str, bytes | None],
    ) -> "PreparedSourceSet":
        return cls(references, instruction_inventory, MappingProxyType(dict(raw_by_path)))

    @property
    def paths(self) -> frozenset[str]:
        return frozenset(reference.path for reference in self.references)

    def text(self, path: str) -> str:
        if path not in self.raw_by_path:
            raise OrchestrateError(
                f"Reader requested a source outside the completed packet stage: {path}",
                code="packet_identity_conflict",
            )
        raw = self.raw_by_path[path]
        if raw is None:
            raise OrchestrateError(f"Required source is unavailable: {path}", code="source_unavailable")
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OrchestrateError(f"Configured source is not UTF-8: {path}", code="source_encoding") from exc

    def identities(self, paths: set[str] | frozenset[str]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for path in sorted(paths):
            if path not in self.raw_by_path:
                raise OrchestrateError(
                    f"Source identity was not completed before its consumer: {path}",
                    code="packet_identity_conflict",
                )
            raw = self.raw_by_path[path]
            result[path] = {
                "sha256": _sha256(raw) if raw is not None else None,
                "bytes": len(raw) if raw is not None else 0,
            }
        return result

    def revalidate(
        self,
        root: Path,
        paths: set[str] | frozenset[str],
    ) -> SourceRevalidation:
        """Recheck completed identities while preserving absence vs unavailability."""

        for path, expected in self.identities(paths).items():
            try:
                raw = read_project_bytes(root, path)
            except OrchestrateError as exc:
                carried_state = project_source_failure_state(exc)
                if carried_state is not None:
                    return SourceRevalidation(carried_state, path)
                if exc.code == "source_unavailable":
                    return SourceRevalidation(ProjectSourceState.UNAVAILABLE, path)
                if exc.code in {
                    "source_boundary_unresolved",
                    "source_identity_changed",
                    "source_too_large",
                }:
                    return SourceRevalidation(ProjectSourceState.CHANGED, path)
                raise
            except OSError as exc:
                return SourceRevalidation(project_source_error_state(exc), path)
            if {"sha256": _sha256(raw), "bytes": len(raw)} != expected:
                return SourceRevalidation(ProjectSourceState.CHANGED, path)
        return SourceRevalidation(ProjectSourceState.PRESENT)


def decode_source_records(value: object) -> tuple[SourceRecord, ...]:
    if not isinstance(value, list):
        raise OrchestrateError("Worker packet source identity is malformed", code="packet_identity_conflict")
    records = tuple(SourceRecord.from_wire(item) for item in value)
    paths = [record.path for record in records]
    if len(paths) != len(set(paths)):
        raise OrchestrateError("Worker packet source identity is malformed", code="packet_identity_conflict")
    return records


def classify_source_reference(
    profile: ProjectProfile,
    path: str,
    *,
    packet_record: SourceRecord | None = None,
    reader_routed: bool = False,
    milestone_plan: bool = False,
    packet_bound: bool = False,
) -> SourceReference:
    """Derive source membership and read permission in one shared model."""

    kinds: set[SourceKind] = set()
    if path == PROFILE_NAME:
        kinds.add(SourceKind.PROFILE)
    if path in profile.value["instructions"] or Path(path).name in INSTRUCTION_NAMES:
        kinds.add(SourceKind.INSTRUCTION)
    if path in profile.value["taskEntrypoints"]:
        kinds.add(SourceKind.TASK_ENTRYPOINT)
    if path in profile.value["commandManifests"]:
        kinds.add(SourceKind.COMMAND_MANIFEST)
    if path in profile.value.get("candidateSources", []):
        kinds.add(SourceKind.CANDIDATE)
    if reader_routed:
        kinds.add(SourceKind.READER_ROUTED)
    if milestone_plan:
        kinds.add(SourceKind.MILESTONE_PLAN)
    if packet_bound:
        kinds.add(SourceKind.PACKET_BOUND)
    if packet_record is not None and packet_record.sha256 is None:
        access = SourceAccess.REFERENCE_ONLY
    elif SourceKind.INSTRUCTION not in kinds:
        access = SourceAccess.DIRECT_BYTES
    elif (
        path in profile.value["instructions"]
        and profile.selection_source == PROFILE_SELECTION_ACKNOWLEDGMENT_SOURCE
    ):
        access = SourceAccess.DIRECT_BYTES
    else:
        access = SourceAccess.INVENTORY_GUARDED_BYTES
    return SourceReference(path, frozenset(kinds), access, packet_record)


def _git(root: Path, *arguments: str, allow_failure: bool = False) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(root), *arguments),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("Git inspection is unavailable", code="git_inspection_failed") from exc
    if completed.returncode and not allow_failure:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise OrchestrateError(f"Git inspection failed: {detail}", code="git_inspection_failed")
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError("Git inspection exceeded its output boundary", code="git_inspection_failed")
    try:
        return completed.stdout.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise OrchestrateError("Git output is not strict UTF-8", code="git_path_encoding") from exc


def _git_bytes(root: Path, *arguments: str) -> bytes:
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(root), *arguments),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("Git inspection is unavailable", code="git_inspection_failed") from exc
    if completed.returncode:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise OrchestrateError(f"Git inspection failed: {detail}", code="git_inspection_failed")
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError("Git inspection exceeded its output boundary", code="git_inspection_failed")
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
    try:
        raw = read_project_bytes(root, relative)
    except OrchestrateError:
        raise
    except OSError as exc:
        state = project_source_error_state(exc)
        code = (
            "source_identity_changed"
            if state == ProjectSourceState.CHANGED
            else "source_unavailable"
        )
        raise OrchestrateError(
            f"Required source cannot be read safely: {relative}",
            code=code,
        ) from exc
    return root / relative, raw


def _git_source_presence(root: Path, kind: Literal["head", "index"], relative: str) -> bool:
    arguments = (
        ("ls-tree", "-z", "--full-tree", "HEAD", "--", relative)
        if kind == "head"
        else ("ls-files", "--stage", "-z", "--", relative)
    )
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(root), *arguments),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("Git source inspection is unavailable", code="git_inspection_failed") from exc
    if completed.returncode:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise OrchestrateError(f"Git source inspection failed: {detail}", code="git_inspection_failed")
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError("Git source inspection exceeded its output boundary", code="git_inspection_failed")
    if kind == "index" and completed.stdout:
        stages = {
            entry.split(b"\t", 1)[0].rsplit(b" ", 1)[-1]
            for entry in completed.stdout.rstrip(b"\0").split(b"\0")
        }
        if b"0" not in stages:
            raise OrchestrateError(
                "Git index source has no canonical stage-zero object",
                code="source_binding_changed",
            )
    return bool(completed.stdout)


def _head_blob(root: Path, relative: str) -> bytes | None:
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(root), "show", f"HEAD:{relative}"),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("Git source inspection is unavailable", code="git_inspection_failed") from exc
    if completed.returncode == 0:
        if len(completed.stdout) > MAX_SOURCE_BYTES:
            raise OrchestrateError("Git source exceeds the bounded read limit", code="source_too_large")
        return completed.stdout
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError("Git source inspection exceeded its output boundary", code="git_inspection_failed")
    if not _git_source_presence(root, "head", relative):
        return None
    detail = completed.stderr.decode("utf-8", errors="replace").strip()
    raise OrchestrateError(f"Git source inspection failed: {detail}", code="git_inspection_failed")


@dataclass(frozen=True, slots=True)
class SourceIndex:
    value: dict[str, Any]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return self.value


def _index_bytes(root: Path, relative: str) -> bytes | None:
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(root), "show", f":{relative}"),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("Git source inspection is unavailable", code="git_inspection_failed") from exc
    if completed.returncode == 0:
        if len(completed.stdout) > MAX_SOURCE_BYTES:
            raise OrchestrateError("Git source exceeds the bounded read limit", code="source_too_large")
        return completed.stdout
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError("Git source inspection exceeded its output boundary", code="git_inspection_failed")
    if not _git_source_presence(root, "index", relative):
        return None
    detail = completed.stderr.decode("utf-8", errors="replace").strip()
    raise OrchestrateError(f"Git source inspection failed: {detail}", code="git_inspection_failed")


def build_source_index(
    profile: ProjectProfile,
    *,
    extra_sources: set[str] | None = None,
    milestone_sources: set[str] | None = None,
    prepared: PreparedSourceSet | None = None,
) -> SourceIndex:
    root = profile.root
    for relative in profile.value["instructions"]:
        if is_sensitive_source(relative):
            raise OrchestrateError(
                f"Secret-bearing source is excluded: {relative}",
                code="secret_source_excluded",
            )
    instructions = set(
        prepared.instruction_inventory
        if prepared is not None
        else profile.require_instruction_acknowledgment()
    )
    status = _status_map(root)
    configured = {
        *profile.value["instructions"],
        *instructions,
        *profile.value["taskEntrypoints"],
        *profile.value["commandManifests"],
        PROFILE_NAME,
        *(extra_sources or set()),
        *(milestone_sources or set()),
        *profile.value.get("candidateSources", []),
    }
    prepared_references = {
        reference.path: reference
        for reference in (prepared.references if prepared is not None else ())
    }
    references = {
        relative: prepared_references.get(relative) or classify_source_reference(
            profile,
            relative,
            reader_routed=relative in (extra_sources or set()),
            milestone_plan=relative in (milestone_sources or set()),
        )
        for relative in configured
    }
    records: list[dict[str, Any]] = []
    for relative in sorted(configured):
        path_state = project_source_state(root, relative)
        if path_state == ProjectSourceState.UNAVAILABLE:
            raise OrchestrateError(
                f"Configured source cannot currently be observed: {relative}",
                code="source_temporarily_unavailable",
            )
        if path_state == ProjectSourceState.CHANGED:
            raise OrchestrateError(
                f"Configured source identity changed: {relative}",
                code="source_binding_changed",
            )
        if path_state == ProjectSourceState.ABSENT:
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
        if prepared is not None and relative in prepared.raw_by_path:
            raw = prepared.raw_by_path[relative]
            if raw is None:
                raise OrchestrateError(
                    "A reference-only packet source became available during validation",
                    code="source_binding_changed",
                )
        else:
            _, raw = _assert_readable_source(root, relative)
        head = _head_blob(root, relative)
        changed_authority = relative == PROFILE_NAME and head != raw
        changed_instruction = (
            references[relative].instruction_class
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
