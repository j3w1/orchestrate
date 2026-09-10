"""Small, reviewable project profile discovery and validation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any
import uuid

from .errors import OrchestrateError
from .safeio import approved_project_path, read_project_bytes
from .state import RunLock, make_private_state_directory, project_key, require_private_state_target, state_home, utc_now


PROFILE_SCHEMA = "orchestrate-profile/v1"
PROFILE_NAME = ".orchestrate.json"
PROFILE_SELECTION_SCHEMA = "orchestrate-operational-profile-selection/v1"
INSTRUCTION_NAMES = ("AGENTS.md", "CLAUDE.md")
MAX_INSTRUCTION_PATHS = 256
MAX_GIT_PATH_BYTES = 4 * 1024 * 1024
TASK_ENTRYPOINTS = (
    "CURRENT.md",
    "docs/implementation-plan.md",
    "docs/tasks.md",
    "TASKS.md",
    "docs/tasks/README.md",
    "docs/project-log/manifest.json",
)
COMMAND_MANIFESTS = (
    "pyproject.toml",
    "package.json",
    "Makefile",
    "Taskfile.yml",
    "justfile",
)


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise OrchestrateError(
            f"Profile entry escapes the project root: {path}",
            code="profile_path_escape",
        ) from exc


def _find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    raise OrchestrateError(
        f"No Git worktree contains {start}",
        code="project_not_found",
    )


def find_project_root(start: Path) -> Path:
    """Resolve repository identity without reading or trusting its profile bytes."""

    return _find_repo_root(start)


def _git_bytes(root: Path, *arguments: str, allow_failure: bool = False) -> bytes | None:
    completed = subprocess.run(
        ("git", "-C", os.fspath(root), *arguments),
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        if allow_failure:
            return None
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise OrchestrateError(f"Git instruction inventory failed: {detail}", code="git_inspection_failed")
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError(
            "Git instruction inventory exceeded its bounded output limit",
            code="instruction_inventory_too_large",
        )
    return completed.stdout


def instruction_inventory(root: Path) -> tuple[str, ...]:
    """List conventional tracked, untracked, and ignored instruction files."""

    repo = _find_repo_root(root)
    commands = (
        ("ls-files", "-z", "--cached"),
        ("ls-files", "-z", "--others", "--exclude-standard"),
        ("ls-files", "-z", "--others", "--ignored", "--exclude-standard"),
    )
    paths: set[str] = set()
    for arguments in commands:
        raw = _git_bytes(repo, *arguments)
        assert raw is not None
        try:
            names = raw.decode("utf-8", errors="strict").split("\0")
        except UnicodeDecodeError as exc:
            raise OrchestrateError("Git instruction paths are not strict UTF-8", code="git_path_encoding") from exc
        paths.update(
            name.replace("\\", "/")
            for name in names
            if name and Path(name).name in INSTRUCTION_NAMES
        )
        if len(paths) > MAX_INSTRUCTION_PATHS:
            raise OrchestrateError(
                f"More than {MAX_INSTRUCTION_PATHS} conventional instruction files require an explicit scope decision",
                code="instruction_inventory_too_large",
            )
    return tuple(sorted(paths))


def _profile_candidate(
    root: Path,
    *,
    require_sources: bool = True,
) -> tuple[bytes, dict[str, Any]]:
    path = root / PROFILE_NAME
    if not path.is_file():
        raise OrchestrateError(
            f"{PROFILE_NAME} is missing; run 'orchestrate setup' first",
            code="profile_missing",
        )
    try:
        raw = read_project_bytes(root, PROFILE_NAME)
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError(f"Cannot read {path}: {exc}", code="profile_unreadable") from exc
    return raw, validate_profile(value, root=root, require_sources=require_sources)


def _selection_path(root: Path) -> Path:
    home = state_home()
    directory = require_private_state_target(
        root,
        home / "operational-profiles" / project_key(root),
    )
    return directory / "selection.json"


def _selection_record(root: Path) -> tuple[dict[str, Any], str]:
    path = _selection_path(root)
    try:
        raw = path.read_bytes()
        record = json.loads(raw)
    except FileNotFoundError as exc:
        raise OrchestrateError(
            "No host-local operational profile selection exists; run orchestrate setup explicitly",
            code="profile_selection_missing",
        ) from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError(
            "The host-local operational profile selection history is unreadable",
            code="profile_selection_invalid",
        ) from exc
    if (
        not isinstance(record, dict)
        or record.get("schema") != PROFILE_SELECTION_SCHEMA
        or record.get("projectKey") != project_key(root)
        or not isinstance(record.get("activeDigest"), str)
        or not isinstance(record.get("history"), list)
        or not record["history"]
    ):
        raise OrchestrateError(
            "The host-local operational profile selection history is invalid",
            code="profile_selection_invalid",
        )
    active: dict[str, Any] | None = None
    for entry in record["history"]:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("digest"), str)
            or not isinstance(entry.get("selectedRaw"), str)
            or not isinstance(entry.get("selectedAt"), str)
            or entry.get("source") not in {"initial-setup-selection", "explicit-configuration-acknowledgment"}
        ):
            raise OrchestrateError(
                "The host-local operational profile selection history is invalid",
                code="profile_selection_invalid",
            )
        selected_raw = entry["selectedRaw"].encode("utf-8")
        if hashlib.sha256(selected_raw).hexdigest() != entry["digest"]:
            raise OrchestrateError(
                "The host-local operational profile selection digest does not match its source bytes",
                code="profile_selection_invalid",
            )
        try:
            decoded = json.loads(selected_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OrchestrateError(
                "The selected operational profile source is invalid",
                code="profile_selection_invalid",
            ) from exc
        if not isinstance(decoded, dict) or decoded.get("schema") != PROFILE_SCHEMA:
            raise OrchestrateError(
                "The selected operational profile source is invalid",
                code="profile_selection_invalid",
            )
        if entry["digest"] == record["activeDigest"]:
            active = entry
    if active is None:
        raise OrchestrateError(
            "The active operational profile digest is absent from its selection history",
            code="profile_selection_invalid",
        )
    return record, hashlib.sha256(raw).hexdigest()


def _write_profile_selection(
    root: Path,
    raw: bytes,
    *,
    acknowledge: bool,
    allow_initial: bool,
) -> None:
    path = _selection_path(root)
    make_private_state_directory(root, path.parent)
    with RunLock(path.parent / "selection.lock"):
        _write_profile_selection_locked(
            root,
            path,
            raw,
            acknowledge=acknowledge,
            allow_initial=allow_initial,
        )


def _write_profile_selection_locked(
    root: Path,
    path: Path,
    raw: bytes,
    *,
    acknowledge: bool,
    allow_initial: bool,
) -> None:
    digest = hashlib.sha256(raw).hexdigest()
    if path.exists():
        record, _ = _selection_record(root)
        if record["activeDigest"] == digest:
            return
        if not acknowledge:
            raise OrchestrateError(
                "The project profile differs from the selected operational configuration; review it and rerun setup with --acknowledge-profile",
                code="profile_selection_changed",
                data={"selectedDigest": record["activeDigest"], "candidateDigest": digest},
            )
        source = "explicit-configuration-acknowledgment"
    else:
        if not allow_initial and not acknowledge:
            raise OrchestrateError(
                "An existing project profile has no selection history; review it and rerun setup with --acknowledge-profile",
                code="profile_selection_missing",
            )
        record = {
            "schema": PROFILE_SELECTION_SCHEMA,
            "projectKey": project_key(root),
            "activeDigest": digest,
            "history": [],
        }
        source = "initial-setup-selection" if allow_initial else "explicit-configuration-acknowledgment"
    record["activeDigest"] = digest
    record["history"].append(
        {
            "digest": digest,
            "selectedRaw": raw.decode("utf-8"),
            "selectedAt": utc_now(),
            "source": source,
        }
    )
    encoded = _canonical_json(record)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(encoded)
        try:
            temporary.chmod(0o600)
        except OSError:
            pass
        temporary.replace(path)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise OrchestrateError(
            "The host-local operational profile selection could not be written",
            code="profile_selection_unavailable",
        ) from exc


def discover_profile(root: Path) -> dict[str, Any]:
    """Perform bounded name-based discovery without running hooks or checks."""

    repo = _find_repo_root(root)
    instructions = list(instruction_inventory(repo))
    tasks = [name for name in TASK_ENTRYPOINTS if (repo / name).is_file()]
    manifests = [name for name in COMMAND_MANIFESTS if (repo / name).is_file()]
    reader = (
        "ce-gd"
        if {"docs/tasks/README.md", "docs/project-log/manifest.json"}.issubset(tasks)
        else "repo"
    )
    return {
        "schema": PROFILE_SCHEMA,
        "project": {"kind": "git", "root": "."},
        "reader": {"kind": reader},
        "instructions": instructions,
        "taskEntrypoints": tasks,
        "commandManifests": manifests,
        "candidateSources": [],
        "checks": [],
        "strategy": {"kind": "single-owner-first", "maxWorkers": 3},
    }


def validate_profile(
    value: object,
    *,
    root: Path,
    require_sources: bool = True,
) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema") != PROFILE_SCHEMA:
        raise OrchestrateError(
            f"{PROFILE_NAME} must use schema {PROFILE_SCHEMA}",
            code="profile_schema_unsupported",
        )
    project = value.get("project")
    if not isinstance(project, dict) or project != {"kind": "git", "root": "."}:
        raise OrchestrateError(
            "The v1 profile must identify the containing Git project with root '.'",
            code="profile_project_invalid",
        )
    reader = value.get("reader")
    if not isinstance(reader, dict) or reader.get("kind") not in {"repo", "ce-gd"}:
        raise OrchestrateError("Profile reader must be 'repo' or 'ce-gd'", code="profile_reader_invalid")
    if "candidateSources" not in value:
        value["candidateSources"] = []
    for key in ("instructions", "taskEntrypoints", "commandManifests", "candidateSources", "checks"):
        entries = value.get(key)
        if not isinstance(entries, list) or not all(isinstance(item, str) for item in entries):
            raise OrchestrateError(f"Profile field {key} must be a string array", code="profile_invalid")
        if key == "checks":
            if any(not entry.strip() or "\x00" in entry for entry in entries):
                raise OrchestrateError("Check commands must be non-empty strings", code="profile_invalid")
            continue
        for entry in entries:
            try:
                candidate = approved_project_path(
                    root,
                    entry,
                    require_file=require_sources and key != "candidateSources",
                )
                _relative(root.resolve(), candidate)
            except OrchestrateError:
                raise
            if (root / entry).exists() and not candidate.is_file():
                raise OrchestrateError(
                    f"Required profile source is unavailable: {entry}",
                    code="profile_source_unavailable",
                )
    strategy = value.get("strategy")
    if not isinstance(strategy, dict) or strategy.get("kind") != "single-owner-first":
        raise OrchestrateError("Only single-owner-first is implemented", code="profile_strategy_invalid")
    max_workers = strategy.get("maxWorkers")
    if not isinstance(max_workers, int) or not 1 <= max_workers <= 32:
        raise OrchestrateError("strategy.maxWorkers must be an integer from 1 to 32", code="profile_invalid")
    return value


@dataclass(frozen=True, slots=True)
class ProjectProfile:
    root: Path
    path: Path
    value: dict[str, Any]
    digest: str
    candidate_digest: str
    selection_source: str
    selection_history_digest: str
    candidate_changed: bool

    @classmethod
    def load(
        cls,
        root: Path,
        *,
        require_sources: bool = True,
    ) -> "ProjectProfile":
        repo = _find_repo_root(root)
        path = repo / PROFILE_NAME
        raw, _candidate = _profile_candidate(repo, require_sources=require_sources)
        candidate_digest = hashlib.sha256(raw).hexdigest()
        selection, selection_history_digest = _selection_record(repo)
        matching = [
            entry for entry in selection["history"]
            if entry["digest"] == selection["activeDigest"]
        ]
        selected = matching[-1]
        selected_raw = selected["selectedRaw"].encode("utf-8")
        try:
            selected_value = json.loads(selected_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OrchestrateError(
                "The selected operational profile source cannot be decoded",
                code="profile_selection_invalid",
            ) from exc
        operational = validate_profile(selected_value, root=repo, require_sources=require_sources)
        return cls(
            repo,
            path,
            operational,
            selection["activeDigest"],
            candidate_digest,
            selected["source"],
            selection_history_digest,
            raw != selected_raw,
        )


def setup_project(
    root: Path,
    *,
    force: bool = False,
    acknowledge_profile: bool = False,
) -> ProjectProfile:
    repo = _find_repo_root(root)
    path = repo / PROFILE_NAME
    created = not path.exists()
    if created or force:
        approved_project_path(repo, PROFILE_NAME, require_file=False)
        raw = _canonical_json(discover_profile(repo))
        try:
            path.write_bytes(raw)
        except OSError as exc:
            raise OrchestrateError(f"Cannot write {path}: {exc}", code="profile_write_failed") from exc
    raw, _ = _profile_candidate(repo)
    _write_profile_selection(
        repo,
        raw,
        acknowledge=acknowledge_profile,
        allow_initial=created,
    )
    return ProjectProfile.load(repo)
