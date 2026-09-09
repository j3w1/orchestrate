"""Small, reviewable project profile discovery and validation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from .errors import OrchestrateError
from .safeio import approved_project_path, read_project_bytes


PROFILE_SCHEMA = "orchestrate-profile/v1"
PROFILE_NAME = ".orchestrate.json"
INSTRUCTION_NAMES = ("AGENTS.md", "CLAUDE.md")
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


def discover_profile(root: Path) -> dict[str, Any]:
    """Perform bounded name-based discovery without running hooks or checks."""

    repo = _find_repo_root(root)
    instructions = [name for name in INSTRUCTION_NAMES if (repo / name).is_file()]
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


def validate_profile(value: object, *, root: Path) -> dict[str, Any]:
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
                    require_file=key != "candidateSources",
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

    @classmethod
    def load(cls, root: Path) -> "ProjectProfile":
        repo = _find_repo_root(root)
        path = repo / PROFILE_NAME
        if not path.is_file():
            raise OrchestrateError(
                f"{PROFILE_NAME} is missing; run 'orchestrate setup' first",
                code="profile_missing",
            )
        try:
            raw = read_project_bytes(repo, PROFILE_NAME)
            value = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OrchestrateError(f"Cannot read {path}: {exc}", code="profile_unreadable") from exc
        validated = validate_profile(value, root=repo)
        return cls(repo, path, validated, hashlib.sha256(raw).hexdigest())


def setup_project(root: Path, *, force: bool = False) -> ProjectProfile:
    repo = _find_repo_root(root)
    path = repo / PROFILE_NAME
    if path.exists() and not force:
        return ProjectProfile.load(repo)
    approved_project_path(repo, PROFILE_NAME, require_file=False)
    value = discover_profile(repo)
    raw = _canonical_json(value)
    try:
        path.write_bytes(raw)
    except OSError as exc:
        raise OrchestrateError(f"Cannot write {path}: {exc}", code="profile_write_failed") from exc
    return ProjectProfile(repo, path, validate_profile(value, root=repo), hashlib.sha256(raw).hexdigest())
