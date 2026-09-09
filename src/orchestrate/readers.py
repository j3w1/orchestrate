"""Project-specific bounded readers selected by the project profile."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any

from .errors import OrchestrateError
from .profile import ProjectProfile
from .safeio import approved_project_path
from .sources import read_project_text, read_source_text


CE_TASK_ID = re.compile(r"\bCE-\d{4,}\b")
MARKDOWN_LINK = re.compile(r"\[[^]]*\]\(([^)]+)\)")
BACKTICK_PATH = re.compile(r"`([^`]+\.(?:md|json))`")


@dataclass(frozen=True, slots=True)
class ReaderResult:
    kind: str
    instructions: dict[str, str]
    task_sources: dict[str, str]
    command_manifests: dict[str, str]
    routing: dict[str, Any]
    consulted_paths: frozenset[str]


def _configured_text(profile: ProjectProfile, key: str) -> dict[str, str]:
    return {relative: read_source_text(profile, relative) for relative in profile.value[key]}


def _relative_target(profile: ProjectProfile, source: str, target: str) -> str | None:
    clean = target.split("#", 1)[0].strip()
    if not clean or "://" in clean or clean.startswith("mailto:"):
        return None
    raw_candidates = [(Path(source).parent / clean), Path(clean)]
    missing: str | None = None
    for raw_candidate in raw_candidates:
        normalized = Path(os.path.normpath(os.fspath(raw_candidate)))
        if normalized.is_absolute() or ".." in normalized.parts:
            continue
        relative = normalized.as_posix()
        try:
            candidate = approved_project_path(profile.root, relative, require_file=False)
        except OrchestrateError as exc:
            if exc.code == "source_boundary_unresolved":
                raise
            continue
        try:
            candidate.relative_to(profile.root.resolve())
        except ValueError:
            continue
        if candidate.is_file():
            return relative
        missing = missing or relative
    if missing is not None:
        return missing
    raise OrchestrateError("CE reader route escapes the project", code="ce_route_invalid")


def _line_targets(profile: ProjectProfile, source: str, line: str) -> list[str]:
    values = [*MARKDOWN_LINK.findall(line), *BACKTICK_PATH.findall(line)]
    result: list[str] = []
    for value in values:
        relative = _relative_target(profile, source, value)
        if relative and (profile.root / relative).is_file():
            result.append(relative)
    return result


def _contains_exact_task(value: object, task_id: str) -> bool:
    if isinstance(value, str):
        return task_id in set(CE_TASK_ID.findall(value))
    if isinstance(value, dict):
        return any(_contains_exact_task(key, task_id) or _contains_exact_task(nested, task_id) for key, nested in value.items())
    if isinstance(value, list):
        return any(_contains_exact_task(item, task_id) for item in value)
    return False


def _path_values(value: object) -> list[str]:
    if isinstance(value, str):
        clean = value.split("#", 1)[0].lower()
        return [value] if clean.endswith((".md", ".json")) else []
    if isinstance(value, dict):
        return [path for nested in value.values() for path in _path_values(nested)]
    if isinstance(value, list):
        return [path for nested in value for path in _path_values(nested)]
    return []


def _manifest_routes(value: object, task_id: str) -> list[str]:
    routes: list[str] = []
    if isinstance(value, dict):
        direct_match = any(
            _contains_exact_task(key, task_id)
            or (not isinstance(nested, (dict, list)) and _contains_exact_task(nested, task_id))
            for key, nested in value.items()
        )
        if direct_match:
            routes.extend(_path_values(value))
        for key, nested in value.items():
            if _contains_exact_task(key, task_id):
                routes.extend(_path_values(nested))
            if isinstance(nested, (dict, list)):
                routes.extend(_manifest_routes(nested, task_id))
    elif isinstance(value, list):
        for item in value:
            routes.extend(_manifest_routes(item, task_id))
    return routes


def _read_ce(profile: ProjectProfile, objective: str | None, instructions: dict[str, str], manifests: dict[str, str]) -> ReaderResult:
    if objective is None:
        raise OrchestrateError("The CE reader requires an exact authorized CE task in the objective", code="ce_task_missing")
    task_ids = sorted(set(CE_TASK_ID.findall(objective)))
    if not task_ids:
        raise OrchestrateError("The CE objective must name an exact CE task", code="ce_task_missing")
    if len(task_ids) != 1:
        raise OrchestrateError("The CE objective must name exactly one CE task", code="ce_task_ambiguous")
    task_id = task_ids[0]
    registry_path = "docs/tasks/README.md"
    manifest_path = "docs/project-log/manifest.json"
    configured = set(profile.value["taskEntrypoints"])
    if not {registry_path, manifest_path}.issubset(configured):
        raise OrchestrateError(
            "The CE reader requires the task registry and project-log manifest entrypoints",
            code="ce_entrypoints_missing",
        )
    registry = read_source_text(profile, registry_path)
    matching_lines = [line for line in registry.splitlines() if task_id in set(CE_TASK_ID.findall(line))]
    task_targets = sorted({target for line in matching_lines for target in _line_targets(profile, registry_path, line)})
    if len(task_targets) != 1:
        raise OrchestrateError(
            f"The CE registry did not resolve exactly one source for {task_id}",
            code="ce_task_unresolved",
        )
    task_path = task_targets[0]
    task_text = read_project_text(profile, task_path)
    context_lines = [
        line for line in task_text.splitlines()
        if "context" in line.lower() and ("packet" in line.lower() or "must read" in line.lower())
    ]
    context_targets = sorted({target for line in context_lines for target in _line_targets(profile, task_path, line)})
    if not context_targets:
        raise OrchestrateError(
            f"The CE task {task_id} has no resolvable required Context packet",
            code="ce_context_unresolved",
        )
    context_documents = {path: read_project_text(profile, path) for path in context_targets}

    raw_manifest = read_source_text(profile, manifest_path)
    try:
        manifest_value = json.loads(raw_manifest)
    except json.JSONDecodeError as exc:
        raise OrchestrateError("The project-log manifest is invalid JSON", code="ce_manifest_invalid") from exc
    log_targets: list[str] = []
    for raw_target in _manifest_routes(manifest_value, task_id):
        relative = _relative_target(profile, manifest_path, raw_target)
        if relative and (profile.root / relative).is_file():
            log_targets.append(relative)
    log_targets = sorted(set(log_targets))
    if not log_targets:
        raise OrchestrateError(
            f"The project-log manifest has no exact route for {task_id}",
            code="ce_log_unresolved",
        )
    log_documents = {path: read_project_text(profile, path) for path in log_targets}
    task_sources = {
        registry_path: registry,
        manifest_path: raw_manifest,
        task_path: task_text,
        **context_documents,
        **log_documents,
    }
    consulted = frozenset({*instructions, *manifests, *task_sources})
    return ReaderResult(
        "ce-gd",
        instructions,
        task_sources,
        manifests,
        {
            "taskId": task_id,
            "registry": registry_path,
            "task": task_path,
            "contextPackets": context_targets,
            "logManifest": manifest_path,
            "logs": log_targets,
        },
        consulted,
    )


def read_project(profile: ProjectProfile, objective: str | None = None) -> ReaderResult:
    instructions = _configured_text(profile, "instructions")
    manifests = _configured_text(profile, "commandManifests")
    kind = profile.value["reader"]["kind"]
    if kind == "ce-gd":
        return _read_ce(profile, objective, instructions, manifests)
    if kind != "repo":
        raise OrchestrateError(f"Unsupported reader: {kind}", code="profile_reader_invalid")
    tasks = _configured_text(profile, "taskEntrypoints")
    consulted = frozenset({*instructions, *tasks, *manifests})
    return ReaderResult(
        "repo",
        instructions,
        tasks,
        manifests,
        {"authority": list(instructions), "tasks": list(tasks)},
        consulted,
    )
