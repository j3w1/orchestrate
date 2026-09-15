"""Project-specific bounded readers selected by the project profile."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any

from .errors import OrchestrateError
from .profile import MAX_GIT_PATH_BYTES, ProjectProfile
from .safeio import ProjectSourceState, approved_project_path, project_source_state
from .sources import PreparedSourceSet, SourceRevalidation, read_project_text, read_source_text


CE_TASK_ID = re.compile(r"\bCE-[0-9]{4}\b")
MARKDOWN_LINK = re.compile(r"\[[^]]*\]\(([^)]+)\)")
BACKTICK_PATH = re.compile(r"`([^`]+\.(?:md|json))`")
CE_MANIFEST_KIND = "ce-systems-project-log-manifest"
CE_QUERY_COMMAND = "pnpm run project-log:query -- <term>"
CE_QUERY_SCRIPT = "scripts/quality/project-log.mjs"
CE_QUERY_PACKAGE_SCRIPT = "node scripts/quality/project-log.mjs query"
CE_QUERY_MAX_ENTRIES = 12
CE_QUERY_MAX_BYTES = 32768
CE_QUERY_STDOUT_LIMIT = 65536
CE_QUERY_SOURCES = ("package.json", CE_QUERY_SCRIPT, "docs/project-log/manifest.json")


@dataclass(frozen=True, slots=True)
class ReaderResult:
    kind: str
    instructions: dict[str, str]
    task_sources: dict[str, str]
    command_manifests: dict[str, str]
    routing: dict[str, Any]
    consulted_paths: frozenset[str]


def _source_text(
    profile: ProjectProfile,
    relative: str,
    prepared_sources: PreparedSourceSet | None,
    *,
    configured: bool = False,
) -> str:
    if prepared_sources is not None:
        return prepared_sources.text(relative)
    return read_source_text(profile, relative) if configured else read_project_text(profile, relative)


def _configured_text(
    profile: ProjectProfile,
    key: str,
    prepared_sources: PreparedSourceSet | None,
) -> dict[str, str]:
    return {
        relative: _source_text(profile, relative, prepared_sources, configured=True)
        for relative in profile.value[key]
    }


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
            if exc.code == "source_unavailable":
                raise OrchestrateError(
                    f"Reader source cannot currently be observed: {relative}",
                    code="source_temporarily_unavailable",
                ) from exc
            continue
        try:
            candidate.relative_to(profile.root.resolve())
        except ValueError:
            continue
        source_state = project_source_state(profile.root, relative)
        if source_state == ProjectSourceState.PRESENT:
            return relative
        if source_state == ProjectSourceState.UNAVAILABLE:
            raise OrchestrateError(
                f"Reader source cannot currently be observed: {relative}",
                code="source_temporarily_unavailable",
            )
        missing = missing or relative
    if missing is not None:
        return missing
    raise OrchestrateError("CE reader route escapes the project", code="ce_route_invalid")


def _line_targets(profile: ProjectProfile, source: str, line: str) -> list[str]:
    values = [*MARKDOWN_LINK.findall(line), *BACKTICK_PATH.findall(line)]
    result: list[str] = []
    for value in values:
        relative = _relative_target(profile, source, value)
        if relative:
            source_state = project_source_state(profile.root, relative)
            if source_state == ProjectSourceState.PRESENT:
                result.append(relative)
            elif source_state == ProjectSourceState.UNAVAILABLE:
                raise OrchestrateError(
                    f"Reader source cannot currently be observed: {relative}",
                    code="source_temporarily_unavailable",
                )
    return result


def _manifest_shards(value: object, task_id: str, root: Path) -> list[dict[str, Any]]:
    """Validate the public CE shard manifest and select every exact-task shard."""

    if (
        not isinstance(value, dict)
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("kind") != CE_MANIFEST_KIND
        or not isinstance(value.get("shards"), list)
    ):
        raise OrchestrateError("The CE project-log manifest has an unknown schema", code="ce_manifest_invalid")
    access = value.get("agent_access")
    if (
        not isinstance(access, dict)
        or access.get("default_loading") != "manifest-only"
        or access.get("closed_shards_preloaded") is not False
        or access.get("query_command") != CE_QUERY_COMMAND
    ):
        raise OrchestrateError("The CE project-log query contract is unavailable", code="ce_manifest_invalid")
    required_strings = ("path", "state", "sha256", "git_blob_sha")
    required_integers = (
        "sequence",
        "bytes",
        "entry_count",
        "legacy_start_byte",
        "legacy_end_byte_exclusive",
    )
    selected: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    seen_sequences: set[int] = set()
    for shard in value["shards"]:
        if (
            not isinstance(shard, dict)
            or any(not isinstance(shard.get(key), str) for key in required_strings)
            or any(shard.get(key) is not None and not isinstance(shard.get(key), str) for key in ("first_heading", "last_heading"))
            or any(type(shard.get(key)) is not int or shard[key] < 0 for key in required_integers)
            or not re.fullmatch(r"[0-9a-fA-F]{64}", shard["sha256"])
            or not re.fullmatch(r"[0-9a-fA-F]{40,64}", shard["git_blob_sha"])
            or not isinstance(shard.get("task_ids"), list)
            or not all(isinstance(item, str) and CE_TASK_ID.fullmatch(item) for item in shard["task_ids"])
        ):
            raise OrchestrateError("The CE project-log manifest contains a malformed shard", code="ce_manifest_invalid")
        path = shard["path"]
        if "\\" in path:
            raise OrchestrateError("The CE project-log manifest contains a non-canonical shard path", code="ce_manifest_invalid")
        if (
            path in seen_paths
            or shard["sequence"] in seen_sequences
            or len(set(shard["task_ids"])) != len(shard["task_ids"])
            or shard["legacy_end_byte_exclusive"] < shard["legacy_start_byte"]
        ):
            raise OrchestrateError("The CE project-log manifest repeats or contradicts a shard identity", code="ce_manifest_invalid")
        seen_paths.add(path)
        seen_sequences.add(shard["sequence"])
        if task_id in shard["task_ids"]:
            source_state = project_source_state(root, path)
            if source_state in {ProjectSourceState.ABSENT, ProjectSourceState.CHANGED}:
                raise OrchestrateError(
                    "A manifest-selected project-log shard is absent or changed",
                    code="source_binding_changed",
                    data={"sourcePath": path, "sourceState": source_state.value},
                )
            if source_state == ProjectSourceState.UNAVAILABLE:
                raise OrchestrateError(
                    "A manifest-selected project-log shard cannot currently be observed",
                    code="source_temporarily_unavailable",
                    data={"sourcePath": path, "sourceState": source_state.value},
                )
            selected.append({**shard, "path": path})
    if not selected:
        raise OrchestrateError(
            f"The project-log manifest has no exact shard for {task_id}",
            code="ce_log_unresolved",
        )
    return selected


def _git_source_state(root: Path, paths: list[str]) -> bytes:
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(root), "status", "--porcelain=v1", "-z", "--", *paths),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("CE query source binding could not be inspected", code="ce_query_source_unavailable") from exc
    if completed.returncode:
        raise OrchestrateError("CE query source binding could not be inspected", code="ce_query_source_unavailable")
    if len(completed.stdout) > MAX_GIT_PATH_BYTES:
        raise OrchestrateError("CE query source inspection exceeded its output boundary", code="ce_query_source_unavailable")
    return completed.stdout


def _require_tracked_query_sources(root: Path, paths: list[str]) -> None:
    for path in paths:
        try:
            tracked = subprocess.run(
                ("git", "-C", os.fspath(root), "ls-files", "-z", "--", path),
                capture_output=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise OrchestrateError("The CE query implementation cannot be inspected", code="ce_query_source_unavailable") from exc
        if tracked.returncode:
            raise OrchestrateError("The CE query implementation cannot be inspected", code="ce_query_source_unavailable")
        if len(tracked.stdout) > MAX_GIT_PATH_BYTES:
            raise OrchestrateError("CE query source inspection exceeded its output boundary", code="ce_query_source_unavailable")
        if tracked.stdout != path.encode("utf-8") + b"\0":
            raise OrchestrateError("The CE query implementation is not a tracked source", code="ce_query_source_changed")
    if _git_source_state(root, paths):
        raise OrchestrateError("The CE query implementation or manifest is mutable", code="ce_query_source_changed")


def _query_source_identities(profile: ProjectProfile) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in CE_QUERY_SOURCES:
        try:
            raw = read_project_text(profile, path).encode("utf-8")
        except OrchestrateError as exc:
            if exc.code != "source_unavailable":
                raise
            source_state = project_source_state(profile.root, path)
            if source_state in {ProjectSourceState.PRESENT, ProjectSourceState.UNAVAILABLE}:
                raise OrchestrateError(
                    "A CE query source cannot currently be observed",
                    code="ce_query_source_unavailable",
                    data={"sourcePath": path, "sourceState": source_state.value},
                ) from exc
            raise OrchestrateError(
                "A CE query source is absent or changed",
                code="ce_query_source_changed",
                data={"sourcePath": path, "sourceState": source_state.value},
            ) from exc
        result[path] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    return result


def _require_matching_source_revalidation(
    result: SourceRevalidation,
    *,
    boundary: str,
) -> None:
    if result.state == ProjectSourceState.PRESENT:
        return
    details = {
        "sourcePath": result.path,
        "sourceState": result.state.value,
        "boundary": boundary,
    }
    if result.state == ProjectSourceState.UNAVAILABLE:
        raise OrchestrateError(
            f"The CE query source binding cannot currently be observed {boundary}",
            code="ce_query_source_unavailable",
            data=details,
        )
    raise OrchestrateError(
        f"The CE query source binding changed {boundary}",
        code="ce_query_source_changed",
        data=details,
    )


def _run_ce_query(
    profile: ProjectProfile,
    task_id: str,
    *,
    manifest_text: str,
    package_text: str,
    script_text: str,
    expected_source_identities: Mapping[str, Mapping[str, Any]] | None = None,
    prepared_sources: PreparedSourceSet | None = None,
) -> dict[str, Any]:
    paths = list(CE_QUERY_SOURCES)
    captured = {
        "package.json": package_text.encode("utf-8"),
        CE_QUERY_SCRIPT: script_text.encode("utf-8"),
        "docs/project-log/manifest.json": manifest_text.encode("utf-8"),
    }
    expected = {
        path: {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        for path, raw in captured.items()
    } if expected_source_identities is None else {
        path: dict(expected_source_identities.get(path, {}))
        for path in paths
    }
    captured_identities = {
        path: {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        for path, raw in captured.items()
    }
    if captured_identities != expected:
        raise OrchestrateError("The CE query source binding changed before execution", code="ce_query_source_changed")
    _require_tracked_query_sources(profile.root, paths)
    baseline = _git_source_state(profile.root, paths)
    if prepared_sources is not None:
        _require_matching_source_revalidation(
            prepared_sources.revalidate(profile.root, frozenset(paths)),
            boundary="before execution",
        )
    elif _query_source_identities(profile) != expected:
        raise OrchestrateError("The CE query source binding changed before execution", code="ce_query_source_changed")
    arguments = (
        "pnpm",
        "--silent",
        "run",
        "project-log:query",
        "--",
        task_id,
        "--json",
        "--max-entries",
        str(CE_QUERY_MAX_ENTRIES),
        "--max-bytes",
        str(CE_QUERY_MAX_BYTES),
    )
    try:
        completed = subprocess.run(
            arguments,
            cwd=profile.root,
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OrchestrateError("The bounded CE project-log query is unavailable", code="ce_query_unavailable") from exc
    if completed.returncode:
        raise OrchestrateError("The bounded CE project-log query failed", code="ce_query_failed")
    if len(completed.stdout) > CE_QUERY_STDOUT_LIMIT:
        raise OrchestrateError("The CE project-log query exceeded its output boundary", code="ce_query_incomplete")
    try:
        result = json.loads(completed.stdout.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError("The CE project-log query did not return strict JSON", code="ce_query_invalid") from exc
    git_state_stable = _git_source_state(profile.root, paths) == baseline
    if not git_state_stable:
        raise OrchestrateError(
            "The CE query source binding changed during execution",
            code="ce_query_source_changed",
            data={"stable": {"gitState": False, "sourceIdentities": None}},
        )
    if prepared_sources is not None:
        _require_matching_source_revalidation(
            prepared_sources.revalidate(profile.root, frozenset(paths)),
            boundary="during execution",
        )
    elif _query_source_identities(profile) != expected:
        raise OrchestrateError(
            "The CE query source binding changed during execution",
            code="ce_query_source_changed",
            data={"stable": {"gitState": True, "sourceIdentities": False}},
        )
    if not isinstance(result, dict):
        raise OrchestrateError("The CE project-log query returned an unknown shape", code="ce_query_invalid")
    return result


def _validate_ce_query(result: object, task_id: str, shards: list[dict[str, Any]]) -> dict[str, Any]:
    expected_paths = [item["path"] for item in shards]
    if (
        not isinstance(result, dict)
        or set(result) != {"term", "exact_task", "selected_shards", "matches", "omitted_matches"}
        or result.get("term") != task_id
        or result.get("exact_task") != task_id
        or result.get("selected_shards") != expected_paths
        or not isinstance(result.get("matches"), list)
        or type(result.get("omitted_matches")) is not int
        or result["omitted_matches"] < 0
    ):
        raise OrchestrateError("The CE project-log query did not bind the exact task and shards", code="ce_query_invalid")
    sequences = {item["path"]: item["sequence"] for item in shards}
    for match in result["matches"]:
        if (
            not isinstance(match, dict)
            or set(match) != {"path", "sequence", "heading", "text"}
            or match.get("path") not in sequences
            or type(match.get("sequence")) is not int
            or match.get("sequence") != sequences[match["path"]]
            or not isinstance(match.get("heading"), str)
            or not isinstance(match.get("text"), str)
        ):
            raise OrchestrateError("The CE project-log query returned an unrelated or malformed match", code="ce_query_invalid")
    if result["omitted_matches"]:
        raise OrchestrateError(
            "The CE project-log query omitted matching entries and is incomplete",
            code="ce_query_incomplete",
            data={"omittedMatches": result["omitted_matches"]},
        )
    return result


def resolve_ce_task_source(
    profile: ProjectProfile,
    objective: str | None,
    registry: str,
) -> tuple[str, str]:
    """Resolve the one CE task path from the configured registry text."""

    if objective is None:
        raise OrchestrateError("The CE reader requires an exact authorized CE task in the objective", code="ce_task_missing")
    task_ids = sorted(set(CE_TASK_ID.findall(objective)))
    if not task_ids:
        raise OrchestrateError("The CE objective must name an exact CE task", code="ce_task_missing")
    if len(task_ids) != 1:
        raise OrchestrateError("The CE objective must name exactly one CE task", code="ce_task_ambiguous")
    task_id = task_ids[0]
    registry_path = "docs/tasks/README.md"
    matching_lines = [line for line in registry.splitlines() if task_id in set(CE_TASK_ID.findall(line))]
    task_targets = sorted({target for line in matching_lines for target in _line_targets(profile, registry_path, line)})
    if len(task_targets) != 1:
        raise OrchestrateError(
            f"The CE registry did not resolve exactly one source for {task_id}",
            code="ce_task_unresolved",
        )
    return task_id, task_targets[0]


def resolve_ce_context_sources(
    profile: ProjectProfile,
    task_id: str,
    task_path: str,
    task_text: str,
) -> list[str]:
    """Resolve required context paths from the already-authorized task text."""

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
    return context_targets


def _read_ce(
    profile: ProjectProfile,
    objective: str | None,
    instructions: dict[str, str],
    manifests: dict[str, str],
    expected_query_sources: Mapping[str, Mapping[str, Any]] | None,
    prepared_sources: PreparedSourceSet | None,
) -> ReaderResult:
    registry_path = "docs/tasks/README.md"
    manifest_path = "docs/project-log/manifest.json"
    configured = set(profile.value["taskEntrypoints"])
    if not {registry_path, manifest_path}.issubset(configured):
        raise OrchestrateError(
            "The CE reader requires the task registry and project-log manifest entrypoints",
            code="ce_entrypoints_missing",
        )
    registry = _source_text(profile, registry_path, prepared_sources, configured=True)
    task_id, task_path = resolve_ce_task_source(profile, objective, registry)
    task_text = _source_text(profile, task_path, prepared_sources)
    context_targets = resolve_ce_context_sources(profile, task_id, task_path, task_text)
    context_documents = {
        path: _source_text(profile, path, prepared_sources)
        for path in context_targets
    }

    raw_manifest = _source_text(profile, manifest_path, prepared_sources, configured=True)
    try:
        manifest_value = json.loads(raw_manifest)
    except json.JSONDecodeError as exc:
        raise OrchestrateError("The project-log manifest is invalid JSON", code="ce_manifest_invalid") from exc
    selected_shards = _manifest_shards(manifest_value, task_id, profile.root)
    package_text = manifests.get("package.json")
    if not isinstance(package_text, str):
        raise OrchestrateError(
            "The selected CE operational profile does not include package.json command authority",
            code="ce_query_authority_missing",
        )
    try:
        package = json.loads(package_text)
    except json.JSONDecodeError as exc:
        raise OrchestrateError("The CE package command manifest is invalid JSON", code="ce_query_authority_missing") from exc
    scripts = package.get("scripts") if isinstance(package, dict) else None
    if not isinstance(scripts, dict) or scripts.get("project-log:query") != CE_QUERY_PACKAGE_SCRIPT:
        raise OrchestrateError("The CE project-log query command is not exactly bound", code="ce_query_authority_missing")
    script_text = _source_text(profile, CE_QUERY_SCRIPT, prepared_sources)
    query_result = _validate_ce_query(
        _run_ce_query(
            profile,
            task_id,
            manifest_text=raw_manifest,
            package_text=package_text,
            script_text=script_text,
            expected_source_identities=expected_query_sources,
            prepared_sources=prepared_sources,
        ),
        task_id,
        selected_shards,
    )
    log_targets = [item["path"] for item in selected_shards]
    task_sources = {
        registry_path: registry,
        manifest_path: raw_manifest,
        task_path: task_text,
        CE_QUERY_SCRIPT: script_text,
        **context_documents,
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
            "logShards": selected_shards,
            "projectLogQuery": {
                "command": "pnpm --silent run project-log:query -- <exact-task> --json --max-entries 12 --max-bytes 32768",
                "resultSha256": hashlib.sha256(
                    json.dumps(query_result, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                "matches": query_result["matches"],
                "omittedMatches": query_result["omitted_matches"],
            },
        },
        consulted,
    )


def read_project(
    profile: ProjectProfile,
    objective: str | None = None,
    *,
    expected_ce_query_sources: Mapping[str, Mapping[str, Any]] | None = None,
    prepared_sources: PreparedSourceSet | None = None,
) -> ReaderResult:
    current_instructions = (
        prepared_sources.instruction_inventory
        if prepared_sources is not None
        else profile.require_instruction_acknowledgment()
    )
    instruction_paths = sorted({
        *profile.value["instructions"],
        *current_instructions,
    })
    instructions = {
        relative: _source_text(profile, relative, prepared_sources)
        for relative in instruction_paths
    }
    manifests = _configured_text(profile, "commandManifests", prepared_sources)
    kind = profile.value["reader"]["kind"]
    if kind == "ce-gd":
        return _read_ce(
            profile,
            objective,
            instructions,
            manifests,
            expected_ce_query_sources,
            prepared_sources,
        )
    if kind != "repo":
        raise OrchestrateError(f"Unsupported reader: {kind}", code="profile_reader_invalid")
    tasks = _configured_text(profile, "taskEntrypoints", prepared_sources)
    consulted = frozenset({*instructions, *tasks, *manifests})
    return ReaderResult(
        "repo",
        instructions,
        tasks,
        manifests,
        {"authority": list(instructions), "tasks": list(tasks)},
        consulted,
    )
