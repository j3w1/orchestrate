"""One fail-closed primitive for reading approved project sources."""

from __future__ import annotations

import os
from pathlib import Path
import stat

from .errors import OrchestrateError


SENSITIVE_NAMES = {
    "credentials.json",
    "secrets.json",
    "id_rsa",
    "id_ed25519",
}


def is_sensitive_source(relative: str) -> bool:
    name = Path(relative).name.lower()
    return (
        name == ".env"
        or name.startswith(".env.")
        or name in SENSITIVE_NAMES
        or Path(relative).suffix.lower() in {".pem", ".key"}
    )


def _is_reparse(info: os.stat_result) -> bool:
    attributes = getattr(info, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return stat.S_ISLNK(info.st_mode) or bool(reparse_flag and attributes & reparse_flag)


def approved_project_path(root: Path, relative: str, *, require_file: bool = True) -> Path:
    """Return an in-root path only after refusing links in every existing component."""

    relative_path = Path(relative)
    if relative_path.is_absolute() or not relative_path.parts or ".." in relative_path.parts:
        raise OrchestrateError(f"Source path escapes the project: {relative}", code="source_boundary_unresolved")
    canonical_root = root.resolve()
    current = canonical_root
    for part in relative_path.parts:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            if require_file:
                raise OrchestrateError(f"Required source is unavailable: {relative}", code="source_unavailable")
            return current
        except OSError as exc:
            raise OrchestrateError(f"Required source is unavailable: {relative}", code="source_unavailable") from exc
        if _is_reparse(info):
            raise OrchestrateError(
                f"Symlink or reparse boundary is unresolved: {current.relative_to(canonical_root).as_posix()}",
                code="source_boundary_unresolved",
            )
    resolved = current.resolve()
    try:
        resolved.relative_to(canonical_root)
    except ValueError as exc:
        raise OrchestrateError(
            f"Source resolves outside the project: {relative}",
            code="source_boundary_unresolved",
        ) from exc
    if require_file and not resolved.is_file():
        raise OrchestrateError(f"Required source is not a file: {relative}", code="source_unavailable")
    return resolved


def read_project_bytes(root: Path, relative: str) -> bytes:
    if is_sensitive_source(relative):
        raise OrchestrateError(f"Secret-bearing source is excluded: {relative}", code="secret_source_excluded")
    path = approved_project_path(root, relative)
    try:
        return path.read_bytes()
    except OSError as exc:
        raise OrchestrateError(f"Required source cannot be read: {relative}", code="source_unavailable") from exc
