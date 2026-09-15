"""Fail-closed, handle-based reads for selected project sources."""

from __future__ import annotations

import errno
from enum import Enum
import os
from pathlib import Path
import stat
import sys
from typing import Final

from .errors import OrchestrateError


MAX_SOURCE_BYTES: Final = 4 * 1024 * 1024
SENSITIVE_NAMES = {
    ".dockercfg",
    ".git-credentials",
    ".netrc",
    ".npmrc",
    ".pypirc",
    "application_default_credentials.json",
    "auth.json",
    "credentials",
    "credentials.db",
    "credentials.json",
    "gcloud_credentials.db",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
    "secrets.json",
    "service-account.json",
    "service_account.json",
    "terraform.tfstate",
    "token.json",
}
SENSITIVE_DIRECTORIES = {
    ".aws",
    ".azure",
    ".docker",
    ".git",
    ".gnupg",
    ".kube",
    ".password-store",
    ".ssh",
}
SENSITIVE_SUFFIXES = {".jks", ".key", ".keystore", ".p12", ".pem", ".pfx", ".tfstate", ".tfvars"}


class ProjectSourceState(str, Enum):
    """What a source-local metadata observation proves about one path."""

    PRESENT = "present"
    ABSENT = "absent"
    CHANGED = "changed"
    UNAVAILABLE = "unavailable"


def project_source_error_state(exc: OSError) -> ProjectSourceState:
    """Classify path lookup failures without losing structural evidence."""

    if isinstance(exc, NotADirectoryError) or exc.errno == errno.ENOTDIR:
        return ProjectSourceState.CHANGED
    if isinstance(exc, FileNotFoundError) or exc.errno == errno.ENOENT:
        return ProjectSourceState.ABSENT
    return ProjectSourceState.UNAVAILABLE


def _relative_parts(relative: str) -> tuple[str, ...]:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts or any(not part for part in path.parts):
        raise OrchestrateError(
            f"Source path escapes the project: {relative}",
            code="source_boundary_unresolved",
        )
    return path.parts


def is_sensitive_source(relative: str) -> bool:
    path = Path(relative)
    lowered_parts = tuple(part.lower() for part in path.parts)
    name = lowered_parts[-1] if lowered_parts else ""
    return (
        name == ".env"
        or name.startswith(".env.")
        or name in SENSITIVE_NAMES
        or (name.startswith("id_") and not name.endswith(".pub"))
        or path.suffix.lower() in SENSITIVE_SUFFIXES
        or any(part in SENSITIVE_DIRECTORIES for part in lowered_parts[:-1])
        or (len(lowered_parts) >= 3 and lowered_parts[-3:-1] == (".config", "gcloud"))
    )


def _is_reparse(info: os.stat_result) -> bool:
    attributes = getattr(info, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return stat.S_ISLNK(info.st_mode) or bool(reparse_flag and attributes & reparse_flag)


def approved_project_path(root: Path, relative: str, *, require_file: bool = True) -> Path:
    """Validate a name for discovery/write use; content reads use an opened handle."""

    parts = _relative_parts(relative)
    canonical_root = root.resolve()
    current = canonical_root
    for index, part in enumerate(parts):
        current = current / part
        try:
            info = current.lstat()
        except OSError as exc:
            source_state = project_source_error_state(exc)
            if source_state == ProjectSourceState.ABSENT:
                if require_file:
                    raise OrchestrateError(
                        f"Required source is unavailable: {relative}",
                        code="source_unavailable",
                    ) from exc
                return current
            if source_state == ProjectSourceState.CHANGED:
                raise OrchestrateError(
                    f"Required source has a non-directory ancestor: {relative}",
                    code="source_identity_changed",
                ) from exc
            raise OrchestrateError(f"Required source is unavailable: {relative}", code="source_unavailable") from exc
        if _is_reparse(info):
            raise OrchestrateError(
                f"Symlink or reparse boundary is unresolved: {current.relative_to(canonical_root).as_posix()}",
                code="source_boundary_unresolved",
            )
        if index < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise OrchestrateError(
                f"Required source has a non-directory ancestor: {relative}",
                code="source_identity_changed",
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


def project_source_state(root: Path, relative: str) -> ProjectSourceState:
    """Classify one path without using another source as an availability proxy."""

    try:
        candidate = approved_project_path(root, relative, require_file=False)
    except OrchestrateError as exc:
        if exc.code == "source_unavailable":
            return ProjectSourceState.UNAVAILABLE
        if exc.code == "source_identity_changed":
            return ProjectSourceState.CHANGED
        raise
    try:
        info = candidate.lstat()
    except OSError as exc:
        return project_source_error_state(exc)
    if _is_reparse(info) or not stat.S_ISREG(info.st_mode):
        return ProjectSourceState.CHANGED
    return ProjectSourceState.PRESENT


def _read_posix_handle(root: Path, parts: tuple[str, ...], relative: str) -> bytes:
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    if not no_follow or not directory:
        raise OrchestrateError(
            "This host cannot provide no-follow project source reads",
            code="source_boundary_unresolved",
        )
    descriptors: list[int] = []
    try:
        current = os.open(os.fspath(root.resolve()), os.O_RDONLY | directory | close_on_exec)
        descriptors.append(current)
        for index, part in enumerate(parts):
            final = index == len(parts) - 1
            flags = os.O_RDONLY | no_follow | close_on_exec
            if not final:
                flags |= directory
            current = os.open(part, flags, dir_fd=current)
            descriptors.append(current)
        info = os.fstat(current)
        if not stat.S_ISREG(info.st_mode):
            raise OrchestrateError(f"Required source is not a file: {relative}", code="source_unavailable")
        if info.st_size > MAX_SOURCE_BYTES:
            raise OrchestrateError(f"Required source exceeds the bounded read limit: {relative}", code="source_too_large")
        chunks: list[bytes] = []
        remaining = MAX_SOURCE_BYTES + 1
        while remaining:
            chunk = os.read(current, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > MAX_SOURCE_BYTES:
            raise OrchestrateError(f"Required source exceeds the bounded read limit: {relative}", code="source_too_large")
        return raw
    except OrchestrateError:
        raise
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            code = "source_boundary_unresolved"
        elif project_source_error_state(exc) == ProjectSourceState.CHANGED:
            code = "source_identity_changed"
        else:
            code = "source_unavailable"
        raise OrchestrateError(f"Required source cannot be opened safely: {relative}", code=code) from exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _read_windows_handle(root: Path, relative: str) -> bytes:
    # OPEN_REPARSE_POINT refuses a final redirect. The final opened identity is
    # then checked in-root before ReadFile consumes bytes, so no path is reopened.
    import ctypes
    from ctypes import wintypes

    file_read_data = 0x0001
    file_read_attributes = 0x0080
    # Deny concurrent write/delete handles for the short source read. Existing
    # incompatible handles make this open fail closed instead of allowing a
    # mutable byte stream to be represented as one immutable identity.
    share_read = 0x00000001
    open_existing = 3
    backup_semantics = 0x02000000
    open_reparse_point = 0x00200000
    file_attribute_reparse_point = 0x00000400
    file_attribute_directory = 0x00000010
    file_attribute_tag_info_class = 9

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [("FileAttributes", wintypes.DWORD), ("ReparseTag", wintypes.DWORD)]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    get_info = kernel32.GetFileInformationByHandleEx
    get_info.argtypes = (wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD)
    get_info.restype = wintypes.BOOL
    get_final_path = kernel32.GetFinalPathNameByHandleW
    get_final_path.argtypes = (wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD)
    get_final_path.restype = wintypes.DWORD
    get_size = kernel32.GetFileSizeEx
    get_size.argtypes = (wintypes.HANDLE, ctypes.POINTER(ctypes.c_longlong))
    get_size.restype = wintypes.BOOL
    read_file = kernel32.ReadFile
    read_file.argtypes = (
        wintypes.HANDLE,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPVOID,
    )
    read_file.restype = wintypes.BOOL

    candidate = root.resolve() / relative
    handle = create_file(
        os.fspath(candidate),
        file_read_data | file_read_attributes,
        share_read,
        None,
        open_existing,
        backup_semantics | open_reparse_point,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle == invalid:
        raise OrchestrateError(
            f"Required source cannot be opened safely: {relative}",
            code="source_unavailable",
        )
    try:
        tag = FileAttributeTagInfo()
        if not get_info(handle, file_attribute_tag_info_class, ctypes.byref(tag), ctypes.sizeof(tag)):
            raise OrchestrateError(f"Required source identity is unavailable: {relative}", code="source_boundary_unresolved")
        if tag.FileAttributes & file_attribute_reparse_point:
            raise OrchestrateError(f"Symlink or reparse source is excluded: {relative}", code="source_boundary_unresolved")
        if tag.FileAttributes & file_attribute_directory:
            raise OrchestrateError(f"Required source is not a file: {relative}", code="source_unavailable")

        needed = get_final_path(handle, None, 0, 0)
        if not needed:
            raise OrchestrateError(f"Required source identity is unavailable: {relative}", code="source_boundary_unresolved")
        buffer = ctypes.create_unicode_buffer(needed + 1)
        written = get_final_path(handle, buffer, len(buffer), 0)
        if not written or written >= len(buffer):
            raise OrchestrateError(f"Required source identity is unavailable: {relative}", code="source_boundary_unresolved")
        opened = buffer.value
        if opened.startswith("\\\\?\\UNC\\"):
            opened = "\\\\" + opened[8:]
        elif opened.startswith("\\\\?\\"):
            opened = opened[4:]
        canonical_root = os.path.normcase(os.path.abspath(os.fspath(root.resolve())))
        canonical_opened = os.path.normcase(os.path.abspath(opened))
        canonical_candidate = os.path.normcase(os.path.abspath(os.fspath(candidate)))
        try:
            contained = os.path.commonpath((canonical_root, canonical_opened)) == canonical_root
        except ValueError:
            contained = False
        if not contained or canonical_opened != canonical_candidate:
            raise OrchestrateError(
                f"Opened source did not preserve its no-follow project identity: {relative}",
                code="source_boundary_unresolved",
            )

        size = ctypes.c_longlong()
        if not get_size(handle, ctypes.byref(size)) or size.value < 0:
            raise OrchestrateError(f"Required source size is unavailable: {relative}", code="source_unavailable")
        if size.value > MAX_SOURCE_BYTES:
            raise OrchestrateError(f"Required source exceeds the bounded read limit: {relative}", code="source_too_large")
        raw = bytearray()
        while len(raw) <= MAX_SOURCE_BYTES:
            amount = min(64 * 1024, MAX_SOURCE_BYTES + 1 - len(raw))
            chunk = ctypes.create_string_buffer(amount)
            received = wintypes.DWORD()
            if not read_file(handle, chunk, amount, ctypes.byref(received), None):
                raise OrchestrateError(f"Required source cannot be read: {relative}", code="source_unavailable")
            if received.value == 0:
                break
            raw.extend(chunk.raw[: received.value])
        if len(raw) > MAX_SOURCE_BYTES:
            raise OrchestrateError(f"Required source exceeds the bounded read limit: {relative}", code="source_too_large")
        if len(raw) != size.value:
            raise OrchestrateError(f"Required source changed during read: {relative}", code="source_identity_changed")
        return bytes(raw)
    finally:
        close_handle(handle)


def read_project_bytes(root: Path, relative: str) -> bytes:
    parts = _relative_parts(relative)
    if is_sensitive_source(relative):
        raise OrchestrateError(f"Secret-bearing source is excluded: {relative}", code="secret_source_excluded")
    if sys.platform == "win32":
        return _read_windows_handle(root, relative)
    return _read_posix_handle(root, parts, relative)
