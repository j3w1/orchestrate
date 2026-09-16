"""First-machine installation for the canonical Windows orchestrate command.

The production adapter owns only one user-scope registry value and one
dedicated installation directory.  The small protocols and layout argument
keep tests entirely synthetic: no test needs to inspect or change a real user
PATH, registry, or Python installation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
import hashlib
import json
import os
import ntpath
from pathlib import Path
import secrets
import shutil
import stat
import subprocess
import sys
import zipfile
import io
from typing import Protocol

from .errors import OrchestrateError
from .state import RunLock


MINIMUM_PYTHON = (3, 13)
WINDOWS_COMMAND = "orchestrate.cmd"
WSL_COMMAND = "orchestrate"
INSTALL_RECEIPT_SCHEMA = "orchestrate-machine-install/v5"
INSTALL_ANCHOR_SCHEMA = "orchestrate-machine-install-anchor/v1"
MAX_INSTALL_RECEIPT_BYTES = 4096
MAX_PYVENV_CONFIG_BYTES = 16 * 1024
MAX_SHIM_BYTES = 32 * 1024
MAX_COMMAND_BYTES = 2 * 1024 * 1024
MAX_SOURCE_FILES = 4096
MAX_SOURCE_BYTES = 64 * 1024 * 1024
MAX_SOURCE_ARCHIVE_BYTES = 72 * 1024 * 1024
MACHINE_LOCK_NAME = ".machine-bootstrap.lock"
SIMULATED_WIN32_PROVIDER_ENV = "ORCHESTRATE_BOOTSTRAP_PROVIDER"


def _binary_open_flag() -> int:
    """Return the CRT binary-mode flag where the host exposes one."""

    return getattr(os, "O_BINARY", 0)


class _PlatformProvider(Protocol):
    """One seam for every platform-dependent bootstrap operation.

    ``windows_semantics`` selects command, path, share-binding, and staging
    forms.  Only ``native_windows`` is allowed to call Win32 APIs; the
    simulated provider implements the same contract with disposable POSIX
    descriptors so it is safe to run on development hosts.
    """

    name: str
    windows_semantics: bool
    native_windows: bool

    def read_bounded_file(self, path: Path, limit: int) -> _BoundedFile: ...

    def open_path_pin(self, path: Path, *, directory: bool) -> object: ...

    def close_path_pin(self, handle: object) -> None: ...


@dataclass(frozen=True, slots=True)
class UserPathValue:
    """Exact registry value plus the kind that controls expansion semantics."""

    value: str
    kind: int
    exists: bool = True


class UserPathStore(Protocol):
    """The Windows user's PATH value, never the process or machine PATH."""

    def read(self) -> UserPathValue: ...

    def replace(self, expected: UserPathValue, value: str) -> None: ...


class RegistryUserPathStore:
    r"""Read and write only ``HKCU\Environment\Path``."""

    def __init__(
        self,
        *,
        transactional_replace: Callable[[UserPathValue, str], None] | None = None,
    ) -> None:
        self._transactional_replace = (
            _replace_windows_registry_value
            if transactional_replace is None
            else transactional_replace
        )

    @staticmethod
    def _validate(value: object, kind: int, *, winreg: object) -> UserPathValue:
        supported = (winreg.REG_SZ, winreg.REG_EXPAND_SZ)  # type: ignore[attr-defined]
        if not isinstance(value, str) or type(kind) is not int or kind not in supported:
            raise OrchestrateError(
                "The Windows user PATH has an unsupported registry type",
                code="machine_bootstrap_path_unsupported",
            )
        return UserPathValue(value, kind)

    def read(self) -> UserPathValue:
        if sys.platform != "win32":
            raise OrchestrateError(
                "Windows user PATH is available only on native Windows",
                code="machine_bootstrap_platform_unsupported",
            )
        import winreg

        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
                value, kind = winreg.QueryValueEx(key, "Path")
        except FileNotFoundError:
            return UserPathValue("", winreg.REG_EXPAND_SZ, exists=False)
        except OSError as exc:
            raise OrchestrateError(
                "The Windows user PATH could not be read; no installation changes were made",
                code="machine_bootstrap_path_unavailable",
            ) from exc
        return self._validate(value, kind, winreg=winreg)

    def replace(self, expected: UserPathValue, value: str) -> None:
        if sys.platform != "win32":
            raise OrchestrateError(
                "Windows user PATH is available only on native Windows",
                code="machine_bootstrap_platform_unsupported",
            )
        try:
            self._transactional_replace(expected, value)
            if self.read() != UserPathValue(value, expected.kind):
                raise OrchestrateError(
                    "The Windows user PATH changed while bootstrap committed its update; the interfering value was preserved, so rerun setup",
                    code="machine_bootstrap_path_changed",
                )
        except OrchestrateError:
            raise
        except OSError as exc:
            raise OrchestrateError(
                "The Windows user PATH could not be updated; rerun setup after correcting registry access",
                code="machine_bootstrap_path_write_failed",
            ) from exc
        # Explorer and other session owners can refresh future child-process
        # environments.  The registry write is already authoritative, so a
        # missing notification surface is not treated as a second mutation to
        # retry or as proof that the committed value failed.
        try:
            import ctypes

            ctypes.windll.user32.SendMessageTimeoutW(  # type: ignore[attr-defined]
                0xFFFF,  # HWND_BROADCAST
                0x001A,  # WM_SETTINGCHANGE
                0,
                "Environment",
                0x0002,  # SMTO_ABORTIFHUNG
                5000,
                None,
            )
        except (AttributeError, OSError):
            pass


def _replace_windows_registry_value(expected: UserPathValue, value: str) -> None:
    """Atomically compare and replace HKCU PATH with a registry transaction.

    There is no safe QueryValueEx/SetValueEx compare-and-swap.  A transacted
    key makes the comparison and write one commit; a competing writer makes
    that commit fail instead of losing unrelated PATH bytes.
    """

    import ctypes
    from ctypes import wintypes
    import winreg

    error_success = 0
    error_file_not_found = 2
    key_query_value = 0x0001
    key_set_value = 0x0002
    invalid_handle = ctypes.c_void_p(-1).value

    ktmw32 = ctypes.WinDLL("ktmw32", use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_transaction = ktmw32.CreateTransaction
    create_transaction.argtypes = (
        wintypes.LPVOID,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPCWSTR,
    )
    create_transaction.restype = wintypes.HANDLE
    commit_transaction = ktmw32.CommitTransaction
    commit_transaction.argtypes = (wintypes.HANDLE,)
    commit_transaction.restype = wintypes.BOOL
    rollback_transaction = ktmw32.RollbackTransaction
    rollback_transaction.argtypes = (wintypes.HANDLE,)
    rollback_transaction.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    open_key = advapi32.RegOpenKeyTransactedW
    open_key.argtypes = (
        wintypes.HANDLE,
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.HANDLE,
        wintypes.LPVOID,
    )
    open_key.restype = wintypes.LONG
    query_value = advapi32.RegQueryValueExW
    query_value.argtypes = (
        wintypes.HANDLE,
        wintypes.LPCWSTR,
        wintypes.LPVOID,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPVOID,
        ctypes.POINTER(wintypes.DWORD),
    )
    query_value.restype = wintypes.LONG
    set_value = advapi32.RegSetValueExW
    set_value.argtypes = (
        wintypes.HANDLE,
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPCVOID,
        wintypes.DWORD,
    )
    set_value.restype = wintypes.LONG
    close_key = advapi32.RegCloseKey
    close_key.argtypes = (wintypes.HANDLE,)
    close_key.restype = wintypes.LONG

    transaction = create_transaction(None, None, 0, 0, 0, 0, None)
    if transaction in (None, 0, invalid_handle):
        raise OrchestrateError(
            "The Windows user PATH transaction could not be started; no PATH value was written",
            code="machine_bootstrap_path_write_failed",
        )
    key = wintypes.HANDLE()
    committed = False
    try:
        status = open_key(
            wintypes.HANDLE(ctypes.c_long(int(winreg.HKEY_CURRENT_USER)).value),
            "Environment",
            0,
            key_query_value | key_set_value,
            ctypes.byref(key),
            transaction,
            None,
        )
        if status != error_success:
            raise OSError(status, "RegOpenKeyTransactedW")

        def read_current() -> UserPathValue:
            kind = wintypes.DWORD()
            size = wintypes.DWORD()
            result = query_value(key, "Path", None, ctypes.byref(kind), None, ctypes.byref(size))
            if result == error_file_not_found:
                return UserPathValue("", winreg.REG_EXPAND_SZ, exists=False)
            if result != error_success:
                raise OSError(result, "RegQueryValueExW")
            buffer = ctypes.create_string_buffer(size.value)
            result = query_value(
                key,
                "Path",
                None,
                ctypes.byref(kind),
                buffer,
                ctypes.byref(size),
            )
            if result != error_success:
                raise OSError(result, "RegQueryValueExW")
            raw = buffer.raw[: size.value]
            text = raw.decode("utf-16-le")
            if text.endswith("\x00"):
                text = text[:-1]
            return RegistryUserPathStore._validate(text, kind.value, winreg=winreg)

        if read_current() != expected:
            raise OrchestrateError(
                "The Windows user PATH changed during bootstrap; no stale PATH value was written, so rerun setup",
                code="machine_bootstrap_path_changed",
            )
        encoded = (value + "\x00").encode("utf-16-le")
        buffer = ctypes.create_string_buffer(encoded)
        status = set_value(key, "Path", 0, expected.kind, buffer, len(encoded))
        if status != error_success:
            raise OSError(status, "RegSetValueExW")
        if read_current() != UserPathValue(value, expected.kind):
            raise OrchestrateError(
                "The Windows user PATH transaction could not verify its staged value",
                code="machine_bootstrap_path_changed",
            )
        if not commit_transaction(transaction):
            raise OrchestrateError(
                "The Windows user PATH changed during bootstrap; the atomic update was not committed, so rerun setup",
                code="machine_bootstrap_path_changed",
            )
        committed = True
    finally:
        if key.value:
            close_key(key)
        if not committed:
            rollback_transaction(transaction)
        close_handle(transaction)


@dataclass(frozen=True, slots=True)
class MachineLayout:
    source_root: Path
    install_root: Path
    venv_root: Path
    scripts_root: Path
    command_path: Path
    install_receipt: Path
    install_anchor: Path
    source_archive: Path
    bin_root: Path
    windows_shim: Path
    wsl_shim: Path


@dataclass(frozen=True, slots=True)
class MachineBootstrapResult:
    state: str
    actions: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {"state": self.state, "actions": list(self.actions)}


Runner = Callable[..., subprocess.CompletedProcess[object]]
Resolver = Callable[[str, str], str | None]


def default_layout(
    *,
    environment: Mapping[str, str] | None = None,
    source_root: Path | None = None,
) -> MachineLayout:
    env = os.environ if environment is None else environment
    local_app_data = env.get("LOCALAPPDATA", "").strip()
    if not local_app_data:
        raise OrchestrateError(
            "LOCALAPPDATA is unavailable; the dedicated user installation cannot be selected",
            code="machine_bootstrap_home_unavailable",
        )
    install_root = Path(local_app_data) / "orchestrate"
    state_root = Path(local_app_data) / "orchestrate-state"
    venv_root = install_root / "venv"
    scripts_root = venv_root / "Scripts"
    selected_source = Path(__file__).resolve().parents[2] if source_root is None else source_root.resolve()
    return MachineLayout(
        source_root=selected_source,
        install_root=install_root,
        venv_root=venv_root,
        scripts_root=scripts_root,
        command_path=scripts_root / "orchestrate.exe",
        install_receipt=install_root / "install.json",
        install_anchor=state_root / "machine-install.json",
        source_archive=install_root / "installed-source.zip",
        bin_root=install_root / "bin",
        windows_shim=install_root / "bin" / WINDOWS_COMMAND,
        wsl_shim=install_root / "bin" / WSL_COMMAND,
    )


def _windows_shim_text() -> str:
    return '@echo off\r\n"%~dp0..\\venv\\Scripts\\orchestrate.exe" %*\r\n'


def _wsl_shim_text() -> str:
    # Python is used only to encode the existing bounded transport schema.  It
    # does not import orchestrate or create Linux-side application state.
    return """#!/bin/sh
set -eu
: "${WSL_DISTRO_NAME:?orchestrate requires WSL_DISTRO_NAME}"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
windows_command="$script_dir/../venv/Scripts/orchestrate.exe"
payload=$(python3 - "$WSL_DISTRO_NAME" "$PWD" "$@" <<'PY'
import base64
import json
import sys

value = {
    "schema": "orchestrate-wsl-forward/v1",
    "distro": sys.argv[1],
    "linuxCwd": sys.argv[2],
    "argv": sys.argv[3:],
}
raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
print(base64.urlsafe_b64encode(raw).decode("ascii").rstrip("="))
PY
)
exec "$windows_command" _wsl-forward --payload "$payload"
"""


def _normalize_path_entry(value: str) -> str:
    expanded = os.path.expandvars(value.strip().strip('"'))
    return ntpath.normcase(ntpath.normpath(expanded)) if expanded else ""


def register_user_path(store: UserPathStore, entry: Path) -> bool:
    """Ensure exactly one matching entry while preserving every unrelated byte."""

    current = store.read()
    parts = current.value.split(";") if current.value else []
    wanted = _normalize_path_entry(os.fspath(entry))
    matches = [index for index, item in enumerate(parts) if _normalize_path_entry(item) == wanted]
    if len(matches) == 1 and matches[0] == 0:
        return False
    retained = [item for index, item in enumerate(parts) if index not in matches]
    retained.insert(0, os.fspath(entry))
    store.replace(current, ";".join(retained))
    return True


def _default_resolver(command: str, path: str) -> str | None:
    return shutil.which(command, path=os.path.expandvars(path))


def _same_path(left: str | Path, right: str | Path) -> bool:
    return ntpath.normcase(ntpath.abspath(os.fspath(left))) == ntpath.normcase(
        ntpath.abspath(os.fspath(right))
    )


def _is_reparse(info: os.stat_result) -> bool:
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(info, "st_file_attributes", 0) & reparse)


@dataclass(frozen=True, slots=True)
class _PathIdentity:
    device: int
    inode: int
    mode: int
    links: int
    size: int
    modified_ns: int

    @classmethod
    def from_stat(cls, info: os.stat_result) -> _PathIdentity:
        return cls(
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_nlink,
            info.st_size,
            info.st_mtime_ns,
        )


@dataclass(frozen=True, slots=True)
class _BoundedFile:
    raw: bytes
    identity: _PathIdentity


def _operation_diagnostic(
    operation: str,
    form: str,
    path: str | Path,
    *,
    path_form: str,
    explicit_application_name: bool | None = None,
    access: str | None = None,
    share: str | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "operation": operation,
        "form": form,
        "path": os.fspath(path),
        "pathForm": path_form,
    }
    if explicit_application_name is not None:
        value["explicitApplicationName"] = explicit_application_name
    if access is not None:
        value["access"] = access
    if share is not None:
        value["share"] = share
    return value


def _zip_structure(raw: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            archive.infolist()
            return "valid"
    except (OSError, ValueError, zipfile.BadZipFile, zipfile.LargeZipFile):
        return "invalid"


def _archive_stage_diagnostic(
    stage: str,
    raw: bytes,
    *,
    form: str,
    path: str | Path,
) -> dict[str, object]:
    return {
        "stage": stage,
        "form": form,
        "path": os.fspath(path),
        "size": len(raw),
        "sha256": f"sha256:{hashlib.sha256(raw).hexdigest()}",
        "zipStructure": _zip_structure(raw),
    }


def _diagnostic_exception_note(exc: BaseException, data: Mapping[str, object]) -> None:
    if isinstance(exc, OrchestrateError):
        before = str(exc)
        exc.add_diagnostic(data)
        if str(exc) != before:
            return
    add_note = getattr(exc, "add_note", None)
    if callable(add_note):
        rendered = json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        add_note(f"machine-bootstrap diagnostic: {rendered[:4096]}")


def _anchor_ancestry_diagnostic(path: Path) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    current = path.parent
    for _ in range(8):
        try:
            info = current.lstat()
        except OSError as exc:
            results.append(
                {
                    "path": os.fspath(current),
                    "result": "unavailable",
                    "attributes": f"errno:{getattr(exc, 'errno', None)};winerror:{getattr(exc, 'winerror', None)}",
                }
            )
            break
        attributes = getattr(info, "st_file_attributes", 0)
        result = "redirected" if _is_reparse(info) else (
            "directory" if stat.S_ISDIR(info.st_mode) else "unexpected-node"
        )
        results.append(
            {
                "path": os.fspath(current),
                "result": result,
                "attributes": f"0x{attributes:08x}",
            }
        )
        parent = current.parent
        if parent == current:
            break
        current = parent
    return results


def _opened_file_identity_matches(opened: os.stat_result, linked: os.stat_result) -> bool:
    """Compare Windows file identity, never the spelling used to reach it."""

    return (
        opened.st_dev,
        opened.st_ino,
        stat.S_IFMT(opened.st_mode),
    ) == (
        linked.st_dev,
        linked.st_ino,
        stat.S_IFMT(linked.st_mode),
    )


def _before_bounded_file_open(_: Path) -> None:
    """Host-neutral fault seam immediately before the platform file open."""


def _same_identity(left: _PathIdentity, right: _PathIdentity, *, directory: bool) -> bool:
    if directory:
        return (
            left.device,
            left.inode,
            stat.S_IFMT(left.mode),
        ) == (
            right.device,
            right.inode,
            stat.S_IFMT(right.mode),
        )
    return left == right


def _path_identity(path: Path, *, directory: bool | None = None) -> _PathIdentity:
    try:
        info = path.lstat()
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap cannot prove the dedicated path identity: {path.name}",
            code="machine_bootstrap_install_identity_unproven",
            data={
                "component": "installNode.fileIdentity",
                "expected": "available-and-matching",
                "observed": "unavailable",
                "path": os.fspath(path),
                "errno": exc.errno,
                "winerror": getattr(exc, "winerror", None),
            },
        ) from exc
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if _is_reparse(info) or not expected:
        raise OrchestrateError(
            f"Machine bootstrap refuses a redirect or unexpected node in the dedicated installation: {path.name}",
            code="machine_bootstrap_install_identity_unproven",
            data={
                "component": "installNode.nodeType",
                "expected": "directory" if directory else "regular-file",
                "observed": "redirected" if _is_reparse(info) else "unexpected-node",
                "path": os.fspath(path),
                "attributes": f"0x{getattr(info, 'st_file_attributes', 0):08x}",
            },
        )
    return _PathIdentity.from_stat(info)


def _read_posix_bounded_file(path: Path, limit: int) -> _BoundedFile:
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    non_blocking = getattr(os, "O_NONBLOCK", 0)
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    operation = _operation_diagnostic(
        "bounded-read",
        "POSIX-open-descriptor",
        path,
        path_form="native-path",
        access="O_RDONLY|O_NOFOLLOW|O_NONBLOCK",
        share="not-applicable",
    )
    if not no_follow or not non_blocking:
        raise OrchestrateError(
            "This host cannot provide no-follow machine-bootstrap reads",
            code="machine_bootstrap_safe_io_unavailable",
            data={
                "component": "boundedRead.flags",
                "expected": "O_NOFOLLOW|O_NONBLOCK",
                "observed": f"O_NOFOLLOW:{bool(no_follow)};O_NONBLOCK:{bool(non_blocking)}",
                "operations": [operation],
            },
        )
    descriptor: int | None = None
    try:
        _before_bounded_file_open(path)
        descriptor = os.open(
            os.fspath(path),
            os.O_RDONLY | no_follow | non_blocking | close_on_exec | _binary_open_flag(),
        )
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or _is_reparse(info):
            raise OrchestrateError(
                f"Machine bootstrap refuses a non-file state entry: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.nodeType",
                    "expected": "regular-file",
                    "observed": "redirected" if _is_reparse(info) else "unexpected-node",
                    "path": os.fspath(path),
                    "operations": [operation],
                },
            )
        if info.st_size > limit:
            raise OrchestrateError(
                f"Machine bootstrap state exceeds its bounded read limit: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.handleSize",
                    "expected": f"0..{limit}",
                    "observed": info.st_size,
                    "handleSize": info.st_size,
                    "path": os.fspath(path),
                    "operations": [operation],
                },
            )
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > limit or len(raw) != info.st_size:
            raise OrchestrateError(
                f"Machine bootstrap state changed during its bounded read: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.bytes",
                    "expected": info.st_size,
                    "observed": len(raw),
                    "handleSize": info.st_size,
                    "path": os.fspath(path),
                    "operations": [operation],
                },
            )
        return _BoundedFile(raw, _PathIdentity.from_stat(info))
    except OrchestrateError:
        raise
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap state cannot be opened safely: {path.name}",
            code="machine_bootstrap_safe_io_unavailable",
            data={
                "component": "boundedRead.open",
                "expected": "valid-handle",
                "observed": "unavailable",
                "path": os.fspath(path),
                "errno": exc.errno,
                "operations": [operation],
            },
        ) from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _read_windows_bounded_file(path: Path, limit: int) -> _BoundedFile:
    """Read one exact non-reparse Windows file through its pinned handle."""

    import ctypes
    from ctypes import wintypes

    file_read_data = 0x0001
    file_read_attributes = 0x0080
    share_read = 0x00000001
    open_existing = 3
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
    operation = _operation_diagnostic(
        "bounded-read",
        "CreateFileW-handle",
        path,
        path_form="native-path",
        access="FILE_READ_DATA|FILE_READ_ATTRIBUTES",
        share="FILE_SHARE_READ",
    )

    _before_bounded_file_open(path)
    handle = create_file(
        os.fspath(path),
        file_read_data | file_read_attributes,
        share_read,
        None,
        open_existing,
        open_reparse_point,
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        winerror = ctypes.get_last_error()
        raise OrchestrateError(
            f"Machine bootstrap state cannot be opened safely: {path.name}",
            code="machine_bootstrap_safe_io_unavailable",
            data={
                "component": "boundedRead.open",
                "expected": "valid-handle",
                "observed": "invalid-handle",
                "winerror": winerror,
                "operations": [operation],
            },
        )
    descriptor: int | None = None
    try:
        import msvcrt

        descriptor = msvcrt.open_osfhandle(
            handle,
            os.O_RDONLY | _binary_open_flag(),
        )
        handle = None
        opened_info = os.stat(descriptor)
        linked_info = os.stat(path, follow_symlinks=False)
        if not _opened_file_identity_matches(opened_info, linked_info):
            raise OrchestrateError(
                f"Machine bootstrap state changed while its handle identity was proved: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.fileIdentity",
                    "expected": repr(_PathIdentity.from_stat(linked_info)),
                    "observed": repr(_PathIdentity.from_stat(opened_info)),
                    "handleIdentity": repr(_PathIdentity.from_stat(opened_info)),
                    "linkedIdentity": repr(_PathIdentity.from_stat(linked_info)),
                    "operations": [operation],
                },
            )
        tag = FileAttributeTagInfo()
        raw_handle = msvcrt.get_osfhandle(descriptor)
        if not get_info(raw_handle, file_attribute_tag_info_class, ctypes.byref(tag), ctypes.sizeof(tag)):
            winerror = ctypes.get_last_error()
            raise OrchestrateError(
                f"Machine bootstrap state identity is unavailable: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.attributes",
                    "expected": "available",
                    "observed": "unavailable",
                    "winerror": winerror,
                    "operations": [operation],
                },
            )
        if tag.FileAttributes & (file_attribute_reparse_point | file_attribute_directory):
            raise OrchestrateError(
                f"Machine bootstrap refuses a redirected or non-file state entry: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.nodeType",
                    "expected": "regular-file",
                    "observed": (
                        "redirected"
                        if tag.FileAttributes & file_attribute_reparse_point
                        else "directory"
                    ),
                    "attributes": f"0x{tag.FileAttributes:08x}",
                    "nodeType": "reparse-or-directory",
                    "operations": [operation],
                },
            )
        size = ctypes.c_longlong()
        size_available = bool(get_size(raw_handle, ctypes.byref(size)))
        size_error = ctypes.get_last_error() if not size_available else None
        if not size_available or size.value < 0 or size.value > limit:
            raise OrchestrateError(
                f"Machine bootstrap state exceeds or lacks its bounded size: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.handleSize",
                    "expected": f"0..{limit}",
                    "observed": size.value if size_available else "unavailable",
                    "handleSize": size.value if size_available else None,
                    "winerror": size_error,
                    "operations": [operation],
                },
            )
        raw = bytearray()
        while len(raw) <= limit:
            amount = min(64 * 1024, limit + 1 - len(raw))
            chunk = ctypes.create_string_buffer(amount)
            received = wintypes.DWORD()
            if not read_file(raw_handle, chunk, amount, ctypes.byref(received), None):
                winerror = ctypes.get_last_error()
                raise OrchestrateError(
                    f"Machine bootstrap state cannot be read: {path.name}",
                    code="machine_bootstrap_safe_io_unavailable",
                    data={
                        "component": "boundedRead.bytes",
                        "expected": size.value,
                        "observed": len(raw),
                        "handleSize": size.value,
                        "winerror": winerror,
                        "operations": [operation],
                    },
                )
            if received.value == 0:
                break
            raw.extend(chunk.raw[: received.value])
        if len(raw) > limit or len(raw) != size.value:
            raise OrchestrateError(
                f"Machine bootstrap state changed during its bounded read: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
                data={
                    "component": "boundedRead.bytes",
                    "expected": size.value,
                    "observed": len(raw),
                    "handleSize": size.value,
                    "operations": [operation],
                },
            )
        return _BoundedFile(bytes(raw), _PathIdentity.from_stat(opened_info))
    finally:
        if descriptor is not None:
            os.close(descriptor)
        elif handle not in (None, ctypes.c_void_p(-1).value):
            close_handle(handle)


def _read_bounded_regular_file(path: Path, limit: int) -> _BoundedFile:
    return _active_platform_provider().read_bounded_file(path, limit)


def _read_bounded_regular(path: Path, limit: int) -> bytes:
    return _read_bounded_regular_file(path, limit).raw


def _shim_matches(path: Path, expected: str) -> bool:
    try:
        return _read_bounded_regular(path, MAX_SHIM_BYTES).decode("utf-8", errors="strict") == expected
    except (OrchestrateError, UnicodeDecodeError):
        return False


def _shim_identity_data(path: Path, expected: str) -> dict[str, object]:
    expected_digest = f"sha256:{hashlib.sha256(expected.encode('utf-8')).hexdigest()}"
    try:
        raw = _read_bounded_regular(path, MAX_SHIM_BYTES)
        observed: object = f"sha256:{hashlib.sha256(raw).hexdigest()}"
    except OrchestrateError as exc:
        observed = exc.code
    return {
        "component": f"launcher.{path.name}.sha256",
        "expected": expected_digest,
        "observed": observed,
        "form": "windows-cmd" if path.suffix.casefold() == ".cmd" else "wsl-shell",
        "path": os.fspath(path),
        "pathForm": "native-path",
    }


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate receipt key: {key}")
        value[key] = item
    return value


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"unsupported receipt constant: {value}")


def _decode_pyvenv_config(raw: bytes) -> tuple[str, str]:
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise OrchestrateError(
            "The dedicated environment has a non-UTF-8 pyvenv.cfg and its identity cannot be proven",
            code="machine_bootstrap_venv_identity_unproven",
            data={
                "component": "pyvenvConfig.form",
                "expected": "utf8-key-value-record",
                "observed": "non-utf8",
            },
        ) from exc
    values: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        key, separator, value = line.partition("=")
        normalized = key.strip().casefold()
        if not separator or not normalized or normalized in values:
            raise OrchestrateError(
                "The dedicated environment has an ambiguous pyvenv.cfg and its identity cannot be proven",
                code="machine_bootstrap_venv_identity_unproven",
                data={
                    "component": "pyvenvConfig.form",
                    "expected": "unique-key-value-record",
                    "observed": "ambiguous",
                },
            )
        values[normalized] = value.strip()
    home = values.get("home", "")
    version = values.get("version", "")
    try:
        version_pair = tuple(int(part) for part in version.split(".")[:2])
    except ValueError as exc:
        raise OrchestrateError(
            "The dedicated environment has an invalid Python version identity",
            code="machine_bootstrap_venv_identity_unproven",
            data={
                "component": "pyvenvConfig.version.form",
                "expected": "numeric-major-minor",
                "observed": "invalid",
            },
        ) from exc
    if not home or len(version_pair) != 2 or version_pair < MINIMUM_PYTHON:
        raise OrchestrateError(
            "The dedicated environment does not prove a supported Python identity",
            code="machine_bootstrap_venv_identity_unproven",
            data={
                "component": "pyvenvConfig.pythonIdentity",
                "expected": "home-present-and-version-at-least-3.13",
                "observed": "incomplete-or-unsupported",
            },
        )
    return home, version


def _open_windows_path_pin(path: Path, *, directory: bool) -> object:
    """Hold a non-reparse node with sharing that denies replacement.

    Attribute-only access is not sufficient: Windows exempts attribute access
    from the ordinary share checks.  Files are therefore opened for actual
    read access, while directories allow child creation but never delete
    sharing.  The latter is what prevents a pinned directory entry from being
    renamed out from under a pathname-based child operation.
    """

    import ctypes
    from ctypes import wintypes

    generic_read = 0x80000000
    share_read = 0x00000001
    share_write = 0x00000002
    open_existing = 3
    open_reparse_point = 0x00200000
    backup_semantics = 0x02000000
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
    get_info = kernel32.GetFileInformationByHandleEx
    get_info.argtypes = (wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD)
    get_info.restype = wintypes.BOOL
    flags = open_reparse_point | (backup_semantics if directory else 0)
    access_name = "GENERIC_READ"
    share_name = "FILE_SHARE_READ|FILE_SHARE_WRITE" if directory else "FILE_SHARE_READ"
    operation = _operation_diagnostic(
        "path-pin",
        "CreateFileW-retained-handle",
        path,
        path_form="native-path",
        access=access_name,
        share=share_name,
    )
    handle = create_file(
        os.fspath(path),
        generic_read,
        share_read | (share_write if directory else 0),
        None,
        open_existing,
        flags,
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        winerror = ctypes.get_last_error()
        raise OrchestrateError(
            "The dedicated environment could not pin its Windows path identity",
            code="machine_bootstrap_venv_identity_unproven",
            data={
                "component": "pathPin.handle",
                "expected": "valid-handle",
                "observed": "invalid-handle",
                "path": os.fspath(path),
                "winerror": winerror,
                "operations": [operation],
            },
        )
    tag = FileAttributeTagInfo()
    if not get_info(handle, file_attribute_tag_info_class, ctypes.byref(tag), ctypes.sizeof(tag)):
        winerror = ctypes.get_last_error()
        _close_windows_path_pin(handle)
        raise OrchestrateError(
            "The dedicated environment could not inspect its pinned Windows path identity",
            code="machine_bootstrap_venv_identity_unproven",
            data={
                "component": "pathPin.attributes",
                "expected": "available",
                "observed": "unavailable",
                "path": os.fspath(path),
                "winerror": winerror,
                "operations": [operation],
            },
        )
    actual_directory = bool(tag.FileAttributes & file_attribute_directory)
    if tag.FileAttributes & file_attribute_reparse_point or actual_directory != directory:
        _close_windows_path_pin(handle)
        raise OrchestrateError(
            "The dedicated environment contains a redirected or unexpected Windows node",
            code="machine_bootstrap_venv_identity_unproven",
            data={
                "component": "pathPin.nodeType",
                "expected": "directory" if directory else "regular-file",
                "observed": (
                    "redirected"
                    if tag.FileAttributes & file_attribute_reparse_point
                    else ("directory" if actual_directory else "file")
                ),
                "path": os.fspath(path),
                "attributes": f"0x{tag.FileAttributes:08x}",
                "operations": [operation],
            },
        )
    return handle


def _close_windows_path_pin(handle: object) -> None:
    import ctypes
    from ctypes import wintypes

    close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    close_handle(handle)


class _PosixPlatformProvider:
    name = "posix"
    windows_semantics = False
    native_windows = False

    def read_bounded_file(self, path: Path, limit: int) -> _BoundedFile:
        return _read_posix_bounded_file(path, limit)

    def open_path_pin(self, path: Path, *, directory: bool) -> object:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | _binary_open_flag()
        )
        if directory:
            flags |= getattr(os, "O_DIRECTORY", 0)
        else:
            flags |= getattr(os, "O_NONBLOCK", 0)
        return os.open(os.fspath(path), flags)

    def close_path_pin(self, handle: object) -> None:
        os.close(int(handle))


class _NativeWin32PlatformProvider:
    name = "native-win32"
    windows_semantics = True
    native_windows = True

    def read_bounded_file(self, path: Path, limit: int) -> _BoundedFile:
        return _read_windows_bounded_file(path, limit)

    def open_path_pin(self, path: Path, *, directory: bool) -> object:
        return _open_windows_path_pin(path, directory=directory)

    def close_path_pin(self, handle: object) -> None:
        _close_windows_path_pin(handle)


class _SimulatedWin32PlatformProvider(_PosixPlatformProvider):
    """Win32 contract implemented with disposable host filesystem handles."""

    name = "simulated-win32"
    windows_semantics = True
    native_windows = False


_POSIX_PROVIDER = _PosixPlatformProvider()
_NATIVE_WIN32_PROVIDER = _NativeWin32PlatformProvider()
_SIMULATED_WIN32_PROVIDER = _SimulatedWin32PlatformProvider()


def _active_platform_provider() -> _PlatformProvider:
    selected = os.environ.get(SIMULATED_WIN32_PROVIDER_ENV, "").strip()
    if selected:
        if selected != _SIMULATED_WIN32_PROVIDER.name:
            raise OrchestrateError(
                "Machine bootstrap received an unsupported platform provider",
                code="machine_bootstrap_platform_unsupported",
                data={"observed": selected, "expected": _SIMULATED_WIN32_PROVIDER.name},
            )
        return _SIMULATED_WIN32_PROVIDER
    return _NATIVE_WIN32_PROVIDER if sys.platform == "win32" else _POSIX_PROVIDER


def _physical_ancestry(path: Path) -> list[Path]:
    """Return the absolute lexical directory ancestry from root to ``path``."""

    current = Path(os.path.abspath(path))
    ancestry = [current]
    while current.parent != current:
        current = current.parent
        ancestry.append(current)
        if len(ancestry) > 64:
            raise OrchestrateError(
                "The host-local anchor ancestry exceeds its bounded depth",
                code="machine_bootstrap_anchor_identity_unproven",
                data={
                    "component": "anchorAncestry.depth",
                    "expected": "0..64",
                    "observed": len(ancestry),
                },
            )
    return list(reversed(ancestry))


class _AnchorAncestryBinding(AbstractContextManager["_AnchorAncestryBinding"]):
    """Reject redirected anchor ancestors and retain their physical identities."""

    def __init__(self, anchor: Path) -> None:
        self.anchor = anchor
        self._provider = _active_platform_provider()
        self._pins: list[object] = []
        self._identities: list[tuple[Path, _PathIdentity]] = []
        try:
            for directory in _physical_ancestry(anchor.parent):
                identity = _path_identity(directory, directory=True)
                pin = self._provider.open_path_pin(directory, directory=True)
                observed = _path_identity(directory, directory=True)
                if not _same_identity(observed, identity, directory=True):
                    self._provider.close_path_pin(pin)
                    raise OrchestrateError(
                        "The host-local anchor ancestry changed while it was bound",
                        code="machine_bootstrap_anchor_identity_changed",
                        data={
                            "component": "anchorAncestry.fileIdentity",
                            "expected": repr(identity),
                            "observed": repr(observed),
                            "path": os.fspath(directory),
                        },
                    )
                self._pins.append(pin)
                self._identities.append((directory, identity))
            self.verify()
        except BaseException:
            self.close()
            raise

    def verify(self) -> None:
        for directory, identity in self._identities:
            try:
                observed = _path_identity(directory, directory=True)
            except OrchestrateError as exc:
                raise OrchestrateError(
                    "The host-local anchor ancestry changed after it was bound",
                    code="machine_bootstrap_anchor_identity_changed",
                    data={
                        "component": "anchorAncestry.fileIdentity",
                        "expected": repr(identity),
                        "observed": "unavailable-or-redirected",
                        "path": os.fspath(directory),
                    },
                ) from exc
            if not _same_identity(observed, identity, directory=True):
                raise OrchestrateError(
                    "The host-local anchor ancestry changed after it was bound",
                    code="machine_bootstrap_anchor_identity_changed",
                    data={
                        "component": "anchorAncestry.fileIdentity",
                        "expected": repr(identity),
                        "observed": repr(observed),
                        "path": os.fspath(directory),
                    },
                )

    def close(self) -> None:
        for pin in reversed(self._pins):
            self._provider.close_path_pin(pin)
        self._pins.clear()

    def __exit__(self, *_: object) -> None:
        self.close()


def _ignored_source_directory(name: str) -> bool:
    """The exact local/generated directory convention excluded from installs."""

    return (
        name in {".git", ".venv", "venv", "__pycache__", ".pytest_cache", "build", "dist"}
        or name.startswith((".venv-", "venv-"))
        or name.endswith((".egg-info", "-venv"))
    )


def _source_tree_records(root: Path) -> list[tuple[str, Path]]:
    records: list[tuple[str, Path]] = []
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise OrchestrateError(
                "The reviewed checkout could not be enumerated safely",
                code="machine_bootstrap_source_unavailable",
                data={"phase": "source-enumeration", "errno": exc.errno},
            ) from exc
        for entry in entries:
            relative = Path(entry.path).relative_to(root).as_posix()
            if entry.name == ".git":
                continue
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise OrchestrateError(
                    "The reviewed checkout changed during source identity collection",
                    code="machine_bootstrap_source_identity_changed",
                    data={
                        "component": "sourceEntry.fileIdentity",
                        "expected": "available-and-matching",
                        "observed": "unavailable-or-divergent",
                        "path": relative,
                        "errno": exc.errno,
                    },
                ) from exc
            if _is_reparse(info) or stat.S_ISLNK(info.st_mode):
                raise OrchestrateError(
                    "The reviewed checkout contains a redirected source entry",
                    code="machine_bootstrap_source_identity_unproven",
                    data={
                        "component": "sourceEntry.nodeType",
                        "expected": "regular-file-or-directory",
                        "observed": "redirected",
                        "path": relative,
                        "attributes": f"0x{getattr(info, 'st_file_attributes', 0):08x}",
                    },
                )
            if stat.S_ISDIR(info.st_mode):
                if not _ignored_source_directory(entry.name):
                    pending.append(Path(entry.path))
                continue
            if not stat.S_ISREG(info.st_mode):
                raise OrchestrateError(
                    "The reviewed checkout contains an unsupported source entry",
                    code="machine_bootstrap_source_identity_unproven",
                    data={
                        "component": "sourceEntry.nodeType",
                        "expected": "regular-file-or-directory",
                        "observed": f"mode:{stat.S_IFMT(info.st_mode):#x}",
                        "path": relative,
                    },
                )
            if entry.name.endswith((".pyc", ".pyo")):
                continue
            records.append((relative, Path(entry.path)))
            if len(records) > MAX_SOURCE_FILES:
                raise OrchestrateError(
                    "The reviewed checkout exceeds the bounded source-file limit",
                    code="machine_bootstrap_source_identity_unproven",
                    data={
                        "component": "sourceTree.fileCount",
                        "expected": MAX_SOURCE_FILES,
                        "observed": len(records),
                    },
                )
    return sorted(records)


def _source_tree_digest(root: Path) -> str:
    """Hash the bounded install input tree without following links."""

    digest = hashlib.sha256()
    consumed = 0
    for relative, path in _source_tree_records(root):
        remaining = MAX_SOURCE_BYTES - consumed
        if remaining < 0:
            raise OrchestrateError(
                "The reviewed checkout exceeds the bounded source-byte limit",
                code="machine_bootstrap_source_identity_unproven",
                data={
                    "component": "sourceTree.byteCount",
                    "expected": MAX_SOURCE_BYTES,
                    "observed": consumed,
                },
            )
        opened = _read_bounded_regular_file(path, remaining)
        consumed += len(opened.raw)
        if consumed > MAX_SOURCE_BYTES:
            raise OrchestrateError(
                "The reviewed checkout exceeds the bounded source-byte limit",
                code="machine_bootstrap_source_identity_unproven",
                data={
                    "component": "sourceTree.byteCount",
                    "expected": MAX_SOURCE_BYTES,
                    "observed": consumed,
                },
            )
        _update_source_tree_digest(digest, relative, opened)
    return digest.hexdigest()


def _update_source_tree_digest(
    digest: object,
    relative: str,
    opened: _BoundedFile,
) -> None:
    """Frame one exact source record for both review and archive digests."""

    name = relative.encode("utf-8", errors="strict")
    digest.update(len(name).to_bytes(4, "big"))  # type: ignore[attr-defined]
    digest.update(name)  # type: ignore[attr-defined]
    digest.update((opened.identity.mode & 0o111).to_bytes(2, "big"))  # type: ignore[attr-defined]
    digest.update(len(opened.raw).to_bytes(8, "big"))  # type: ignore[attr-defined]
    digest.update(opened.raw)  # type: ignore[attr-defined]


class _SourceBinding(AbstractContextManager["_SourceBinding"]):
    """Bind the reviewed checkout used to build the install archive and receipt."""

    def __init__(self, layout: MachineLayout) -> None:
        self._provider = _active_platform_provider()
        self.path = layout.source_root
        self.operation = _operation_diagnostic(
            "source-binding",
            (
                "retained-Windows-directory-handle+public-path"
                if self._provider.native_windows
                else "retained-directory-descriptor+proc-fd-path"
            ),
            self.path,
            path_form="native-path" if self._provider.native_windows else "proc-self-fd",
        )
        self._identity = _path_identity(self.path, directory=True)
        self._descriptor: int | None = None
        self._windows_handle: object | None = None
        try:
            if self._provider.native_windows:
                self._windows_handle = self._provider.open_path_pin(self.path, directory=True)
                self.effect_path = self.path
            else:
                flags = (
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_DIRECTORY", 0)
                )
                self._descriptor = os.open(os.fspath(self.path), flags)
                opened = _PathIdentity.from_stat(os.fstat(self._descriptor))
                if not _same_identity(opened, self._identity, directory=True):
                    raise OrchestrateError(
                        "The reviewed checkout changed while its root was being bound",
                        code="machine_bootstrap_source_identity_unproven",
                        data={
                            "component": "sourceRoot.fileIdentity",
                            "expected": repr(self._identity),
                            "observed": repr(opened),
                            "operations": [self.operation],
                        },
                    )
                descriptor_path = Path(f"/proc/self/fd/{self._descriptor}")
                self.operation = _operation_diagnostic(
                    "source-binding",
                    "retained-directory-descriptor+proc-fd-path",
                    descriptor_path,
                    path_form="proc-self-fd",
                )
                if not descriptor_path.exists():
                    raise OrchestrateError(
                        "This host cannot bind source archiving to the reviewed checkout descriptor",
                        code="machine_bootstrap_source_identity_unproven",
                        data={
                            "component": "sourceRoot.effectPath",
                            "expected": "available-descriptor-path",
                            "observed": "unavailable",
                            "path": os.fspath(descriptor_path),
                            "operations": [self.operation],
                        },
                    )
                self.effect_path = descriptor_path
            self.resolved = os.fspath(self.path.resolve(strict=True))
            self.tree_digest = _source_tree_digest(self.effect_path)
            self.verify()
        except BaseException as exc:
            _diagnostic_exception_note(exc, {"operations": [self.operation]})
            self.close()
            raise

    def verify(self) -> None:
        try:
            current = _path_identity(self.path, directory=True)
        except OrchestrateError as exc:
            failure = OrchestrateError(
                "The reviewed checkout root changed after its identity was bound",
                code="machine_bootstrap_source_identity_changed",
                data={
                    "component": "sourceRoot.fileIdentity",
                    "expected": repr(self._identity),
                    "observed": "unavailable-or-redirected",
                    "operations": [self.operation],
                },
            )
            raise failure from exc
        if not _same_identity(current, self._identity, directory=True):
            raise OrchestrateError(
                "The reviewed checkout root changed after its identity was bound",
                code="machine_bootstrap_source_identity_changed",
                data={
                    "component": "sourceRoot.fileIdentity",
                    "expected": repr(self._identity),
                    "observed": repr(current),
                    "operations": [self.operation],
                },
            )
        observed_digest = _source_tree_digest(self.effect_path)
        if observed_digest != self.tree_digest:
            raise OrchestrateError(
                "The reviewed checkout content changed during machine bootstrap",
                code="machine_bootstrap_source_identity_changed",
                data={
                    "component": "sourceTree.sha256",
                    "expected": f"sha256:{self.tree_digest}",
                    "observed": f"sha256:{observed_digest}",
                    "operations": [self.operation],
                },
            )

    def diagnostic_data(self) -> dict[str, object]:
        return {"operations": [self.operation]}

    def receipt_fields(self) -> dict[str, object]:
        return {
            "sourceRoot": self.resolved,
            "sourceTreeSha256": f"sha256:{self.tree_digest}",
        }

    def pass_fds(self) -> tuple[int, ...]:
        return () if self._descriptor is None else (self._descriptor,)

    def close(self) -> None:
        if self._descriptor is not None:
            try:
                os.close(self._descriptor)
            except OSError:
                pass
            self._descriptor = None
        if self._windows_handle is not None:
            self._provider.close_path_pin(self._windows_handle)
            self._windows_handle = None

    def __exit__(self, *_: object) -> None:
        self.close()


class _ArchiveBinding(AbstractContextManager["_ArchiveBinding"]):
    """Pin the exact immutable archive consumed by pip."""

    def __init__(
        self,
        path: Path,
        *,
        stage_diagnostics: Sequence[Mapping[str, object]] = (),
    ) -> None:
        self.path = path
        self._provider = _active_platform_provider()
        self.operation = _operation_diagnostic(
            "archive-binding",
            (
                "retained-Windows-file-handle+public-path"
                if self._provider.native_windows
                else "retained-file-descriptor+proc-fd-path"
            ),
            path,
            path_form="native-path" if self._provider.native_windows else "proc-self-fd",
        )
        self.stage_diagnostics = [dict(item) for item in stage_diagnostics]
        self._handle: object | None = None
        self._descriptor: int | None = None
        try:
            if self._provider.native_windows:
                self._handle = self._provider.open_path_pin(path, directory=False)
                opened = _read_bounded_regular_file(path, MAX_SOURCE_ARCHIVE_BYTES)
                self.effect_path = path
            else:
                descriptor = self._provider.open_path_pin(path, directory=False)
                self._descriptor = int(descriptor)
                info = os.fstat(self._descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_SOURCE_ARCHIVE_BYTES:
                    raise OrchestrateError(
                        "The installed-source archive identity is unavailable",
                        code="machine_bootstrap_source_identity_unproven",
                        data={
                            "component": "sourceArchive.formAndSize",
                            "expected": f"regular-file;size:0..{MAX_SOURCE_ARCHIVE_BYTES}",
                            "observed": f"mode:{stat.S_IFMT(info.st_mode):#x};size:{info.st_size}",
                            "path": os.fspath(path),
                            "operations": [self.operation],
                        },
                    )
                raw = bytearray()
                while len(raw) <= MAX_SOURCE_ARCHIVE_BYTES:
                    chunk = os.read(self._descriptor, min(64 * 1024, MAX_SOURCE_ARCHIVE_BYTES + 1 - len(raw)))
                    if not chunk:
                        break
                    raw.extend(chunk)
                if len(raw) != info.st_size:
                    raise OrchestrateError(
                        "The installed-source archive changed while it was opened",
                        code="machine_bootstrap_source_identity_changed",
                        data={
                            "component": "sourceArchive.size",
                            "expected": info.st_size,
                            "observed": len(raw),
                            "path": os.fspath(path),
                            "operations": [self.operation],
                        },
                    )
                opened = _BoundedFile(bytes(raw), _PathIdentity.from_stat(info))
                self.effect_path = Path(f"/proc/self/fd/{self._descriptor}")
                self.operation = _operation_diagnostic(
                    "archive-binding",
                    "retained-file-descriptor+proc-fd-path",
                    self.effect_path,
                    path_form="proc-self-fd",
                )
            self.digest = hashlib.sha256(opened.raw).hexdigest()
            self.size = len(opened.raw)
            self.identity = opened.identity
            self.stage_diagnostics.append(
                _archive_stage_diagnostic(
                    "bound-handle",
                    opened.raw,
                    form=self.operation["form"],  # type: ignore[arg-type]
                    path=self.effect_path,
                )
            )
            self.verify()
        except BaseException as exc:
            _diagnostic_exception_note(exc, self.diagnostic_data())
            self.close()
            raise

    def verify(self, *, stage: str | None = None) -> None:
        if self._descriptor is not None:
            os.lseek(self._descriptor, 0, os.SEEK_SET)
            raw = bytearray()
            while len(raw) <= MAX_SOURCE_ARCHIVE_BYTES:
                chunk = os.read(
                    self._descriptor,
                    min(64 * 1024, MAX_SOURCE_ARCHIVE_BYTES + 1 - len(raw)),
                )
                if not chunk:
                    break
                raw.extend(chunk)
            current_raw = bytes(raw)
        else:
            current_raw = _read_bounded_regular_file(
                self.effect_path, MAX_SOURCE_ARCHIVE_BYTES
            ).raw
        observed = f"sha256:{hashlib.sha256(current_raw).hexdigest()}"
        expected = f"sha256:{self.digest}"
        if stage is not None:
            self.stage_diagnostics.append(
                _archive_stage_diagnostic(
                    stage,
                    current_raw,
                    form=self.operation["form"],  # type: ignore[arg-type]
                    path=self.effect_path,
                )
            )
        if len(current_raw) != self.size or observed != expected:
            raise OrchestrateError(
                "The installed-source archive changed after it was bound",
                code="machine_bootstrap_source_identity_changed",
                data={
                    "component": "sourceArchive.sha256",
                    "expected": expected,
                    "observed": observed,
                    **self.diagnostic_data(),
                },
            )

    def diagnostic_data(self) -> dict[str, object]:
        return {
            "operations": [self.operation],
            "archiveStages": list(self.stage_diagnostics),
        }

    def receipt_fields(self) -> dict[str, object]:
        return {
            "sourceArchiveSha256": f"sha256:{self.digest}",
            "sourceArchiveSize": self.size,
        }

    def pass_fds(self) -> tuple[int, ...]:
        return () if self._descriptor is None else (self._descriptor,)

    def close(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        if self._handle is not None:
            self._provider.close_path_pin(self._handle)
            self._handle = None

    def __exit__(self, *_: object) -> None:
        self.close()


def _build_source_archive(
    layout: MachineLayout,
    source: _SourceBinding,
) -> list[dict[str, object]]:
    source.verify()
    buffer = io.BytesIO()
    consumed = 0
    archived_source_digest = hashlib.sha256()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED, strict_timestamps=True) as archive:
        for relative, path in _source_tree_records(source.effect_path):
            opened = _read_bounded_regular_file(path, MAX_SOURCE_BYTES - consumed)
            consumed += len(opened.raw)
            if consumed > MAX_SOURCE_BYTES:
                raise OrchestrateError(
                    "The reviewed checkout exceeds the bounded source-byte limit",
                    code="machine_bootstrap_source_identity_unproven",
                    data={
                        "component": "sourceArchive.inputByteCount",
                        "expected": MAX_SOURCE_BYTES,
                        "observed": consumed,
                        **source.diagnostic_data(),
                    },
                )
            _update_source_tree_digest(archived_source_digest, relative, opened)
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = ((0o755 if opened.identity.mode & 0o111 else 0o644) & 0xFFFF) << 16
            archive.writestr(info, opened.raw)
    archived_digest = archived_source_digest.hexdigest()
    if archived_digest != source.tree_digest:
        raise OrchestrateError(
            "The archive input bytes do not match the reviewed source snapshot",
            code="machine_bootstrap_source_identity_changed",
            data={
                "component": "sourceArchive.inputTreeSha256",
                "expected": f"sha256:{source.tree_digest}",
                "observed": f"sha256:{archived_digest}",
                **source.diagnostic_data(),
            },
        )
    source.verify()
    raw = buffer.getvalue()
    if len(raw) > MAX_SOURCE_ARCHIVE_BYTES:
        raise OrchestrateError(
            "The reviewed checkout archive exceeds its bounded byte limit",
            code="machine_bootstrap_source_identity_unproven",
            data={
                "component": "sourceArchive.size",
                "expected": MAX_SOURCE_ARCHIVE_BYTES,
                "observed": len(raw),
                **source.diagnostic_data(),
            },
        )
    stages = [
        _archive_stage_diagnostic(
            "buffer",
            raw,
            form="in-memory-zip",
            path=layout.source_archive,
        )
    ]
    _atomic_write_owned(
        layout.source_archive,
        raw,
        executable=False,
        expected_target=_target_identity(layout.source_archive),
        archive_stages=stages,
    )
    return stages


class _VenvBinding(AbstractContextManager["_VenvBinding"]):
    """Pin and repeatedly prove the dedicated interpreter before every effect."""

    def __init__(self, layout: MachineLayout) -> None:
        self._provider = _active_platform_provider()
        self.layout = layout
        self.config_path = layout.venv_root / "pyvenv.cfg"
        self.python_path = layout.scripts_root / "python.exe"
        self._identities: dict[Path, _PathIdentity] = {}
        self._descriptors: dict[Path, int] = {}
        self._windows_handles: list[object] = []
        nodes = (
            (layout.install_root, True),
            (layout.venv_root, True),
            (layout.scripts_root, True),
            (self.python_path, False),
            (self.config_path, False),
        )
        try:
            for path, directory in nodes:
                self._identities[path] = _path_identity(path, directory=directory)
                if self._provider.native_windows:
                    self._windows_handles.append(
                        self._provider.open_path_pin(path, directory=directory)
                    )
                else:
                    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
                    if directory:
                        flags |= getattr(os, "O_DIRECTORY", 0)
                    else:
                        flags |= getattr(os, "O_NONBLOCK", 0)
                    descriptor = os.open(os.fspath(path), flags)
                    if not _same_identity(
                        _PathIdentity.from_stat(os.fstat(descriptor)),
                        self._identities[path],
                        directory=directory,
                    ):
                        os.close(descriptor)
                        raise OrchestrateError(
                            "The dedicated environment changed while its physical identity was being pinned",
                            code="machine_bootstrap_venv_identity_unproven",
                            data={
                                "component": "venvNode.fileIdentity",
                                "expected": repr(self._identities[path]),
                                "observed": repr(_PathIdentity.from_stat(os.fstat(descriptor))),
                                "path": os.fspath(path),
                            },
                        )
                    self._descriptors[path] = descriptor
            self.config_raw = _read_bounded_regular(self.config_path, MAX_PYVENV_CONFIG_BYTES)
            self.home, self.version = _decode_pyvenv_config(self.config_raw)
            self.config_digest = hashlib.sha256(self.config_raw).hexdigest()
            self.root_resolved = os.fspath(layout.venv_root.resolve(strict=True))
            self.python_resolved = os.fspath(self.python_path.resolve(strict=True))
        except BaseException:
            self.close()
            raise

    def verify(self) -> None:
        for path, identity in self._identities.items():
            directory = stat.S_ISDIR(identity.mode)
            observed_identity = _path_identity(path, directory=directory)
            if not _same_identity(observed_identity, identity, directory=directory):
                raise OrchestrateError(
                    "The dedicated environment changed after its physical identity was pinned",
                    code="machine_bootstrap_venv_identity_changed",
                    data={
                        "component": "venvNode.fileIdentity",
                        "expected": repr(identity),
                        "observed": repr(observed_identity),
                        "path": os.fspath(path),
                    },
                )
        observed_config_digest = hashlib.sha256(
            _read_bounded_regular(self.config_path, MAX_PYVENV_CONFIG_BYTES)
        ).hexdigest()
        if observed_config_digest != self.config_digest:
            raise OrchestrateError(
                "The dedicated environment configuration changed after identity verification",
                code="machine_bootstrap_venv_identity_changed",
                data={
                    "component": "pyvenvConfig.sha256",
                    "expected": f"sha256:{self.config_digest}",
                    "observed": f"sha256:{observed_config_digest}",
                    "path": os.fspath(self.config_path),
                },
            )
        # Path spellings are not identity evidence on Windows (short/long
        # aliases and case can differ).  The comparisons above use file IDs,
        # while retained Windows handles also deny write/delete replacement.

    def receipt_fields(self) -> dict[str, object]:
        command = _command_proof(self.layout.command_path)
        return {
            "venvRoot": self.root_resolved,
            "pythonPath": self.python_resolved,
            "pyvenvConfigSha256": f"sha256:{self.config_digest}",
            "commandSize": command.identity.size,
            "commandSha256": f"sha256:{hashlib.sha256(command.raw).hexdigest()}",
        }

    def runner_kwargs(self, *, pass_fds: tuple[int, ...] = ()) -> dict[str, object]:
        if self._provider.native_windows:
            # subprocess forwards this as CreateProcess's explicit application
            # name.  The retained GENERIC_READ handle permits image reads while
            # denying writes/deletes until the post-effect identity proof.
            return {"executable": os.fspath(self.python_path)}
        descriptor = self._descriptors[self.python_path]
        executable = f"/proc/self/fd/{descriptor}"
        if not os.path.exists(executable):
            raise OrchestrateError(
                "This host cannot execute the dedicated interpreter through its pinned descriptor",
                code="machine_bootstrap_venv_identity_unproven",
                data={
                    "component": "interpreter.effectPath",
                    "expected": "available-descriptor-path",
                    "observed": "unavailable",
                    "operations": [
                        _operation_diagnostic(
                            "interpreter-effect",
                            "descriptor-executable+pass-fds",
                            executable,
                            path_form="proc-self-fd",
                            explicit_application_name=False,
                        )
                    ],
                },
            )
        return {"executable": executable, "pass_fds": (descriptor, *pass_fds)}

    def raise_for_denied_identity_mutation(self, failure: BaseException) -> None:
        """Promote a denied write to a bound venv node over process failure."""

        cause = failure.__cause__
        if not isinstance(cause, OSError):
            return
        attempted = [
            Path(value)
            for value in (getattr(cause, "filename", None), getattr(cause, "filename2", None))
            if isinstance(value, (str, bytes, os.PathLike))
        ]
        for candidate in attempted:
            for bound in self._identities:
                if _same_path(candidate, bound):
                    raise OrchestrateError(
                        "A machine-bootstrap effect attempted to change the bound dedicated environment",
                        code="machine_bootstrap_venv_identity_changed",
                        data={
                            "component": "venvNode.deniedMutation",
                            "expected": "unchanged-bound-node",
                            "observed": "mutation-denied",
                            "path": os.fspath(bound),
                            "errno": cause.errno,
                            "winerror": getattr(cause, "winerror", None),
                        },
                    ) from failure

    def close(self) -> None:
        for descriptor in reversed(tuple(self._descriptors.values())):
            try:
                os.close(descriptor)
            except OSError:
                pass
        self._descriptors.clear()
        for handle in reversed(self._windows_handles):
            self._provider.close_path_pin(handle)
        self._windows_handles.clear()

    def __exit__(self, *_: object) -> None:
        self.close()


def _receipt_value(
    binding: _VenvBinding,
    source: _SourceBinding,
    archive: _ArchiveBinding,
    *,
    installation_id: str,
) -> dict[str, object]:
    payload = {
        "schema": INSTALL_RECEIPT_SCHEMA,
        "installationId": installation_id,
        **source.receipt_fields(),
        **archive.receipt_fields(),
        **binding.receipt_fields(),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        **payload,
        "receiptSha256": f"sha256:{hashlib.sha256(canonical).hexdigest()}",
    }


def _receipt_integrity_matches(value: dict[str, object]) -> bool:
    recorded = value.get("receiptSha256")
    if not isinstance(recorded, str):
        return False
    payload = {key: item for key, item in value.items() if key != "receiptSha256"}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return recorded == f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _anchor_value(receipt: dict[str, object]) -> dict[str, object]:
    payload = {
        "schema": INSTALL_ANCHOR_SCHEMA,
        "installationId": receipt["installationId"],
        "receiptSha256": receipt["receiptSha256"],
        "sourceTreeSha256": receipt["sourceTreeSha256"],
        "sourceArchiveSha256": receipt["sourceArchiveSha256"],
        "commandSha256": receipt["commandSha256"],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {**payload, "anchorSha256": f"sha256:{hashlib.sha256(canonical).hexdigest()}"}


def _read_canonical_json_diagnostic(
    path: Path,
    limit: int,
) -> tuple[object | None, str]:
    try:
        raw_bytes = _read_bounded_regular(path, limit)
        raw = raw_bytes.decode("utf-8", errors="strict")
        value = json.loads(
            raw,
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
        )
    except OrchestrateError as exc:
        return None, exc.code
    except UnicodeDecodeError:
        return None, "non-utf8"
    except json.JSONDecodeError:
        return None, "invalid-json"
    except ValueError:
        return None, "duplicate-or-nonfinite-json"
    canonical = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    return (value, "canonical") if raw_bytes == canonical else (None, "noncanonical-json")


def _read_canonical_json(path: Path, limit: int) -> object | None:
    return _read_canonical_json_diagnostic(path, limit)[0]


def _receipt_match_diagnostic(
    layout: MachineLayout,
    binding: _VenvBinding,
    source: _SourceBinding,
    archive: _ArchiveBinding,
) -> tuple[bool, dict[str, object]]:
    def mismatch(data: dict[str, object]) -> tuple[bool, dict[str, object]]:
        archive_data = archive.diagnostic_data()
        return False, {
            **data,
            "operations": [
                *source.diagnostic_data()["operations"],  # type: ignore[misc]
                *archive_data["operations"],  # type: ignore[misc]
            ],
            "archiveStages": archive_data["archiveStages"],
            "anchorAncestry": _anchor_ancestry_diagnostic(layout.install_anchor),
        }

    value, receipt_form = _read_canonical_json_diagnostic(
        layout.install_receipt,
        MAX_INSTALL_RECEIPT_BYTES,
    )
    expected_keys = {
        "schema",
        "installationId",
        "sourceRoot",
        "sourceTreeSha256",
        "sourceArchiveSha256",
        "sourceArchiveSize",
        "venvRoot",
        "pythonPath",
        "pyvenvConfigSha256",
        "commandSize",
        "commandSha256",
        "receiptSha256",
    }
    if not isinstance(value, dict):
        return mismatch(
            {
                "component": "receipt.form",
                "expected": "canonical-object",
                "observed": receipt_form,
            }
        )
    if set(value) != expected_keys:
        return mismatch(
            {
                "component": "receipt.fields",
                "expected": sorted(expected_keys),
                "observed": sorted(str(key) for key in value),
            }
        )
    string_keys = (
        "schema",
        "installationId",
        "sourceRoot",
        "sourceTreeSha256",
        "sourceArchiveSha256",
        "venvRoot",
        "pythonPath",
        "pyvenvConfigSha256",
        "commandSha256",
        "receiptSha256",
    )
    integer_keys = (
        "commandSize",
        "sourceArchiveSize",
    )
    for key in string_keys:
        if not isinstance(value.get(key), str):
            return mismatch(
                {
                    "component": f"receipt.{key}.form",
                    "expected": "string",
                    "observed": type(value.get(key)).__name__,
                }
            )
    for key in integer_keys:
        if type(value.get(key)) is not int or value[key] < 0:
            return mismatch(
                {
                    "component": f"receipt.{key}.form",
                    "expected": "nonnegative-integer",
                    "observed": type(value.get(key)).__name__,
                }
            )
    if not value["installationId"]:
        return mismatch(
            {
                "component": "receipt.installationId.form",
                "expected": "nonempty-string",
                "observed": "empty-string",
            }
        )
    if not _receipt_integrity_matches(value):
        payload = {key: item for key, item in value.items() if key != "receiptSha256"}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        expected_digest = f"sha256:{hashlib.sha256(canonical).hexdigest()}"
        return mismatch(
            {
                "component": "receipt.receiptSha256",
                "expected": expected_digest,
                "observed": value["receiptSha256"],
            }
        )
    expected = _receipt_value(binding, source, archive, installation_id=value["installationId"])
    for key in sorted(expected):
        if key == "receiptSha256":
            continue
        if key in {"sourceRoot", "venvRoot", "pythonPath"} and _same_path(
            value[key], expected[key]
        ):
            continue
        if value[key] != expected[key]:
            diagnostic: dict[str, object] = {
                "component": f"receipt.{key}",
                "expected": expected[key],
                "observed": value[key],
            }
            if key in {"sourceRoot", "venvRoot", "pythonPath"}:
                diagnostic["pathComparison"] = {
                    "expectedNormalized": _normalize_path_entry(os.fspath(expected[key])),
                    "observedNormalized": _normalize_path_entry(os.fspath(value[key])),
                    "matched": False,
                }
            return mismatch(diagnostic)
    with _AnchorAncestryBinding(layout.install_anchor) as anchor_ancestry:
        anchor, anchor_form = _read_canonical_json_diagnostic(
            layout.install_anchor,
            MAX_INSTALL_RECEIPT_BYTES,
        )
        anchor_ancestry.verify()
    expected_anchor = _anchor_value(value)
    if not isinstance(anchor, dict):
        return mismatch(
            {
                "component": "anchor.form",
                "expected": "canonical-object",
                "observed": anchor_form,
            }
        )
    for key in sorted(set(expected_anchor) | set(anchor)):
        if anchor.get(key) != expected_anchor.get(key):
            return mismatch(
                {
                    "component": f"anchor.{key}",
                    "expected": expected_anchor.get(key),
                    "observed": anchor.get(key),
                }
            )
    return True, {"component": "receipt-and-anchor", "expected": "matching", "observed": "matching"}


def _install_receipt_matches(
    layout: MachineLayout,
    binding: _VenvBinding,
    source: _SourceBinding,
    archive: _ArchiveBinding | None = None,
) -> bool:
    try:
        if archive is None:
            with _ArchiveBinding(layout.source_archive) as selected_archive:
                return _receipt_match_diagnostic(layout, binding, source, selected_archive)[0]
        return _receipt_match_diagnostic(layout, binding, source, archive)[0]
    except OrchestrateError:
        return False


def _command_proof(path: Path) -> _BoundedFile:
    opened = _read_bounded_regular_file(path, MAX_COMMAND_BYTES)
    if opened.identity.links != 1:
        raise OrchestrateError(
            "The installed orchestrate command is not a single owned file",
            code="machine_bootstrap_command_identity_unproven",
            data={
                "component": "command.linkCount",
                "expected": 1,
                "observed": opened.identity.links,
                "path": os.fspath(path),
            },
        )
    return opened


def _read_install_receipt(layout: MachineLayout) -> object | None:
    return _read_canonical_json(layout.install_receipt, MAX_INSTALL_RECEIPT_BYTES)


def _target_identity(path: Path) -> _PathIdentity | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap cannot inspect the replacement target: {path.name}",
            code="machine_bootstrap_target_identity_unproven",
            data={
                "component": "target.fileIdentity",
                "expected": "available-and-matching",
                "observed": "unavailable",
                "path": os.fspath(path),
                "errno": exc.errno,
                "winerror": getattr(exc, "winerror", None),
            },
        ) from exc
    if _is_reparse(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise OrchestrateError(
            f"Machine bootstrap refuses to replace an unowned target entry: {path.name}",
            code="machine_bootstrap_target_identity_unproven",
            data={
                "component": "target.nodeTypeAndLinks",
                "expected": "owned-single-link-regular-file",
                "observed": (
                    "redirected"
                    if _is_reparse(info)
                    else f"mode:{stat.S_IFMT(info.st_mode):#x};links:{info.st_nlink}"
                ),
                "path": os.fspath(path),
            },
        )
    return _PathIdentity.from_stat(info)


def _target_identity_at(parent_fd: int | None, parent: Path, name: str) -> _PathIdentity | None:
    if parent_fd is None:
        return _target_identity(parent / name)
    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap cannot inspect the replacement target: {name}",
            code="machine_bootstrap_target_identity_unproven",
            data={
                "component": "target.fileIdentity",
                "expected": "available-and-matching",
                "observed": "unavailable",
                "path": os.fspath(parent / name),
                "errno": exc.errno,
                "winerror": getattr(exc, "winerror", None),
            },
        ) from exc
    if _is_reparse(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise OrchestrateError(
            f"Machine bootstrap refuses to replace an unowned target entry: {name}",
            code="machine_bootstrap_target_identity_unproven",
            data={
                "component": "target.nodeTypeAndLinks",
                "expected": "owned-single-link-regular-file",
                "observed": (
                    "redirected"
                    if _is_reparse(info)
                    else f"mode:{stat.S_IFMT(info.st_mode):#x};links:{info.st_nlink}"
                ),
                "path": os.fspath(parent / name),
            },
        )
    return _PathIdentity.from_stat(info)


def _commit_staged_no_replace(
    temporary_name: str,
    target_name: str,
    *,
    parent_fd: int | None,
    parent: Path,
    staged_descriptor: int | None = None,
    native_windows: bool = False,
) -> None:
    """One fault-injection seam around the platform's no-overwrite commit."""

    if staged_descriptor is not None and not native_windows:
        descriptor_path = f"/proc/self/fd/{staged_descriptor}"
        if not os.path.exists(descriptor_path):
            raise OrchestrateError(
                "Machine bootstrap cannot bind its commit to the staged descriptor",
                code="machine_bootstrap_commit_identity_unproven",
                data={
                    "component": "commit.stagedDescriptor",
                    "expected": "available",
                    "observed": "unavailable",
                },
            )
        os.link(
            descriptor_path,
            target_name if parent_fd is not None else os.fspath(parent / target_name),
            **({} if parent_fd is None else {"dst_dir_fd": parent_fd}),
            follow_symlinks=True,
        )
    elif parent_fd is None:
        os.link(
            os.fspath(parent / temporary_name),
            os.fspath(parent / target_name),
            follow_symlinks=False,
        )
    else:
        os.link(
            temporary_name,
            target_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
            follow_symlinks=False,
        )


def _before_staging_parent_open(_: Path) -> None:
    """Host-neutral fault seam immediately before the parent is bound."""


def _before_staged_commit(_: Path) -> None:
    """Fault seam after fsync while the staged descriptor remains retained."""


def _atomic_write_owned(
    path: Path,
    raw: bytes,
    *,
    executable: bool,
    expected_target: _PathIdentity | None,
    archive_stages: list[dict[str, object]] | None = None,
) -> None:
    provider = _active_platform_provider()
    staging_operation = _operation_diagnostic(
        "atomic-staging",
        (
            "Windows-path+retained-parent-handle"
            if provider.native_windows
            else (
                "Windows-path+descriptor-backed-parent-pin"
                if provider.windows_semantics
                else "directory-fd-relative"
            )
        ),
        path,
        path_form="native-path" if provider.windows_semantics else "directory-fd-relative",
    )
    parent_identity = _path_identity(path.parent, directory=True)
    if expected_target is not None:
        raise OrchestrateError(
            f"Machine bootstrap refuses to overwrite the existing target: {path.name}",
            code="machine_bootstrap_target_identity_unproven",
            data={
                "component": "target.fileIdentity",
                "expected": "absent",
                "observed": repr(expected_target),
                "path": os.fspath(path),
                "operations": [staging_operation],
            },
        )
    parent_fd: int | None = None
    parent_handle: object | None = None
    descriptor: int | None = None
    temporary_name: str | None = None
    try:
        _before_staging_parent_open(path.parent)
        if provider.windows_semantics:
            try:
                parent_handle = provider.open_path_pin(path.parent, directory=True)
            except (OSError, OrchestrateError) as exc:
                raise OrchestrateError(
                    "Machine bootstrap staging parent could not be bound",
                    code="machine_bootstrap_parent_identity_changed",
                    data={
                        "component": "stagingParent.fileIdentity",
                        "expected": repr(parent_identity),
                        "observed": getattr(exc, "code", type(exc).__name__),
                        "path": os.fspath(path.parent),
                        "errno": getattr(exc, "errno", None),
                        "winerror": getattr(exc, "winerror", None),
                        "operations": [staging_operation],
                    },
                ) from exc
            observed_parent = _path_identity(path.parent, directory=True)
            if not _same_identity(observed_parent, parent_identity, directory=True):
                raise OrchestrateError(
                    "Machine bootstrap staging parent changed while it was being bound",
                    code="machine_bootstrap_parent_identity_changed",
                    data={
                        "component": "stagingParent.fileIdentity",
                        "expected": repr(parent_identity),
                        "observed": repr(observed_parent),
                        "path": os.fspath(path.parent),
                        "operations": [staging_operation],
                    },
                )
        else:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                parent_fd = os.open(os.fspath(path.parent), flags)
            except OSError as exc:
                raise OrchestrateError(
                    "Machine bootstrap staging parent could not be bound",
                    code="machine_bootstrap_parent_identity_changed",
                    data={
                        "component": "stagingParent.fileIdentity",
                        "expected": repr(parent_identity),
                        "observed": "unavailable",
                        "path": os.fspath(path.parent),
                        "errno": exc.errno,
                        "operations": [staging_operation],
                    },
                ) from exc
            observed_parent = _PathIdentity.from_stat(os.fstat(parent_fd))
            if not _same_identity(observed_parent, parent_identity, directory=True):
                raise OrchestrateError(
                    "Machine bootstrap staging parent changed while it was being bound",
                    code="machine_bootstrap_parent_identity_changed",
                    data={
                        "component": "stagingParent.fileIdentity",
                        "expected": repr(parent_identity),
                        "observed": repr(observed_parent),
                        "path": os.fspath(path.parent),
                        "operations": [staging_operation],
                    },
                )
        observed_target = _target_identity_at(parent_fd, path.parent, path.name)
        if observed_target != expected_target:
            raise OrchestrateError(
                f"Machine bootstrap target changed before staging: {path.name}",
                code="machine_bootstrap_target_identity_changed",
                data={
                    "component": "target.fileIdentity",
                    "expected": repr(expected_target),
                    "observed": repr(observed_target),
                    "path": os.fspath(path),
                    "operations": [staging_operation],
                },
            )
        for _ in range(32):
            temporary_name = f".{path.name}.{secrets.token_hex(16)}.tmp"
            try:
                descriptor = os.open(
                    temporary_name if parent_fd is not None else os.fspath(path.parent / temporary_name),
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | _binary_open_flag(),
                    0o600,
                    **({} if parent_fd is None else {"dir_fd": parent_fd}),
                )
                break
            except FileExistsError:
                temporary_name = None
        if descriptor is None or temporary_name is None:
            raise OrchestrateError(
                "Machine bootstrap could not reserve an exclusive staging file",
                code="machine_bootstrap_temporary_identity_unproven",
                data={
                    "component": "stagingTemporary.form",
                    "expected": "exclusive-owned-regular-file",
                    "observed": "reservation-exhausted",
                    "path": os.fspath(path.parent),
                    "operations": [staging_operation],
                },
            )
        opened = os.fstat(descriptor)
        linked = os.stat(
            temporary_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        ) if parent_fd is not None else (path.parent / temporary_name).lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or _is_reparse(opened)
            or opened.st_nlink != 1
            or _PathIdentity.from_stat(opened) != _PathIdentity.from_stat(linked)
        ):
            raise OrchestrateError(
                "Machine bootstrap could not prove exclusive ownership of its staging file",
                code="machine_bootstrap_temporary_identity_unproven",
                data={
                    "component": "stagingTemporary.fileIdentity",
                    "expected": "regular-file;links:1;opened-equals-linked",
                    "observed": (
                        f"opened:{_PathIdentity.from_stat(opened)!r};"
                        f"linked:{_PathIdentity.from_stat(linked)!r};"
                        f"reparse:{_is_reparse(opened)}"
                    ),
                    "path": os.fspath(path.parent / temporary_name),
                    "operations": [staging_operation],
                },
            )
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short staging write")
            view = view[written:]
        if not provider.windows_semantics:
            os.fchmod(descriptor, 0o755 if executable else 0o600)
        else:
            os.chmod(path.parent / temporary_name, 0o755 if executable else 0o600)
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        staged_raw = bytearray()
        while len(staged_raw) <= len(raw):
            chunk = os.read(descriptor, min(64 * 1024, len(raw) + 1 - len(staged_raw)))
            if not chunk:
                break
            staged_raw.extend(chunk)
        if bytes(staged_raw) != raw:
            raise OrchestrateError(
                "Machine bootstrap staging changed the exact bytes before commit",
                code="machine_bootstrap_commit_identity_changed",
                data={
                    "component": "commit.stagedSha256",
                    "expected": f"sha256:{hashlib.sha256(raw).hexdigest()}",
                    "observed": f"sha256:{hashlib.sha256(staged_raw).hexdigest()}",
                    "expectedSize": len(raw),
                    "observedSize": len(staged_raw),
                    "operations": [staging_operation],
                },
            )
        if archive_stages is not None:
            archive_stages.append(
                _archive_stage_diagnostic(
                    "staged-temp-after-fsync",
                    bytes(staged_raw),
                    form=(
                        "Windows-path-staging"
                        if provider.windows_semantics
                        else "directory-fd-staging"
                    ),
                    path=path.parent / temporary_name,
                )
            )
        observed_target = _target_identity_at(parent_fd, path.parent, path.name)
        if observed_target != expected_target:
            raise OrchestrateError(
                f"Machine bootstrap target changed before atomic replacement: {path.name}",
                code="machine_bootstrap_target_identity_changed",
                data={
                    "component": "target.fileIdentity",
                    "expected": repr(expected_target),
                    "observed": repr(observed_target),
                    "path": os.fspath(path),
                    "operations": [staging_operation],
                },
            )
        _before_staged_commit(path.parent / temporary_name)
        _commit_staged_no_replace(
            temporary_name,
            path.name,
            parent_fd=parent_fd,
            parent=path.parent,
            staged_descriptor=descriptor,
            native_windows=provider.native_windows,
        )
        committed_file = provider.read_bounded_file(path, max(len(raw), 1))
        committed = committed_file.raw
        staged_identity = _PathIdentity.from_stat(os.fstat(descriptor))
        if (
            (
                staged_identity.device,
                staged_identity.inode,
                stat.S_IFMT(staged_identity.mode),
            )
            != (
                committed_file.identity.device,
                committed_file.identity.inode,
                stat.S_IFMT(committed_file.identity.mode),
            )
            or committed != raw
        ):
            if archive_stages is not None:
                archive_stages.append(
                    _archive_stage_diagnostic(
                        "committed-target",
                        committed,
                        form="public-path",
                        path=path,
                    )
                )
            raise OrchestrateError(
                "Machine bootstrap committed bytes differ from the retained staging file",
                code="machine_bootstrap_commit_identity_changed",
                data={
                    "component": "commit.committedSha256",
                    "expected": f"sha256:{hashlib.sha256(raw).hexdigest()}",
                    "observed": f"sha256:{hashlib.sha256(committed).hexdigest()}",
                    "expectedSize": len(raw),
                    "observedSize": len(committed),
                    "operations": [staging_operation],
                },
            )
        if archive_stages is not None:
            archive_stages.append(
                _archive_stage_diagnostic(
                    "committed-target",
                    committed,
                    form="public-path",
                    path=path,
                )
            )
        os.close(descriptor)
        descriptor = None
        os.unlink(
            temporary_name if parent_fd is not None else os.fspath(path.parent / temporary_name),
            **({} if parent_fd is None else {"dir_fd": parent_fd}),
        )
        temporary_name = None
    except BaseException as exc:
        _diagnostic_exception_note(
            exc,
            {
                "operations": [staging_operation],
                "archiveStages": [] if archive_stages is None else archive_stages,
            },
        )
        raise
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary_name is not None:
            try:
                os.unlink(
                    temporary_name if parent_fd is not None else os.fspath(path.parent / temporary_name),
                    **({} if parent_fd is None else {"dir_fd": parent_fd}),
                )
            except OSError:
                pass
        if parent_fd is not None:
            try:
                os.close(parent_fd)
            except OSError:
                pass
        if parent_handle is not None:
            provider.close_path_pin(parent_handle)


def _write_install_receipt(
    layout: MachineLayout,
    binding: _VenvBinding,
    source: _SourceBinding,
    archive: _ArchiveBinding,
    *,
    expected_target: _PathIdentity | None,
    expected_anchor: _PathIdentity | None,
) -> None:
    receipt = _receipt_value(binding, source, archive, installation_id=secrets.token_hex(32))
    anchor = _anchor_value(receipt)
    raw = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    anchor_raw = (json.dumps(anchor, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    try:
        layout.install_anchor.parent.mkdir(parents=True, exist_ok=True)
        with _AnchorAncestryBinding(layout.install_anchor) as anchor_ancestry:
            _atomic_write_owned(
                layout.install_anchor,
                anchor_raw,
                executable=False,
                expected_target=expected_anchor,
            )
            anchor_ancestry.verify()
            _atomic_write_owned(
                layout.install_receipt,
                raw,
                executable=False,
                expected_target=expected_target,
            )
            anchor_ancestry.verify()
    except OrchestrateError as exc:
        if exc.code in {
            "machine_bootstrap_commit_identity_changed",
            "machine_bootstrap_commit_identity_unproven",
        }:
            raise
        raise OrchestrateError(
            "Machine bootstrap installed the checkout but could not record its local receipt; rerun setup to converge",
            code="machine_bootstrap_receipt_write_failed",
            data={
                "component": "receipt-and-anchor.write",
                "expected": "both-canonical-records",
                "observed": exc.code,
            },
        ) from exc
    except OSError as exc:
        raise OrchestrateError(
            "Machine bootstrap installed the checkout but could not record its local receipt; rerun setup to converge",
            code="machine_bootstrap_receipt_write_failed",
            data={
                "component": "receipt-and-anchor.write",
                "expected": "both-canonical-records",
                "observed": type(exc).__name__,
            },
        ) from exc


def machine_ready(
    layout: MachineLayout,
    path_store: UserPathStore,
    *,
    resolver: Resolver = _default_resolver,
) -> bool:
    """One bounded registry read, command lookup, and fixed-path file checks."""

    user_path = path_store.read().value
    entries = user_path.split(";") if user_path else []
    wanted = _normalize_path_entry(os.fspath(layout.bin_root))
    matches = [index for index, item in enumerate(entries) if _normalize_path_entry(item) == wanted]
    if matches != [0]:
        return False
    resolved = resolver("orchestrate", user_path)
    if not resolved or not _same_path(resolved, layout.windows_shim):
        return False
    try:
        _require_source_checkout(layout)
        with (
            _SourceBinding(layout) as source,
            _ArchiveBinding(layout.source_archive) as archive,
            _VenvBinding(layout) as binding,
        ):
            return bool(
                _owned_file_exists(layout.command_path)
                and _install_receipt_matches(layout, binding, source, archive)
                and _shim_matches(layout.windows_shim, _windows_shim_text())
                and _shim_matches(layout.wsl_shim, _wsl_shim_text())
            )
    except OrchestrateError:
        return False


def _machine_readiness_diagnostic(
    layout: MachineLayout,
    path_store: UserPathStore,
    *,
    resolver: Resolver,
) -> dict[str, object]:
    user_path = path_store.read().value
    entries = user_path.split(";") if user_path else []
    wanted = _normalize_path_entry(os.fspath(layout.bin_root))
    matches = [index for index, item in enumerate(entries) if _normalize_path_entry(item) == wanted]
    if matches != [0]:
        return {"component": "userPath.indices", "expected": [0], "observed": matches}
    resolved = resolver("orchestrate", user_path)
    if not resolved or not _same_path(resolved, layout.windows_shim):
        return {
            "component": "command.resolution",
            "expected": os.fspath(layout.windows_shim),
            "observed": resolved,
            "pathComparison": {
                "expectedNormalized": _normalize_path_entry(os.fspath(layout.windows_shim)),
                "observedNormalized": (
                    _normalize_path_entry(os.fspath(resolved)) if resolved else "absent"
                ),
                "matched": False,
            },
        }
    if not _shim_matches(layout.windows_shim, _windows_shim_text()):
        return _shim_identity_data(layout.windows_shim, _windows_shim_text())
    if not _shim_matches(layout.wsl_shim, _wsl_shim_text()):
        return _shim_identity_data(layout.wsl_shim, _wsl_shim_text())
    try:
        with (
            _SourceBinding(layout) as source,
            _ArchiveBinding(layout.source_archive) as archive,
            _VenvBinding(layout) as binding,
        ):
            matches_receipt, diagnostic = _receipt_match_diagnostic(
                layout, binding, source, archive
            )
            if not matches_receipt:
                return diagnostic
            if not _owned_file_exists(layout.command_path):
                return {"component": "command.form", "expected": "owned-regular-file", "observed": "absent"}
    except OrchestrateError as exc:
        return exc.data or {
            "component": "readiness.identity",
            "expected": "available-and-matching",
            "observed": exc.code,
        }
    return {"component": "readiness", "expected": "complete", "observed": "incomplete"}


def _run_step(
    runner: Runner,
    argv: Sequence[str],
    *,
    phase: str,
    runner_kwargs: Mapping[str, object] | None = None,
    operation_diagnostics: Sequence[Mapping[str, object]] = (),
    extra_diagnostics: Mapping[str, object] | None = None,
) -> None:
    diagnostic = {
        **({} if extra_diagnostics is None else extra_diagnostics),
        "operations": list(operation_diagnostics),
        "phase": phase,
    }
    try:
        completed = runner(
            tuple(argv),
            check=False,
            capture_output=True,
            text=True,
            **({} if runner_kwargs is None else runner_kwargs),
        )
    except OSError as exc:
        failure = OrchestrateError(
            f"Machine bootstrap could not start its {phase} step; correct the reported OS error and rerun setup",
            code="machine_bootstrap_process_failed",
            data={
                "phase": phase,
                "errno": exc.errno,
                "winerror": getattr(exc, "winerror", None),
            },
        )
        _diagnostic_exception_note(
            failure,
            {
                **diagnostic,
                "component": "process.launch",
                "expected": "success",
                "observed": "os-error",
            },
        )
        raise failure from exc
    except Exception as exc:
        _diagnostic_exception_note(
            exc,
            diagnostic,
        )
        raise
    if type(completed.returncode) is not int or completed.returncode != 0:
        failure = OrchestrateError(
            f"Machine bootstrap {phase} failed; the dedicated installation is incomplete and setup may be rerun safely",
            code="machine_bootstrap_command_failed",
            data={"phase": phase, "returnCode": completed.returncode},
        )
        _diagnostic_exception_note(
            failure,
            diagnostic,
        )
        raise failure


def _write_shim(
    path: Path,
    text: str,
    *,
    executable: bool,
    expected_target: _PathIdentity | None,
) -> bool:
    if _shim_matches(path, text):
        return False
    if expected_target is not None:
        raise OrchestrateError(
            f"Machine bootstrap refuses to overwrite the unrecognized launcher {path.name}; move it aside after inspection and rerun setup",
            code="machine_bootstrap_shim_identity_unproven",
            data=_shim_identity_data(path, text),
        )
    try:
        _atomic_write_owned(
            path,
            text.encode("utf-8"),
            executable=executable,
            expected_target=expected_target,
        )
    except OrchestrateError as exc:
        if exc.code in {
            "machine_bootstrap_commit_identity_changed",
            "machine_bootstrap_commit_identity_unproven",
        }:
            raise
        raise OrchestrateError(
            f"Machine bootstrap could not install {path.name}; correct filesystem access and rerun setup",
            code="machine_bootstrap_shim_write_failed",
            data=exc.data,
        ) from exc
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap could not install {path.name}; correct filesystem access and rerun setup",
            code="machine_bootstrap_shim_write_failed",
        ) from exc
    return True


def _ensure_directory(path: Path, *, code: str) -> None:
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap could not create its dedicated directory: {path.name}",
            code=code,
        ) from exc
    _path_identity(path, directory=True)


def _ensure_install_root(layout: MachineLayout) -> None:
    try:
        layout.install_root.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise OrchestrateError(
            "The dedicated orchestrate installation parent is unavailable",
            code="machine_bootstrap_home_unavailable",
        ) from exc
    _ensure_directory(layout.install_root, code="machine_bootstrap_home_unavailable")


def _validate_existing_install_tree(layout: MachineLayout) -> None:
    _path_identity(layout.install_root, directory=True)
    for candidate in (layout.venv_root, layout.scripts_root, layout.bin_root):
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        _path_identity(candidate, directory=True)
    for candidate in (
        layout.venv_root / "pyvenv.cfg",
        layout.scripts_root / "python.exe",
        layout.command_path,
        layout.install_receipt,
        layout.source_archive,
        layout.windows_shim,
        layout.wsl_shim,
    ):
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        _target_identity(candidate)
    try:
        layout.install_anchor.parent.lstat()
    except FileNotFoundError:
        pass
    else:
        with _AnchorAncestryBinding(layout.install_anchor):
            try:
                layout.install_anchor.lstat()
            except FileNotFoundError:
                pass
            else:
                _target_identity(layout.install_anchor)


def _run_bound_step(
    binding: _VenvBinding,
    runner: Runner,
    argv: Sequence[str],
    *,
    phase: str,
    source: _SourceBinding | None = None,
) -> None:
    binding.verify()
    if source is not None:
        if isinstance(source, _ArchiveBinding):
            source.verify(stage="pre-effect")
        else:
            source.verify()
    try:
        runner_kwargs = binding.runner_kwargs(
            pass_fds=() if source is None else source.pass_fds()
        )
        operations: list[Mapping[str, object]] = [
            _operation_diagnostic(
                "interpreter-effect",
                (
                    "native-path+explicit-application-name"
                    if binding._provider.native_windows
                    else "descriptor-executable+pass-fds"
                ),
                (
                    binding.python_path
                    if binding._provider.native_windows
                    else runner_kwargs["executable"]
                ),
                path_form=(
                    "native-path" if binding._provider.native_windows else "proc-self-fd"
                ),
                explicit_application_name=binding._provider.native_windows,
            )
        ]
        if source is not None:
            operations.extend(source.diagnostic_data().get("operations", []))  # type: ignore[arg-type]
        extra_diagnostics = {} if source is None else source.diagnostic_data()
        _run_step(
            runner,
            argv,
            phase=phase,
            runner_kwargs=runner_kwargs,
            operation_diagnostics=operations,
            extra_diagnostics=extra_diagnostics,
        )
    except OrchestrateError as exc:
        # Identity divergence is more actionable than the downstream process
        # symptom and must win error precedence.
        binding.raise_for_denied_identity_mutation(exc)
        binding.verify()
        if source is not None:
            if isinstance(source, _ArchiveBinding):
                source.verify(stage="post-failure")
            else:
                source.verify()
            _diagnostic_exception_note(
                exc,
                {
                    **source.diagnostic_data(),
                    "component": "postFailure.identity",
                    "expected": "matching",
                    "observed": "matching",
                },
            )
        else:
            _diagnostic_exception_note(
                exc,
                {
                    "component": "postFailure.identity",
                    "expected": "matching",
                    "observed": "matching",
                },
            )
        raise
    binding.verify()
    if source is not None:
        if isinstance(source, _ArchiveBinding):
            source.verify(stage="post-effect")
        else:
            source.verify()


def _require_source_checkout(layout: MachineLayout) -> None:
    if not (layout.source_root / "pyproject.toml").is_file() or not (
        layout.source_root / "src" / "orchestrate" / "__init__.py"
    ).is_file():
        raise OrchestrateError(
            "Machine bootstrap needs a reviewed orchestrate checkout; invoke its bootstrap.py entry point and rerun setup",
            code="machine_bootstrap_source_unavailable",
        )


def _owned_file_exists(path: Path) -> bool:
    return _target_identity(path) is not None


def _before_venv_parent_open(_: Path) -> None:
    """Fault seam before the absent-venv parent is physically bound."""


class _VenvCreationBinding(AbstractContextManager["_VenvCreationBinding"]):
    """Create the absent venv relative to a bound parent and retain its identity."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._provider = _active_platform_provider()
        self.operation = _operation_diagnostic(
            "venv-creation",
            (
                "retained-Windows-parent-and-child-handles+public-path"
                if self._provider.native_windows
                else "retained-parent-and-child-descriptors+proc-fd-path"
            ),
            path,
            path_form="native-path" if self._provider.native_windows else "proc-self-fd",
        )
        self._parent_handle: object | None = None
        self._parent_fd: int | None = None
        self._child_handle: object | None = None
        self._child_fd: int | None = None
        self._descendant_handles: list[object] = []
        self._descendant_fds: list[int] = []
        self._descendant_identities: list[tuple[Path, _PathIdentity]] = []
        parent_identity = _path_identity(path.parent, directory=True)
        try:
            _before_venv_parent_open(path.parent)
            if self._provider.native_windows:
                self._parent_handle = self._provider.open_path_pin(path.parent, directory=True)
                observed_parent = _path_identity(path.parent, directory=True)
                if not _same_identity(observed_parent, parent_identity, directory=True):
                    raise OrchestrateError(
                        "The virtual-environment parent changed while it was bound",
                        code="machine_bootstrap_parent_identity_changed",
                        data={
                            "component": "venvParent.fileIdentity",
                            "expected": repr(parent_identity),
                            "observed": repr(observed_parent),
                            "path": os.fspath(path.parent),
                            **self.diagnostic_data(),
                        },
                    )
                os.mkdir(path)
                self._child_handle = self._provider.open_path_pin(path, directory=True)
                self.effect_path = path
            else:
                self._parent_fd = int(self._provider.open_path_pin(path.parent, directory=True))
                opened_parent = _PathIdentity.from_stat(os.fstat(self._parent_fd))
                if not _same_identity(opened_parent, parent_identity, directory=True):
                    raise OrchestrateError(
                        "The virtual-environment parent changed while it was bound",
                        code="machine_bootstrap_parent_identity_changed",
                        data={
                            "component": "venvParent.fileIdentity",
                            "expected": repr(parent_identity),
                            "observed": repr(opened_parent),
                            "path": os.fspath(path.parent),
                            **self.diagnostic_data(),
                        },
                    )
                os.mkdir(path.name, dir_fd=self._parent_fd)
                flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_DIRECTORY", 0)
                self._child_fd = os.open(path.name, flags, dir_fd=self._parent_fd)
                self.effect_path = Path(f"/proc/self/fd/{self._child_fd}")
                self.operation = _operation_diagnostic(
                    "venv-creation",
                    "retained-parent-and-child-descriptors+proc-fd-path",
                    self.effect_path,
                    path_form="proc-self-fd",
                )
            self._identity = _path_identity(path, directory=True)
            for relative in (Path("Include"), Path("Lib"), Path("Lib") / "site-packages", Path("Scripts")):
                descendant = path / relative
                descendant.mkdir(exist_ok=True)
                identity = _path_identity(descendant, directory=True)
                pin = self._provider.open_path_pin(descendant, directory=True)
                if self._provider.native_windows:
                    self._descendant_handles.append(pin)
                else:
                    self._descendant_fds.append(int(pin))
                self._descendant_identities.append((descendant, identity))
            self.verify()
        except BaseException as exc:
            _diagnostic_exception_note(exc, self.diagnostic_data())
            self.close()
            raise

    def verify(self) -> None:
        try:
            observed = _path_identity(self.path, directory=True)
        except OrchestrateError as exc:
            raise OrchestrateError(
                "The dedicated environment changed during its bound creation",
                code="machine_bootstrap_venv_identity_changed",
                data={
                    "component": "venv.fileIdentity",
                    "expected": repr(self._identity),
                    "observed": "unavailable-or-redirected",
                    **self.diagnostic_data(),
                },
            ) from exc
        if not _same_identity(observed, self._identity, directory=True):
            raise OrchestrateError(
                "The dedicated environment changed during its bound creation",
                code="machine_bootstrap_venv_identity_changed",
                data={
                    "component": "venv.fileIdentity",
                    "expected": repr(self._identity),
                    "observed": repr(observed),
                    **self.diagnostic_data(),
                },
            )
        for descendant, identity in self._descendant_identities:
            try:
                observed_descendant = _path_identity(descendant, directory=True)
            except OrchestrateError as exc:
                raise OrchestrateError(
                    "A dedicated-environment descendant changed during bound creation",
                    code="machine_bootstrap_venv_identity_changed",
                    data={
                        "component": "venvDescendant.fileIdentity",
                        "expected": repr(identity),
                        "observed": "unavailable-or-redirected",
                        "path": os.fspath(descendant),
                        **self.diagnostic_data(),
                    },
                ) from exc
            if not _same_identity(observed_descendant, identity, directory=True):
                raise OrchestrateError(
                    "A dedicated-environment descendant changed during bound creation",
                    code="machine_bootstrap_venv_identity_changed",
                    data={
                        "component": "venvDescendant.fileIdentity",
                        "expected": repr(identity),
                        "observed": repr(observed_descendant),
                        "path": os.fspath(descendant),
                        **self.diagnostic_data(),
                    },
                )

    def diagnostic_data(self) -> dict[str, object]:
        return {"operations": [self.operation]}

    def raise_for_denied_descendant_redirect(self, failure: BaseException) -> None:
        """Classify a refused descendant replacement as an identity failure."""

        cause = failure.__cause__
        if not isinstance(cause, OSError):
            return
        attempted = [
            Path(value)
            for value in (getattr(cause, "filename", None), getattr(cause, "filename2", None))
            if isinstance(value, (str, bytes, os.PathLike))
        ]
        for candidate in attempted:
            for descendant, identity in self._descendant_identities:
                if _same_path(candidate, descendant):
                    raise OrchestrateError(
                        "Machine bootstrap refused a redirected dedicated-environment descendant",
                        code="machine_bootstrap_venv_identity_changed",
                        data={
                            "component": "venvDescendant.deniedRedirect",
                            "expected": repr(identity),
                            "observed": "redirect-denied",
                            "path": os.fspath(descendant),
                            "errno": cause.errno,
                            "winerror": getattr(cause, "winerror", None),
                            **self.diagnostic_data(),
                        },
                    ) from failure

    def pass_fds(self) -> tuple[int, ...]:
        return () if self._child_fd is None else (self._child_fd,)

    def close(self) -> None:
        for descriptor in reversed(self._descendant_fds):
            os.close(descriptor)
        self._descendant_fds.clear()
        for handle in reversed(self._descendant_handles):
            self._provider.close_path_pin(handle)
        self._descendant_handles.clear()
        for descriptor_name in ("_child_fd", "_parent_fd"):
            descriptor = getattr(self, descriptor_name)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, descriptor_name, None)
        for handle_name in ("_child_handle", "_parent_handle"):
            handle = getattr(self, handle_name)
            if handle is not None:
                self._provider.close_path_pin(handle)
                setattr(self, handle_name, None)

    def __exit__(self, *_: object) -> None:
        self.close()


def ensure_machine(
    *,
    layout: MachineLayout | None = None,
    path_store: UserPathStore | None = None,
    resolver: Resolver = _default_resolver,
    runner: Runner = subprocess.run,
    platform: str | None = None,
    version_info: tuple[int, int] | None = None,
) -> MachineBootstrapResult:
    """Repair the dedicated user installation, or return through the fast path."""

    provider = _active_platform_provider()
    selected_platform = (
        "win32" if provider.windows_semantics else sys.platform
    ) if platform is None else platform
    if selected_platform != "win32":
        return MachineBootstrapResult("not_applicable")
    selected_version = sys.version_info[:2] if version_info is None else version_info
    if selected_version < MINIMUM_PYTHON:
        raise OrchestrateError(
            "orchestrate requires Python 3.13 or newer; install it and rerun the checkout bootstrap entry point",
            code="machine_bootstrap_python_unsupported",
        )
    selected_layout = default_layout() if layout is None else layout
    selected_store = RegistryUserPathStore() if path_store is None else path_store
    if machine_ready(selected_layout, selected_store, resolver=resolver):
        return MachineBootstrapResult("ready")

    _ensure_install_root(selected_layout)
    lock = RunLock(
        selected_layout.install_root / MACHINE_LOCK_NAME,
        timeout_seconds=30.0,
        contention_code="machine_bootstrap_contention",
        contention_message="Timed out waiting for the machine-bootstrap mutation lock",
    )
    with lock:
        _validate_existing_install_tree(selected_layout)
        if machine_ready(selected_layout, selected_store, resolver=resolver):
            return MachineBootstrapResult("ready")

        actions: list[str] = []
        venv_python = selected_layout.scripts_root / "python.exe"
        if not _owned_file_exists(venv_python):
            try:
                selected_layout.venv_root.lstat()
            except FileNotFoundError:
                pass
            else:
                raise OrchestrateError(
                    "The dedicated orchestrate environment is incomplete; move it aside and rerun setup",
                    code="machine_bootstrap_venv_incomplete",
                    data={"path": os.fspath(selected_layout.venv_root)},
                )
            with _VenvCreationBinding(selected_layout.venv_root) as creation:
                try:
                    _run_step(
                        runner,
                        (sys.executable, "-m", "venv", os.fspath(creation.effect_path)),
                        phase="virtual-environment creation",
                        runner_kwargs=(
                            {"pass_fds": creation.pass_fds()}
                            if creation.pass_fds()
                            else None
                        ),
                        operation_diagnostics=[creation.operation],
                    )
                except OrchestrateError as exc:
                    creation.raise_for_denied_descendant_redirect(exc)
                    creation.verify()
                    _diagnostic_exception_note(
                        exc,
                        {
                            **creation.diagnostic_data(),
                            "component": "postFailure.venvIdentity",
                            "expected": "matching",
                            "observed": "matching",
                        },
                    )
                    raise
                creation.verify()
            _validate_existing_install_tree(selected_layout)
            if not _owned_file_exists(venv_python):
                raise OrchestrateError(
                    "Python reported success without creating the dedicated environment",
                    code="machine_bootstrap_venv_incomplete",
                )
            actions.append("created_environment")

        _require_source_checkout(selected_layout)
        archive_stages_evidence: list[dict[str, object]] = []
        with _SourceBinding(selected_layout) as source, _VenvBinding(selected_layout) as binding:
            receipt_target = _target_identity(selected_layout.install_receipt)
            anchor_target = _target_identity(selected_layout.install_anchor)
            windows_target = _target_identity(selected_layout.windows_shim)
            wsl_target = _target_identity(selected_layout.wsl_shim)
            # Existing launchers are independent owned targets.  Report their
            # verdict before a receipt verdict so an unrecognized launcher is
            # never masked by unrelated receipt derivation.
            if windows_target is not None and not _shim_matches(
                selected_layout.windows_shim, _windows_shim_text()
            ):
                raise OrchestrateError(
                    f"Machine bootstrap refuses to overwrite the unrecognized launcher {selected_layout.windows_shim.name}; move it aside after inspection and rerun setup",
                    code="machine_bootstrap_shim_identity_unproven",
                    data=_shim_identity_data(
                        selected_layout.windows_shim, _windows_shim_text()
                    ),
                )
            if wsl_target is not None and not _shim_matches(
                selected_layout.wsl_shim, _wsl_shim_text()
            ):
                raise OrchestrateError(
                    f"Machine bootstrap refuses to overwrite the unrecognized launcher {selected_layout.wsl_shim.name}; move it aside after inspection and rerun setup",
                    code="machine_bootstrap_shim_identity_unproven",
                    data=_shim_identity_data(selected_layout.wsl_shim, _wsl_shim_text()),
                )
            archive_target = _target_identity(selected_layout.source_archive)
            archive: _ArchiveBinding | None = None
            if archive_target is not None:
                archive = _ArchiveBinding(selected_layout.source_archive)
            try:
                if archive is None:
                    receipt_matches = False
                    receipt_diagnostic = {
                        "component": "sourceArchive.form",
                        "expected": "owned-regular-file",
                        "observed": "absent",
                    }
                else:
                    receipt_matches, receipt_diagnostic = _receipt_match_diagnostic(
                        selected_layout, binding, source, archive
                    )
                if receipt_target != _target_identity(selected_layout.install_receipt):
                    raise OrchestrateError(
                        "The machine-install receipt changed during ownership verification",
                        code="machine_bootstrap_receipt_identity_changed",
                        data={"component": "receipt.fileIdentity", "expected": repr(receipt_target), "observed": repr(_target_identity(selected_layout.install_receipt))},
                    )
                command_target = _target_identity(selected_layout.command_path)
                if not receipt_matches:
                    if receipt_target is not None:
                        raise OrchestrateError(
                            "The existing machine-install receipt does not prove the current command; move it aside after inspection and rerun setup",
                            code="machine_bootstrap_receipt_identity_unproven",
                            data=receipt_diagnostic,
                        )
                    if command_target is not None:
                        raise OrchestrateError(
                            "The existing command has no matching machine-install receipt; move it aside after inspection and rerun setup",
                            code="machine_bootstrap_command_identity_unproven",
                            data={"component": "command.receipt", "expected": "matching-receipt", "observed": "absent"},
                        )
                    if anchor_target is not None:
                        raise OrchestrateError(
                            "A host-local installation anchor exists without its receipt; move it aside after inspection and rerun setup",
                            code="machine_bootstrap_receipt_identity_unproven",
                            data={"component": "anchor.receipt", "expected": "matching-receipt", "observed": "receipt-absent"},
                        )
                    if archive is not None:
                        raise OrchestrateError(
                            "An unreceipted installed-source archive already exists; move it aside after inspection and rerun setup",
                            code="machine_bootstrap_source_identity_unproven",
                            data={"component": "sourceArchive.receipt", "expected": "absent-before-install", "observed": "existing-unreceipted-archive"},
                        )
                    _run_bound_step(
                        binding,
                        runner,
                        (os.fspath(binding.python_path), "-m", "ensurepip", "--upgrade"),
                        phase="pip preparation",
                    )
                    _run_bound_step(
                        binding,
                        runner,
                        (os.fspath(binding.python_path), "-m", "pip", "install", "--upgrade", "pip"),
                        phase="pip upgrade",
                    )
                    archive_stages = _build_source_archive(selected_layout, source)
                    archive = _ArchiveBinding(
                        selected_layout.source_archive,
                        stage_diagnostics=archive_stages,
                    )
                    source.verify()
                    _run_bound_step(
                        binding,
                        runner,
                        (
                            os.fspath(binding.python_path),
                            "-m",
                            "pip",
                            "install",
                            os.fspath(archive.effect_path),
                        ),
                        phase="reviewed source install",
                        source=archive,  # type: ignore[arg-type]
                    )
                    source.verify()
                    if not _owned_file_exists(selected_layout.command_path):
                        raise OrchestrateError(
                            "Source install reported success without creating the orchestrate command",
                            code="machine_bootstrap_command_missing",
                            data={"component": "command.form", "expected": "owned-regular-file", "observed": "absent"},
                        )
                    binding.verify()
                    _write_install_receipt(
                        selected_layout,
                        binding,
                        source,
                        archive,
                        expected_target=receipt_target,
                        expected_anchor=anchor_target,
                    )
                    actions.append("installed_reviewed_source")
            finally:
                if archive is not None:
                    archive_stages_evidence = list(archive.stage_diagnostics)
                    archive.close()

            _ensure_directory(selected_layout.bin_root, code="machine_bootstrap_shim_write_failed")
            source.verify()
            if _write_shim(
                selected_layout.windows_shim,
                _windows_shim_text(),
                executable=False,
                expected_target=windows_target,
            ):
                actions.append("installed_windows_shim")
            if _write_shim(
                selected_layout.wsl_shim,
                _wsl_shim_text(),
                executable=True,
                expected_target=wsl_target,
            ):
                actions.append("installed_wsl_shim")
            if register_user_path(selected_store, selected_layout.bin_root):
                actions.append("registered_user_path")
            binding.verify()

        user_path = selected_store.read().value
        resolved = resolver("orchestrate", user_path)
        if not resolved or not _same_path(resolved, selected_layout.windows_shim):
            raise OrchestrateError(
                "Machine bootstrap completed its writes but the user PATH does not resolve the canonical orchestrate command; open a new terminal and rerun setup",
                code="machine_bootstrap_verification_failed",
                data={
                    "component": "command.resolution",
                    "expected": os.fspath(selected_layout.windows_shim),
                    "observed": resolved,
                    "pathComparison": {
                        "expectedNormalized": _normalize_path_entry(
                            os.fspath(selected_layout.windows_shim)
                        ),
                        "observedNormalized": (
                            _normalize_path_entry(os.fspath(resolved))
                            if resolved
                            else "absent"
                        ),
                        "matched": False,
                    },
                },
            )
        if not machine_ready(selected_layout, selected_store, resolver=resolver):
            readiness_diagnostic = _machine_readiness_diagnostic(
                selected_layout, selected_store, resolver=resolver
            )
            if archive_stages_evidence:
                readiness_diagnostic = {
                    **readiness_diagnostic,
                    "archiveStages": archive_stages_evidence,
                }
            raise OrchestrateError(
                "Machine bootstrap verification found an incomplete Windows or WSL launcher; rerun setup to repair it",
                code="machine_bootstrap_verification_failed",
                data=readiness_diagnostic,
            )
        return MachineBootstrapResult("repaired", tuple(actions))
