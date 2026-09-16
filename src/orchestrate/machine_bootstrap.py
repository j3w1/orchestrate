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
import shutil
import stat
import subprocess
import sys
import tempfile
from typing import Protocol

from .errors import OrchestrateError
from .state import RunLock


MINIMUM_PYTHON = (3, 13)
WINDOWS_COMMAND = "orchestrate.cmd"
WSL_COMMAND = "orchestrate"
INSTALL_RECEIPT_SCHEMA = "orchestrate-machine-install/v3"
MAX_INSTALL_RECEIPT_BYTES = 4096
MAX_PYVENV_CONFIG_BYTES = 16 * 1024
MAX_SHIM_BYTES = 32 * 1024
MAX_COMMAND_BYTES = 2 * 1024 * 1024
MACHINE_LOCK_NAME = ".machine-bootstrap.lock"


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
        ) from exc
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if _is_reparse(info) or not expected:
        raise OrchestrateError(
            f"Machine bootstrap refuses a redirect or unexpected node in the dedicated installation: {path.name}",
            code="machine_bootstrap_install_identity_unproven",
            data={"path": os.fspath(path)},
        )
    return _PathIdentity.from_stat(info)


def _read_posix_bounded_file(path: Path, limit: int) -> _BoundedFile:
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    non_blocking = getattr(os, "O_NONBLOCK", 0)
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    if not no_follow or not non_blocking:
        raise OrchestrateError(
            "This host cannot provide no-follow machine-bootstrap reads",
            code="machine_bootstrap_safe_io_unavailable",
        )
    descriptor: int | None = None
    try:
        descriptor = os.open(
            os.fspath(path),
            os.O_RDONLY | no_follow | non_blocking | close_on_exec,
        )
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or _is_reparse(info):
            raise OrchestrateError(
                f"Machine bootstrap refuses a non-file state entry: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
            )
        if info.st_size > limit:
            raise OrchestrateError(
                f"Machine bootstrap state exceeds its bounded read limit: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
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
            )
        return _BoundedFile(raw, _PathIdentity.from_stat(info))
    except OrchestrateError:
        raise
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap state cannot be opened safely: {path.name}",
            code="machine_bootstrap_safe_io_unavailable",
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
        raise OrchestrateError(
            f"Machine bootstrap state cannot be opened safely: {path.name}",
            code="machine_bootstrap_safe_io_unavailable",
        )
    descriptor: int | None = None
    try:
        import msvcrt

        descriptor = msvcrt.open_osfhandle(
            handle,
            os.O_RDONLY | getattr(os, "O_BINARY", 0),
        )
        handle = None
        opened_info = os.stat(descriptor)
        linked_info = os.stat(path, follow_symlinks=False)
        if not _opened_file_identity_matches(opened_info, linked_info):
            raise OrchestrateError(
                f"Machine bootstrap state changed while its handle identity was proved: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
            )
        tag = FileAttributeTagInfo()
        raw_handle = msvcrt.get_osfhandle(descriptor)
        if not get_info(raw_handle, file_attribute_tag_info_class, ctypes.byref(tag), ctypes.sizeof(tag)):
            raise OrchestrateError(
                f"Machine bootstrap state identity is unavailable: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
            )
        if tag.FileAttributes & (file_attribute_reparse_point | file_attribute_directory):
            raise OrchestrateError(
                f"Machine bootstrap refuses a redirected or non-file state entry: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
            )
        size = ctypes.c_longlong()
        if not get_size(raw_handle, ctypes.byref(size)) or size.value < 0 or size.value > limit:
            raise OrchestrateError(
                f"Machine bootstrap state exceeds or lacks its bounded size: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
            )
        raw = bytearray()
        while len(raw) <= limit:
            amount = min(64 * 1024, limit + 1 - len(raw))
            chunk = ctypes.create_string_buffer(amount)
            received = wintypes.DWORD()
            if not read_file(raw_handle, chunk, amount, ctypes.byref(received), None):
                raise OrchestrateError(
                    f"Machine bootstrap state cannot be read: {path.name}",
                    code="machine_bootstrap_safe_io_unavailable",
                )
            if received.value == 0:
                break
            raw.extend(chunk.raw[: received.value])
        if len(raw) > limit or len(raw) != size.value:
            raise OrchestrateError(
                f"Machine bootstrap state changed during its bounded read: {path.name}",
                code="machine_bootstrap_safe_io_unavailable",
            )
        return _BoundedFile(bytes(raw), _PathIdentity.from_stat(opened_info))
    finally:
        if descriptor is not None:
            os.close(descriptor)
        elif handle not in (None, ctypes.c_void_p(-1).value):
            close_handle(handle)


def _read_bounded_regular_file(path: Path, limit: int) -> _BoundedFile:
    if sys.platform == "win32":
        return _read_windows_bounded_file(path, limit)
    return _read_posix_bounded_file(path, limit)


def _read_bounded_regular(path: Path, limit: int) -> bytes:
    return _read_bounded_regular_file(path, limit).raw


def _shim_matches(path: Path, expected: str) -> bool:
    try:
        return _read_bounded_regular(path, MAX_SHIM_BYTES).decode("utf-8", errors="strict") == expected
    except (OrchestrateError, UnicodeDecodeError):
        return False


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
        ) from exc
    if not home or len(version_pair) != 2 or version_pair < MINIMUM_PYTHON:
        raise OrchestrateError(
            "The dedicated environment does not prove a supported Python identity",
            code="machine_bootstrap_venv_identity_unproven",
        )
    return home, version


def _open_windows_path_pin(path: Path, *, directory: bool) -> object:
    """Hold a non-reparse node without write/delete sharing until bootstrap ends."""

    import ctypes
    from ctypes import wintypes

    file_read_attributes = 0x0080
    share_read = 0x00000001
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
    handle = create_file(
        os.fspath(path),
        file_read_attributes,
        share_read,
        None,
        open_existing,
        flags,
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        raise OrchestrateError(
            "The dedicated environment could not pin its Windows path identity",
            code="machine_bootstrap_venv_identity_unproven",
            data={"path": path.name},
        )
    tag = FileAttributeTagInfo()
    if not get_info(handle, file_attribute_tag_info_class, ctypes.byref(tag), ctypes.sizeof(tag)):
        _close_windows_path_pin(handle)
        raise OrchestrateError(
            "The dedicated environment could not inspect its pinned Windows path identity",
            code="machine_bootstrap_venv_identity_unproven",
            data={"path": path.name},
        )
    actual_directory = bool(tag.FileAttributes & file_attribute_directory)
    if tag.FileAttributes & file_attribute_reparse_point or actual_directory != directory:
        _close_windows_path_pin(handle)
        raise OrchestrateError(
            "The dedicated environment contains a redirected or unexpected Windows node",
            code="machine_bootstrap_venv_identity_unproven",
            data={"path": path.name},
        )
    return handle


def _close_windows_path_pin(handle: object) -> None:
    import ctypes
    from ctypes import wintypes

    close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    close_handle(handle)


class _VenvBinding(AbstractContextManager["_VenvBinding"]):
    """Pin and repeatedly prove the dedicated interpreter before every effect."""

    def __init__(self, layout: MachineLayout) -> None:
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
                if sys.platform == "win32":
                    self._windows_handles.append(
                        _open_windows_path_pin(path, directory=directory)
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
            if not _same_identity(_path_identity(path, directory=directory), identity, directory=directory):
                raise OrchestrateError(
                    "The dedicated environment changed after its physical identity was pinned",
                    code="machine_bootstrap_venv_identity_changed",
                )
        if hashlib.sha256(_read_bounded_regular(self.config_path, MAX_PYVENV_CONFIG_BYTES)).hexdigest() != self.config_digest:
            raise OrchestrateError(
                "The dedicated environment configuration changed after identity verification",
                code="machine_bootstrap_venv_identity_changed",
            )
        # Path spellings are not identity evidence on Windows (short/long
        # aliases and case can differ).  The comparisons above use file IDs,
        # while retained Windows handles also deny write/delete replacement.

    def receipt_value(self) -> dict[str, object]:
        command = _command_proof(self.layout.command_path)
        return {
            "schema": INSTALL_RECEIPT_SCHEMA,
            "sourceRoot": os.fspath(self.layout.source_root.resolve()),
            "venvRoot": self.root_resolved,
            "pythonPath": self.python_resolved,
            "pyvenvConfigSha256": f"sha256:{self.config_digest}",
            "commandDevice": command.identity.device,
            "commandInode": command.identity.inode,
            "commandSize": command.identity.size,
            "commandSha256": f"sha256:{hashlib.sha256(command.raw).hexdigest()}",
        }

    def runner_kwargs(self) -> dict[str, object]:
        if sys.platform == "win32":
            # The retained no-write/no-delete handles on the complete parent
            # chain make this pathname resolve to the pinned interpreter.
            return {}
        descriptor = self._descriptors[self.python_path]
        executable = f"/proc/self/fd/{descriptor}"
        if not os.path.exists(executable):
            raise OrchestrateError(
                "This host cannot execute the dedicated interpreter through its pinned descriptor",
                code="machine_bootstrap_venv_identity_unproven",
            )
        return {"executable": executable, "pass_fds": (descriptor,)}

    def close(self) -> None:
        for descriptor in reversed(tuple(self._descriptors.values())):
            try:
                os.close(descriptor)
            except OSError:
                pass
        self._descriptors.clear()
        for handle in reversed(self._windows_handles):
            _close_windows_path_pin(handle)
        self._windows_handles.clear()

    def __exit__(self, *_: object) -> None:
        self.close()


def _install_receipt_matches(layout: MachineLayout, binding: _VenvBinding) -> bool:
    try:
        value = _read_install_receipt(layout)
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "sourceRoot",
            "venvRoot",
            "pythonPath",
            "pyvenvConfigSha256",
            "commandDevice",
            "commandInode",
            "commandSize",
            "commandSha256",
        }:
            return False
        if any(
            not isinstance(value.get(key), str)
            for key in (
                "schema",
                "sourceRoot",
                "venvRoot",
                "pythonPath",
                "pyvenvConfigSha256",
                "commandSha256",
            )
        ) or any(
            type(value.get(key)) is not int or value[key] < 0
            for key in ("commandDevice", "commandInode", "commandSize")
        ):
            return False
        return value == binding.receipt_value()
    except OrchestrateError:
        return False


def _command_proof(path: Path) -> _BoundedFile:
    opened = _read_bounded_regular_file(path, MAX_COMMAND_BYTES)
    if opened.identity.links != 1:
        raise OrchestrateError(
            "The installed orchestrate command is not a single owned file",
            code="machine_bootstrap_command_identity_unproven",
        )
    return opened


def _read_install_receipt(layout: MachineLayout) -> object | None:
    try:
        raw_bytes = _read_bounded_regular(layout.install_receipt, MAX_INSTALL_RECEIPT_BYTES)
        raw = raw_bytes.decode("utf-8", errors="strict")
        value = json.loads(
            raw,
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
        )
    except (OrchestrateError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    canonical = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    return value if raw_bytes == canonical else None


def _target_identity(path: Path) -> _PathIdentity | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap cannot inspect the replacement target: {path.name}",
            code="machine_bootstrap_target_identity_unproven",
        ) from exc
    if _is_reparse(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise OrchestrateError(
            f"Machine bootstrap refuses to replace an unowned target entry: {path.name}",
            code="machine_bootstrap_target_identity_unproven",
        )
    return _PathIdentity.from_stat(info)


def _atomic_write_owned(
    path: Path,
    raw: bytes,
    *,
    executable: bool,
    expected_target: _PathIdentity | None,
) -> None:
    _path_identity(path.parent, directory=True)
    if expected_target is not None:
        raise OrchestrateError(
            f"Machine bootstrap refuses to overwrite the existing target: {path.name}",
            code="machine_bootstrap_target_identity_unproven",
        )
    if _target_identity(path) != expected_target:
        raise OrchestrateError(
            f"Machine bootstrap target changed before staging: {path.name}",
            code="machine_bootstrap_target_identity_changed",
        )
    descriptor: int | None = None
    temporary_name: str | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=os.fspath(path.parent),
        )
        temporary = Path(temporary_name)
        opened = os.fstat(descriptor)
        linked = temporary.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or _is_reparse(opened)
            or opened.st_nlink != 1
            or _PathIdentity.from_stat(opened) != _PathIdentity.from_stat(linked)
        ):
            raise OrchestrateError(
                "Machine bootstrap could not prove exclusive ownership of its staging file",
                code="machine_bootstrap_temporary_identity_unproven",
            )
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short staging write")
            view = view[written:]
        if os.name != "nt":
            os.fchmod(descriptor, 0o755 if executable else 0o600)
        else:
            os.chmod(temporary_name, 0o755 if executable else 0o600)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        if _target_identity(path) != expected_target:
            raise OrchestrateError(
                f"Machine bootstrap target changed before atomic replacement: {path.name}",
                code="machine_bootstrap_target_identity_changed",
            )
        os.link(temporary_name, os.fspath(path), follow_symlinks=False)
        os.unlink(temporary_name)
        temporary_name = None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass


def _write_install_receipt(
    layout: MachineLayout,
    binding: _VenvBinding,
    *,
    expected_target: _PathIdentity | None,
) -> None:
    raw = (
        json.dumps(binding.receipt_value(), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    try:
        _atomic_write_owned(
            layout.install_receipt,
            raw,
            executable=False,
            expected_target=expected_target,
        )
    except (OSError, OrchestrateError) as exc:
        raise OrchestrateError(
            "Machine bootstrap installed the checkout but could not record its local receipt; rerun setup to converge",
            code="machine_bootstrap_receipt_write_failed",
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
    if sum(_normalize_path_entry(item) == wanted for item in entries) != 1:
        return False
    resolved = resolver("orchestrate", user_path)
    if not resolved or not _same_path(resolved, layout.windows_shim):
        return False
    try:
        with _VenvBinding(layout) as binding:
            return bool(
                _owned_file_exists(layout.command_path)
                and _install_receipt_matches(layout, binding)
                and _shim_matches(layout.windows_shim, _windows_shim_text())
                and _shim_matches(layout.wsl_shim, _wsl_shim_text())
            )
    except OrchestrateError:
        return False


def _run_step(
    runner: Runner,
    argv: Sequence[str],
    *,
    phase: str,
    runner_kwargs: Mapping[str, object] | None = None,
) -> None:
    try:
        completed = runner(
            tuple(argv),
            check=False,
            capture_output=True,
            text=True,
            **({} if runner_kwargs is None else runner_kwargs),
        )
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap could not start its {phase} step; correct the reported OS error and rerun setup",
            code="machine_bootstrap_process_failed",
            data={"phase": phase},
        ) from exc
    if type(completed.returncode) is not int or completed.returncode != 0:
        raise OrchestrateError(
            f"Machine bootstrap {phase} failed; the dedicated installation is incomplete and setup may be rerun safely",
            code="machine_bootstrap_command_failed",
            data={"phase": phase, "returnCode": completed.returncode},
        )


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
        )
    try:
        _atomic_write_owned(
            path,
            text.encode("utf-8"),
            executable=executable,
            expected_target=expected_target,
        )
    except (OSError, OrchestrateError) as exc:
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
        layout.windows_shim,
        layout.wsl_shim,
    ):
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        _target_identity(candidate)


def _run_bound_step(
    binding: _VenvBinding,
    runner: Runner,
    argv: Sequence[str],
    *,
    phase: str,
) -> None:
    binding.verify()
    _run_step(
        runner,
        argv,
        phase=phase,
        runner_kwargs=binding.runner_kwargs(),
    )
    binding.verify()


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

    selected_platform = sys.platform if platform is None else platform
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
            _run_step(
                runner,
                (sys.executable, "-m", "venv", os.fspath(selected_layout.venv_root)),
                phase="virtual-environment creation",
            )
            _validate_existing_install_tree(selected_layout)
            if not _owned_file_exists(venv_python):
                raise OrchestrateError(
                    "Python reported success without creating the dedicated environment",
                    code="machine_bootstrap_venv_incomplete",
                )
            actions.append("created_environment")

        with _VenvBinding(selected_layout) as binding:
            receipt_target = _target_identity(selected_layout.install_receipt)
            receipt_matches = _install_receipt_matches(selected_layout, binding)
            if receipt_target != _target_identity(selected_layout.install_receipt):
                raise OrchestrateError(
                    "The machine-install receipt changed during ownership verification",
                    code="machine_bootstrap_receipt_identity_changed",
                )
            command_target = _target_identity(selected_layout.command_path)
            if not receipt_matches:
                if receipt_target is not None:
                    raise OrchestrateError(
                        "The existing machine-install receipt does not prove the current command; move it aside after inspection and rerun setup",
                        code="machine_bootstrap_receipt_identity_unproven",
                    )
                if command_target is not None:
                    raise OrchestrateError(
                        "The existing command has no matching machine-install receipt; move it aside after inspection and rerun setup",
                        code="machine_bootstrap_command_identity_unproven",
                    )
                _require_source_checkout(selected_layout)
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
                _run_bound_step(
                    binding,
                    runner,
                    (
                        os.fspath(binding.python_path),
                        "-m",
                        "pip",
                        "install",
                        "--editable",
                        os.fspath(selected_layout.source_root),
                    ),
                    phase="editable install",
                )
                if not _owned_file_exists(selected_layout.command_path):
                    raise OrchestrateError(
                        "Editable install reported success without creating the orchestrate command",
                        code="machine_bootstrap_command_missing",
                    )
                binding.verify()
                _write_install_receipt(
                    selected_layout,
                    binding,
                    expected_target=receipt_target,
                )
                actions.append("installed_editable_checkout")

            _ensure_directory(selected_layout.bin_root, code="machine_bootstrap_shim_write_failed")
            windows_target = _target_identity(selected_layout.windows_shim)
            if _write_shim(
                selected_layout.windows_shim,
                _windows_shim_text(),
                executable=False,
                expected_target=windows_target,
            ):
                actions.append("installed_windows_shim")
            wsl_target = _target_identity(selected_layout.wsl_shim)
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
            )
        if not machine_ready(selected_layout, selected_store, resolver=resolver):
            raise OrchestrateError(
                "Machine bootstrap verification found an incomplete Windows or WSL launcher; rerun setup to repair it",
                code="machine_bootstrap_verification_failed",
            )
        return MachineBootstrapResult("repaired", tuple(actions))
