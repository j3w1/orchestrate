"""First-machine installation for the canonical Windows orchestrate command.

The production adapter owns only one user-scope registry value and one
dedicated installation directory.  The small protocols and layout argument
keep tests entirely synthetic: no test needs to inspect or change a real user
PATH, registry, or Python installation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
import ntpath
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Protocol

from .errors import OrchestrateError


MINIMUM_PYTHON = (3, 13)
WINDOWS_COMMAND = "orchestrate.cmd"
WSL_COMMAND = "orchestrate"
INSTALL_RECEIPT_SCHEMA = "orchestrate-machine-install/v1"


class UserPathStore(Protocol):
    """The Windows user's PATH value, never the process or machine PATH."""

    def read(self) -> str: ...

    def write(self, value: str) -> None: ...


class RegistryUserPathStore:
    r"""Read and write only ``HKCU\Environment\Path``."""

    def read(self) -> str:
        if sys.platform != "win32":
            raise OrchestrateError(
                "Windows user PATH is available only on native Windows",
                code="machine_bootstrap_platform_unsupported",
            )
        import winreg

        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
                value, _kind = winreg.QueryValueEx(key, "Path")
        except FileNotFoundError:
            return ""
        except OSError as exc:
            raise OrchestrateError(
                "The Windows user PATH could not be read; no installation changes were made",
                code="machine_bootstrap_path_unavailable",
            ) from exc
        if not isinstance(value, str):
            raise OrchestrateError(
                "The Windows user PATH has an unsupported registry type",
                code="machine_bootstrap_path_unsupported",
            )
        return value

    def write(self, value: str) -> None:
        if sys.platform != "win32":
            raise OrchestrateError(
                "Windows user PATH is available only on native Windows",
                code="machine_bootstrap_platform_unsupported",
            )
        import winreg

        try:
            with winreg.CreateKeyEx(
                winreg.HKEY_CURRENT_USER,
                "Environment",
                0,
                winreg.KEY_SET_VALUE,
            ) as key:
                winreg.SetValueEx(key, "Path", 0, winreg.REG_EXPAND_SZ, value)
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
    parts = current.split(";") if current else []
    wanted = _normalize_path_entry(os.fspath(entry))
    matches = [index for index, item in enumerate(parts) if _normalize_path_entry(item) == wanted]
    if len(matches) == 1 and matches[0] == 0:
        return False
    retained = [item for index, item in enumerate(parts) if index not in matches]
    retained.insert(0, os.fspath(entry))
    store.write(";".join(retained))
    return True


def _default_resolver(command: str, path: str) -> str | None:
    return shutil.which(command, path=os.path.expandvars(path))


def _same_path(left: str | Path, right: str | Path) -> bool:
    return ntpath.normcase(ntpath.abspath(os.fspath(left))) == ntpath.normcase(
        ntpath.abspath(os.fspath(right))
    )


def _shim_matches(path: Path, expected: str) -> bool:
    try:
        if not path.is_file():
            return False
        with path.open("r", encoding="utf-8", newline="") as stream:
            return stream.read() == expected
    except OSError:
        return False


def _install_receipt_matches(layout: MachineLayout) -> bool:
    try:
        value = json.loads(layout.install_receipt.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return value == {
        "schema": INSTALL_RECEIPT_SCHEMA,
        "sourceRoot": os.fspath(layout.source_root.resolve()),
    }


def _write_install_receipt(layout: MachineLayout) -> None:
    value = {
        "schema": INSTALL_RECEIPT_SCHEMA,
        "sourceRoot": os.fspath(layout.source_root.resolve()),
    }
    try:
        temporary = layout.install_receipt.with_name(layout.install_receipt.name + ".tmp")
        temporary.write_text(
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        temporary.replace(layout.install_receipt)
    except OSError as exc:
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

    user_path = path_store.read()
    entries = user_path.split(";") if user_path else []
    wanted = _normalize_path_entry(os.fspath(layout.bin_root))
    if sum(_normalize_path_entry(item) == wanted for item in entries) != 1:
        return False
    resolved = resolver("orchestrate", user_path)
    return bool(
        resolved
        and _same_path(resolved, layout.windows_shim)
        and layout.command_path.is_file()
        and _install_receipt_matches(layout)
        and _shim_matches(layout.windows_shim, _windows_shim_text())
        and _shim_matches(layout.wsl_shim, _wsl_shim_text())
    )


def _run_step(runner: Runner, argv: Sequence[str], *, phase: str) -> None:
    try:
        completed = runner(tuple(argv), check=False, capture_output=True, text=True)
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


def _write_shim(path: Path, text: str, *, executable: bool) -> bool:
    if _shim_matches(path, text):
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(text, encoding="utf-8", newline="")
        if executable:
            temporary.chmod(0o755)
        temporary.replace(path)
    except OSError as exc:
        raise OrchestrateError(
            f"Machine bootstrap could not install {path.name}; correct filesystem access and rerun setup",
            code="machine_bootstrap_shim_write_failed",
        ) from exc
    return True


def _require_source_checkout(layout: MachineLayout) -> None:
    if not (layout.source_root / "pyproject.toml").is_file() or not (
        layout.source_root / "src" / "orchestrate" / "__init__.py"
    ).is_file():
        raise OrchestrateError(
            "Machine bootstrap needs a reviewed orchestrate checkout; invoke its bootstrap.py entry point and rerun setup",
            code="machine_bootstrap_source_unavailable",
        )


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

    actions: list[str] = []
    try:
        selected_layout.install_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise OrchestrateError(
            "The dedicated orchestrate installation directory could not be created; correct filesystem access and rerun setup",
            code="machine_bootstrap_home_unavailable",
        ) from exc
    venv_python = selected_layout.scripts_root / "python.exe"
    if not venv_python.is_file():
        if selected_layout.venv_root.exists():
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
        if not venv_python.is_file():
            raise OrchestrateError(
                "Python reported success without creating the dedicated environment",
                code="machine_bootstrap_venv_incomplete",
            )
        actions.append("created_environment")

    if not selected_layout.command_path.is_file() or not _install_receipt_matches(selected_layout):
        _require_source_checkout(selected_layout)
        _run_step(
            runner,
            (os.fspath(venv_python), "-m", "ensurepip", "--upgrade"),
            phase="pip preparation",
        )
        _run_step(
            runner,
            (os.fspath(venv_python), "-m", "pip", "install", "--upgrade", "pip"),
            phase="pip upgrade",
        )
        _run_step(
            runner,
            (
                os.fspath(venv_python),
                "-m",
                "pip",
                "install",
                "--editable",
                os.fspath(selected_layout.source_root),
            ),
            phase="editable install",
        )
        if not selected_layout.command_path.is_file():
            raise OrchestrateError(
                "Editable install reported success without creating the orchestrate command",
                code="machine_bootstrap_command_missing",
            )
        _write_install_receipt(selected_layout)
        actions.append("installed_editable_checkout")

    if _write_shim(selected_layout.windows_shim, _windows_shim_text(), executable=False):
        actions.append("installed_windows_shim")
    if _write_shim(selected_layout.wsl_shim, _wsl_shim_text(), executable=True):
        actions.append("installed_wsl_shim")
    if register_user_path(selected_store, selected_layout.bin_root):
        actions.append("registered_user_path")

    user_path = selected_store.read()
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
