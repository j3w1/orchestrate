"""Transport-only WSL launcher for one canonical Windows installation.

The Linux side owns no orchestrate state and performs no project I/O. It sends
the distro, exact Linux cwd, and argv as one authenticated-by-local-process
opaque payload to an explicitly configured Windows argument array. The Windows
receiver decides how to bind that context to an Orca execution surface.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import base64
import json
import os
from pathlib import PurePosixPath
import subprocess
import sys
from typing import Any

from .errors import OrchestrateError


WSL_FORWARD_SCHEMA = "orchestrate-wsl-forward/v1"
WINDOWS_COMMAND_ENV = "ORCHESTRATE_WINDOWS_COMMAND_JSON"
MAX_FORWARD_PAYLOAD_BYTES = 128 * 1024


@dataclass(frozen=True, slots=True)
class WslInvocation:
    distro: str
    linux_cwd: str
    argv: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": WSL_FORWARD_SCHEMA,
            "distro": self.distro,
            "linuxCwd": self.linux_cwd,
            "argv": list(self.argv),
        }


def make_invocation(*, distro: str, linux_cwd: str, argv: Sequence[str]) -> WslInvocation:
    if (
        not isinstance(distro, str)
        or not distro.strip()
        or "\x00" in distro
        or "/" in distro
        or "\\" in distro
    ):
        raise OrchestrateError("WSL distro identity is missing or malformed", code="wsl_context_invalid")
    if (
        not isinstance(linux_cwd, str)
        or not linux_cwd.startswith("/")
        or "\x00" in linux_cwd
        or ".." in PurePosixPath(linux_cwd).parts
    ):
        raise OrchestrateError("WSL cwd must be one absolute Linux path", code="wsl_context_invalid")
    selected = tuple(argv)
    if not selected or any(not isinstance(item, str) or "\x00" in item for item in selected):
        raise OrchestrateError("WSL forwarding requires a non-empty argument array", code="wsl_context_invalid")
    return WslInvocation(distro.strip(), linux_cwd, selected)


def windows_unc_path(invocation: WslInvocation, linux_path: str | None = None) -> str:
    """Translate only the explicitly transported WSL identity, without a shell."""

    selected = invocation.linux_cwd if linux_path is None else linux_path
    validated = make_invocation(distro=invocation.distro, linux_cwd=selected, argv=invocation.argv)
    suffix = "\\".join(PurePosixPath(validated.linux_cwd).parts[1:])
    return f"\\\\wsl.localhost\\{validated.distro}" + (f"\\{suffix}" if suffix else "")


def encode_invocation(invocation: WslInvocation) -> str:
    raw = json.dumps(
        invocation.as_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if len(raw) > MAX_FORWARD_PAYLOAD_BYTES:
        raise OrchestrateError("WSL forwarding payload exceeds the bounded size", code="wsl_payload_too_large")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_invocation(token: str) -> WslInvocation:
    if not isinstance(token, str) or not token or len(token) > MAX_FORWARD_PAYLOAD_BYTES * 2:
        raise OrchestrateError("WSL forwarding payload is missing or oversized", code="wsl_payload_invalid")
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        value = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError("WSL forwarding payload is not canonical encoded JSON", code="wsl_payload_invalid") from exc
    if not isinstance(value, Mapping) or set(value) != {"schema", "distro", "linuxCwd", "argv"}:
        raise OrchestrateError("WSL forwarding payload has an unknown shape", code="wsl_payload_invalid")
    if value.get("schema") != WSL_FORWARD_SCHEMA or not isinstance(value.get("argv"), list):
        raise OrchestrateError("WSL forwarding payload has an unsupported schema", code="wsl_payload_invalid")
    invocation = make_invocation(
        distro=value.get("distro"),  # type: ignore[arg-type]
        linux_cwd=value.get("linuxCwd"),  # type: ignore[arg-type]
        argv=value["argv"],
    )
    if encode_invocation(invocation) != token:
        raise OrchestrateError("WSL forwarding payload is not canonical", code="wsl_payload_invalid")
    return invocation


def windows_command(environment: Mapping[str, str]) -> tuple[str, ...]:
    raw = environment.get(WINDOWS_COMMAND_ENV, "")
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise OrchestrateError(
            f"{WINDOWS_COMMAND_ENV} must be a JSON argument array for the canonical Windows installation",
            code="wsl_windows_command_missing",
        ) from exc
    if (
        not isinstance(decoded, list)
        or not decoded
        or not all(isinstance(item, str) and item and "\x00" not in item for item in decoded)
    ):
        raise OrchestrateError(
            f"{WINDOWS_COMMAND_ENV} must be a non-empty JSON argument array",
            code="wsl_windows_command_missing",
        )
    return tuple(decoded)


def launch_from_wsl(
    argv: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    linux_cwd: str | None = None,
    platform: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> int:
    """Forward once with inherited stdio and return the Windows exit code."""

    env = os.environ if environment is None else environment
    selected_platform = sys.platform if platform is None else platform
    if not selected_platform.startswith("linux") or not env.get("WSL_DISTRO_NAME", "").strip():
        raise OrchestrateError("The thin launcher may run only inside WSL", code="wsl_context_invalid")
    invocation = make_invocation(
        distro=env["WSL_DISTRO_NAME"],
        linux_cwd=os.getcwd() if linux_cwd is None else linux_cwd,
        argv=argv,
    )
    command = (*windows_command(env), "_wsl-forward", "--payload", encode_invocation(invocation))
    try:
        completed = runner(command, check=False)
    except FileNotFoundError as exc:
        raise OrchestrateError(
            "The configured canonical Windows orchestrate command was not found",
            code="wsl_windows_command_missing",
        ) from exc
    returncode = completed.returncode
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise OrchestrateError("Windows forwarding returned no exact exit code", code="wsl_forward_exit_invalid")
    return returncode


def receive_on_windows(
    token: str,
    handler: Callable[[WslInvocation], int],
    *,
    platform: str | None = None,
) -> int:
    """Decode on Windows and delegate without creating a second state owner."""

    selected_platform = sys.platform if platform is None else platform
    if selected_platform != "win32":
        raise OrchestrateError("WSL forwarding must terminate in the Windows installation", code="wsl_receiver_invalid")
    invocation = decode_invocation(token)
    returncode = handler(invocation)
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise OrchestrateError("Windows controller returned no exact exit code", code="wsl_forward_exit_invalid")
    return returncode


def launcher_main(argv: Sequence[str] | None = None) -> int:
    selected = tuple(sys.argv[1:] if argv is None else argv)
    if selected in {("-h",), ("--help",)}:
        print(
            "usage: orchestrate-wsl <orchestrate arguments...>\n\n"
            "Forward one argument array from WSL to the canonical Windows installation.\n"
            f"Set {WINDOWS_COMMAND_ENV} to that installation's JSON command array."
        )
        return 0
    try:
        return launch_from_wsl(selected)
    except OrchestrateError as exc:
        print(f"orchestrate-wsl: {exc}", file=sys.stderr)
        return 1
