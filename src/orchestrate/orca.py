"""Small, strict subprocess client for the public Orca CLI."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
import shutil
import subprocess
import sys
from typing import Any


JsonObject = dict[str, Any]
Runner = Callable[..., subprocess.CompletedProcess[Any]]
MAX_DIAGNOSTIC_CHARS = 8192
ORCA_TASK_TITLE_MAX_UTF16_UNITS = 80
_ORCA_TASK_TITLE_ELLIPSIS = "..."
_ECMASCRIPT_WHITESPACE_CODEPOINTS = frozenset(
    {
        *range(0x0009, 0x000E),
        0x0020,
        0x00A0,
        0x1680,
        *range(0x2000, 0x200B),
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
        0xFEFF,
    }
)


def _is_ecmascript_whitespace(character: str) -> bool:
    return ord(character) in _ECMASCRIPT_WHITESPACE_CODEPOINTS


def _collapse_ecmascript_whitespace(value: str) -> str:
    output: list[str] = []
    pending_space = False
    for character in value:
        if _is_ecmascript_whitespace(character):
            pending_space = bool(output)
            continue
        if pending_space:
            output.append(" ")
            pending_space = False
        output.append(character)
    return "".join(output)


def orca_task_title(value: str) -> str:
    """Return the exact bounded title shape stored by Orca 1.4.198."""

    normalized = _collapse_ecmascript_whitespace(value)
    if not normalized:
        return "orchestrate task"

    def utf16_units(character: str) -> int:
        return 2 if ord(character) > 0xFFFF else 1

    if sum(utf16_units(character) for character in normalized) <= ORCA_TASK_TITLE_MAX_UTF16_UNITS:
        return normalized

    budget = ORCA_TASK_TITLE_MAX_UTF16_UNITS - len(_ORCA_TASK_TITLE_ELLIPSIS)
    body_characters: list[str] = []
    used = 0
    for character in normalized:
        width = utf16_units(character)
        if used + width > budget:
            break
        body_characters.append(character)
        used += width
    if body_characters and 0xD800 <= ord(body_characters[-1]) <= 0xDBFF:
        body_characters.pop()
    while body_characters and _is_ecmascript_whitespace(body_characters[-1]):
        body_characters.pop()
    return "".join(body_characters) + _ORCA_TASK_TITLE_ELLIPSIS


def _diagnostic_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        decoded = value.decode("utf-8", errors="replace")
    else:
        decoded = value
    return decoded[:MAX_DIAGNOSTIC_CHARS]


def _strict_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return value


def _decode_object(value: str) -> JsonObject | None:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def resolve_orca_command(
    environment: Mapping[str, str] | None = None,
    platform: str | None = None,
) -> tuple[str, ...]:
    """Resolve the Orca executable once using the installed-guide precedence."""

    env = os.environ if environment is None else environment
    selected_platform = sys.platform if platform is None else platform

    forwarded = env.get("ORCA_CLI_COMMAND", "").strip()
    if forwarded:
        return (forwarded,)
    if env.get("ORCA_DEV_REPO_ROOT", "").strip():
        return ("orca-dev",)
    if selected_platform.startswith("linux"):
        return ("orca-ide",)
    return ("orca",)


@dataclass(frozen=True, slots=True)
class OrcaCommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    payload: JsonObject | None = None


class OrcaCommandError(RuntimeError):
    """A bounded Orca CLI launch, transport, contract, or command failure."""

    def __init__(self, message: str, result: OrcaCommandResult | None = None):
        super().__init__(message)
        self.result = result

    @property
    def code(self) -> str:
        payload = self.result.payload if self.result else None
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict) and isinstance(error.get("code"), str):
            return error["code"]
        return "orca_command_failed"


class OrcaClient:
    """Invoke one resolved Orca executable with argument arrays and split streams."""

    def __init__(
        self,
        command: Sequence[str] | None = None,
        *,
        environment: Mapping[str, str] | None = None,
        runner: Runner = subprocess.run,
        timeout_seconds: float = 30.0,
    ) -> None:
        resolved = tuple(command) if command is not None else resolve_orca_command(environment)
        if not resolved or any(not part for part in resolved):
            raise ValueError("The Orca command must contain at least one non-empty argument")
        self.command = resolved
        self.environment = dict(environment) if environment is not None else None
        self._runner = runner
        self.timeout_seconds = timeout_seconds

    def executable_available(self) -> bool:
        executable = self.command[0]
        env = self.environment if self.environment is not None else os.environ
        return shutil.which(executable, path=env.get("PATH")) is not None

    def run_text(self, *arguments: str, timeout_seconds: float | None = None) -> OrcaCommandResult:
        argv = (*self.command, *arguments)
        try:
            completed = self._runner(
                argv,
                capture_output=True,
                text=False,
                env=self.environment,
                timeout=self.timeout_seconds if timeout_seconds is None else timeout_seconds,
                check=False,
            )
        except FileNotFoundError as exc:
            raise OrcaCommandError(f"Orca executable was not found: {self.command[0]}") from exc
        except subprocess.TimeoutExpired as exc:
            result = OrcaCommandResult(
                argv=argv,
                returncode=-1,
                stdout=_diagnostic_text(exc.stdout),
                stderr=_diagnostic_text(exc.stderr),
            )
            raise OrcaCommandError(f"Orca command timed out after {exc.timeout} seconds", result) from exc

        try:
            stdout = _strict_text(completed.stdout)
        except UnicodeDecodeError as exc:
            result = OrcaCommandResult(argv, completed.returncode, "<invalid UTF-8>", _diagnostic_text(completed.stderr))
            raise OrcaCommandError("Orca stdout was not valid UTF-8", result) from exc
        stderr = _diagnostic_text(completed.stderr)
        result = OrcaCommandResult(argv, completed.returncode, stdout, stderr)
        if completed.returncode != 0:
            detail = stderr.strip() or stdout.strip() or "no diagnostic output"
            raise OrcaCommandError(
                f"Orca command exited with {completed.returncode}: {detail}",
                result,
            )
        return result

    def run_json(self, *arguments: str, timeout_seconds: float | None = None) -> JsonObject:
        argv = (*self.command, *arguments)
        try:
            completed = self._runner(
                argv,
                capture_output=True,
                text=False,
                env=self.environment,
                timeout=self.timeout_seconds if timeout_seconds is None else timeout_seconds,
                check=False,
            )
        except FileNotFoundError as exc:
            raise OrcaCommandError(f"Orca executable was not found: {self.command[0]}") from exc
        except subprocess.TimeoutExpired as exc:
            try:
                stdout = _strict_text(exc.stdout)
            except UnicodeDecodeError:
                stdout = "<invalid UTF-8>"
            result = OrcaCommandResult(
                argv=argv,
                returncode=-1,
                stdout=stdout,
                stderr=_diagnostic_text(exc.stderr),
                payload=_decode_object(stdout),
            )
            raise OrcaCommandError(f"Orca command timed out after {exc.timeout} seconds", result) from exc

        try:
            stdout = _strict_text(completed.stdout)
        except UnicodeDecodeError as exc:
            result = OrcaCommandResult(
                argv=argv,
                returncode=completed.returncode,
                stdout="<invalid UTF-8>",
                stderr=_diagnostic_text(completed.stderr),
            )
            raise OrcaCommandError("Orca stdout was not valid UTF-8", result) from exc
        stderr = _diagnostic_text(completed.stderr)
        try:
            decoded = json.loads(stdout)
        except json.JSONDecodeError as exc:
            result = OrcaCommandResult(
                argv=argv,
                returncode=completed.returncode,
                stdout=stdout,
                stderr=stderr,
            )
            raise OrcaCommandError("Orca stdout was not one JSON document", result) from exc

        if not isinstance(decoded, dict):
            result = OrcaCommandResult(
                argv=argv,
                returncode=completed.returncode,
                stdout=stdout,
                stderr=stderr,
            )
            raise OrcaCommandError("Orca JSON response was not an object", result)

        result = OrcaCommandResult(
            argv=argv,
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
            payload=decoded,
        )
        if completed.returncode != 0 or decoded.get("ok") is not True:
            error = decoded.get("error")
            message = error.get("message") if isinstance(error, dict) else None
            detail = message or stderr.strip() or "Orca response did not prove ok=true"
            raise OrcaCommandError(detail, result)
        return decoded
