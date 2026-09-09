from __future__ import annotations

from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.bootstrap import _exit_code, controller_command, decode_payload, encode_payload, launch_controller
from orchestrate.cli import main
from orchestrate.errors import OrchestrateError


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if arguments[0:2] == ("terminal", "create"):
            return {"result": {"terminal": {"handle": "term_controller"}}}
        if arguments[0:2] == ("terminal", "wait"):
            command = self.calls[0][self.calls[0].index("--command") + 1]
            marker = "--result-path '"
            start = command.index(marker) + len(marker)
            end = command.index("'", start)
            Path(command[start:end]).write_text(
                '{"schema":"orchestrate-bootstrap/v1","exitCode":7,"stdout":"controller output\\n"}',
                encoding="utf-8",
            )
            return {
                "result": {
                    "wait": {
                        "handle": "term_controller",
                        "condition": "exit",
                        "satisfied": True,
                        "status": "exited",
                        "exitCode": 7,
                        "exitCause": {"kind": "exited", "exitCode": 7},
                    }
                }
            }
        if arguments[0:2] == ("terminal", "close"):
            return {"result": {"closed": True}}
        raise AssertionError(arguments)


class BootstrapTests(unittest.TestCase):
    def test_payload_round_trip_preserves_spaces_unicode_and_argument_boundaries(self) -> None:
        arguments = ["implement", "fix spaced path 雪", "--project", "C:/a b/雪"]
        self.assertEqual(decode_payload(encode_payload(arguments)), arguments)

    def test_commands_preserve_foreground_exit_contract_on_both_shells(self) -> None:
        payload = encode_payload(["status"])
        result = Path("C:/result path/雪.json")
        windows = controller_command(payload, result, python="C:/Program Files/Python/python.exe", platform="win32")
        linux = controller_command(payload, result, python="/opt/python 3.13/bin/python", platform="linux")
        self.assertIn("exit $LASTEXITCODE", windows)
        self.assertIn("& 'C:/Program Files/Python/python.exe'", windows)
        self.assertIn(" exec ", linux)
        self.assertIn("'/opt/python 3.13/bin/python'", linux)

    def test_launcher_mirrors_output_and_returns_inner_exit_code(self) -> None:
        client = FakeClient()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"ORCA_AGENT_HOOK_TOKEN": "", "ORCA_AGENT_LAUNCH_TOKEN": ""},
        ):
            output = io.StringIO()
            with redirect_stdout(output):
                result = launch_controller(Path(directory), ["status"], client=client)  # type: ignore[arg-type]
        self.assertEqual(result, 7)
        self.assertIn("controller output", output.getvalue())
        create = client.calls[0]
        self.assertIn("--focus", create)
        self.assertIn(f"path:{Path(directory).resolve()}", create)
        self.assertEqual(client.calls[-1][0:2], ("terminal", "close"))

    def test_agent_terminal_cannot_use_bootstrap_as_depth_bypass(self) -> None:
        with patch.dict(os.environ, {"ORCA_AGENT_HOOK_TOKEN": "present"}):
            with self.assertRaises(OrchestrateError) as caught:
                launch_controller(Path.cwd(), ["implement", "x"], client=FakeClient())  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "bootstrap_from_agent_forbidden")

    def test_missing_native_exit_code_fails_closed(self) -> None:
        with self.assertRaises(OrchestrateError) as caught:
            _exit_code({"result": {"wait": {"condition": "exit", "satisfied": True, "status": "exited"}}})
        self.assertEqual(caught.exception.code, "bootstrap_exit_unproven")

    def test_cli_returns_exact_bootstrapped_exit_code(self) -> None:
        with patch("orchestrate.cli.launch_controller", return_value=7), redirect_stdout(io.StringIO()):
            result = main(["bootstrap", "--", "status"])
        self.assertEqual(result, 7)


if __name__ == "__main__":
    unittest.main()
