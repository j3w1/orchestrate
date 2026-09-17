from __future__ import annotations

from contextlib import closing, redirect_stdout
import io
import json
import os
from pathlib import Path
import shlex
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.bootstrap import _exit_code, controller_command, decode_payload, encode_payload, launch_controller
from orchestrate.cli import build_parser, main
from orchestrate.errors import OrchestrateError
from orchestrate.orca import OrcaCommandError, OrcaCommandResult


requires_native_windows_admission = unittest.skipUnless(
    sys.platform in {"linux", "win32"},
    "managed worker admission requires native Windows or Linux",
)


class FakeClient:
    def __init__(self, *, exit_code: int = 7, write_result: bool = True, interrupt_first: bool = False) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.exit_code = exit_code
        self.write_result = write_result
        self.interrupt_first = interrupt_first
        self.waits = 0
        self.worktree_path = ""
        self.command = ""

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if arguments[0:2] == ("terminal", "create"):
            selector = arguments[arguments.index("--worktree") + 1]
            self.worktree_path = selector.removeprefix("path:")
            self.command = arguments[arguments.index("--command") + 1]
            return {
                "result": {
                    "terminal": {
                        "handle": "term_controller",
                        "tabId": "tab_controller",
                        "paneKey": "tab_controller:leaf_controller",
                        "ptyId": "pty_controller",
                        "worktreeId": "repo::fixture",
                        "title": "orchestrate controller",
                        "executionHostId": "local",
                        "incarnationId": "incarnation_controller",
                        "hostPlatform": sys.platform,
                        "surface": "visible",
                    }
                },
                "_meta": {"runtimeId": "runtime_test"},
            }
        if arguments[0:2] == ("terminal", "show"):
            return {
                "result": {
                    "terminal": {
                        "handle": "term_controller",
                        "worktreeId": "repo::fixture",
                        "worktreePath": self.worktree_path,
                        "executionHostId": "local",
                        "tabId": "tab_controller",
                        "incarnationId": "incarnation_controller",
                        "orphaned": False,
                        "hostPlatform": sys.platform,
                        "connected": True,
                        "writable": True,
                    }
                },
                "_meta": {"runtimeId": "runtime_test"},
            }
        if arguments[0:2] == ("terminal", "wait"):
            self.waits += 1
            if self.interrupt_first and self.waits == 1:
                raise KeyboardInterrupt
            if self.write_result:
                if sys.platform == "win32":
                    marker = "--result-path '"
                    start = self.command.index(marker) + len(marker)
                    end = self.command.index("'", start)
                    result_path = self.command[start:end]
                else:
                    command = shlex.split(self.command)
                    result_path = command[command.index("--result-path") + 1]
                Path(result_path).write_text(
                    json.dumps(
                        {
                            "schema": "orchestrate-bootstrap/v1",
                            "exitCode": self.exit_code,
                            "stdout": "controller output\n",
                        }
                    ),
                    encoding="utf-8",
                )
            return {
                "result": {
                    "wait": {
                        "handle": "term_controller",
                        "condition": "exit",
                        "satisfied": True,
                        "status": "exited",
                        "exitCode": self.exit_code,
                        "exitCause": {"kind": "exited", "exitCode": self.exit_code},
                    }
                },
                "_meta": {"runtimeId": "runtime_test"},
            }
        if arguments[0:2] == ("terminal", "send"):
            return {"result": {"send": {"handle": "term_controller", "accepted": True, "bytesWritten": 1}}}
        if arguments[0:2] == ("terminal", "close"):
            return {
                "result": {
                    "close": {
                        "handle": "term_controller",
                        "tabId": "tab_controller",
                        "closeMode": "tab",
                        "ptyKilled": False,
                    }
                }
            }
        raise AssertionError(arguments)


class AgentIdentityClient:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if arguments[0:2] == ("terminal", "show"):
            return {
                "result": {
                    "terminal": {
                        "handle": "term_agent",
                        "worktreeId": "worktree_1",
                        "worktreePath": str(self.root),
                        "executionHostId": "local",
                        "connected": True,
                        "writable": True,
                        "agentIdentity": "codex",
                    }
                }
            }
        raise AssertionError(arguments)


class UncertainCreateClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        raise OrcaCommandError(
            "response lost",
            OrcaCommandResult(arguments, -1, "", "response lost", None),
        )


class UncertainInterruptClient(FakeClient):
    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        if arguments[0:2] == ("terminal", "send"):
            self.calls.append(arguments)
            raise OrcaCommandError(
                "interrupt response lost",
                OrcaCommandResult(arguments, -1, "", "interrupt response lost", None),
            )
        return super().run_json(*arguments, **keywords)


class UncertainCloseClient(FakeClient):
    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        if arguments[0:2] == ("terminal", "close"):
            self.calls.append(arguments)
            raise OrcaCommandError(
                "close response lost",
                OrcaCommandResult(arguments, -1, "", "close response lost", None),
            )
        return super().run_json(*arguments, **keywords)


class ReconciledCloseClient(FakeClient):
    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        if arguments[0:2] == ("terminal", "show"):
            self.calls.append(arguments)
            return {
                "result": {"terminal": {
                    "handle": "term_controller", "tabId": "tab_controller",
                    "incarnationId": "incarnation_controller", "ptyId": "fixture@@pty-controller",
                    "worktreeId": "repo::fixture", "worktreePath": self.worktree_path,
                    "executionHostId": "local",
                    "connected": False, "writable": False, "orphaned": False,
                }},
                "_meta": {"runtimeId": "runtime_test"},
            }
        if arguments[0:2] == ("terminal", "list"):
            self.calls.append(arguments)
            return {
                "result": {
                    "terminals": [{
                        "handle": "term_caller", "incarnationId": "incarnation_caller",
                        "worktreeId": "repo::fixture", "worktreePath": self.worktree_path,
                        "tabId": "tab_caller", "executionHostId": "local",
                    }],
                    "visualLayouts": [{
                        "worktreeId": "repo::fixture", "worktreePath": self.worktree_path,
                        "root": {"type": "group", "groupId": "group_1", "activeTabId": "tab_caller", "tabs": [{
                            "tabId": "tab_caller", "activeLeafId": "leaf_caller",
                            "panes": {"type": "terminal", "handle": "term_caller", "tabId": "tab_caller", "leafId": "leaf_caller"},
                        }]},
                    }],
                    "hostScope": {"hostIds": ["local"], "omittedHostIds": []},
                    "topologyRevisions": {"repo::fixture": 2},
                    "totalCount": 1,
                    "truncated": False,
                },
                "_meta": {"runtimeId": "runtime_test"},
            }
        return super().run_json(*arguments, **keywords)


class StaleRuntimeCloseClient(ReconciledCloseClient):
    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        response = super().run_json(*arguments, **keywords)
        if arguments[0:2] == ("terminal", "list"):
            response["_meta"] = {"runtimeId": "runtime_restarted"}
        return response


class HistoricalPlatformCloseClient(ReconciledCloseClient):
    def __init__(self, host_platform: object) -> None:
        super().__init__()
        self.host_platform = host_platform

    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        response = super().run_json(*arguments, **keywords)
        if arguments[0:2] == ("terminal", "show"):
            response["result"]["terminal"]["hostPlatform"] = self.host_platform  # type: ignore[index]
        return response


class BooleanExitCloseClient(ReconciledCloseClient):
    def __init__(self, *, exit_code: int, receipt: bool) -> None:
        super().__init__(exit_code=exit_code)
        self.receipt = receipt

    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        response = super().run_json(*arguments, **keywords)
        if arguments[0:2] == ("terminal", "wait"):
            wait = response["result"]["wait"]  # type: ignore[index]
            wait["exitCode"] = self.receipt  # type: ignore[index]
            wait["exitCause"]["exitCode"] = self.receipt  # type: ignore[index]
        return response


class BooleanInventoryCloseClient(ReconciledCloseClient):
    def __init__(self, *, field: str, value: bool) -> None:
        super().__init__()
        self.field = field
        self.value = value

    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        response = super().run_json(*arguments, **keywords)
        if arguments[0:2] == ("terminal", "list"):
            inventory = response["result"]  # type: ignore[index]
            if self.field == "totalCount":
                inventory["totalCount"] = self.value  # type: ignore[index]
            else:
                inventory["topologyRevisions"]["repo::fixture"] = self.value  # type: ignore[index]
        return response


class BootstrapTests(unittest.TestCase):
    def test_cli_routes_the_explicit_versioned_milestone_plan(self) -> None:
        parsed = build_parser().parse_args(
            [
                "implement",
                "authorized objective",
                "--plan",
                "plans/milestone.json",
                "--allow-exceptional-capacity",
                "--capacity-reason",
                "four independent checks",
                "--json",
            ]
        )
        self.assertEqual(parsed.command, "implement")
        self.assertEqual(parsed.objective, "authorized objective")
        self.assertEqual(parsed.plan, "plans/milestone.json")
        self.assertTrue(parsed.allow_exceptional_capacity)
        self.assertEqual(parsed.capacity_reason, "four independent checks")

    def test_payload_round_trip_preserves_spaces_unicode_and_argument_boundaries(self) -> None:
        arguments = [
            "implement",
            "fix spaced path 雪",
            "--project",
            "C:/a b/雪",
            "--allow-exceptional-capacity",
            "--capacity-reason",
            "four bounded workers 雪",
        ]
        self.assertEqual(decode_payload(encode_payload(arguments)), arguments)

    def test_bootstrap_rejects_project_contained_state_before_terminal_creation(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": str(Path(directory) / ".private"), "ORCA_TERMINAL_HANDLE": ""},
        ):
            client = FakeClient()
            with self.assertRaises(OrchestrateError) as caught:
                launch_controller(Path(directory), ["status"], client=client)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "state_storage_unsafe")
            self.assertEqual(client.calls, [])

    @unittest.skipUnless(sys.platform == "linux", "requires native Linux")
    def test_wsl_controller_bootstrap_is_rejected_before_terminal_creation(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.bootstrap.is_wsl",
            return_value=True,
        ):
            client = UncertainCreateClient()
            with self.assertRaises(OrchestrateError) as rejected:
                launch_controller(Path(directory), ["status"], client=client)  # type: ignore[arg-type]
            self.assertEqual(rejected.exception.code, "bootstrap_host_unsupported")
            self.assertEqual(client.calls, [])

    @unittest.skipUnless(sys.platform == "linux", "requires native Linux")
    def test_wsl_common_cli_boundary_rejects_before_setup_or_orchestration(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"ORCA_TERMINAL_HANDLE": "term_existing"},
        ), patch(
            "orchestrate.cli.is_wsl",
            return_value=True,
        ), patch("orchestrate.cli.setup_project") as project_setup, patch(
            "orchestrate.cli.implement"
        ) as implementation, patch("orchestrate.cli.launch_controller") as controller_launch:
            for arguments in (
                ["setup", "--project", directory, "--json"],
                ["implement", "fixture", "--project", directory, "--json"],
            ):
                with self.subTest(command=arguments[0]), redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(main(arguments), 1)
                    self.assertIn('"code": "wsl_native_host_unsupported"', output.getvalue())
            project_setup.assert_not_called()
            implementation.assert_not_called()
            controller_launch.assert_not_called()
            self.assertFalse((Path(directory) / ".private").exists())

    @unittest.skipUnless(sys.platform == "linux", "requires native Linux")
    def test_wsl_private_controller_rejects_before_result_path_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.cli.is_wsl",
            return_value=True,
        ), patch("orchestrate.cli.setup_project") as project_setup, redirect_stdout(
            io.StringIO()
        ) as output:
            result_path = Path(directory) / "new-parent" / "result.json"
            exit_code = main(
                [
                    "_controller",
                    "--payload",
                    encode_payload(["setup", "--project", directory, "--json"]),
                    "--result-path",
                    str(result_path),
                ]
            )
            self.assertEqual(exit_code, 1)
            self.assertIn('"code": "wsl_native_host_unsupported"', output.getvalue())
            project_setup.assert_not_called()
            self.assertFalse(result_path.parent.exists())

    def test_unsupported_platform_rejects_before_setup_or_preflight_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.cli.os.sys.platform",
            "darwin",
        ), patch("orchestrate.cli.is_wsl", return_value=False), patch(
            "orchestrate.cli.setup_project"
        ) as project_setup, patch("orchestrate.cli.worker_preflight") as preflight:
            commands = (
                ["setup", "--project", directory, "--json"],
                [
                    "worker-preflight",
                    "--project",
                    directory,
                    "--run",
                    "run_1",
                    "--task",
                    "task_1",
                    "--dispatch",
                    "dispatch_1",
                    "--packet-id",
                    "packet_1",
                    "--json",
                ],
            )
            for arguments in commands:
                with self.subTest(command=arguments[0]), redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(main(arguments), 1)
                    self.assertIn('"code": "native_host_unsupported"', output.getvalue())
            project_setup.assert_not_called()
            preflight.assert_not_called()
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_unsupported_platform_private_controller_rejects_before_result_path_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.cli.os.sys.platform",
            "darwin",
        ), patch("orchestrate.cli.is_wsl", return_value=False), redirect_stdout(io.StringIO()) as output:
            result_path = Path(directory) / "new-parent" / "result.json"
            exit_code = main(
                [
                    "_controller",
                    "--payload",
                    encode_payload(["setup", "--project", directory, "--json"]),
                    "--result-path",
                    str(result_path),
                ]
            )
            self.assertEqual(exit_code, 1)
            self.assertIn('"code": "native_host_unsupported"', output.getvalue())
            self.assertFalse(result_path.parent.exists())

    def test_commands_preserve_foreground_exit_contract_on_both_shells(self) -> None:
        payload = encode_payload(["status"])
        result = Path("C:/result path/雪.json")
        windows = controller_command(payload, result, python="C:/Program Files/Python/python.exe", platform="win32")
        linux = controller_command(payload, result, python="/opt/python 3.13/bin/python", platform="linux")
        self.assertIn("exit $LASTEXITCODE", windows)
        self.assertIn("& 'C:/Program Files/Python/python.exe'", windows)
        self.assertIn(" -I -m orchestrate ", windows)
        self.assertIn(" exec ", linux)
        self.assertIn("'/opt/python 3.13/bin/python'", linux)
        self.assertIn(" -I -m orchestrate ", linux)

    @requires_native_windows_admission
    def test_launcher_journals_mirrors_output_and_returns_inner_exit_code(self) -> None:
        client = FakeClient()
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            output = io.StringIO()
            with redirect_stdout(output):
                result = launch_controller(Path(directory), ["status"], client=client)  # type: ignore[arg-type]
            database = Path(state) / "bootstrap" / "state.sqlite3"
            self.assertTrue(database.is_file())
        self.assertEqual(result, 7)
        self.assertEqual(output.getvalue(), "controller output\n")
        self.assertEqual(client.calls[0][0:2], ("terminal", "create"))
        self.assertEqual(client.calls[-1][0:2], ("terminal", "close"))

    def test_agent_terminal_is_rejected_by_native_identity_not_hook_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {
                "ORCHESTRATE_HOME": state,
                "ORCA_TERMINAL_HANDLE": "term_agent",
                "ORCA_AGENT_HOOK_TOKEN": "ordinary-and-agent-both-have-this",
            },
        ):
            client = AgentIdentityClient(Path(directory))
            with self.assertRaises(OrchestrateError) as caught:
                launch_controller(Path(directory), ["implement", "x"], client=client)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "agent_terminal_not_controller")
        self.assertEqual(len(client.calls), 1)

    def test_missing_or_contradictory_exit_evidence_never_closes_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            client = FakeClient(write_result=False)
            with self.assertRaises(OrchestrateError) as caught:
                launch_controller(Path(directory), ["status"], client=client)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "bootstrap_result_unavailable")
            self.assertFalse(any(call[:2] == ("terminal", "close") for call in client.calls))

        contradictory = {
            "result": {
                "wait": {
                    "handle": "term_controller",
                    "condition": "exit",
                    "satisfied": True,
                    "status": "exited",
                    "exitCode": 0,
                    "exitCause": {"kind": "signal", "signal": "SIGKILL"},
                }
            }
        }
        with self.assertRaises(OrchestrateError) as mismatch:
            _exit_code(contradictory, expected_handle="term_controller")
        self.assertEqual(mismatch.exception.code, "bootstrap_exit_unproven")

        for boolean in (False, True):
            boolean_receipt = {
                "result": {
                    "wait": {
                        "handle": "term_controller",
                        "condition": "exit",
                        "satisfied": True,
                        "status": "exited",
                        "exitCode": boolean,
                        "exitCause": {"kind": "exited", "exitCode": boolean},
                    }
                }
            }
            with self.subTest(boolean_exit_code=boolean), self.assertRaises(OrchestrateError) as boolean_mismatch:
                _exit_code(boolean_receipt, expected_handle="term_controller")
            self.assertEqual(boolean_mismatch.exception.code, "bootstrap_exit_unproven")

    @requires_native_windows_admission
    def test_ctrl_c_still_requires_durable_result_and_exact_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            client = FakeClient(exit_code=130, interrupt_first=True)
            output = io.StringIO()
            with redirect_stdout(output):
                result = launch_controller(Path(directory), ["status"], client=client)  # type: ignore[arg-type]
        self.assertEqual(result, 130)
        self.assertTrue(any(call[:2] == ("terminal", "send") for call in client.calls))
        self.assertEqual(output.getvalue(), "controller output\n")

    def test_uncertain_create_blocks_automatic_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            first = UncertainCreateClient()
            with self.assertRaises(OrchestrateError) as initial:
                launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
            self.assertEqual(initial.exception.code, "bootstrap_effect_uncertain")
            second = FakeClient()
            with self.assertRaises(OrchestrateError) as replay:
                launch_controller(Path(directory), ["status"], client=second)  # type: ignore[arg-type]
            self.assertEqual(replay.exception.code, "bootstrap_effect_uncertain")
            self.assertEqual(second.calls, [])

    @requires_native_windows_admission
    def test_uncertain_interrupt_is_never_repeated_and_exact_exit_can_recover(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            first = UncertainInterruptClient(exit_code=130, interrupt_first=True)
            with self.assertRaises(OrchestrateError) as initial:
                launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
            self.assertEqual(initial.exception.code, "bootstrap_effect_uncertain")
            self.assertEqual(sum(call[:2] == ("terminal", "send") for call in first.calls), 1)

            recovery = FakeClient(exit_code=130)
            recovery.command = first.command
            recovery.worktree_path = first.worktree_path
            output = io.StringIO()
            with redirect_stdout(output):
                result = launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]

            self.assertEqual(result, 130)
            self.assertEqual(output.getvalue(), "controller output\n")
            self.assertFalse(any(call[:2] == ("terminal", "send") for call in recovery.calls))
            self.assertFalse(any(call[:2] == ("terminal", "create") for call in recovery.calls))

    @requires_native_windows_admission
    def test_uncertain_close_reconciles_live_historical_show_without_host_platform(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            first = UncertainCloseClient()
            with self.assertRaises(OrchestrateError) as initial:
                launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
            self.assertEqual(initial.exception.code, "bootstrap_effect_uncertain")
            self.assertEqual(sum(call[:2] == ("terminal", "close") for call in first.calls), 1)

            recovery = ReconciledCloseClient()
            recovery.command = first.command
            recovery.worktree_path = first.worktree_path
            output = io.StringIO()
            with redirect_stdout(output):
                result = launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
            self.assertEqual(result, 7)
            self.assertEqual(output.getvalue(), "controller output\n")
            self.assertFalse(any(call[:2] == ("terminal", "close") for call in recovery.calls))
            self.assertTrue(any(call[:2] == ("terminal", "list") for call in recovery.calls))
            database = Path(state) / "bootstrap" / "state.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.row_factory = sqlite3.Row
                journal = connection.execute("SELECT * FROM invocations").fetchone()
            self.assertEqual(journal["phase"], "reported")
            self.assertIsNotNone(journal["error_json"])
            self.assertIsNotNone(journal["exit_response_json"])
            cleanup = json.loads(journal["cleanup_observation_json"])
            self.assertEqual(cleanup["outcome"], "observed-exited-and-absent")

    @requires_native_windows_admission
    def test_uncertain_close_rejects_contradictory_historical_host_platform(self) -> None:
        other_platform = "linux" if sys.platform == "win32" else "win32"
        for host_platform in (other_platform, None, False):
            with self.subTest(host_platform=host_platform), tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
                os.environ,
                {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
            ):
                first = UncertainCloseClient()
                with self.assertRaises(OrchestrateError):
                    launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
                recovery = HistoricalPlatformCloseClient(host_platform)
                recovery.command = first.command
                recovery.worktree_path = first.worktree_path
                with self.assertRaises(OrchestrateError) as held:
                    launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
                self.assertEqual(held.exception.code, "bootstrap_effect_uncertain")
                self.assertFalse(any(call[:2] == ("terminal", "close") for call in recovery.calls))

    @requires_native_windows_admission
    def test_uncertain_close_holds_when_durable_child_result_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            first = UncertainCloseClient()
            with self.assertRaises(OrchestrateError):
                launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
            database = Path(state) / "bootstrap" / "state.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                result_path = connection.execute("SELECT result_path FROM invocations").fetchone()[0]
            Path(result_path).unlink()

            recovery = ReconciledCloseClient()
            recovery.command = first.command
            recovery.worktree_path = first.worktree_path
            with self.assertRaises(OrchestrateError) as held:
                launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
            self.assertEqual(held.exception.code, "bootstrap_effect_uncertain")
            self.assertEqual(recovery.calls, [])

    @requires_native_windows_admission
    def test_uncertain_close_holds_on_stale_runtime_without_repeating_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
            os.environ,
            {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
        ):
            first = UncertainCloseClient()
            with self.assertRaises(OrchestrateError):
                launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
            recovery = StaleRuntimeCloseClient()
            recovery.command = first.command
            recovery.worktree_path = first.worktree_path
            with self.assertRaises(OrchestrateError) as held:
                launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
            self.assertEqual(held.exception.code, "bootstrap_effect_uncertain")
            self.assertFalse(any(call[:2] == ("terminal", "close") for call in recovery.calls))

    @requires_native_windows_admission
    def test_uncertain_close_rejects_boolean_exit_receipts_and_durable_results(self) -> None:
        for integer, boolean in ((0, False), (1, True)):
            with self.subTest(receipt=boolean), tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
                os.environ,
                {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
            ):
                first = UncertainCloseClient(exit_code=integer)
                with self.assertRaises(OrchestrateError):
                    launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
                recovery = BooleanExitCloseClient(exit_code=integer, receipt=boolean)
                recovery.command = first.command
                recovery.worktree_path = first.worktree_path
                with self.assertRaises(OrchestrateError) as held:
                    launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
                self.assertEqual(held.exception.code, "bootstrap_effect_uncertain")
                self.assertFalse(any(call[:2] == ("terminal", "close") for call in recovery.calls))

            with self.subTest(durable_result=boolean), tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
                os.environ,
                {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
            ):
                first = UncertainCloseClient(exit_code=integer)
                with self.assertRaises(OrchestrateError):
                    launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
                database = Path(state) / "bootstrap" / "state.sqlite3"
                with closing(sqlite3.connect(database)) as connection:
                    result_path = Path(connection.execute("SELECT result_path FROM invocations").fetchone()[0])
                result_path.write_text(
                    json.dumps({
                        "schema": "orchestrate-bootstrap/v1",
                        "exitCode": boolean,
                        "stdout": "controller output\n",
                    }),
                    encoding="utf-8",
                )
                recovery = ReconciledCloseClient(exit_code=integer)
                recovery.command = first.command
                recovery.worktree_path = first.worktree_path
                with self.assertRaises(OrchestrateError) as held:
                    launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
                self.assertEqual(held.exception.code, "bootstrap_effect_uncertain")
                self.assertEqual(recovery.calls, [])

    @requires_native_windows_admission
    def test_uncertain_close_rejects_boolean_inventory_completeness(self) -> None:
        for field, boolean in (("totalCount", True), ("topologyRevision", False), ("topologyRevision", True)):
            with self.subTest(field=field, value=boolean), tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as state, patch.dict(
                os.environ,
                {"ORCHESTRATE_HOME": state, "ORCA_TERMINAL_HANDLE": ""},
            ):
                first = UncertainCloseClient()
                with self.assertRaises(OrchestrateError):
                    launch_controller(Path(directory), ["status"], client=first)  # type: ignore[arg-type]
                recovery = BooleanInventoryCloseClient(field=field, value=boolean)
                recovery.command = first.command
                recovery.worktree_path = first.worktree_path
                with self.assertRaises(OrchestrateError) as held:
                    launch_controller(Path(directory), ["status"], client=recovery)  # type: ignore[arg-type]
                self.assertEqual(held.exception.code, "bootstrap_effect_uncertain")
                self.assertFalse(any(call[:2] == ("terminal", "close") for call in recovery.calls))

    def test_cli_bootstrap_passthrough_emits_one_json_document_and_exact_exit(self) -> None:
        child = {"schema": "orchestrate-report/v1", "status": "worker_succeeded"}

        def launch(*_: object, **__: object) -> int:
            print(json.dumps(child))
            return 0

        output = io.StringIO()
        with patch("orchestrate.cli._needs_bootstrap", return_value=True), patch(
            "orchestrate.cli.launch_controller",
            side_effect=launch,
        ), redirect_stdout(output):
            result = main(["implement", "synthetic objective", "--json"])
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue()), child)

        with patch("orchestrate.cli.launch_controller", side_effect=launch), redirect_stdout(io.StringIO()):
            self.assertEqual(main(["bootstrap", "--", "status", "--json"]), 0)

    def test_cli_bootstrap_error_paths_emit_one_json_document(self) -> None:
        failure = OrchestrateError("synthetic bootstrap failure", code="bootstrap_effect_uncertain")
        for arguments, needs_bootstrap in (
            (["implement", "synthetic objective", "--json"], True),
            (["bootstrap", "--", "status", "--json"], False),
        ):
            output = io.StringIO()
            with patch("orchestrate.cli._needs_bootstrap", return_value=needs_bootstrap), patch(
                "orchestrate.cli.launch_controller",
                side_effect=failure,
            ), redirect_stdout(output):
                result = main(arguments)
            document = json.loads(output.getvalue())
            self.assertEqual(result, 1)
            self.assertEqual(document["status"], "blocked")
            self.assertEqual(document["error"]["code"], "bootstrap_effect_uncertain")

    def test_cli_bootstrap_passthrough_preserves_nonzero_child_exit(self) -> None:
        def launch(*_: object, **__: object) -> int:
            print(json.dumps({"schema": "orchestrate-report/v1", "status": "blocked"}))
            return 23

        output = io.StringIO()
        with patch("orchestrate.cli._needs_bootstrap", return_value=True), patch(
            "orchestrate.cli.launch_controller",
            side_effect=launch,
        ), redirect_stdout(output):
            result = main(["implement", "synthetic objective", "--json"])
        self.assertEqual(result, 23)
        self.assertEqual(json.loads(output.getvalue())["status"], "blocked")

    def test_held_and_unadmitted_states_are_nonzero_directly_and_through_bootstrap(self) -> None:
        for status in (
            "milestone_blocked",
            "milestone_cleanup_pending",
            "preflight_held",
            "worker_unadmitted",
        ):
            with self.subTest(status=status):
                child = {
                    "schema": "orchestrate-report/v1",
                    "status": status,
                    "verification": "not_run",
                }
                direct_output = io.StringIO()
                with patch("orchestrate.cli._execute", return_value=(child, True)), redirect_stdout(direct_output):
                    direct = main(["status", "--json"])
                self.assertEqual(direct, 1)
                self.assertEqual(json.loads(direct_output.getvalue()), child)

                def launch(*_: object, **__: object) -> int:
                    print(json.dumps(child))
                    return direct

                bootstrap_output = io.StringIO()
                with patch("orchestrate.cli._needs_bootstrap", return_value=True), patch(
                    "orchestrate.cli.launch_controller",
                    side_effect=launch,
                ), redirect_stdout(bootstrap_output):
                    outer = main(["implement", "synthetic objective", "--json"])
                self.assertEqual(outer, 1)
                self.assertEqual(json.loads(bootstrap_output.getvalue()), child)


if __name__ == "__main__":
    unittest.main()
