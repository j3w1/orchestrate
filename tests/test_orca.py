from __future__ import annotations

import json
import subprocess
import unittest

from orchestrate.orca import OrcaClient, OrcaCommandError, orca_task_title, resolve_orca_command
from orchestrate.orca_compat import (
    WorkerShowShapeError,
    worker_execution_identity,
    worker_show_dispatch_identity,
    worker_show_identity,
)


class ResolveCommandTests(unittest.TestCase):
    def test_forwarded_command_has_highest_precedence(self) -> None:
        command = resolve_orca_command(
            {"ORCA_CLI_COMMAND": "forwarded-orca", "ORCA_DEV_REPO_ROOT": "present"},
            "linux",
        )
        self.assertEqual(command, ("forwarded-orca",))

    def test_dev_command_precedes_linux_outside_command(self) -> None:
        self.assertEqual(resolve_orca_command({"ORCA_DEV_REPO_ROOT": "present"}, "linux"), ("orca-dev",))

    def test_linux_outside_uses_orca_ide(self) -> None:
        self.assertEqual(resolve_orca_command({}, "linux"), ("orca-ide",))

    def test_windows_uses_packaged_orca(self) -> None:
        self.assertEqual(resolve_orca_command({}, "win32"), ("orca",))


class TaskTitleTests(unittest.TestCase):
    def test_short_title_is_preserved_and_ecmascript_whitespace_is_collapsed(self) -> None:
        self.assertEqual(orca_task_title("  short\t\n title  "), "short title")
        self.assertEqual(orca_task_title("\ufeff\u2003bounded\u00a0title\ufeff"), "bounded title")

    def test_title_at_the_80_utf16_unit_limit_is_not_truncated(self) -> None:
        title = "a" * 78 + "\U0001f40b"
        self.assertEqual(orca_task_title(title), title)

    def test_long_title_uses_77_utf16_units_without_splitting_a_surrogate_pair(self) -> None:
        self.assertEqual(orca_task_title("a" * 81), "a" * 77 + "...")
        self.assertEqual(orca_task_title("a" * 76 + "\U0001f40b" + "tail"), "a" * 76 + "...")

    def test_truncation_trims_boundary_whitespace_and_empty_input_has_a_stable_fallback(self) -> None:
        self.assertEqual(orca_task_title("a" * 76 + " " + "tail"), "a" * 76 + "...")
        self.assertEqual(orca_task_title(" \t\ufeff "), "orchestrate task")


class ClientTests(unittest.TestCase):
    def test_json_command_uses_argument_array_and_keeps_stderr_separate(self) -> None:
        calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

        def runner(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
            calls.append((argv, kwargs))
            return subprocess.CompletedProcess(argv, 0, '{"ok":true,"result":{}}', "keepalive")

        client = OrcaClient(("orca",), runner=runner)
        payload = client.run_json("status", "--json")

        self.assertTrue(payload["ok"])
        self.assertEqual(calls[0][0], ("orca", "status", "--json"))
        self.assertTrue(calls[0][1]["capture_output"])
        self.assertFalse(calls[0][1]["text"])

    def test_structured_error_preserves_public_error_code(self) -> None:
        response = {
            "ok": False,
            "error": {"code": "no_active_sender_terminal", "message": "terminal required"},
        }

        def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(argv, 1, json.dumps(response), "")

        client = OrcaClient(("orca",), runner=runner)
        with self.assertRaises(OrcaCommandError) as caught:
            client.run_json("orchestration", "run-create", "--json")
        self.assertEqual(caught.exception.code, "no_active_sender_terminal")

    def test_non_json_stdout_fails_closed(self) -> None:
        def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(argv, 0, "not json", "")

        with self.assertRaisesRegex(OrcaCommandError, "not one JSON document"):
            OrcaClient(("orca",), runner=runner).run_json("status", "--json")

    def test_invalid_utf8_stdout_fails_without_replacement(self) -> None:
        def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[bytes]:
            return subprocess.CompletedProcess(argv, 0, b'{"ok":true,"id":"\xff"}', b"")

        with self.assertRaisesRegex(OrcaCommandError, "valid UTF-8") as caught:
            OrcaClient(("orca",), runner=runner).run_json("status", "--json")
        self.assertNotIn("\ufffd", caught.exception.result.stdout)

    def test_missing_or_non_boolean_success_envelope_fails_closed(self) -> None:
        for value in ({"result": {}}, {"ok": 1, "result": {}}, {"ok": "true", "result": {}}):
            with self.subTest(value=value):
                def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
                    return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")

                with self.assertRaisesRegex(OrcaCommandError, "ok=true"):
                    OrcaClient(("orca",), runner=runner).run_json("status", "--json")

    def test_nonzero_ok_receipt_requires_exact_worker_start_failure_contract(self) -> None:
        response = {
            "ok": True,
            "result": {
                "state": "failed",
                "stage": "dispatch_input",
                "lastError": "agent_prompt_stalled",
            },
        }

        def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(argv, 1, json.dumps(response), "")

        client = OrcaClient(("orca",), runner=runner)
        with self.assertRaisesRegex(OrcaCommandError, "exited with 1"):
            client.run_json("orchestration", "worker-start", "--json")
        accepted = client.run_json(
            "orchestration",
            "worker-start",
            "--json",
            allow_worker_start_nonzero=True,
        )
        self.assertEqual(accepted, response)
        self.assertEqual(accepted.returncode, 1)

    def test_worker_start_ready_or_unexpected_nonzero_is_never_suppressed(self) -> None:
        for returncode, state in ((1, "ready"), (37, "failed"), (37, "outcome_unknown")):
            with self.subTest(returncode=returncode, state=state):
                response = {"ok": True, "result": {"state": state}}

                def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
                    return subprocess.CompletedProcess(argv, returncode, json.dumps(response), "")

                with self.assertRaisesRegex(OrcaCommandError, f"exited with {returncode}"):
                    OrcaClient(("orca",), runner=runner).run_json(
                        "orchestration",
                        "worker-start",
                        "--json",
                        allow_worker_start_nonzero=True,
                    )

    def test_worker_show_compatibility_accepts_only_complete_version_shapes(self) -> None:
        dispatch_199 = {
            "id": "dispatch_1",
            "runId": "run_1",
            "taskId": "task_1",
            "task_id": "task_1",
            "lastFailure": None,
        }
        worker_199 = {
            "dispatchId": "dispatch_1",
            "worktreeId": "worktree_1",
            "agentTerminalHandle": "term_1",
            "lastError": None,
        }
        dispatch_198 = {
            "id": "dispatch_1",
            "run_id": "run_1",
            "task_id": "task_1",
            "last_failure": None,
        }
        worker_198 = {
            "worktree_id": "worktree_1",
            "agent_terminal_handle": "term_1",
            "last_error": None,
        }
        self.assertEqual(worker_show_identity(dispatch_199, worker_199).dispatch.version, "1.4.199")
        self.assertEqual(worker_show_identity(dispatch_198, worker_198).dispatch.version, "1.4.198")
        with self.assertRaisesRegex(WorkerShowShapeError, "identities conflict"):
            worker_show_identity(dispatch_199, {**worker_199, "dispatchId": "dispatch_other"})

        for missing in ("runId", "taskId", "task_id", "lastFailure"):
            with self.subTest(version="1.4.199", missing=missing):
                malformed = dict(dispatch_199)
                malformed.pop(missing)
                with self.assertRaises(WorkerShowShapeError):
                    worker_show_dispatch_identity(malformed)
        for alias in ("run_id", "last_failure"):
            with self.subTest(version="1.4.199", alias=alias):
                malformed = {**dispatch_199, alias: "conflict"}
                with self.assertRaises(WorkerShowShapeError):
                    worker_show_dispatch_identity(malformed)
        conflicting_tasks = {**dispatch_199, "task_id": "task_other"}
        with self.assertRaisesRegex(WorkerShowShapeError, "conflicting"):
            worker_show_dispatch_identity(conflicting_tasks)

        for missing in ("run_id", "task_id", "last_failure"):
            with self.subTest(version="1.4.198", missing=missing):
                malformed = dict(dispatch_198)
                malformed.pop(missing)
                with self.assertRaises(WorkerShowShapeError):
                    worker_show_dispatch_identity(malformed)
        for alias in ("runId", "taskId", "lastFailure"):
            with self.subTest(version="1.4.198", alias=alias):
                malformed = {**dispatch_198, alias: "conflict"}
                with self.assertRaises(WorkerShowShapeError):
                    worker_show_dispatch_identity(malformed)

        for version, dispatch, worker, required, aliases in (
            (
                "1.4.199",
                dispatch_199,
                worker_199,
                ("dispatchId", "worktreeId", "agentTerminalHandle", "lastError"),
                ("dispatch_id", "worktree_id", "agent_terminal_handle", "last_error"),
            ),
            (
                "1.4.198",
                dispatch_198,
                worker_198,
                ("worktree_id", "agent_terminal_handle", "last_error"),
                ("dispatchId", "worktreeId", "agentTerminalHandle", "lastError"),
            ),
        ):
            for missing in required:
                with self.subTest(version=version, missing=missing):
                    malformed = dict(worker)
                    malformed.pop(missing)
                    with self.assertRaises(WorkerShowShapeError):
                        worker_show_identity(dispatch, malformed)
            for alias in aliases:
                with self.subTest(version=version, alias=alias):
                    malformed = {**worker, alias: "conflict"}
                    with self.assertRaises(WorkerShowShapeError):
                        worker_show_identity(dispatch, malformed)

    def test_worker_show_compatibility_rejects_mixed_version_identity(self) -> None:
        dispatch = {
            "id": "dispatch_1",
            "runId": "run_1",
            "taskId": "task_1",
            "task_id": "task_1",
            "lastFailure": None,
        }
        mixed_worker = {
            "dispatchId": "dispatch_1",
            "worktree_id": "worktree_1",
            "agentTerminalHandle": "term_1",
            "lastError": None,
        }
        with self.assertRaisesRegex(WorkerShowShapeError, "mixed or incomplete"):
            worker_show_identity(dispatch, mixed_worker)

    def test_worker_execution_semantics_are_exact_for_both_supported_versions(self) -> None:
        cases = (
            ("succeeded", "completed", None, "succeeded", "settled", None),
            ("failed", "failed", "worker_failed", "failed", "settled", "worker_failed"),
            (
                "prompt_stall",
                "failed",
                "agent_prompt_stalled",
                "failed",
                "dispatch_input",
                "agent_prompt_stalled",
            ),
        )
        for current_shape in (False, True):
            for semantics, status, failure, state, stage, error in cases:
                with self.subTest(current_shape=current_shape, semantics=semantics):
                    dispatch = {"id": "dispatch_1", "status": status}
                    worker = {"state": state, "stage": stage}
                    if current_shape:
                        dispatch.update(
                            runId="run_1",
                            taskId="task_1",
                            task_id="task_1",
                            lastFailure=failure,
                        )
                        worker.update(
                            dispatchId="dispatch_1",
                            worktreeId="worktree_1",
                            agentTerminalHandle="term_1",
                            lastError=error,
                        )
                    else:
                        dispatch.update(
                            run_id="run_1",
                            task_id="task_1",
                            last_failure=failure,
                        )
                        worker.update(
                            worktree_id="worktree_1",
                            agent_terminal_handle="term_1",
                            last_error=error,
                        )
                    identity = worker_execution_identity(
                        dispatch,
                        worker,
                        semantics=semantics,
                        terminal_handle="term_1",
                    )
                    self.assertEqual(identity.dispatch.version, "1.4.199" if current_shape else "1.4.198")

                    with self.assertRaises(WorkerShowShapeError):
                        worker_execution_identity(
                            dispatch,
                            {**worker, "stage": "released"},
                            semantics=semantics,
                            terminal_handle="term_1",
                        )


if __name__ == "__main__":
    unittest.main()
