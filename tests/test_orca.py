from __future__ import annotations

import json
import subprocess
import unittest

from orchestrate.orca import OrcaClient, OrcaCommandError, orca_task_title, resolve_orca_command


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


if __name__ == "__main__":
    unittest.main()
