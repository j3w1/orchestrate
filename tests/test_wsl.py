from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from orchestrate.errors import OrchestrateError
from orchestrate.wsl import (
    WINDOWS_COMMAND_ENV,
    decode_invocation,
    encode_invocation,
    launch_from_wsl,
    launcher_main,
    make_invocation,
    receive_on_windows,
    windows_unc_path,
)


class WslForwardingTests(unittest.TestCase):
    def test_launcher_help_is_state_free_and_available_on_either_host(self) -> None:
        self.assertEqual(launcher_main(("--help",)), 0)

    def test_spaces_unicode_empty_arguments_and_exit_code_are_forwarded_exactly(self) -> None:
        seen: list[tuple[tuple[str, ...], dict[str, object]]] = []
        environment = {
            "WSL_DISTRO_NAME": "Arch Test",
            WINDOWS_COMMAND_ENV: json.dumps(["C:\\Program Files\\orchestrate\\orchestrate.exe"]),
        }
        arguments = ("implement", "fix café 雪", "--project", "/home/user/project with spaces", "--label=")

        def runner(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            seen.append((argv, kwargs))
            return subprocess.CompletedProcess(argv, 37)

        code = launch_from_wsl(
            arguments,
            environment=environment,
            linux_cwd="/home/user/project with spaces/雪",
            platform="linux",
            runner=runner,
        )

        self.assertEqual(code, 37)
        self.assertEqual(seen[0][0][0], r"C:\Program Files\orchestrate\orchestrate.exe")
        self.assertEqual(seen[0][0][-2], "--payload")
        self.assertFalse(seen[0][1]["check"])
        invocation = decode_invocation(seen[0][0][-1])
        self.assertEqual(invocation.distro, "Arch Test")
        self.assertEqual(invocation.linux_cwd, "/home/user/project with spaces/雪")
        self.assertEqual(invocation.argv, arguments)

    def test_windows_receiver_gets_one_canonical_context_and_preserves_exit(self) -> None:
        invocation = make_invocation(distro="Arch", linux_cwd="/home/iqbal/源 code", argv=("status", "--json"))
        observed: list[object] = []
        code = receive_on_windows(
            encode_invocation(invocation),
            lambda value: observed.append(value) or 19,
            platform="win32",
        )
        self.assertEqual(code, 19)
        self.assertEqual(observed, [invocation])
        self.assertEqual(windows_unc_path(invocation), r"\\wsl.localhost\Arch\home\iqbal\源 code")

    def test_linux_launcher_creates_no_second_state_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            forbidden_state = Path(directory) / "state-must-not-exist"
            environment = {
                "WSL_DISTRO_NAME": "Arch",
                WINDOWS_COMMAND_ENV: json.dumps(["orchestrate.exe"]),
                "ORCHESTRATE_HOME": str(forbidden_state),
            }

            def runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[bytes]:
                return subprocess.CompletedProcess(argv, 0)

            self.assertEqual(
                launch_from_wsl(("status",), environment=environment, linux_cwd="/repo", platform="linux", runner=runner),
                0,
            )
            self.assertFalse(forbidden_state.exists())

    def test_non_wsl_or_malformed_transport_fails_closed(self) -> None:
        with self.assertRaises(OrchestrateError) as not_wsl:
            launch_from_wsl(("status",), environment={}, linux_cwd="/repo", platform="linux")
        self.assertEqual(not_wsl.exception.code, "wsl_context_invalid")
        with self.assertRaises(OrchestrateError) as malformed:
            decode_invocation("not-base64!")
        self.assertEqual(malformed.exception.code, "wsl_payload_invalid")
        with self.assertRaises(OrchestrateError) as wrong_host:
            receive_on_windows(encode_invocation(make_invocation(distro="Arch", linux_cwd="/repo", argv=("status",))), lambda _: 0, platform="linux")
        self.assertEqual(wrong_host.exception.code, "wsl_receiver_invalid")


if __name__ == "__main__":
    unittest.main()
