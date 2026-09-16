from __future__ import annotations

from contextlib import nullcontext, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from orchestrate.errors import OrchestrateError
from orchestrate.cli import _print_human, main
from orchestrate.machine_bootstrap import (
    MachineLayout,
    MachineBootstrapResult,
    _windows_shim_text,
    _wsl_shim_text,
    ensure_machine,
    machine_ready,
    register_user_path,
)
from path_faults import ResolvedPathFault


class FakeUserPath:
    def __init__(self, value: str = "") -> None:
        self.value = value
        self.reads = 0
        self.writes: list[str] = []

    def read(self) -> str:
        self.reads += 1
        return self.value

    def write(self, value: str) -> None:
        self.value = value
        self.writes.append(value)


def fixture_layout(root: Path) -> MachineLayout:
    source = root / "checkout"
    (source / "src" / "orchestrate").mkdir(parents=True)
    (source / "src" / "orchestrate" / "__init__.py").write_text("", encoding="utf-8")
    (source / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    install = root / "local" / "orchestrate"
    scripts = install / "venv" / "Scripts"
    return MachineLayout(
        source_root=source,
        install_root=install,
        venv_root=install / "venv",
        scripts_root=scripts,
        command_path=scripts / "orchestrate.exe",
        install_receipt=install / "install.json",
        bin_root=install / "bin",
        windows_shim=install / "bin" / "orchestrate.cmd",
        wsl_shim=install / "bin" / "orchestrate",
    )


def install_ready_files(layout: MachineLayout) -> None:
    layout.scripts_root.mkdir(parents=True, exist_ok=True)
    (layout.scripts_root / "python.exe").write_bytes(b"fixture")
    layout.command_path.write_bytes(b"fixture")
    layout.install_receipt.write_text(
        json.dumps(
            {"schema": "orchestrate-machine-install/v1", "sourceRoot": str(layout.source_root.resolve())},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    layout.bin_root.mkdir(parents=True, exist_ok=True)
    layout.windows_shim.write_text(_windows_shim_text(), encoding="utf-8", newline="")
    layout.wsl_shim.write_text(_wsl_shim_text(), encoding="utf-8", newline="")


class SyntheticInstaller:
    def __init__(self, layout: MachineLayout, *, fail_phase: str | None = None) -> None:
        self.layout = layout
        self.fail_phase = fail_phase
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[object]:
        self.calls.append(argv)
        if self.fail_phase and self.fail_phase in " ".join(argv):
            return subprocess.CompletedProcess(argv, 31)
        if argv[1:3] == ("-m", "venv"):
            self.layout.scripts_root.mkdir(parents=True)
            (self.layout.scripts_root / "python.exe").write_bytes(b"fixture")
        if "--editable" in argv:
            self.layout.command_path.write_bytes(b"fixture")
        return subprocess.CompletedProcess(argv, 0)


def resolving(layout: MachineLayout, store: FakeUserPath):
    def resolve(command: str, path: str) -> str | None:
        if command != "orchestrate" or path != store.value:
            return None
        first = path.split(";", 1)[0] if path else ""
        if first.casefold().replace("/", "\\") != str(layout.bin_root).casefold().replace("/", "\\"):
            return str(Path(first) / "orchestrate.cmd") if first else None
        return str(layout.windows_shim) if layout.windows_shim.is_file() else None

    return resolve


class MachineBootstrapTests(unittest.TestCase):
    def test_healthy_fast_path_is_one_read_silent_and_effect_free(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            store = FakeUserPath(str(layout.bin_root))
            runner = SyntheticInstaller(layout)
            output = io.StringIO()

            with redirect_stdout(output):
                result = ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(result.state, "ready")
            self.assertEqual(result.actions, ())
            self.assertEqual(output.getvalue(), "")
            self.assertEqual(store.reads, 1)
            self.assertEqual(store.writes, [])
            self.assertEqual(runner.calls, [])

    def test_first_run_installs_once_and_second_run_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            store = FakeUserPath(r"C:\existing")
            runner = SyntheticInstaller(layout)
            resolver = resolving(layout, store)

            first = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolver,
                runner=runner,
                platform="win32",
                version_info=(3, 13),
            )
            call_count = len(runner.calls)
            write_count = len(store.writes)
            second = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolver,
                runner=runner,
                platform="win32",
                version_info=(3, 13),
            )

            self.assertEqual(first.state, "repaired")
            self.assertEqual(
                first.actions,
                (
                    "created_environment",
                    "installed_editable_checkout",
                    "installed_windows_shim",
                    "installed_wsl_shim",
                    "registered_user_path",
                ),
            )
            self.assertEqual(len(runner.calls), 4)
            self.assertIn((str(layout.scripts_root / "python.exe"), "-m", "ensurepip", "--upgrade"), runner.calls)
            self.assertEqual(second.state, "ready")
            self.assertEqual(len(runner.calls), call_count)
            self.assertEqual(len(store.writes), write_count)
            self.assertEqual(store.value.split(";").count(str(layout.bin_root)), 1)

    def test_path_repair_collapses_only_its_duplicates_and_takes_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            foreign_one = r"C:\foreign-one"
            foreign_two = r"C:\foreign-two"
            duplicate = str(layout.bin_root).upper()
            store = FakeUserPath(";".join((foreign_one, str(layout.bin_root), foreign_two, duplicate)))

            result = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=SyntheticInstaller(layout),
                platform="win32",
                version_info=(3, 13),
            )

            self.assertEqual(result.actions, ("registered_user_path",))
            self.assertEqual(store.value.split(";"), [str(layout.bin_root), foreign_one, foreign_two])

    def test_existing_environment_repairs_launchers_without_reinstall(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            layout.command_path.write_bytes(b"fixture")
            layout.install_receipt.write_text(
                json.dumps(
                    {
                        "schema": "orchestrate-machine-install/v1",
                        "sourceRoot": str(layout.source_root.resolve()),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            store = FakeUserPath()
            runner = SyntheticInstaller(layout)

            result = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=runner,
                platform="win32",
                version_info=(3, 13),
            )

            self.assertEqual(runner.calls, [])
            self.assertNotIn("installed_editable_checkout", result.actions)
            self.assertTrue(machine_ready(layout, store, resolver=resolving(layout, store)))

    def test_failed_install_is_explicit_and_rerunnable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            store = FakeUserPath()
            runner = SyntheticInstaller(layout, fail_phase="ensurepip")

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_command_failed")
            self.assertEqual(held.exception.data, {"phase": "pip preparation", "returnCode": 31})
            self.assertEqual(store.writes, [])
            self.assertFalse(layout.windows_shim.exists())

    def test_failed_editable_install_cannot_be_mistaken_for_a_completed_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            store = FakeUserPath()

            def partial_runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[object]:
                if "--editable" in argv:
                    layout.command_path.write_bytes(b"partial")
                    return subprocess.CompletedProcess(argv, 17)
                return subprocess.CompletedProcess(argv, 0)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=partial_runner,
                    platform="win32",
                    version_info=(3, 13),
                )
            self.assertEqual(held.exception.code, "machine_bootstrap_command_failed")
            self.assertTrue(layout.command_path.exists())
            self.assertFalse(layout.install_receipt.exists())

            runner = SyntheticInstaller(layout)
            result = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=runner,
                platform="win32",
                version_info=(3, 13),
            )
            self.assertEqual(result.state, "repaired")
            self.assertTrue(any("--editable" in call for call in runner.calls))
            self.assertTrue(layout.install_receipt.exists())

    def test_unsupported_python_and_incomplete_venv_fail_before_unrelated_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            store = FakeUserPath()
            runner = SyntheticInstaller(layout)
            with self.assertRaises(OrchestrateError) as old_python:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 12),
                )
            self.assertEqual(old_python.exception.code, "machine_bootstrap_python_unsupported")
            self.assertEqual(store.reads, 0)
            self.assertFalse(layout.install_root.exists())

            layout.venv_root.mkdir(parents=True)
            with self.assertRaises(OrchestrateError) as incomplete:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )
            self.assertEqual(incomplete.exception.code, "machine_bootstrap_venv_incomplete")
            self.assertEqual(runner.calls, [])

    def test_path_sensitive_shim_write_fault_is_explicit_and_recoverable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            layout.command_path.write_bytes(b"fixture")
            layout.install_receipt.write_text(
                json.dumps(
                    {
                        "schema": "orchestrate-machine-install/v1",
                        "sourceRoot": str(layout.source_root.resolve()),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            store = FakeUserPath()
            fault = ResolvedPathFault(layout.wsl_shim)
            real_replace = Path.replace

            def replace(candidate: Path, target: Path) -> Path:
                if fault.matches(target):
                    raise PermissionError("synthetic launcher denial")
                return real_replace(candidate, target)

            with patch.object(Path, "replace", replace):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=store,
                        resolver=resolving(layout, store),
                        runner=SyntheticInstaller(layout),
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(fault.interceptions, 1)
            self.assertEqual(held.exception.code, "machine_bootstrap_shim_write_failed")
            self.assertEqual(store.writes, [])
            repaired = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=SyntheticInstaller(layout),
                platform="win32",
                version_info=(3, 13),
            )
            self.assertEqual(repaired.state, "repaired")

    def test_non_windows_setup_has_no_machine_state_owner(self) -> None:
        result = ensure_machine(platform="linux", version_info=(3, 13))
        self.assertEqual(result.state, "not_applicable")

    def test_cli_machine_failure_prevents_project_setup(self) -> None:
        failure = OrchestrateError("synthetic machine failure", code="machine_bootstrap_test_failure")
        output = io.StringIO()
        with patch("orchestrate.cli.ensure_machine", side_effect=failure), patch(
            "orchestrate.cli.setup_project"
        ) as project_setup, redirect_stdout(output):
            self.assertEqual(main(["setup", "--json"]), 1)
        project_setup.assert_not_called()
        self.assertIn('"code": "machine_bootstrap_test_failure"', output.getvalue())

    def test_cli_machine_phase_precedes_and_continues_project_setup(self) -> None:
        calls: list[str] = []
        profile = SimpleNamespace(
            root=Path.cwd(),
            path=Path.cwd() / ".orchestrate.json",
            digest="sha256:fixture",
            selection_source="initial-setup-selection",
            selection_history_digest="sha256:history",
            value={"reader": {"kind": "repository"}, "instructions": [], "taskEntrypoints": []},
        )

        def machine() -> MachineBootstrapResult:
            calls.append("machine")
            return MachineBootstrapResult("ready")

        def project(*_: object, **__: object) -> SimpleNamespace:
            calls.append("project")
            return profile

        output = io.StringIO()
        with patch("orchestrate.cli.ensure_machine", machine), patch(
            "orchestrate.cli.setup_project", project
        ), patch("orchestrate.cli.StateStore", return_value=nullcontext()), redirect_stdout(output):
            self.assertEqual(main(["setup", "--json"]), 0)

        self.assertEqual(calls, ["machine", "project"])
        self.assertIn('"machineBootstrap"', output.getvalue())
        self.assertIn('"state": "ready"', output.getvalue())

    def test_human_setup_output_reports_repairs_without_treating_not_run_as_check_rows(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            _print_human(
                {
                    "status": "configured",
                    "checks": "NOT_RUN",
                    "machineBootstrap": {
                        "state": "repaired",
                        "actions": ["installed_windows_shim", "registered_user_path"],
                    },
                }
            )
        self.assertEqual(
            output.getvalue(),
            "orchestrate: configured\n"
            "  Machine bootstrap: repaired (installed_windows_shim, registered_user_path)\n",
        )

    def test_register_user_path_preserves_unrelated_empty_and_spelled_entries(self) -> None:
        store = FakeUserPath(r";C:\one;C:\two")
        entry = Path(r"C:\Orchestrate\bin")
        self.assertTrue(register_user_path(store, entry))
        self.assertEqual(store.value, f"{entry};;C:\\one;C:\\two")
        self.assertFalse(register_user_path(store, entry))
        self.assertEqual(len(store.writes), 1)


if __name__ == "__main__":
    unittest.main()
