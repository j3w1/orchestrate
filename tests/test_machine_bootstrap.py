from __future__ import annotations

from contextlib import nullcontext, redirect_stdout
import io
import errno
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
from types import ModuleType
from types import SimpleNamespace
import unittest
import zipfile
from unittest.mock import patch

from orchestrate.errors import OrchestrateError
from orchestrate.cli import _print_human, main
from orchestrate.machine_bootstrap import (
    MachineLayout,
    MachineBootstrapResult,
    RegistryUserPathStore,
    UserPathValue,
    _anchor_value,
    _ArchiveBinding,
    _atomic_write_owned,
    _build_source_archive,
    _commit_staged_no_replace,
    _ensure_directory,
    _ensure_install_parent,
    _install_receipt_matches,
    _NATIVE_WIN32_PROVIDER,
    _SIMULATED_WIN32_PROVIDER,
    _opened_file_identity_matches,
    _read_bounded_regular,
    _read_bounded_regular_file,
    _receipt_value,
    _run_bound_step,
    _SourceBinding,
    _target_identity,
    _verify_committed_stage,
    _VenvBinding,
    _windows_shim_text,
    _wsl_shim_text,
    default_layout,
    ensure_machine,
    machine_ready,
    register_user_path,
)
from path_faults import ResolvedPathFault


LIVENESS_TIMEOUT = 10.0


class FakeUserPath:
    def __init__(self, value: str = "", *, kind: int = 2, exists: bool = True) -> None:
        self.value = value
        self.kind = kind
        self.exists = exists
        self.reads = 0
        self.writes: list[str] = []
        self.before_replace: object = None
        self._mutex = threading.Lock()

    def read(self) -> UserPathValue:
        with self._mutex:
            self.reads += 1
            return UserPathValue(self.value, self.kind, self.exists)

    def replace(self, expected: UserPathValue, value: str) -> None:
        hook = self.before_replace
        if callable(hook):
            hook()
        with self._mutex:
            if UserPathValue(self.value, self.kind, self.exists) != expected:
                raise OrchestrateError(
                    "synthetic concurrent PATH change",
                    code="machine_bootstrap_path_changed",
                )
            self.value = value
            self.exists = True
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
        install_anchor=root / "local" / "orchestrate-state" / "machine-install.json",
        source_archive=install / "installed-source.zip",
        bin_root=install / "bin",
        windows_shim=install / "bin" / "orchestrate.cmd",
        wsl_shim=install / "bin" / "orchestrate",
    )


def write_pyvenv_config(layout: MachineLayout) -> bytes:
    raw = b"home = C:\\Python313\ninclude-system-site-packages = false\nversion = 3.13.9\n"
    (layout.venv_root / "pyvenv.cfg").write_bytes(raw)
    return raw


def stat_view(info: os.stat_result, **changes: int) -> SimpleNamespace:
    values = {
        "st_dev": info.st_dev,
        "st_ino": info.st_ino,
        "st_mode": info.st_mode,
        "st_nlink": info.st_nlink,
        "st_size": info.st_size,
        "st_mtime_ns": info.st_mtime_ns,
        "st_ctime_ns": info.st_ctime_ns,
        "st_file_attributes": getattr(info, "st_file_attributes", 0),
    }
    values.update(changes)
    return SimpleNamespace(**values)


def install_ready_files(layout: MachineLayout) -> None:
    for stale in (layout.install_receipt, layout.install_anchor, layout.source_archive):
        stale.unlink(missing_ok=True)
    layout.scripts_root.mkdir(parents=True, exist_ok=True)
    write_pyvenv_config(layout)
    (layout.scripts_root / "python.exe").write_bytes(b"fixture")
    layout.command_path.write_bytes(b"fixture")
    with _SourceBinding(layout) as source:
        with _build_source_archive(layout, source) as archive, _VenvBinding(layout) as binding:
            receipt = _receipt_value(
                binding,
                source,
                archive,
                installation_id="fixture-installation",
            )
    layout.install_receipt.write_bytes(
        (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
    )
    layout.install_anchor.parent.mkdir(parents=True, exist_ok=True)
    layout.install_anchor.write_bytes(
        (
            json.dumps(_anchor_value(receipt), sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
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
            self.layout.scripts_root.mkdir(parents=True, exist_ok=True)
            write_pyvenv_config(self.layout)
            (self.layout.scripts_root / "python.exe").write_bytes(b"fixture")
        if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
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
    def test_canonical_machine_record_fixtures_never_use_text_writes(self) -> None:
        source = Path(__file__).read_text(encoding="utf-8")
        forbidden_call = "." + "write" + "_text("
        for fixture_name in ("install_receipt", "install_anchor", "source_archive"):
            self.assertNotIn(fixture_name + forbidden_call, source)

    def test_windows_alias_identity_is_handle_based_not_spelling_based(self) -> None:
        opened = SimpleNamespace(st_dev=7, st_ino=99, st_mode=0o100600)
        same_file_through_alias = SimpleNamespace(st_dev=7, st_ino=99, st_mode=0o100444)
        different_file = SimpleNamespace(st_dev=7, st_ino=100, st_mode=0o100600)

        self.assertTrue(_opened_file_identity_matches(opened, same_file_through_alias))
        self.assertFalse(_opened_file_identity_matches(opened, different_file))

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
                    "installed_reviewed_source",
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

    def test_installed_default_layout_uses_authenticated_source_and_continues_setup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
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
            self.assertEqual(first.state, "repaired")
            call_count = len(runner.calls)
            write_count = len(store.writes)
            installed_module = (
                root
                / "wheel-env"
                / "lib"
                / "python3.13"
                / "site-packages"
                / "orchestrate"
                / "machine_bootstrap.py"
            )
            profile = SimpleNamespace(
                root=root / "project",
                path=root / "project" / ".orchestrate.json",
                digest="sha256:fixture",
                selection_source="initial-setup-selection",
                selection_history_digest="sha256:history",
                value={"reader": {"kind": "repository"}, "instructions": [], "taskEntrypoints": []},
            )
            calls: list[str] = []

            def installed_machine() -> MachineBootstrapResult:
                calls.append("machine")
                return ensure_machine(
                    path_store=store,
                    resolver=resolver,
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            def project(*_: object, **__: object) -> SimpleNamespace:
                calls.append("project")
                return profile

            with patch("orchestrate.machine_bootstrap.__file__", os.fspath(installed_module)), patch.dict(
                os.environ,
                {"LOCALAPPDATA": os.fspath(root / "local")},
            ):
                selected = default_layout()
                self.assertEqual(selected.source_root, layout.source_root.resolve())
                self.assertTrue(selected.source_from_install_record)
                read_count = store.reads
                ready = installed_machine()
                self.assertEqual(ready.state, "ready")
                self.assertEqual(store.reads, read_count + 1)
                calls.clear()
                with patch("orchestrate.cli.ensure_machine", installed_machine), patch(
                    "orchestrate.cli.setup_project", project
                ), patch("orchestrate.cli.StateStore", return_value=nullcontext()), redirect_stdout(
                    io.StringIO()
                ):
                    self.assertEqual(main(["setup", "--json"]), 0)

            self.assertEqual(calls, ["machine", "project"])
            self.assertEqual(len(runner.calls), call_count)
            self.assertEqual(len(store.writes), write_count)

    def test_installed_default_layout_rejects_moved_persisted_source_definitively(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            install_ready_files(layout)
            layout.source_root.rename(root / "moved-checkout")
            installed_module = root / "wheel-env" / "site-packages" / "orchestrate" / "machine_bootstrap.py"

            with patch("orchestrate.machine_bootstrap.__file__", os.fspath(installed_module)), patch.dict(
                os.environ,
                {"LOCALAPPDATA": os.fspath(root / "local")},
            ):
                with self.assertRaises(OrchestrateError) as held:
                    default_layout()

            self.assertEqual(held.exception.code, "machine_bootstrap_persisted_source_unavailable")
            self.assertEqual(held.exception.data["disposition"], "definitive")  # type: ignore[index]
            self.assertIn("bootstrap.py", held.exception.data["recovery"])  # type: ignore[index]

    def test_installed_default_layout_classifies_temporary_source_read_failure_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            install_ready_files(layout)
            installed_module = root / "wheel-env" / "site-packages" / "orchestrate" / "machine_bootstrap.py"
            fault = ResolvedPathFault(layout.source_root / "pyproject.toml")

            def unavailable(path: Path, limit: int) -> bytes:
                if fault.matches(path):
                    raise OrchestrateError(
                        "synthetic access failure",
                        code="machine_bootstrap_safe_io_unavailable",
                        data={
                            "component": "boundedRead.open",
                            "expected": "valid-handle",
                            "observed": "unavailable",
                            "errno": errno.EACCES,
                        },
                    )
                return _read_bounded_regular(path, limit)

            with patch("orchestrate.machine_bootstrap.__file__", os.fspath(installed_module)), patch.dict(
                os.environ,
                {"LOCALAPPDATA": os.fspath(root / "local")},
            ), patch("orchestrate.machine_bootstrap._read_bounded_regular", unavailable):
                with self.assertRaises(OrchestrateError) as held:
                    default_layout()

            self.assertEqual(fault.interceptions, 1)
            self.assertEqual(held.exception.code, "machine_bootstrap_persisted_source_unavailable")
            self.assertEqual(held.exception.data["disposition"], "retryable")  # type: ignore[index]

    def test_installed_default_layout_rejects_rewritten_receipt_without_anchor_grant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            install_ready_files(layout)
            receipt = json.loads(layout.install_receipt.read_text(encoding="utf-8"))
            receipt["sourceRoot"] = os.fspath(root / "attacker-checkout")
            payload = {key: item for key, item in receipt.items() if key != "receiptSha256"}
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            receipt["receiptSha256"] = f"sha256:{hashlib.sha256(canonical).hexdigest()}"
            layout.install_receipt.write_bytes(
                (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            )
            installed_module = root / "wheel-env" / "site-packages" / "orchestrate" / "machine_bootstrap.py"

            with patch("orchestrate.machine_bootstrap.__file__", os.fspath(installed_module)), patch.dict(
                os.environ,
                {"LOCALAPPDATA": os.fspath(root / "local")},
            ):
                with self.assertRaises(OrchestrateError) as held:
                    default_layout()

            self.assertEqual(
                held.exception.code,
                "machine_bootstrap_persisted_source_identity_unproven",
            )
            self.assertEqual(held.exception.data["disposition"], "definitive")  # type: ignore[index]

    def test_installed_default_layout_rejects_changed_persisted_source_without_reinstall(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            install_ready_files(layout)
            store = FakeUserPath(str(layout.bin_root))
            runner = SyntheticInstaller(layout)
            layout.source_root.joinpath("pyproject.toml").write_text(
                "[project]\nname='changed'\n",
                encoding="utf-8",
            )
            installed_module = root / "wheel-env" / "site-packages" / "orchestrate" / "machine_bootstrap.py"

            with patch("orchestrate.machine_bootstrap.__file__", os.fspath(installed_module)), patch.dict(
                os.environ,
                {"LOCALAPPDATA": os.fspath(root / "local")},
            ):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        path_store=store,
                        resolver=resolving(layout, store),
                        runner=runner,
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(held.exception.code, "machine_bootstrap_persisted_source_changed")
            self.assertEqual(held.exception.data["disposition"], "definitive")  # type: ignore[index]
            self.assertEqual(runner.calls, [])
            self.assertEqual(store.writes, [])

    @unittest.skipIf(
        sys.platform == "win32",
        "POSIX directory-symlink analogue; native Windows uses reparse-point checks",
    )
    def test_redirected_venv_is_rejected_before_any_pip_effect(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            unrelated = root / "unrelated-environment"
            scripts = unrelated / "Scripts"
            scripts.mkdir(parents=True)
            (unrelated / "pyvenv.cfg").write_bytes(
                b"home = /unrelated\ninclude-system-site-packages = false\nversion = 3.13.9\n"
            )
            (scripts / "python.exe").write_bytes(b"unrelated")
            layout.install_root.mkdir(parents=True)
            layout.venv_root.symlink_to(unrelated, target_is_directory=True)
            runner = SyntheticInstaller(layout)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_install_identity_unproven")
            self.assertEqual(runner.calls, [])
            self.assertFalse((scripts / "orchestrate.exe").exists())

    def test_bound_venv_change_stops_before_the_next_pip_effect(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            calls: list[tuple[str, ...]] = []

            def mutate_after_first(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[object]:
                calls.append(argv)
                (layout.venv_root / "pyvenv.cfg").write_bytes(
                    b"home = C:\\Changed\ninclude-system-site-packages = false\nversion = 3.13.9\n"
                )
                return subprocess.CompletedProcess(argv, 0)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=mutate_after_first,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
            self.assertEqual(len(calls), 1)

    def test_denied_bound_venv_mutation_wins_over_process_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")

            def denied_runner(
                argv: tuple[str, ...], **_: object
            ) -> subprocess.CompletedProcess[object]:
                raise PermissionError(13, "synthetic sharing denial", layout.venv_root / "pyvenv.cfg")

            with _VenvBinding(layout) as binding:
                with self.assertRaises(OrchestrateError) as held:
                    _run_bound_step(
                        binding,
                        denied_runner,
                        (os.fspath(binding.python_path), "-c", "pass"),
                        phase="denied identity mutation",
                    )

            self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
            self.assertEqual(held.exception.data["component"], "venvNode.deniedMutation")

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "requires descriptor execution")
    def test_bound_step_executes_the_pinned_interpreter_across_path_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            shutil.copy2(sys.executable, layout.scripts_root / "python.exe")
            backup = layout.scripts_root / "python.original"
            marker = layout.install_root / "external-ran"

            def swapping_runner(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[object]:
                os.replace(layout.scripts_root / "python.exe", backup)
                (layout.scripts_root / "python.exe").write_text(
                    f"#!/bin/sh\nprintf external > '{marker}'\n",
                    encoding="utf-8",
                )
                os.chmod(layout.scripts_root / "python.exe", 0o755)
                try:
                    return subprocess.run(argv, **kwargs)
                finally:
                    (layout.scripts_root / "python.exe").unlink()
                    os.replace(backup, layout.scripts_root / "python.exe")

            with _VenvBinding(layout) as binding:
                _run_bound_step(
                    binding,
                    swapping_runner,
                    (os.fspath(binding.python_path), "-c", "raise SystemExit(0)"),
                    phase="pinned interpreter probe",
                )

            self.assertFalse(marker.exists())

    def test_windows_runner_names_the_bound_interpreter_as_the_application(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")

            with _VenvBinding(layout) as binding:
                binding._provider = _NATIVE_WIN32_PROVIDER
                kwargs = binding.runner_kwargs()

            self.assertEqual(
                kwargs,
                {
                    "executable": str(layout.scripts_root / "python.exe"),
                    "cwd": str(layout.venv_root),
                },
            )

    @unittest.skipUnless(sys.platform == "win32", "requires Windows sharing semantics")
    def test_windows_binding_denies_interpreter_replacement_while_effects_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            shutil.copy2(sys.executable, layout.scripts_root / "python.exe")

            with _VenvBinding(layout):
                with self.assertRaises(OSError):
                    os.replace(
                        layout.scripts_root / "python.exe",
                        layout.scripts_root / "python.replaced",
                    )

            self.assertTrue((layout.scripts_root / "python.exe").is_file())

    @unittest.skipIf(
        sys.platform == "win32",
        "POSIX symlink victim probe; Windows staging uses the same exclusive random-name path",
    )
    def test_predictable_legacy_temp_links_cannot_overwrite_unrelated_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            install_ready_files(layout)
            layout.install_receipt.write_bytes(
                b'{"schema":"orchestrate-machine-install/v1","sourceRoot":"stale"}\n'
            )
            layout.windows_shim.write_text("wrong\n", encoding="utf-8")
            receipt_victim = root / "receipt-victim"
            shim_victim = root / "shim-victim"
            receipt_victim.write_text("receipt-safe", encoding="utf-8")
            shim_victim.write_text("shim-safe", encoding="utf-8")
            (layout.install_root / "install.json.tmp").symlink_to(receipt_victim)
            (layout.bin_root / "orchestrate.cmd.tmp").symlink_to(shim_victim)
            store = FakeUserPath()

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=SyntheticInstaller(layout),
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_shim_identity_unproven")
            self.assertEqual(receipt_victim.read_text(encoding="utf-8"), "receipt-safe")
            self.assertEqual(shim_victim.read_text(encoding="utf-8"), "shim-safe")

    def test_missing_target_commit_preserves_an_interfering_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "install.json"
            fault = ResolvedPathFault(target)
            real_commit = _commit_staged_no_replace

            def interfering_commit(
                temporary_name: str,
                target_name: str,
                *,
                parent_fd: int | None,
                parent: Path,
                **kwargs: object,
            ) -> None:
                if fault.matches(target_name, relative_to=parent):
                    target.write_bytes(b"unrelated-owner-data")
                real_commit(
                    temporary_name,
                    target_name,
                    parent_fd=parent_fd,
                    parent=parent,
                    **kwargs,
                )

            with patch("orchestrate.machine_bootstrap._commit_staged_no_replace", interfering_commit):
                with self.assertRaises(FileExistsError):
                    _atomic_write_owned(
                        target,
                        b"receipt",
                        executable=False,
                        expected_target=None,
                    )

            self.assertEqual(fault.interceptions, 1)
            self.assertEqual(target.read_bytes(), b"unrelated-owner-data")

    def test_atomic_commit_is_byte_exact_and_detects_newline_translation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exact = root / "exact"
            raw = b"first\nsecond\n"
            _atomic_write_owned(
                exact,
                raw,
                executable=False,
                expected_target=None,
            )
            self.assertEqual(exact.read_bytes(), raw)

            drifted = root / "drifted"
            real_write = os.write
            injected = False

            def translating_write(descriptor: int, data: object) -> int:
                nonlocal injected
                outgoing = bytes(data)
                if not injected:
                    injected = True
                    outgoing = outgoing.replace(b"\n", b"\r\n")
                return real_write(descriptor, outgoing)

            with patch("orchestrate.machine_bootstrap.os.write", translating_write):
                with self.assertRaises(OrchestrateError) as held:
                    _atomic_write_owned(
                        drifted,
                        raw,
                        executable=False,
                        expected_target=None,
                    )

            self.assertEqual(held.exception.code, "machine_bootstrap_commit_identity_changed")
            self.assertEqual(held.exception.data["component"], "commit.stagedSha256")
            self.assertFalse(drifted.exists())

    def test_forced_named_staging_branch_commits_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "named-stage"
            with patch(
                "orchestrate.machine_bootstrap._staging_strategy",
                return_value="named",
            ):
                _atomic_write_owned(
                    target,
                    b"forced-named\n",
                    executable=False,
                    expected_target=None,
                )
            self.assertEqual(target.read_bytes(), b"forced-named\n")

    def test_commit_verifier_rejects_wrong_operation_identity_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "committed-stage"
            raw = b"reviewed-bytes"
            target.write_bytes(raw)
            descriptor = os.open(target, os.O_RDWR)
            try:
                opened = os.fstat(descriptor)
                for field in ("st_dev", "st_ino"):
                    with self.subTest(field=field):
                        linked = stat_view(
                            opened,
                            **{field: getattr(opened, field) + 1},
                        )
                        with self.assertRaises(OrchestrateError) as held:
                            _verify_committed_stage(
                                descriptor,
                                linked,
                                raw,
                                path=target,
                                staging_operation={"operation": "test"},
                                archive_stages=None,
                            )
                        self.assertEqual(
                            held.exception.data["component"],
                            "commit.operationFileIdentity",
                        )
                        self.assertIn("device:", held.exception.data["expected"])
                        self.assertIn("inode:", held.exception.data["expected"])
                        self.assertIn("type:", held.exception.data["expected"])
                        self.assertNotEqual(
                            held.exception.data["expected"],
                            held.exception.data["observed"],
                        )
            finally:
                os.close(descriptor)

    def test_commit_verifier_rejects_wrong_type_and_reparse_separately(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "committed-stage"
            raw = b"reviewed-bytes"
            target.write_bytes(raw)
            descriptor = os.open(target, os.O_RDWR)
            try:
                opened = os.fstat(descriptor)
                cases = (
                    (
                        "wrong-type",
                        stat_view(opened, st_mode=stat.S_IFDIR | 0o700),
                        "unexpected-node",
                    ),
                    (
                        "reparse",
                        stat_view(
                            opened,
                            st_file_attributes=getattr(
                                stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
                            ),
                        ),
                        "redirected",
                    ),
                )
                for label, linked, observed in cases:
                    with self.subTest(label=label):
                        with self.assertRaises(OrchestrateError) as held:
                            _verify_committed_stage(
                                descriptor,
                                linked,
                                raw,
                                path=target,
                                staging_operation={"operation": "test"},
                                archive_stages=None,
                            )
                        self.assertEqual(
                            held.exception.data["component"],
                            "commit.nodeTypeAndReparse",
                        )
                        self.assertEqual(held.exception.data["observed"], observed)
            finally:
                os.close(descriptor)

    def test_commit_verifier_rejects_mode_size_change_token_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "committed-stage"
            raw = b"reviewed-bytes"
            target.write_bytes(raw)
            descriptor = os.open(target, os.O_RDWR)
            try:
                opened = os.fstat(descriptor)
                changed = stat_view(opened, st_ctime_ns=opened.st_ctime_ns + 1)
                with patch(
                    "orchestrate.machine_bootstrap.os.fstat",
                    side_effect=(opened, changed),
                ):
                    with self.assertRaises(OrchestrateError) as held:
                        _verify_committed_stage(
                            descriptor,
                            opened,
                            raw,
                            path=target,
                            staging_operation={"operation": "test"},
                            archive_stages=None,
                        )
                self.assertEqual(
                    held.exception.data["component"],
                    "commit.modeSizeChangeToken",
                )
                self.assertIn("mode:", held.exception.data["expected"])
                self.assertIn("size:", held.exception.data["expected"])
                self.assertIn("change-token:", held.exception.data["expected"])
            finally:
                os.close(descriptor)

    def test_commit_verifier_reports_content_only_for_different_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "committed-stage"
            raw = b"reviewed-bytes"
            target.write_bytes(b"x" * len(raw))
            descriptor = os.open(target, os.O_RDWR)
            try:
                linked = os.fstat(descriptor)
                with self.assertRaises(OrchestrateError) as held:
                    _verify_committed_stage(
                        descriptor,
                        linked,
                        raw,
                        path=target,
                        staging_operation={"operation": "test"},
                        archive_stages=None,
                    )
                self.assertEqual(
                    held.exception.data["component"],
                    "commit.committedSha256",
                )
                self.assertNotEqual(
                    held.exception.data["expected"],
                    held.exception.data["observed"],
                )
            finally:
                os.close(descriptor)

    @unittest.skipIf(sys.platform == "win32", "simulated provider uses descriptor-host APIs")
    def test_simulated_commit_accepts_path_descriptor_mode_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.machine_bootstrap._active_platform_provider",
            return_value=_SIMULATED_WIN32_PROVIDER,
        ), patch(
            "orchestrate.machine_bootstrap._staging_strategy",
            return_value="named",
        ):
            target = Path(directory) / "orchestrate.cmd"
            raw = b"@echo off\r\n"
            real_stat = os.stat
            fault = ResolvedPathFault(target)

            def path_sensitive_mode(
                candidate: object, *args: object, **kwargs: object
            ) -> os.stat_result:
                info = real_stat(candidate, *args, **kwargs)
                if (
                    kwargs.get("dir_fd") is not None
                    and fault.matches(candidate, relative_to=target.parent)  # type: ignore[arg-type]
                ):
                    return stat_view(info, st_mode=info.st_mode | 0o111)  # type: ignore[return-value]
                return info

            with patch("orchestrate.machine_bootstrap.os.stat", path_sensitive_mode):
                _atomic_write_owned(
                    target,
                    raw,
                    executable=False,
                    expected_target=None,
                )

            self.assertEqual(fault.interceptions, 1)
            self.assertEqual(target.read_bytes(), raw)

    @unittest.skipIf(sys.platform == "win32", "simulated provider uses descriptor-host APIs")
    def test_forced_named_staging_uses_provider_lifetime_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.machine_bootstrap._active_platform_provider",
            return_value=_SIMULATED_WIN32_PROVIDER,
        ), patch(
            "orchestrate.machine_bootstrap._staging_strategy",
            return_value="named",
        ), patch.object(
            _SIMULATED_WIN32_PROVIDER,
            "open_staging_file",
            wraps=_SIMULATED_WIN32_PROVIDER.open_staging_file,
        ) as opened:
            target = Path(directory) / "provider-named-stage"
            _atomic_write_owned(
                target,
                b"provider-owned-lifetime\n",
                executable=False,
                expected_target=None,
            )
            self.assertEqual(target.read_bytes(), b"provider-owned-lifetime\n")
            opened.assert_called_once()
            self.assertIs(opened.call_args.kwargs["delete_on_close"], False)

    @unittest.skipIf(sys.platform == "win32", "O_TMPFILE is a Linux test-provider branch")
    def test_forced_anonymous_staging_branch_commits_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "anonymous-stage"
            with patch(
                "orchestrate.machine_bootstrap._staging_strategy",
                return_value="anonymous",
            ):
                _atomic_write_owned(
                    target,
                    b"forced-anonymous\n",
                    executable=False,
                    expected_target=None,
                )
            self.assertEqual(target.read_bytes(), b"forced-anonymous\n")

    @unittest.skipIf(sys.platform == "win32", "exercises the POSIX O_TMPFILE fallback")
    def test_anonymous_commit_exdev_falls_back_with_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "install.json"
            raw = b"canonical\nreceipt\n"
            interceptions = 0

            def cross_device(*_: object) -> None:
                nonlocal interceptions
                interceptions += 1
                raise OSError(errno.EXDEV, "synthetic cross-device link")

            with patch(
                "orchestrate.machine_bootstrap._link_descriptor_no_replace",
                side_effect=cross_device,
            ):
                _atomic_write_owned(
                    target,
                    raw,
                    executable=False,
                    expected_target=None,
                )

            self.assertEqual(interceptions, 1)
            self.assertEqual(target.read_bytes(), raw)
            self.assertEqual(target.stat().st_nlink, 1)

    def test_commit_uses_retained_staged_bytes_after_name_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "installed-source.zip"
            displaced = root / "displaced-stage"
            raw = b"reviewed\narchive\n"
            interceptions = 0

            def substitute(staged: Path) -> None:
                nonlocal interceptions
                interceptions += 1
                try:
                    os.replace(staged, displaced)
                except (FileNotFoundError, PermissionError):
                    return
                staged.write_bytes(b"attacker\narchive\n")

            with patch("orchestrate.machine_bootstrap._before_staged_commit", substitute):
                _atomic_write_owned(
                    target,
                    raw,
                    executable=False,
                    expected_target=None,
                )

            self.assertEqual(interceptions, 1)
            self.assertEqual(target.read_bytes(), raw)

    def test_existing_target_is_refused_before_replace_can_displace_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "install.json"
            target.write_bytes(b"recognized-old-receipt")
            expected = _target_identity(target)
            fault = ResolvedPathFault(target)
            replace_calls: list[tuple[str, str]] = []
            real_replace = os.replace

            def racing_replace(source: str, destination: str) -> None:
                replace_calls.append((source, destination))
                if fault.matches(destination):
                    target.write_bytes(b"unrelated-owner-data")
                real_replace(source, destination)

            with patch("orchestrate.machine_bootstrap.os.replace", racing_replace):
                with self.assertRaises(OrchestrateError) as held:
                    _atomic_write_owned(
                        target,
                        b"new-receipt",
                        executable=False,
                        expected_target=expected,
                    )

            self.assertEqual(held.exception.code, "machine_bootstrap_target_identity_unproven")
            self.assertEqual(replace_calls, [])
            self.assertEqual(fault.interceptions, 0)
            self.assertEqual(target.read_bytes(), b"recognized-old-receipt")

    def test_receipt_replacement_is_bounded_on_the_opened_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            replacement = layout.install_root / "oversized-receipt"
            replacement.write_bytes(layout.install_receipt.read_bytes() + b" " * (8 * 1024 * 1024))
            fault = ResolvedPathFault(layout.install_receipt)
            def swapping_open(candidate: Path) -> None:
                if fault.matches(candidate):
                    os.replace(replacement, layout.install_receipt)

            with patch("orchestrate.machine_bootstrap._before_bounded_file_open", swapping_open):
                with _SourceBinding(layout) as source, _VenvBinding(layout) as binding:
                    self.assertFalse(_install_receipt_matches(layout, binding, source))

            self.assertEqual(fault.interceptions, 1)

    @unittest.skipIf(sys.platform == "win32", "uses a POSIX directory replacement analogue")
    def test_source_archive_is_bound_to_the_reviewed_source_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            replacement = root / "replacement-checkout"
            (replacement / "src" / "orchestrate").mkdir(parents=True)
            (replacement / "src" / "orchestrate" / "__init__.py").write_text(
                "replacement = True\n", encoding="utf-8"
            )
            (replacement / "pyproject.toml").write_text(
                "[project]\nname='replacement'\n", encoding="utf-8"
            )
            original = root / "reviewed-checkout"
            observed: list[str] = []

            def swapping_runner(
                argv: tuple[str, ...], **_: object
            ) -> subprocess.CompletedProcess[object]:
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
                    os.replace(layout.source_root, original)
                    layout.source_root.symlink_to(replacement, target_is_directory=True)
                    with zipfile.ZipFile(argv[-1]) as archive:
                        observed.append(archive.read("pyproject.toml").decode("utf-8"))
                    layout.command_path.write_bytes(b"fixture")
                return subprocess.CompletedProcess(argv, 0)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=swapping_runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_source_identity_changed")
            self.assertEqual(observed, ["[project]\nname='fixture'\n"])
            self.assertFalse(layout.install_receipt.exists())

    def test_source_change_and_receipt_tampering_cannot_skip_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            store = FakeUserPath(str(layout.bin_root))
            runner = SyntheticInstaller(layout)
            (layout.source_root / "pyproject.toml").write_text(
                "[project]\nname='changed'\n", encoding="utf-8"
            )

            with self.assertRaises(OrchestrateError) as changed:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )
            self.assertEqual(changed.exception.code, "machine_bootstrap_receipt_identity_unproven")
            self.assertEqual(runner.calls, [])

            install_ready_files(layout)
            receipt = json.loads(layout.install_receipt.read_text(encoding="utf-8"))
            receipt["sourceRoot"] = str(layout.source_root.parent / "laundered")
            layout.install_receipt.write_bytes(
                (
                    json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode("utf-8")
            )
            with self.assertRaises(OrchestrateError) as tampered:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )
            self.assertEqual(tampered.exception.code, "machine_bootstrap_receipt_identity_unproven")
            self.assertEqual(runner.calls, [])

    def test_recomputed_receipt_cannot_launder_a_replacement_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            store = FakeUserPath(str(layout.bin_root))
            old_command = layout.command_path.read_bytes()
            (layout.source_root / "pyproject.toml").write_text(
                "[project]\nname='replacement'\n", encoding="utf-8"
            )
            recorded = json.loads(layout.install_receipt.read_text(encoding="utf-8"))
            with (
                _SourceBinding(layout) as source,
                _ArchiveBinding(layout.source_archive) as archive,
                _VenvBinding(layout) as binding,
            ):
                forged = _receipt_value(
                    binding,
                    source,
                    archive,
                    installation_id=recorded["installationId"],
                )
            layout.install_receipt.write_bytes(
                (
                    json.dumps(forged, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode("utf-8")
            )

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=SyntheticInstaller(layout),
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_receipt_identity_unproven")
            self.assertTrue(held.exception.data["component"].startswith("anchor."))
            self.assertEqual(layout.command_path.read_bytes(), old_command)

    def test_install_consumes_the_pre_effect_source_archive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            store = FakeUserPath()
            runner = SyntheticInstaller(layout)
            original_call = runner.__call__
            source_file = layout.source_root / "pyproject.toml"
            original = source_file.read_bytes()
            observed: list[bytes] = []

            def transient_source(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[object]:
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
                    source_file.write_bytes(b"[project]\nname='transient'\n")
                    with zipfile.ZipFile(argv[-1]) as archive:
                        observed.append(archive.read("pyproject.toml"))
                    source_file.write_bytes(original)
                return original_call(argv, **kwargs)

            result = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=transient_source,
                platform="win32",
                version_info=(3, 13),
            )

            self.assertEqual(result.state, "repaired")
            self.assertEqual(observed, [original])
            self.assertEqual(
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                ).state,
                "ready",
            )

    @unittest.skipIf(sys.platform == "win32", "uses a POSIX replacement analogue")
    def test_post_commit_archive_replacement_is_never_installed_or_receipted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            runner = SyntheticInstaller(layout)
            real_builder = _build_source_archive
            attacker_archive = root / "attacker.zip"
            attacker_buffer = io.BytesIO()
            with zipfile.ZipFile(attacker_buffer, "w") as archive:
                archive.writestr("pyproject.toml", b"[project]\nname='attacker'\n")
            attacker_archive.write_bytes(attacker_buffer.getvalue())

            def replace_after_commit(
                selected_layout: MachineLayout,
                source: _SourceBinding,
                **kwargs: object,
            ) -> _ArchiveBinding:
                binding = real_builder(selected_layout, source, **kwargs)
                os.replace(attacker_archive, selected_layout.source_archive)
                return binding

            with patch(
                "orchestrate.machine_bootstrap._build_source_archive",
                side_effect=replace_after_commit,
            ):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=runner,
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(held.exception.code, "machine_bootstrap_source_identity_changed")
            self.assertEqual(held.exception.data["component"], "sourceArchive.publicIdentity")
            self.assertFalse(layout.install_receipt.exists())
            self.assertFalse(
                any(call[1:4] == ("-m", "pip", "install") and "--upgrade" not in call for call in runner.calls)
            )

    def test_retained_archive_writer_models_windows_share_admission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "stage.zip"
            descriptor = _SIMULATED_WIN32_PROVIDER.open_staging_file(
                target,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            try:
                with self.assertRaises(PermissionError):
                    _SIMULATED_WIN32_PROVIDER.open_path_pin(
                        target,
                        directory=False,
                    )
                bridge = _SIMULATED_WIN32_PROVIDER.open_path_pin(
                    target,
                    directory=False,
                    allow_write_share=True,
                )
                _SIMULATED_WIN32_PROVIDER.close_path_pin(bridge)
            finally:
                _SIMULATED_WIN32_PROVIDER.close_staging_file(descriptor)

    @unittest.skipIf(sys.platform == "win32", "simulated provider uses descriptor-host APIs")
    def test_simulated_named_stage_is_public_until_explicit_abandonment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root / "normal-stage"
            descriptor = _SIMULATED_WIN32_PROVIDER.open_staging_file(
                stage,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
                delete_on_close=False,
            )
            try:
                self.assertTrue(stat.S_ISREG(stage.lstat().st_mode))
            finally:
                _SIMULATED_WIN32_PROVIDER.close_staging_file(descriptor)

            _SIMULATED_WIN32_PROVIDER.unlink_staging_file(stage)
            self.assertFalse(stage.exists())

            delete_pending = root / "delete-pending-stage"
            descriptor = _SIMULATED_WIN32_PROVIDER.open_staging_file(
                delete_pending,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
                delete_on_close=True,
            )
            try:
                self.assertFalse(delete_pending.exists())
            finally:
                _SIMULATED_WIN32_PROVIDER.close_staging_file(descriptor)

    @unittest.skipUnless(sys.platform == "win32", "native Windows staging lifetime")
    def test_native_named_stage_abandonment_closes_before_unlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "orchestrate.cmd"

            def abandon(_: Path) -> None:
                raise RuntimeError("synthetic abandonment")

            with patch(
                "orchestrate.machine_bootstrap._staging_strategy",
                return_value="named",
            ), patch(
                "orchestrate.machine_bootstrap._before_staged_commit",
                side_effect=abandon,
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic abandonment"):
                    _atomic_write_owned(
                        target,
                        b"@echo off\r\n",
                        executable=False,
                        expected_target=None,
                    )

            self.assertFalse(target.exists())
            self.assertEqual(list(root.glob(".orchestrate.cmd.*.tmp")), [])

    @unittest.skipIf(sys.platform == "win32", "simulated provider uses descriptor-host APIs")
    def test_retained_writer_cleanup_hands_off_before_delete(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.machine_bootstrap._active_platform_provider",
            return_value=_SIMULATED_WIN32_PROVIDER,
        ):
            target = Path(directory) / "stage.zip"
            raw = b"retained-stage"
            retained = _atomic_write_owned(
                target,
                raw,
                executable=False,
                expected_target=None,
                retain_descriptor=True,
            )
            self.assertIsNotNone(retained)
            self.assertIsNotNone(retained.temporary_path)
            with self.assertRaises(PermissionError):
                _SIMULATED_WIN32_PROVIDER.unlink_staging_file(retained.temporary_path)
            with _ArchiveBinding(
                target,
                staged_descriptor=retained,
                expected_digest=hashlib.sha256(raw).hexdigest(),
                expected_size=len(raw),
            ):
                self.assertFalse(retained.temporary_path.exists())

    @unittest.skipIf(sys.platform == "win32", "simulated provider uses descriptor-host APIs")
    def test_simulated_share_admission_is_released_on_binding_exception(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "orchestrate.machine_bootstrap._active_platform_provider",
            return_value=_SIMULATED_WIN32_PROVIDER,
        ):
            root = Path(directory)
            target = root / "first.zip"
            retained = _atomic_write_owned(
                target,
                b"first",
                executable=False,
                expected_target=None,
                retain_descriptor=True,
            )
            with self.assertRaises(OrchestrateError):
                _ArchiveBinding(
                    target,
                    staged_descriptor=retained,
                    expected_digest=hashlib.sha256(b"different").hexdigest(),
                    expected_size=5,
                )
            reopened = os.open(target, os.O_RDONLY)
            if reopened != retained.descriptor:
                os.dup2(reopened, retained.descriptor)
                os.close(reopened)
                reopened = retained.descriptor
            reopened_identity = _target_identity(target)
            self.assertIsNotNone(reopened_identity)
            _SIMULATED_WIN32_PROVIDER._admit(
                reopened_identity,
                wants_write=False,
                shares_write=False,
            )
            os.close(reopened)
            self.assertEqual(_SIMULATED_WIN32_PROVIDER._shares, {})

            reused = _SIMULATED_WIN32_PROVIDER.open_staging_file(
                root / "second.zip",
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            _SIMULATED_WIN32_PROVIDER.close_staging_file(reused)

    def test_same_inode_archive_change_between_commit_and_binding_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            runner = SyntheticInstaller(layout)
            real_write = _atomic_write_owned
            changed = False
            fault = ResolvedPathFault(layout.source_archive)

            def change_after_commit(path: Path, raw: bytes, **kwargs: object):
                nonlocal changed
                retained = real_write(path, raw, **kwargs)
                if kwargs.get("retain_descriptor") and fault.matches(path):
                    changed = True
                    self.assertIsNotNone(retained)
                    os.lseek(retained.descriptor, 0, os.SEEK_SET)
                    os.write(retained.descriptor, b"not-the-reviewed-archive")
                    os.ftruncate(retained.descriptor, len(b"not-the-reviewed-archive"))
                    os.fsync(retained.descriptor)
                return retained

            with patch(
                "orchestrate.machine_bootstrap._atomic_write_owned",
                side_effect=change_after_commit,
            ):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=runner,
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(fault.interceptions, 1)
            self.assertTrue(changed)
            self.assertEqual(held.exception.code, "machine_bootstrap_source_identity_changed")
            self.assertEqual(
                held.exception.data["component"],
                "sourceArchive.stageToBindingSha256",
            )
            self.assertFalse(layout.install_receipt.exists())
            self.assertEqual(
                list(layout.install_root.glob(".installed-source.zip.*.tmp")),
                [],
            )

    def test_same_inode_archive_change_during_effect_is_detected_and_not_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            delegate = SyntheticInstaller(layout)
            original_call = delegate.__call__
            observed: list[bytes] = []
            write_denied = False

            def transient_archive(
                argv: tuple[str, ...], **kwargs: object
            ) -> subprocess.CompletedProcess[object]:
                nonlocal write_denied
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
                    original = layout.source_archive.read_bytes()
                    timestamps = layout.source_archive.stat()
                    attacker = io.BytesIO()
                    with zipfile.ZipFile(attacker, "w") as archive:
                        archive.writestr(
                            "pyproject.toml",
                            b"[project]\nname='attacker'\n",
                        )
                    try:
                        layout.source_archive.write_bytes(attacker.getvalue())
                    except PermissionError:
                        write_denied = True
                        return original_call(argv, **kwargs)
                    with zipfile.ZipFile(argv[-1]) as archive:
                        observed.append(archive.read("pyproject.toml"))
                    layout.source_archive.write_bytes(original)
                    os.utime(
                        layout.source_archive,
                        ns=(timestamps.st_atime_ns, timestamps.st_mtime_ns),
                    )
                return original_call(argv, **kwargs)

            if sys.platform == "win32":
                store = FakeUserPath()
                result = ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=transient_archive,
                    platform="win32",
                    version_info=(3, 13),
                )
                self.assertTrue(write_denied)
                self.assertEqual(observed, [])
                self.assertEqual(result.state, "repaired")
                self.assertTrue(layout.install_receipt.exists())
            else:
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=transient_archive,
                        platform="win32",
                        version_info=(3, 13),
                    )

                self.assertFalse(write_denied)
                self.assertEqual(observed, [b"[project]\nname='fixture'\n"])
                self.assertEqual(
                    held.exception.code,
                    "machine_bootstrap_source_identity_changed",
                )
                self.assertFalse(layout.install_receipt.exists())

    @unittest.skipIf(sys.platform == "win32", "non-native effect snapshot branch")
    def test_forced_named_effect_snapshot_rejects_change_and_restore(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            delegate = SyntheticInstaller(layout)
            original_call = delegate.__call__
            consumed: list[bytes] = []

            attacker = io.BytesIO()
            with zipfile.ZipFile(attacker, "w") as archive:
                archive.writestr(
                    "pyproject.toml",
                    b"[project]\nname='attacker'\n",
                )

            def mutate_effect(
                argv: tuple[str, ...], **kwargs: object
            ) -> subprocess.CompletedProcess[object]:
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
                    effect = Path(argv[-1])
                    original = effect.read_bytes()
                    os.chmod(effect, 0o600)
                    effect.write_bytes(attacker.getvalue())
                    with zipfile.ZipFile(effect) as archive:
                        consumed.append(archive.read("pyproject.toml"))
                    effect.write_bytes(original)
                    os.chmod(effect, 0o400)
                return original_call(argv, **kwargs)

            with patch(
                "orchestrate.machine_bootstrap._effect_snapshot_strategy",
                return_value="named",
            ):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=mutate_effect,
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(consumed, [b"[project]\nname='attacker'\n"])
            self.assertEqual(held.exception.code, "machine_bootstrap_source_identity_changed")
            self.assertEqual(
                held.exception.data["component"],
                "sourceArchive.pinToEffectSha256",
            )
            self.assertFalse(layout.install_receipt.exists())

    @unittest.skipIf(sys.platform == "win32", "non-native effect snapshot branch")
    @unittest.skipUnless(hasattr(os, "memfd_create"), "sealed memfd is unavailable")
    def test_forced_memfd_effect_snapshot_installs_reviewed_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            runner = SyntheticInstaller(layout)
            with patch(
                "orchestrate.machine_bootstrap._effect_snapshot_strategy",
                return_value="memfd",
            ):
                result = ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: str(layout.windows_shim),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )
            self.assertEqual(result.state, "repaired")
            self.assertTrue(layout.install_receipt.is_file())

    def test_archive_rejects_bytes_not_causally_bound_to_reviewed_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            source_file = layout.source_root / "pyproject.toml"
            original = source_file.read_bytes()
            target_reads = 0

            with _SourceBinding(layout) as source:
                def laundering_read(path: Path, limit: int):
                    nonlocal target_reads
                    if path.name == "pyproject.toml":
                        target_reads += 1
                        if target_reads == 2:
                            source_file.write_bytes(b"[project]\nname='transient'\n")
                            try:
                                return _read_bounded_regular_file(path, limit)
                            finally:
                                source_file.write_bytes(original)
                    return _read_bounded_regular_file(path, limit)

                with patch(
                    "orchestrate.machine_bootstrap._read_bounded_regular_file",
                    laundering_read,
                ):
                    with self.assertRaises(OrchestrateError) as held:
                        _build_source_archive(layout, source)

            self.assertEqual(held.exception.code, "machine_bootstrap_source_identity_changed")
            self.assertEqual(held.exception.data["component"], "sourceArchive.inputTreeSha256")
            self.assertEqual(source_file.read_bytes(), original)
            self.assertFalse(layout.source_archive.exists())

    @unittest.skipIf(sys.platform == "win32", "POSIX analogue for parent-relative venv creation")
    def test_venv_creation_does_not_follow_a_late_redirect(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            original = root / "bound-venv"
            unrelated = root / "unrelated"
            unrelated.mkdir()

            def redirecting_runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[object]:
                if argv[1:3] == ("-m", "venv"):
                    os.replace(layout.venv_root, original)
                    layout.venv_root.symlink_to(unrelated, target_is_directory=True)
                    effect_root = Path(argv[-1])
                    (effect_root / "Scripts").mkdir()
                    (effect_root / "Scripts" / "python.exe").write_bytes(b"fixture")
                    (effect_root / "pyvenv.cfg").write_bytes(
                        b"home = C:\\Python313\nversion = 3.13.9\n"
                    )
                return subprocess.CompletedProcess(argv, 0)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=redirecting_runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
            self.assertEqual(list(unrelated.iterdir()), [])
            self.assertFalse((original / "Scripts" / "python.exe").exists())

    def test_venv_creation_refuses_descendant_redirect_before_external_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            unrelated = root / "unrelated"
            unrelated.mkdir()

            def redirecting_runner(
                argv: tuple[str, ...], **_: object
            ) -> subprocess.CompletedProcess[object]:
                if argv[1:3] == ("-m", "venv"):
                    layout.scripts_root.symlink_to(unrelated, target_is_directory=True)
                    (layout.scripts_root / "python.exe").write_bytes(b"outside")
                return subprocess.CompletedProcess(argv, 0)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=redirecting_runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
            self.assertEqual(held.exception.data["component"], "venvDescendant.deniedRedirect")
            self.assertEqual(list(unrelated.iterdir()), [])

    def test_venv_creation_refuses_nested_pip_redirect_before_external_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            unrelated = root / "unrelated"
            unrelated.mkdir()
            pip_root = layout.venv_root / "Lib" / "site-packages" / "pip"

            def redirecting_runner(
                argv: tuple[str, ...], **_: object
            ) -> subprocess.CompletedProcess[object]:
                if argv[1:3] == ("-m", "venv"):
                    pip_root.symlink_to(unrelated, target_is_directory=True)
                    (pip_root / "escaped.py").write_bytes(b"outside")
                return subprocess.CompletedProcess(argv, 0)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=redirecting_runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
            self.assertEqual(held.exception.data["component"], "venvDescendant.deniedRedirect")
            self.assertEqual(list(unrelated.iterdir()), [])

    def test_venv_creation_refuses_redirects_at_randomized_arbitrary_depths(self) -> None:
        generator = random.Random(761338)
        for iteration in range(3):
            with self.subTest(iteration=iteration), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                layout = fixture_layout(root)
                unrelated = root / "unrelated"
                unrelated.mkdir()
                depth = generator.randint(3, 9)
                relative = Path("Lib") / "site-packages" / "pip"
                for index in range(depth):
                    relative /= f"layer-{iteration}-{index}"
                redirect = layout.venv_root / relative

                def redirecting_runner(
                    argv: tuple[str, ...], **_: object
                ) -> subprocess.CompletedProcess[object]:
                    if argv[1:3] == ("-m", "venv"):
                        redirect.parent.mkdir(parents=True, exist_ok=True)
                        redirect.symlink_to(unrelated, target_is_directory=True)
                        (redirect / "escaped.py").write_bytes(b"outside")
                    return subprocess.CompletedProcess(argv, 0)

                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=redirecting_runner,
                        platform="win32",
                        version_info=(3, 13),
                    )

                self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
                self.assertEqual(held.exception.data["component"], "venvTree.deniedRedirect")
                self.assertEqual(list(unrelated.iterdir()), [])

    @unittest.skipIf(sys.platform == "win32", "uses a POSIX child-process redirect analogue")
    def test_later_child_effect_redirect_is_rejected_before_the_next_phase(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            outside = root / "outside"
            outside.mkdir()
            delegate = SyntheticInstaller(layout)
            original_call = delegate.__call__
            child_ran = False

            def child_effect(
                argv: tuple[str, ...], **kwargs: object
            ) -> subprocess.CompletedProcess[object]:
                nonlocal child_ran
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" in argv:
                    child_ran = True
                    script = (
                        "import os, pathlib; "
                        f"outside=pathlib.Path({os.fspath(outside)!r}); "
                        "redirect=pathlib.Path('Lib/site-packages/late-redirect'); "
                        "redirect.symlink_to(outside, target_is_directory=True); "
                        "(redirect/'escaped.txt').write_text('outside', encoding='utf-8')"
                    )
                    subprocess.run(
                        (sys.executable, "-c", script),
                        check=True,
                        cwd=kwargs["cwd"],
                    )
                return original_call(argv, **kwargs)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=child_effect,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertTrue(child_ran)
            self.assertEqual(held.exception.code, "machine_bootstrap_venv_identity_changed")
            self.assertEqual(held.exception.data["component"], "venvTree.nodeType")
            self.assertTrue((outside / "escaped.txt").is_file())
            self.assertFalse(layout.source_archive.exists())
            self.assertFalse(layout.install_receipt.exists())

    def test_ancestry_creation_never_follows_a_replaced_earlier_component(self) -> None:
        generator = random.Random(771339)
        for iteration in range(3):
            with self.subTest(iteration=iteration), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                retained = root / "retained"
                retained.mkdir()
                outside = root / "outside"
                outside.mkdir()
                components = [f"level-{iteration}-{index}" for index in range(generator.randint(3, 8))]
                target = retained.joinpath(*components)
                displaced = root / f"displaced-{iteration}"
                intercepted = False

                def replace_ancestor(parent: Path, child: Path) -> None:
                    nonlocal intercepted
                    if intercepted or child.name != components[1]:
                        return
                    intercepted = True
                    os.replace(retained, displaced)
                    retained.symlink_to(outside, target_is_directory=True)

                with patch(
                    "orchestrate.machine_bootstrap._before_directory_component_create",
                    side_effect=replace_ancestor,
                ):
                    with self.assertRaises(OrchestrateError):
                        _ensure_install_parent(target)

                self.assertTrue(intercepted)
                self.assertEqual(list(outside.iterdir()), [])

    @unittest.skipIf(sys.platform == "win32", "native no-delete handles deny root rename")
    def test_repair_keeps_one_retained_root_across_owned_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            runner = SyntheticInstaller(layout)
            retained = root / "retained-install"
            substitute = layout.install_root
            intercepted = False
            fault = ResolvedPathFault(layout.install_root)

            def replace_root(parent: Path) -> None:
                nonlocal intercepted
                if intercepted or not fault.matches(parent):
                    return
                intercepted = True
                os.replace(layout.install_root, retained)
                substitute.mkdir()

            with patch(
                "orchestrate.machine_bootstrap._before_staging_parent_open",
                side_effect=replace_root,
            ):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=runner,
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(fault.interceptions, 1)
            self.assertTrue(intercepted)
            self.assertEqual(held.exception.code, "machine_bootstrap_install_identity_changed")
            self.assertFalse((substitute / layout.source_archive.name).exists())

    @unittest.skipIf(sys.platform == "win32", "native no-delete handles deny root rename")
    def test_bin_creation_refuses_a_replaced_retained_root_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            runner = SyntheticInstaller(layout)
            retained = root / "retained-install"
            substitute = layout.install_root
            intercepted = False
            fault = ResolvedPathFault(layout.bin_root)

            def replace_before_bin(
                path: Path,
                *,
                code: str,
                root_binding: object | None = None,
            ) -> None:
                nonlocal intercepted
                if not intercepted and fault.matches(path):
                    intercepted = True
                    os.replace(layout.install_root, retained)
                    substitute.mkdir()
                if root_binding is None:
                    _ensure_directory(path, code=code)
                else:
                    _ensure_directory(
                        path,
                        code=code,
                        root_binding=root_binding,  # type: ignore[arg-type]
                    )

            with patch(
                "orchestrate.machine_bootstrap._ensure_directory",
                side_effect=replace_before_bin,
            ):
                with self.assertRaises(OrchestrateError) as held:
                    ensure_machine(
                        layout=layout,
                        path_store=FakeUserPath(),
                        resolver=lambda *_: None,
                        runner=runner,
                        platform="win32",
                        version_info=(3, 13),
                    )

            self.assertEqual(fault.interceptions, 1)
            self.assertTrue(intercepted)
            self.assertEqual(list(substitute.iterdir()), [])
            self.assertFalse((retained / "bin").exists())
            self.assertEqual(held.exception.code, "machine_bootstrap_install_identity_changed")

    def test_redirected_install_ancestry_is_refused_before_any_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            unrelated = root / "unrelated"
            unrelated.mkdir()
            layout.install_root.parent.symlink_to(unrelated, target_is_directory=True)
            runner = SyntheticInstaller(layout)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=FakeUserPath(),
                    resolver=lambda *_: None,
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_install_identity_unproven")
            self.assertEqual(held.exception.data["component"], "installNode.nodeType")
            self.assertEqual(runner.calls, [])
            self.assertEqual(list(unrelated.iterdir()), [])

    def test_receipt_wrapper_preserves_anchor_identity_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            store = FakeUserPath()
            runner = SyntheticInstaller(layout)
            original_call = runner.__call__
            unrelated = root / "unrelated-anchor"
            unrelated.mkdir()

            def redirect_anchor_before_receipt(
                argv: tuple[str, ...], **kwargs: object
            ) -> subprocess.CompletedProcess[object]:
                completed = original_call(argv, **kwargs)
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
                    layout.install_anchor.parent.symlink_to(
                        unrelated, target_is_directory=True
                    )
                return completed

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=redirect_anchor_before_receipt,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_receipt_write_failed")
            self.assertEqual(held.exception.data["receiptWriteCause"], "machine_bootstrap_install_identity_unproven")
            self.assertEqual(held.exception.data["component"], "installNode.nodeType")
            self.assertEqual(held.exception.data["expected"], "directory")
            self.assertEqual(held.exception.data["observed"], "redirected")
            self.assertIn('"component":"installNode.nodeType"', str(held.exception))

    def test_redacted_string_identities_are_not_content_digests(self) -> None:
        error = OrchestrateError(
            "synthetic receipt failure",
            code="machine_bootstrap_receipt_write_failed",
            data={
                "component": "receipt-and-anchor.write",
                "expected": "both-canonical-records",
                "observed": "FileNotFoundError",
            },
        )
        self.assertRegex(
            error.data["expected"],
            r"\Aredacted-string-id:sha256:[0-9a-f]{64}\Z",
        )
        self.assertRegex(
            error.data["observed"],
            r"\Aredacted-string-id:sha256:[0-9a-f]{64}\Z",
        )

        digest = "sha256:" + "a" * 64
        content_error = OrchestrateError(
            "synthetic content mismatch",
            code="machine_bootstrap_commit_identity_changed",
            data={
                "component": "commit.stagedSha256",
                "expected": digest,
                "observed": digest,
            },
        )
        self.assertEqual(content_error.data["expected"], digest)
        self.assertEqual(content_error.data["observed"], digest)

    @unittest.skipIf(sys.platform == "win32", "native Windows uses an ancestor junction probe")
    def test_redirected_anchor_ancestor_cannot_authorize_a_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = fixture_layout(root)
            install_ready_files(layout)
            store = FakeUserPath(str(layout.bin_root))
            runner = SyntheticInstaller(layout)
            retained = root / "retained-anchor-state"
            attacker = root / "attacker-anchor-state"
            os.replace(layout.install_anchor.parent, retained)
            attacker.mkdir()
            (attacker / layout.install_anchor.name).write_bytes(
                (retained / layout.install_anchor.name).read_bytes()
            )
            layout.install_anchor.parent.symlink_to(attacker, target_is_directory=True)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_install_identity_unproven")
            self.assertEqual(held.exception.data["component"], "installNode.nodeType")
            self.assertEqual(runner.calls, [])

    def test_named_local_venv_directories_are_excluded_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            local_venv = layout.source_root / ".venv-audit2"
            local_venv.mkdir()
            (local_venv / "lib64").symlink_to("lib")
            (layout.source_root / "cached.pyc").write_bytes(b"generated")
            (layout.source_root / "legacy.pyo").write_bytes(b"generated")
            install_ready_files(layout)
            store = FakeUserPath(str(layout.bin_root))
            self.assertTrue(machine_ready(layout, store, resolver=resolving(layout, store)))
            with zipfile.ZipFile(layout.source_archive) as archive:
                self.assertNotIn("cached.pyc", archive.namelist())
                self.assertNotIn("legacy.pyo", archive.namelist())

    @unittest.skipIf(sys.platform == "win32", "uses a POSIX directory-symlink analogue")
    def test_staging_refuses_a_substituted_parent_before_creating_a_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parent = root / "bin"
            parent.mkdir()
            original = root / "original-bin"
            unrelated = root / "unrelated"
            unrelated.mkdir()
            target = parent / "orchestrate.cmd"
            fault = ResolvedPathFault(parent)

            def substitute(candidate: Path) -> None:
                if fault.matches(candidate):
                    os.replace(parent, original)
                    parent.symlink_to(unrelated, target_is_directory=True)

            with patch("orchestrate.machine_bootstrap._before_staging_parent_open", substitute):
                with self.assertRaises(OrchestrateError) as held:
                    _atomic_write_owned(
                        target,
                        b"launcher",
                        executable=False,
                        expected_target=None,
                    )

            self.assertEqual(fault.interceptions, 1)
            self.assertEqual(held.exception.code, "machine_bootstrap_parent_identity_changed")
            self.assertEqual(list(unrelated.iterdir()), [])
            self.assertEqual(list(original.iterdir()), [])

    def test_machine_bootstrap_serializes_concurrent_first_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            store = FakeUserPath()
            runner = SyntheticInstaller(layout)
            first_effect_started = threading.Event()
            release_first_effect = threading.Event()
            second_fast_check = threading.Event()
            original_call = runner.__call__
            original_read = store.read
            results: list[MachineBootstrapResult] = []
            errors: list[BaseException] = []

            def gated_runner(argv: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[object]:
                if argv[1:3] == ("-m", "venv"):
                    first_effect_started.set()
                    if not release_first_effect.wait(LIVENESS_TIMEOUT):
                        raise AssertionError("concurrency fixture did not release the first effect")
                return original_call(argv, **kwargs)

            def observed_read() -> UserPathValue:
                value = original_read()
                if store.reads >= 3:
                    second_fast_check.set()
                return value

            store.read = observed_read  # type: ignore[method-assign]

            def invoke() -> None:
                try:
                    results.append(
                        ensure_machine(
                            layout=layout,
                            path_store=store,
                            resolver=resolving(layout, store),
                            runner=gated_runner,
                            platform="win32",
                            version_info=(3, 13),
                        )
                    )
                except BaseException as exc:
                    errors.append(exc)

            first = threading.Thread(target=invoke, daemon=True)
            first.start()
            self.assertTrue(first_effect_started.wait(LIVENESS_TIMEOUT), "first invocation reached no effect")
            second = threading.Thread(target=invoke, daemon=True)
            second.start()
            self.assertTrue(second_fast_check.wait(LIVENESS_TIMEOUT), "second invocation reached no fast check")
            release_first_effect.set()
            first.join(LIVENESS_TIMEOUT)
            second.join(LIVENESS_TIMEOUT)

            self.assertFalse(first.is_alive() or second.is_alive(), "concurrent bootstrap did not settle")
            self.assertEqual(errors, [])
            self.assertEqual(sorted(result.state for result in results), ["ready", "repaired"])
            self.assertEqual(len(runner.calls), 4)
            self.assertEqual(len(store.writes), 1)

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

    def test_healthy_fast_path_requires_the_dedicated_path_entry_first(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            foreign = r"C:\foreign"
            store = FakeUserPath(f"{foreign};{layout.bin_root}")
            runner = SyntheticInstaller(layout)

            self.assertFalse(machine_ready(layout, store, resolver=resolving(layout, store)))
            result = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=runner,
                platform="win32",
                version_info=(3, 13),
            )

            self.assertEqual(result.actions, ("registered_user_path",))
            self.assertEqual(store.value.split(";"), [str(layout.bin_root), foreign])
            self.assertEqual(runner.calls, [])

    def test_registry_path_repair_preserves_reg_sz_kind_and_unrelated_text(self) -> None:
        winreg = ModuleType("winreg")
        winreg.HKEY_CURRENT_USER = object()  # type: ignore[attr-defined]
        winreg.REG_SZ = 1  # type: ignore[attr-defined]
        winreg.REG_EXPAND_SZ = 2  # type: ignore[attr-defined]
        winreg.KEY_QUERY_VALUE = 1  # type: ignore[attr-defined]
        winreg.KEY_SET_VALUE = 2  # type: ignore[attr-defined]
        state: dict[str, object] = {
            "value": r";%UNCHANGED%;C:\odd spelling\;",
            "kind": winreg.REG_SZ,  # type: ignore[attr-defined]
        }

        class Key:
            def __enter__(self) -> Key:
                return self

            def __exit__(self, *_: object) -> None:
                return None

        winreg.OpenKey = lambda *_: Key()  # type: ignore[attr-defined]
        winreg.CreateKeyEx = lambda *_: Key()  # type: ignore[attr-defined]
        winreg.QueryValueEx = lambda *_: (state["value"], state["kind"])  # type: ignore[attr-defined]

        entry = Path(r"C:\orchestrate\bin")

        def transactional_replace(expected: UserPathValue, value: str) -> None:
            self.assertEqual(UserPathValue(state["value"], state["kind"]), expected)
            state["kind"] = expected.kind
            state["value"] = value

        with patch.dict(sys.modules, {"winreg": winreg}), patch(
            "orchestrate.machine_bootstrap.sys.platform", "win32"
        ), patch("ctypes.windll", create=True):
            self.assertTrue(
                register_user_path(
                    RegistryUserPathStore(transactional_replace=transactional_replace),
                    entry,
                )
            )

        self.assertEqual(state["kind"], winreg.REG_SZ)  # type: ignore[attr-defined]
        self.assertEqual(state["value"], f"{entry};;%UNCHANGED%;C:\\odd spelling\\;")

    def test_path_compare_and_replace_refuses_a_concurrent_unrelated_change(self) -> None:
        store = FakeUserPath(r"C:\\one", kind=1)

        def concurrent_change() -> None:
            store.before_replace = None
            store.value += r";C:\\new-unrelated"

        store.before_replace = concurrent_change
        with self.assertRaises(OrchestrateError) as held:
            register_user_path(store, Path(r"C:\\orchestrate\\bin"))

        self.assertEqual(held.exception.code, "machine_bootstrap_path_changed")
        self.assertEqual(store.value, r"C:\\one;C:\\new-unrelated")
        self.assertEqual(store.writes, [])

    def test_registry_transaction_interference_preserves_unrelated_bytes(self) -> None:
        winreg = ModuleType("winreg")
        winreg.HKEY_CURRENT_USER = object()  # type: ignore[attr-defined]
        winreg.REG_SZ = 1  # type: ignore[attr-defined]
        winreg.REG_EXPAND_SZ = 2  # type: ignore[attr-defined]
        state = {"value": r"C:\one", "kind": 1}

        class Key:
            def __enter__(self) -> Key:
                return self

            def __exit__(self, *_: object) -> None:
                return None

        winreg.OpenKey = lambda *_: Key()  # type: ignore[attr-defined]
        winreg.QueryValueEx = lambda *_: (state["value"], state["kind"])  # type: ignore[attr-defined]

        def conflicted(_: UserPathValue, __: str) -> None:
            state["value"] += r";C:\external"
            raise OrchestrateError("synthetic transaction conflict", code="machine_bootstrap_path_changed")

        store = RegistryUserPathStore(transactional_replace=conflicted)
        with patch.dict(sys.modules, {"winreg": winreg}), patch(
            "orchestrate.machine_bootstrap.sys.platform", "win32"
        ):
            with self.assertRaises(OrchestrateError) as held:
                register_user_path(store, Path(r"C:\orchestrate\bin"))

        self.assertEqual(held.exception.code, "machine_bootstrap_path_changed")
        self.assertEqual(state["value"], r"C:\one;C:\external")

    def test_existing_environment_repairs_launchers_without_reinstall(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            layout.windows_shim.unlink()
            layout.wsl_shim.unlink()
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
            self.assertNotIn("installed_reviewed_source", result.actions)
            self.assertTrue(machine_ready(layout, store, resolver=resolving(layout, store)))

    def test_failed_install_is_explicit_and_rerunnable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
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

    def test_failed_source_install_cannot_be_mistaken_for_a_completed_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            layout.scripts_root.mkdir(parents=True)
            write_pyvenv_config(layout)
            (layout.scripts_root / "python.exe").write_bytes(b"fixture")
            store = FakeUserPath()

            def partial_runner(argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[object]:
                if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
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
            with self.assertRaises(OrchestrateError) as blocked:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )
            self.assertEqual(blocked.exception.code, "machine_bootstrap_command_identity_unproven")
            layout.command_path.unlink()
            layout.source_archive.unlink()
            result = ensure_machine(
                layout=layout,
                path_store=store,
                resolver=resolving(layout, store),
                runner=runner,
                platform="win32",
                version_info=(3, 13),
            )
            self.assertEqual(result.state, "repaired")
            self.assertTrue(
                any(
                    call[1:4] == ("-m", "pip", "install") and "--upgrade" not in call
                    for call in runner.calls
                )
            )
            self.assertTrue(layout.install_receipt.exists())

    def test_fast_path_rejects_a_poisoned_installed_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            layout.command_path.write_bytes(b"poisoned-command")
            store = FakeUserPath(str(layout.bin_root))

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=SyntheticInstaller(layout),
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_receipt_identity_unproven")
            self.assertEqual(layout.command_path.read_bytes(), b"poisoned-command")

    def test_unrecognized_launcher_is_refused_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            layout.windows_shim.write_bytes(b"unrelated-owner-data")
            store = FakeUserPath()

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=SyntheticInstaller(layout),
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_shim_identity_unproven")
            self.assertEqual(layout.windows_shim.read_bytes(), b"unrelated-owner-data")

    def test_duplicate_key_install_receipt_cannot_skip_reinstallation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            layout = fixture_layout(Path(directory))
            install_ready_files(layout)
            layout.install_receipt.write_bytes(
                b'{"schema":"orchestrate-machine-install/v2",'
                b'"sourceRoot":"wrong","sourceRoot":"also-wrong"}\n'
            )
            store = FakeUserPath(str(layout.bin_root))
            runner = SyntheticInstaller(layout)

            with self.assertRaises(OrchestrateError) as held:
                ensure_machine(
                    layout=layout,
                    path_store=store,
                    resolver=resolving(layout, store),
                    runner=runner,
                    platform="win32",
                    version_info=(3, 13),
                )

            self.assertEqual(held.exception.code, "machine_bootstrap_receipt_identity_unproven")
            self.assertEqual(runner.calls, [])

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
            install_ready_files(layout)
            layout.wsl_shim.unlink()
            store = FakeUserPath()
            fault = ResolvedPathFault(layout.wsl_shim)
            real_commit = _commit_staged_no_replace

            def link(
                temporary_name: str,
                target_name: str,
                *,
                parent_fd: int | None,
                parent: Path,
                **kwargs: object,
            ) -> None:
                if fault.matches(target_name, relative_to=parent):
                    raise PermissionError("synthetic launcher denial")
                real_commit(
                    temporary_name,
                    target_name,
                    parent_fd=parent_fd,
                    parent=parent,
                    **kwargs,
                )

            with patch("orchestrate.machine_bootstrap._commit_staged_no_replace", link):
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
