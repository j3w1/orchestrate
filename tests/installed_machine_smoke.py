"""Exercise installed-command machine convergence without host PATH mutation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

from orchestrate.machine_bootstrap import (
    MachineLayout,
    UserPathValue,
    default_layout,
    ensure_machine,
)


class MemoryUserPath:
    def __init__(self, value: str) -> None:
        self.value = value
        self.writes = 0

    def read(self) -> UserPathValue:
        return UserPathValue(self.value, 2)

    def replace(self, expected: UserPathValue, value: str) -> None:
        if self.read() != expected:
            raise RuntimeError("synthetic PATH changed")
        self.value = value
        self.writes += 1


class SyntheticInstaller:
    def __init__(self, layout: MachineLayout) -> None:
        self.layout = layout
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, argv: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[object]:
        self.calls.append(argv)
        if argv[1:3] == ("-m", "venv"):
            self.layout.scripts_root.mkdir(parents=True, exist_ok=True)
            (self.layout.venv_root / "pyvenv.cfg").write_bytes(
                b"home = C:\\Python313\ninclude-system-site-packages = false\nversion = 3.13.0\n"
            )
            (self.layout.scripts_root / "python.exe").write_bytes(b"fixture-python")
        if argv[1:4] == ("-m", "pip", "install") and "--upgrade" not in argv:
            self.layout.command_path.write_bytes(b"fixture-command")
        return subprocess.CompletedProcess(argv, 0)


def _resolver(layout: MachineLayout, store: MemoryUserPath):
    def resolve(command: str, path: str) -> str | None:
        if command != "orchestrate" or path != store.value:
            return None
        return os.fspath(layout.windows_shim) if layout.windows_shim.is_file() else None

    return resolve


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--command", type=Path, required=True)
    parser.add_argument("--provider", choices=("posix", "simulated-win32"), default="simulated-win32")
    parser.add_argument("--skip-cli", action="store_true")
    args = parser.parse_args()
    source_root = args.source_root.resolve(strict=True)
    command = args.command.resolve(strict=True)

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        local_app_data = root / "local"
        project = root / "project"
        project.mkdir()
        (project / "README.md").write_text("# disposable installed-wheel smoke\n", encoding="utf-8")
        subprocess.run(("git", "init", "-q"), cwd=project, check=True)
        subprocess.run(("git", "add", "README.md"), cwd=project, check=True)
        subprocess.run(
            (
                "git",
                "-c",
                "user.name=orchestrate smoke",
                "-c",
                "user.email=orchestrate-smoke@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ),
            cwd=project,
            check=True,
        )
        environment = {"LOCALAPPDATA": os.fspath(local_app_data)}
        os.environ["LOCALAPPDATA"] = os.fspath(local_app_data)
        if args.provider == "simulated-win32":
            os.environ["ORCHESTRATE_BOOTSTRAP_PROVIDER"] = "simulated-win32"
        else:
            os.environ.pop("ORCHESTRATE_BOOTSTRAP_PROVIDER", None)
        layout = default_layout(environment=environment, source_root=source_root)
        store = MemoryUserPath(r"C:\existing")
        runner = SyntheticInstaller(layout)
        first = ensure_machine(
            layout=layout,
            path_store=store,
            resolver=_resolver(layout, store),
            runner=runner,
            platform="win32",
            version_info=(3, 13),
        )
        if first.state != "repaired" or len(runner.calls) != 4 or store.writes != 1:
            raise RuntimeError("disposable first repair did not complete exactly once")

        call_count = len(runner.calls)
        write_count = store.writes
        second = ensure_machine(
            path_store=store,
            resolver=_resolver(layout, store),
            runner=runner,
            platform="win32",
            version_info=(3, 13),
        )
        if (
            second.state != "ready"
            or len(runner.calls) != call_count
            or store.writes != write_count
        ):
            raise RuntimeError("installed default layout did not take the write-free ready path")
        if args.skip_cli:
            return 0

        protected = (
            layout.install_receipt,
            layout.install_anchor,
            layout.source_archive,
            layout.command_path,
            layout.windows_shim,
            layout.wsl_shim,
        )
        before = {os.fspath(path): _digest(path) for path in protected}
        child_environment = dict(os.environ)
        child_environment.update(
            {
                "LOCALAPPDATA": os.fspath(local_app_data),
                "ORCHESTRATE_BOOTSTRAP_PROVIDER": args.provider,
                "ORCHESTRATE_BOOTSTRAP_SIMULATED_USER_PATH": os.fspath(layout.bin_root),
                "ORCHESTRATE_HOME": os.fspath(root / "state"),
            }
        )
        completed = subprocess.run(
            (os.fspath(command), "setup", "--project", os.fspath(project), "--json"),
            check=False,
            capture_output=True,
            text=True,
            env=child_environment,
            cwd=project,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"installed setup failed ({completed.returncode}): {completed.stdout} {completed.stderr}"
            )
        report = json.loads(completed.stdout)
        if report.get("machineBootstrap") != {"actions": [], "state": "ready"}:
            raise RuntimeError(f"installed setup did not use the ready path: {report!r}")
        if not (project / ".orchestrate.json").is_file():
            raise RuntimeError("installed setup did not continue to project setup")
        after = {os.fspath(path): _digest(path) for path in protected}
        if after != before:
            raise RuntimeError("installed ready path rewrote machine state")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
