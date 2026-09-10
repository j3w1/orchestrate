from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.admission import worker_preflight
from orchestrate.errors import OrchestrateError
from orchestrate.packets import canonical_packet_json, make_packet, packet_spec
from orchestrate.profile import setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from orchestrate.state import StateStore


def git(root: Path, *arguments: str) -> None:
    subprocess.run(("git", "-C", str(root), *arguments), check=True, capture_output=True)


class PreflightClient:
    def __init__(self, root: Path, packet: dict[str, object]) -> None:
        self.root = root
        self.packet = packet
        self.calls: list[tuple[str, ...]] = []

    @staticmethod
    def _wrap(result: dict[str, object]) -> dict[str, object]:
        return {"result": result, "_meta": {"runtimeId": "runtime_test"}}

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        worktree_id = f"repo::{self.root.resolve()}"
        launch = self.packet["admission"]["launch"]  # type: ignore[index]
        if arguments[:2] == ("terminal", "show"):
            return self._wrap({"terminal": {
                "handle": "term_worker", "worktreeId": worktree_id,
                "worktreePath": str(self.root.resolve()), "executionHostId": "local",
                "hostPlatform": "win32", "agentIdentity": "codex",
                "connected": True, "writable": True,
            }})
        if arguments[:2] == ("worktree", "current"):
            return self._wrap({"worktree": {"id": worktree_id, "path": str(self.root.resolve())}})
        if arguments[:2] == ("orchestration", "worker-show"):
            return self._wrap({
                "dispatch": {"id": "dispatch_1", "run_id": "run_1", "task_id": "task_1", "status": "dispatched"},
                "worker": {
                    "state": "ready", "stage": "input_accepted", "worktree_id": worktree_id,
                    "agent_terminal_handle": "term_worker",
                    "startOptions": {
                        "resolvedWorktreeId": worktree_id, "agent": "codex",
                        "launch": {"requested": launch, "effective": launch},
                    },
                },
            })
        if arguments[:2] == ("orchestration", "task-list"):
            return self._wrap({"tasks": [{
                "id": "task_1", "run_id": "run_1", "status": "dispatched", "spec": packet_spec(self.packet),
            }]})
        raise AssertionError(arguments)


class AdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.project = tempfile.TemporaryDirectory()
        self.state = tempfile.TemporaryDirectory()
        self.root = Path(self.project.name)
        self.environment = patch.dict(os.environ, {"ORCHESTRATE_HOME": self.state.name})
        self.environment.start()
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Bound instruction.\n", encoding="utf-8")
        (self.root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")
        self.profile = setup_project(self.root)
        reader = read_project(self.profile, "Implement bounded change")
        sources = build_source_index(self.profile, extra_sources=set(reader.consulted_paths))
        with StateStore(self.root) as store:
            run = store.create_run(objective="Implement bounded change", profile_digest=self.profile.digest, source_digest=sources.digest)
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1", phase="awaiting_preflight")
            self.local_id = run.local_id
            self.packet = make_packet(
                objective=run.objective,
                profile=self.profile,
                sources=sources,
                launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                python_executable=str(Path(sys.executable).resolve()),
                run_id="run_1",
                reader=reader,
            )
            store.save_packet(run.local_id, "task_1", canonical_packet_json(self.packet))

    def tearDown(self) -> None:
        self.environment.stop()
        self.project.cleanup()
        self.state.cleanup()

    def _run(self) -> dict[str, object]:
        return worker_preflight(
            self.root,
            run_id="run_1", task_id="task_1", dispatch_id="dispatch_1",
            packet_id=str(self.packet["packetId"]), client=PreflightClient(self.root, self.packet),
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"}, platform="win32",  # type: ignore[arg-type]
        )

    def test_exact_preflight_is_immutable_and_replay_is_not_a_fresh_grant(self) -> None:
        report = self._run()
        self.assertEqual(report["status"], "admitted")
        self.assertEqual(report["editingGrant"], "fresh")
        with StateStore(self.root) as store:
            observed = store.get_preflight(self.local_id, "task_1", "dispatch_1")
        self.assertEqual(observed["outcome"], "passed")  # type: ignore[index]
        with self.assertRaises(OrchestrateError) as replay:
            self._run()
        self.assertEqual(replay.exception.code, "preflight_already_passed")

    def test_rejected_then_restored_sources_never_become_admitted(self) -> None:
        original = (self.root / "AGENTS.md").read_bytes()
        (self.root / "AGENTS.md").write_text("Drifted.\n", encoding="utf-8")
        with self.assertRaises(OrchestrateError) as rejected:
            self._run()
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        (self.root / "AGENTS.md").write_bytes(original)
        with self.assertRaises(OrchestrateError) as restored:
            self._run()
        self.assertEqual(restored.exception.code, "preflight_already_rejected")
        with StateStore(self.root) as store:
            observed = store.get_preflight(self.local_id, "task_1", "dispatch_1")
        self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]

    def test_wrong_packet_identity_records_rejection_without_native_calls(self) -> None:
        client = PreflightClient(self.root, self.packet)
        with self.assertRaises(OrchestrateError) as rejected:
            worker_preflight(
                self.root,
                run_id="run_1", task_id="task_1", dispatch_id="dispatch_1", packet_id="packet_sha256_wrong",
                client=client, environment={"ORCA_TERMINAL_HANDLE": "term_worker"}, platform="win32",  # type: ignore[arg-type]
            )
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
