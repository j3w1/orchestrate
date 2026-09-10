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
    def __init__(
        self,
        root: Path,
        packet: dict[str, object],
        *,
        current_worker_shape: bool = False,
        include_host_platform: bool = True,
        host_platform: object = "win32",
        execution_host_id: object = "local",
    ) -> None:
        self.root = root
        self.packet = packet
        self.current_worker_shape = current_worker_shape
        self.include_host_platform = include_host_platform
        self.host_platform = host_platform
        self.execution_host_id = execution_host_id
        self.calls: list[tuple[str, ...]] = []

    @staticmethod
    def _wrap(result: dict[str, object]) -> dict[str, object]:
        return {"result": result, "_meta": {"runtimeId": "runtime_test"}}

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        worktree_id = f"repo::{self.root.resolve()}"
        launch = self.packet["admission"]["launch"]  # type: ignore[index]
        if arguments[:2] == ("terminal", "show"):
            terminal = {
                "handle": "term_worker", "worktreeId": worktree_id,
                "worktreePath": str(self.root.resolve()), "executionHostId": self.execution_host_id,
                "agentIdentity": "codex",
                "connected": True, "writable": True,
            }
            if self.include_host_platform:
                terminal["hostPlatform"] = self.host_platform
            return self._wrap({"terminal": terminal})
        if arguments[:2] == ("worktree", "current"):
            return self._wrap({"worktree": {"id": worktree_id, "path": str(self.root.resolve())}})
        if arguments[:2] == ("orchestration", "worker-show"):
            dispatch = {"id": "dispatch_1", "status": "dispatched"}
            worker = {
                "state": "ready", "stage": "input_accepted",
                "startOptions": {
                    "resolvedWorktreeId": worktree_id, "agent": "codex",
                    "launch": {"requested": launch, "effective": launch},
                },
            }
            if self.current_worker_shape:
                dispatch.update(runId="run_1", taskId="task_1", task_id="task_1", lastFailure=None)
                worker.update(
                    dispatchId="dispatch_1", worktreeId=worktree_id,
                    agentTerminalHandle="term_worker", lastError=None,
                )
            else:
                dispatch.update(run_id="run_1", task_id="task_1", last_failure=None)
                worker.update(
                    worktree_id=worktree_id,
                    agent_terminal_handle="term_worker",
                    last_error=None,
                )
            return self._wrap({
                "dispatch": dispatch,
                "worker": worker,
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

    def test_orca_1_4_199_worker_show_identity_is_admitted_explicitly(self) -> None:
        report = worker_preflight(
            self.root,
            run_id="run_1",
            task_id="task_1",
            dispatch_id="dispatch_1",
            packet_id=str(self.packet["packetId"]),
            client=PreflightClient(self.root, self.packet, current_worker_shape=True),  # type: ignore[arg-type]
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
            platform="win32",
        )
        self.assertEqual(report["status"], "admitted")

    def test_orca_1_4_199_local_terminal_without_host_platform_is_admitted_on_native_windows(self) -> None:
        report = worker_preflight(
            self.root,
            run_id="run_1",
            task_id="task_1",
            dispatch_id="dispatch_1",
            packet_id=str(self.packet["packetId"]),
            client=PreflightClient(
                self.root,
                self.packet,
                current_worker_shape=True,
                include_host_platform=False,
            ),  # type: ignore[arg-type]
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
            platform="win32",
        )
        native = report["observation"]["native"]  # type: ignore[index]
        self.assertEqual(native["controllerPlatform"], "win32")
        self.assertIsNone(native["terminalHostPlatform"])
        self.assertEqual(native["hostPlatformEvidence"], "native-controller-and-local-execution-host")

    def test_terminal_reported_non_windows_platform_is_rejected_precisely(self) -> None:
        client = PreflightClient(self.root, self.packet, host_platform="linux")
        with self.assertRaises(OrchestrateError) as rejected:
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                packet_id=str(self.packet["packetId"]),
                client=client,  # type: ignore[arg-type]
                environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                platform="win32",
            )
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "preflight_identity_conflict")  # type: ignore[index]
        mismatch = rejected.exception.data["mismatches"][0]  # type: ignore[index]
        self.assertEqual(mismatch["details"]["fields"][0]["field"], "terminal.hostPlatform")

    def test_terminal_without_execution_host_identity_is_rejected_as_ambiguous(self) -> None:
        client = PreflightClient(
            self.root,
            self.packet,
            include_host_platform=False,
            execution_host_id=None,
        )
        with self.assertRaises(OrchestrateError) as rejected:
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                packet_id=str(self.packet["packetId"]),
                client=client,  # type: ignore[arg-type]
                environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                platform="win32",
            )
        mismatch = rejected.exception.data["mismatches"][0]  # type: ignore[index]
        self.assertEqual(mismatch["details"]["fields"][0]["field"], "terminal.executionHostId")

    def test_terminal_on_a_different_execution_host_is_rejected_even_if_it_reports_windows(self) -> None:
        client = PreflightClient(self.root, self.packet, execution_host_id="connected-host")
        with self.assertRaises(OrchestrateError) as rejected:
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                packet_id=str(self.packet["packetId"]),
                client=client,  # type: ignore[arg-type]
                environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                platform="win32",
            )
        self.assertEqual(rejected.exception.data["cause"], "preflight_identity_conflict")  # type: ignore[index]

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

    def test_clean_committed_ce_query_drift_rejects_before_any_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git(root, "init", "-q")
            git(root, "config", "user.email", "fixture@example.invalid")
            git(root, "config", "user.name", "Fixture")
            (root / "docs" / "tasks").mkdir(parents=True)
            (root / "docs" / "context").mkdir(parents=True)
            (root / "docs" / "project-log").mkdir(parents=True)
            (root / "scripts" / "quality").mkdir(parents=True)
            (root / "AGENTS.md").write_text("Bound CE instruction.\n", encoding="utf-8")
            (root / "docs" / "tasks" / "README.md").write_text(
                "| [CE-1234](CE-1234.md) | active |\n",
                encoding="utf-8",
            )
            (root / "docs" / "tasks" / "CE-1234.md").write_text(
                "Context packet - Must read: [packet](../context/CE-1234.md)\n",
                encoding="utf-8",
            )
            (root / "docs" / "context" / "CE-1234.md").write_text("Bound context.\n", encoding="utf-8")
            (root / "package.json").write_text(
                json.dumps({"scripts": {"project-log:query": "node scripts/quality/project-log.mjs query"}}),
                encoding="utf-8",
            )
            query_script = root / "scripts" / "quality" / "project-log.mjs"
            query_script.write_text("// benign packet-bound query fixture\n", encoding="utf-8")
            shard = {
                "sequence": 1,
                "path": "docs/project-log/shard-001.md",
                "state": "closed",
                "bytes": 8,
                "sha256": "1" * 64,
                "git_blob_sha": "1" * 40,
                "entry_count": 1,
                "first_heading": "Synthetic",
                "last_heading": "Synthetic",
                "task_ids": ["CE-1234"],
                "legacy_start_byte": 0,
                "legacy_end_byte_exclusive": 8,
            }
            (root / "docs" / "project-log" / "shard-001.md").write_text("# Entry\n", encoding="utf-8")
            (root / "docs" / "project-log" / "manifest.json").write_text(
                json.dumps({
                    "schema_version": 1,
                    "kind": "ce-systems-project-log-manifest",
                    "agent_access": {
                        "default_loading": "manifest-only",
                        "closed_shards_preloaded": False,
                        "query_command": "pnpm run project-log:query -- <term>",
                    },
                    "shards": [shard],
                }),
                encoding="utf-8",
            )
            git(root, "add", ".")
            git(root, "commit", "-qm", "CE fixture")
            profile = setup_project(root)
            git(root, "add", ".orchestrate.json")
            git(root, "commit", "-qm", "select fixture profile")
            query_result = {
                "term": "CE-1234",
                "exact_task": "CE-1234",
                "selected_shards": ["docs/project-log/shard-001.md"],
                "matches": [{
                    "path": "docs/project-log/shard-001.md",
                    "sequence": 1,
                    "heading": "Synthetic",
                    "text": "sanitized synthetic match",
                }],
                "omitted_matches": 0,
            }
            with patch("orchestrate.readers._run_ce_query", return_value=query_result):
                reader = read_project(profile, "Implement CE-1234")
            sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
            with StateStore(root) as store:
                run = store.create_run(
                    objective="Implement CE-1234",
                    profile_digest=profile.digest,
                    source_digest=sources.digest,
                )
                run = store.update_run(
                    run.local_id,
                    native_run_id="run_1",
                    task_id="task_1",
                    phase="awaiting_preflight",
                )
                packet = make_packet(
                    objective=run.objective,
                    profile=profile,
                    sources=sources,
                    launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                    python_executable=str(Path(sys.executable).resolve()),
                    run_id="run_1",
                    reader=reader,
                )
                store.save_packet(run.local_id, "task_1", canonical_packet_json(packet))

            query_script.write_text("// changed clean query implementation\n", encoding="utf-8")
            git(root, "add", "scripts/quality/project-log.mjs")
            git(root, "commit", "-qm", "change query implementation")
            client = PreflightClient(root, packet)
            with patch("subprocess.run", side_effect=AssertionError("no subprocess is allowed")) as called:
                with self.assertRaises(OrchestrateError) as rejected:
                    worker_preflight(
                        root,
                        run_id="run_1",
                        task_id="task_1",
                        dispatch_id="dispatch_1",
                        packet_id=str(packet["packetId"]),
                        client=client,
                        environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                        platform="win32",
                    )
            self.assertEqual(rejected.exception.code, "preflight_rejected")
            self.assertEqual(rejected.exception.data["cause"], "source_binding_changed")  # type: ignore[index]
            self.assertEqual(
                rejected.exception.data["mismatches"][0]["code"],  # type: ignore[index]
                "source_binding_changed",
            )
            called.assert_not_called()
            self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
