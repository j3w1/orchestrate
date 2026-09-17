from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
import errno
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from orchestrate.admission import (
    joined_preflight_status,
    other_dispatch_observations,
    validate_packet_sources,
    worker_preflight,
)
from orchestrate.errors import OrchestrateError
from orchestrate.packets import canonical_packet_json, expected_packet_id, make_packet, packet_spec
from orchestrate.profile import INSTRUCTION_PATHSPECS, ProjectProfile, setup_project
from orchestrate.readers import read_project
from orchestrate.safeio import read_project_bytes
from orchestrate.sources import build_source_index
from orchestrate.state import AdmissionEffectFence, RunRecord, StateStore
from tests.path_faults import ResolvedPathFault


LIVENESS_TIMEOUT = 120.0  # Outer deadlock detector, not a semantic progress budget.
requires_native_windows_admission = unittest.skipUnless(
    sys.platform in {"linux", "win32"},
    "managed worker admission requires native Windows or Linux",
)


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
        dispatch_last_failure: object = None,
        worker_last_error: object = None,
    ) -> None:
        self.root = root
        self.packet = packet
        self.current_worker_shape = current_worker_shape
        self.include_host_platform = include_host_platform
        self.host_platform = host_platform
        self.execution_host_id = execution_host_id
        self.dispatch_last_failure = dispatch_last_failure
        self.worker_last_error = worker_last_error
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
                dispatch.update(
                    runId="run_1",
                    taskId="task_1",
                    task_id="task_1",
                    lastFailure=self.dispatch_last_failure,
                )
                worker.update(
                    dispatchId="dispatch_1", worktreeId=worktree_id,
                    agentTerminalHandle="term_worker", lastError=self.worker_last_error,
                )
            else:
                dispatch.update(
                    run_id="run_1",
                    task_id="task_1",
                    last_failure=self.dispatch_last_failure,
                )
                worker.update(
                    worktree_id=worktree_id,
                    agent_terminal_handle="term_worker",
                    last_error=self.worker_last_error,
                )
            return self._wrap({
                "dispatch": dispatch,
                "worker": worker,
                "terminalResource": {
                    "id": "terminal-resource-1",
                    "ownershipState": "owned",
                    "releaseState": "not_requested",
                    "retainedReason": None,
                    "originDispatchId": "dispatch_1",
                    "ownerDispatchId": "dispatch_1",
                    "terminalHandle": "term_worker",
                    "worktreeId": worktree_id,
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
            store.record_worker_resource_binding(
                run.local_id,
                dispatch_id="dispatch_1",
                resource_id="terminal-resource-1",
                terminal_handle="term_worker",
                worktree_id=f"repo::{self.root.resolve()}",
                readback={"fixture": "validated-start-readback"},
            )

    def tearDown(self) -> None:
        self.environment.stop()
        self.project.cleanup()
        self.state.cleanup()

    def _run(self, client: PreflightClient | None = None) -> dict[str, object]:
        return worker_preflight(
            self.root,
            run_id="run_1", task_id="task_1", dispatch_id="dispatch_1",
            packet_id=str(self.packet["packetId"]), client=client or PreflightClient(self.root, self.packet),
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"}, platform="win32",  # type: ignore[arg-type]
        )

    def _prepare_ce_preflight(
        self,
        root: Path,
        *,
        executable_query: bool = False,
    ) -> tuple[ProjectProfile, RunRecord, dict[str, object], dict[str, object]]:
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
        (root / "scripts" / "quality" / "project-log.mjs").write_text(
            "// benign packet-bound query fixture\n",
            encoding="utf-8",
        )
        if executable_query:
            (root / "scripts" / "quality" / "project-log.mjs").chmod(0o755)
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
        query_result: dict[str, object] = {
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
            store.record_worker_resource_binding(
                run.local_id,
                dispatch_id="dispatch_1",
                resource_id="terminal-resource-1",
                terminal_handle="term_worker",
                worktree_id=f"repo::{root.resolve()}",
                readback={"fixture": "validated-start-readback"},
            )
        return profile, run, packet, query_result

    def _assert_query_source_node_substitution_is_definitive(self, node_type: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            query_script = root / "scripts" / "quality" / "project-log.mjs"
            saved_script = query_script.with_name("project-log.mjs-bound-regular")
            real_open = os.open
            real_run = subprocess.run
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }
            bound_socket: socket.socket | None = None
            substituted = False
            substitution_exists = False
            substitution_mode: int | None = None
            armed = False

            def make_substitution() -> None:
                nonlocal bound_socket, substituted, substitution_exists, substitution_mode
                if substituted:
                    return
                query_script.rename(saved_script)
                if node_type == "directory":
                    query_script.mkdir()
                elif node_type == "symlink":
                    query_script.symlink_to(saved_script.name)
                elif node_type == "socket":
                    bound_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    bound_socket.bind(os.fspath(query_script))
                elif node_type == "fifo":
                    os.mkfifo(query_script)
                else:  # pragma: no cover - test helper contract
                    raise AssertionError(node_type)
                substituted = True
                substitution_exists = query_script.exists()
                substitution_mode = query_script.lstat().st_mode

            def arm_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                nonlocal armed
                armed = True
                if sys.platform == "win32":
                    make_substitution()
                return native

            def substitute_at_bound_open(
                path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                if armed and not substituted and path == query_script.name and dir_fd is not None:
                    make_substitution()
                return real_open(path, flags, mode, dir_fd=dir_fd)

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            caught: list[BaseException] = []
            finished = threading.Event()

            def invoke_preflight() -> None:
                try:
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
                except BaseException as exc:
                    caught.append(exc)
                finally:
                    finished.set()

            completed_without_rescue = True
            try:
                with (
                    patch(
                        "orchestrate.admission._native_identity",
                        side_effect=arm_after_completed_stage,
                    ),
                    patch("orchestrate.safeio.os.open", new=substitute_at_bound_open),
                    patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
                ):
                    if node_type == "fifo":
                        worker = threading.Thread(target=invoke_preflight, daemon=True)
                        worker.start()
                        completed_without_rescue = finished.wait(LIVENESS_TIMEOUT)
                        if not completed_without_rescue:
                            # Release a regressed blocking FIFO open so the
                            # daemon can settle before this test reports why.
                            writer = os.open(query_script, os.O_WRONLY | os.O_NONBLOCK)
                            os.close(writer)
                            worker.join(timeout=LIVENESS_TIMEOUT)
                    else:
                        invoke_preflight()
                self.assertTrue(finished.is_set(), "worker preflight did not settle after FIFO-open rescue")
                self.assertTrue(completed_without_rescue, "FIFO substitution blocked project-source admission")
                self.assertEqual(len(caught), 1)
                self.assertIsInstance(caught[0], OrchestrateError)
                rejected = caught[0]
                assert isinstance(rejected, OrchestrateError)
                with StateStore(root) as store:
                    observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                    attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            finally:
                if bound_socket is not None:
                    bound_socket.close()
                if substituted:
                    if query_script.is_dir() and not query_script.is_symlink():
                        query_script.rmdir()
                    else:
                        query_script.unlink()
                if saved_script.exists():
                    saved_script.rename(query_script)

            expected_type = {
                "directory": stat.S_ISDIR,
                "symlink": stat.S_ISLNK,
                "socket": stat.S_ISSOCK,
                "fifo": stat.S_ISFIFO,
            }[node_type]
            self.assertTrue(substitution_exists)
            self.assertIsNotNone(substitution_mode)
            self.assertTrue(expected_type(substitution_mode))  # type: ignore[arg-type]
            self.assertEqual(client.calls, [])

            restored_code: str | None = None
            restored_grant: object = None
            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                try:
                    restored = worker_preflight(
                        root,
                        run_id="run_1",
                        task_id="task_1",
                        dispatch_id="dispatch_1",
                        packet_id=str(packet["packetId"]),
                        client=PreflightClient(root, packet),
                        environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                        platform="win32",
                    )
                    restored_grant = restored.get("editingGrant")
                except OrchestrateError as restored_error:
                    restored_code = restored_error.code

            self.assertEqual(
                (
                    rejected.code,
                    observed.get("outcome") if observed is not None else None,
                    attempts[0]["disposition"],
                    restored_code,
                    restored_grant,
                ),
                (
                    "preflight_rejected",
                    "rejected",
                    "definitive",
                    "preflight_already_rejected",
                    None,
                ),
            )

    def test_bound_query_source_directory_substitution_is_definitive(self) -> None:
        self._assert_query_source_node_substitution_is_definitive("directory")

    def test_bound_query_source_symlink_substitution_is_definitive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            link = Path(directory) / "link"
            target.write_bytes(b"target")
            try:
                link.symlink_to(target.name)
            except OSError as exc:
                self.skipTest(f"file symlink unavailable: {exc}")
            self.assertTrue(link.exists())
        self._assert_query_source_node_substitution_is_definitive("symlink")

    @unittest.skipIf(os.name == "nt", "requires a POSIX AF_UNIX pathname socket")
    def test_bound_query_source_socket_substitution_is_definitive(self) -> None:
        self._assert_query_source_node_substitution_is_definitive("socket")

    @unittest.skipIf(os.name == "nt", "requires a POSIX FIFO")
    def test_bound_query_source_fifo_substitution_is_definitive_without_blocking(self) -> None:
        self._assert_query_source_node_substitution_is_definitive("fifo")

    @requires_native_windows_admission
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

    def _preflight(self, dispatch_id: str, client: PreflightClient) -> dict[str, object]:
        return worker_preflight(
            self.root,
            run_id="run_1", task_id="task_1", dispatch_id=dispatch_id,
            packet_id=str(self.packet["packetId"]), client=client,  # type: ignore[arg-type]
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"}, platform="win32",
        )

    def _stored_rows(self) -> list[tuple[str, str, str]]:
        with StateStore(self.root) as store:
            rows = store.connection.execute(
                """SELECT dispatch_id, outcome, observation_json FROM preflight_observations
                   WHERE run_local_id = ? ORDER BY dispatch_id""",
                (self.local_id,),
            ).fetchall()
        return [(row["dispatch_id"], row["outcome"], row["observation_json"]) for row in rows]

    @requires_native_windows_admission
    def test_second_dispatch_preflight_for_same_task_is_rejected_without_native_calls(self) -> None:
        self.assertEqual(self._run()["status"], "admitted")
        before = self._stored_rows()
        client = PreflightClient(self.root, self.packet)
        with self.assertRaises(OrchestrateError) as rejected:
            self._preflight("dispatch_2", client)
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "preflight_dispatch_conflict")  # type: ignore[index]
        self.assertEqual(client.calls, [])
        after = self._stored_rows()
        self.assertEqual(after[0], before[0])
        self.assertEqual([(row[0], row[1]) for row in after], [("dispatch_1", "passed"), ("dispatch_2", "rejected")])
        rejected_row = json.loads(after[1][2])
        self.assertEqual(rejected_row["mismatches"][0]["details"]["otherObservations"], [
            {"dispatchId": "dispatch_1", "outcome": "passed", "observedAt": json.loads(before[0][2])["observedAt"]},
        ])
        with self.assertRaises(OrchestrateError) as replay:
            self._preflight("dispatch_2", PreflightClient(self.root, self.packet))
        self.assertEqual(replay.exception.code, "preflight_already_rejected")
        self.assertEqual(self._stored_rows(), after)

    @requires_native_windows_admission
    def test_two_concurrent_public_preflights_cannot_both_receive_fresh_grants(self) -> None:
        first_inside_fence = threading.Event()
        release_first = threading.Event()
        first_exited_fence = threading.Event()
        second_at_fence = threading.Event()
        first_thread: dict[str, int | None] = {"identity": None}
        second_thread: dict[str, int | None] = {"identity": None}

        class BlockingFirstClient(PreflightClient):
            def run_json(self, *arguments: str, **kwargs: object) -> dict[str, object]:
                if arguments[:2] == ("terminal", "show"):
                    first_inside_fence.set()
                    if not release_first.wait(LIVENESS_TIMEOUT):
                        raise AssertionError("first preflight was not released")
                return super().run_json(*arguments, **kwargs)

        original_enter = AdmissionEffectFence.__enter__
        original_exit = AdmissionEffectFence.__exit__

        def observed_enter(fence: AdmissionEffectFence) -> AdmissionEffectFence:
            if threading.get_ident() == second_thread["identity"]:
                fence.timeout_seconds = LIVENESS_TIMEOUT
                second_at_fence.set()
            return original_enter(fence)  # type: ignore[return-value]

        def observed_exit(
            fence: AdmissionEffectFence,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: object,
        ) -> None:
            original_exit(fence, exc_type, exc, traceback)  # type: ignore[arg-type]
            if threading.get_ident() == first_thread["identity"]:
                first_exited_fence.set()

        first_client = BlockingFirstClient(self.root, self.packet)
        second_client = PreflightClient(self.root, self.packet)

        def first_preflight() -> dict[str, object]:
            first_thread["identity"] = threading.get_ident()
            return self._preflight("dispatch_1", first_client)

        def second_preflight() -> dict[str, object]:
            second_thread["identity"] = threading.get_ident()
            return self._preflight("dispatch_2", second_client)

        with patch.object(AdmissionEffectFence, "__enter__", observed_enter), patch.object(
            AdmissionEffectFence,
            "__exit__",
            observed_exit,
        ), ThreadPoolExecutor(
            max_workers=2,
        ) as executor:
            first = executor.submit(first_preflight)
            self.assertTrue(first_inside_fence.wait(LIVENESS_TIMEOUT))
            second = executor.submit(second_preflight)
            self.assertTrue(second_at_fence.wait(LIVENESS_TIMEOUT))
            self.assertFalse(second.done())
            self.assertEqual(self._stored_rows(), [])
            # The waiting preflight loaded the earlier unbound Run, then the
            # controller established its Dispatch while the fence was held.
            # Its rejection must use the refreshed post-fence Run binding.
            with StateStore(self.root) as store:
                store.update_run(self.local_id, dispatch_id="dispatch_1")
            release_first.set()

            self.assertTrue(first_exited_fence.wait(LIVENESS_TIMEOUT))
            self.assertEqual(first.result(timeout=LIVENESS_TIMEOUT)["editingGrant"], "fresh")
            with self.assertRaises(OrchestrateError) as rejected:
                second.result(timeout=LIVENESS_TIMEOUT)

        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "preflight_identity_conflict")  # type: ignore[index]
        self.assertEqual(second_client.calls, [])
        rows = self._stored_rows()
        self.assertEqual([(row[0], row[1]) for row in rows], [("dispatch_1", "passed"), ("dispatch_2", "rejected")])

    @requires_native_windows_admission
    def test_join_treats_other_dispatch_observation_as_conflicting_not_absent(self) -> None:
        self.assertEqual(self._run()["status"], "admitted")
        with StateStore(self.root) as store:
            unbound = store.get_run(self.local_id)
            self.assertIsNone(unbound.dispatch_id)
            self.assertEqual(joined_preflight_status(store, unbound), "pending")
            self.assertEqual(
                other_dispatch_observations(store, self.local_id, "task_1", "dispatch_2"),
                [{"dispatchId": "dispatch_1", "outcome": "passed",
                  "observedAt": store.get_preflight(self.local_id, "task_1", "dispatch_1")["observedAt"]}],  # type: ignore[index]
            )
            self.assertEqual(other_dispatch_observations(store, self.local_id, "task_1", "dispatch_1"), [])
            split = store.update_run(self.local_id, dispatch_id="dispatch_2")
            self.assertEqual(joined_preflight_status(store, split), "conflicting")
            self.assertIsNone(store.get_preflight(self.local_id, "task_1", "dispatch_2"))
            # Absent observation for the bound Dispatch with no other rows stays pending.
            self.assertEqual(joined_preflight_status(store, store.update_run(self.local_id, task_id="task_2")), "pending")

    @requires_native_windows_admission
    def test_join_with_multiple_observations_is_conflicting_even_for_the_passed_dispatch(self) -> None:
        self.assertEqual(self._run()["status"], "admitted")
        with self.assertRaises(OrchestrateError):
            self._preflight("dispatch_2", PreflightClient(self.root, self.packet))
        with StateStore(self.root) as store:
            bound = store.update_run(self.local_id, dispatch_id="dispatch_1")
            self.assertEqual(joined_preflight_status(store, bound), "conflicting")
            self.assertEqual(
                [(item["dispatchId"], item["outcome"]) for item in store.list_preflights(self.local_id, "task_1")],
                [("dispatch_1", "passed"), ("dispatch_2", "rejected")],
            )

    @requires_native_windows_admission
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

    @unittest.skipUnless(sys.platform == "linux", "requires native Linux")
    def test_linux_worker_show_identity_is_admitted_explicitly(self) -> None:
        report = worker_preflight(
            self.root,
            run_id="run_1",
            task_id="task_1",
            dispatch_id="dispatch_1",
            packet_id=str(self.packet["packetId"]),
            client=PreflightClient(self.root, self.packet, host_platform="linux"),  # type: ignore[arg-type]
            environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
            platform="linux",
        )
        native = report["observation"]["native"]  # type: ignore[index]
        self.assertEqual(report["status"], "admitted")
        self.assertEqual(native["controllerPlatform"], "linux")
        self.assertEqual(native["terminalHostPlatform"], "linux")
        self.assertEqual(native["hostPlatformEvidence"], "terminal-show")

    @unittest.skipUnless(sys.platform == "linux", "requires native Linux")
    def test_wsl_worker_preflight_is_rejected_before_native_readback(self) -> None:
        client = PreflightClient(self.root, self.packet, host_platform="linux")
        with self.assertRaises(OrchestrateError) as rejected:
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                packet_id=str(self.packet["packetId"]),
                client=client,  # type: ignore[arg-type]
                environment={
                    "ORCA_TERMINAL_HANDLE": "term_worker",
                    "WSL_DISTRO_NAME": "fixture",
                },
                platform="linux",
            )
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "preflight_host_unsupported")  # type: ignore[index]
        self.assertEqual(client.calls, [])

    @requires_native_windows_admission
    def test_unprovable_interpreter_identity_never_falls_back_to_spelling(self) -> None:
        with patch(
            "orchestrate.admission.os.path.samefile",
            side_effect=PermissionError("fixture identity unavailable"),
        ), self.assertRaises(OrchestrateError) as rejected:
            self._run()
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "preflight_host_unsupported")  # type: ignore[index]

    @requires_native_windows_admission
    def test_interpreter_identity_is_not_case_folded_before_samefile(self) -> None:
        real_normcase = os.path.normcase

        def reject_interpreter_case_fold(path: str) -> str:
            if os.path.abspath(path) == os.path.abspath(sys.executable):
                raise AssertionError("interpreter identity must preserve exact path spelling")
            return real_normcase(path)

        with patch(
            "orchestrate.admission.os.path.normcase",
            side_effect=reject_interpreter_case_fold,
        ):
            report = self._run()
        self.assertEqual(report["status"], "admitted")

    @requires_native_windows_admission
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

    def _assert_ready_contradiction_rejected(
        self,
        *,
        current_worker_shape: bool,
        field: str,
    ) -> None:
        options = {field: "contradiction"}
        client = PreflightClient(
            self.root,
            self.packet,
            current_worker_shape=current_worker_shape,
            **options,
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
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "preflight_identity_conflict")  # type: ignore[index]

    @requires_native_windows_admission
    def test_orca_1_4_198_preflight_rejects_ready_dispatch_failure(self) -> None:
        self._assert_ready_contradiction_rejected(
            current_worker_shape=False,
            field="dispatch_last_failure",
        )

    @requires_native_windows_admission
    def test_orca_1_4_198_preflight_rejects_ready_worker_error(self) -> None:
        self._assert_ready_contradiction_rejected(
            current_worker_shape=False,
            field="worker_last_error",
        )

    @requires_native_windows_admission
    def test_orca_1_4_199_preflight_rejects_ready_dispatch_failure(self) -> None:
        self._assert_ready_contradiction_rejected(
            current_worker_shape=True,
            field="dispatch_last_failure",
        )

    @requires_native_windows_admission
    def test_orca_1_4_199_preflight_rejects_ready_worker_error(self) -> None:
        self._assert_ready_contradiction_rejected(
            current_worker_shape=True,
            field="worker_last_error",
        )

    @requires_native_windows_admission
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

    @requires_native_windows_admission
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

    @requires_native_windows_admission
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
        client = PreflightClient(self.root, self.packet)
        with patch("subprocess.run", wraps=subprocess.run) as subprocess_calls:
            with self.assertRaises(OrchestrateError) as rejected:
                self._run(client)
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "source_binding_changed")  # type: ignore[index]
        self.assertEqual(len(subprocess_calls.call_args_list), 1)
        actual_git_argv = subprocess_calls.call_args.args[0]
        self.assertEqual(
            actual_git_argv[:2],
            ("git", "-C"),
        )
        self.assertEqual(Path(actual_git_argv[2]).resolve(), self.root.resolve())
        self.assertEqual(
            actual_git_argv[3:],
            (
                "ls-files", "-z", "--cached",
                "--others", "--exclude-standard", "--", *INSTRUCTION_PATHSPECS,
            ),
        )
        self.assertEqual(client.calls, [])
        (self.root / "AGENTS.md").write_bytes(original)
        with self.assertRaises(OrchestrateError) as restored:
            self._run()
        self.assertEqual(restored.exception.code, "preflight_already_rejected")
        with StateStore(self.root) as store:
            observed = store.get_preflight(self.local_id, "task_1", "dispatch_1")
        self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]

    def test_missing_selected_source_is_rejected_durably_before_native_readback(self) -> None:
        source = self.root / "AGENTS.md"
        original = source.read_bytes()
        source.unlink()
        client = PreflightClient(self.root, self.packet)

        with self.assertRaises(OrchestrateError) as rejected:
            self._run(client)

        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "source_binding_changed")  # type: ignore[index]
        self.assertEqual(client.calls, [])
        with StateStore(self.root) as store:
            observed = store.get_preflight(self.local_id, "task_1", "dispatch_1")
        self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
        self.assertEqual(observed["mismatches"][0]["code"], "source_binding_changed")  # type: ignore[index]

        source.write_bytes(original)
        restored_client = PreflightClient(self.root, self.packet)
        with self.assertRaises(OrchestrateError) as restored:
            self._run(restored_client)
        self.assertEqual(restored.exception.code, "preflight_already_rejected")
        self.assertEqual(restored_client.calls, [])

    def test_strict_packet_decode_requires_every_source_record_field(self) -> None:
        from orchestrate.packets import decode_packet

        mandatory_fields = {
            "path", "state", "sha256", "bytes", "headSha256", "indexSha256", "authority",
        }
        for field in sorted(mandatory_fields):
            with self.subTest(field=field):
                malformed = json.loads(json.dumps(self.packet))
                malformed["sources"][0].pop(field)
                malformed["packetId"] = expected_packet_id(malformed)
                with self.assertRaises(OrchestrateError) as rejected:
                    decode_packet(canonical_packet_json(malformed))
                self.assertEqual(rejected.exception.code, "packet_identity_conflict")

    def test_strict_packet_decode_rejects_drive_relative_source_paths(self) -> None:
        from orchestrate.packets import decode_packet

        malformed = json.loads(json.dumps(self.packet))
        malformed["sources"][0]["path"] = "C:AGENTS.md"
        malformed["packetId"] = expected_packet_id(malformed)
        with self.assertRaises(OrchestrateError) as rejected:
            decode_packet(canonical_packet_json(malformed))
        self.assertEqual(rejected.exception.code, "packet_identity_conflict")

    def test_strict_packet_decode_rejects_duplicate_keys_and_nonfinite_numbers(self) -> None:
        from orchestrate.packets import decode_packet

        packet_json = canonical_packet_json(self.packet)
        duplicate = packet_json.replace(
            '  "schema": "orchestrate-worker-packet/v3",',
            '  "schema": "orchestrate-worker-packet/v3",\n  "schema": "orchestrate-worker-packet/v3",',
            1,
        )
        nonfinite = packet_json.replace(
            '  "unresolvedDecisions": []',
            '  "unresolvedDecisions": [NaN]',
            1,
        )
        overflow = packet_json.replace(
            '  "unresolvedDecisions": []',
            '  "unresolvedDecisions": [1e999]',
            1,
        )
        for malformed in (duplicate, nonfinite, overflow):
            with self.subTest(malformed=malformed[-80:]):
                with self.assertRaises(OrchestrateError) as rejected:
                    decode_packet(malformed)
                self.assertEqual(rejected.exception.code, "packet_identity_conflict")

    def test_nested_packet_decode_failure_is_durable_and_definitive(self) -> None:
        with StateStore(self.root) as store:
            original = store.get_packet_json(self.local_id, "task_1")
            store.connection.execute(
                "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                ("[" * 60_000 + "]" * 60_000, self.local_id, "task_1"),
            )

        with self.assertRaises(OrchestrateError) as rejected:
            self._run(PreflightClient(self.root, self.packet))
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "packet_identity_conflict")  # type: ignore[index]

        with StateStore(self.root) as store:
            observed = store.get_preflight(self.local_id, "task_1", "dispatch_1")
            attempts = store.list_preflight_attempts(self.local_id, "task_1", "dispatch_1")
            store.connection.execute(
                "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                (original, self.local_id, "task_1"),
            )
        self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
        self.assertEqual(attempts[0]["disposition"], "definitive")
        with self.assertRaises(OrchestrateError) as restored:
            self._run(PreflightClient(self.root, self.packet))
        self.assertEqual(restored.exception.code, "preflight_already_rejected")

    def test_missing_git_records_retryable_attempt_without_burning_dispatch(self) -> None:
        with patch("orchestrate.profile.subprocess.run", side_effect=FileNotFoundError("git unavailable")):
            with self.assertRaises(OrchestrateError) as transient:
                self._run(PreflightClient(self.root, self.packet))
        self.assertEqual(transient.exception.code, "preflight_retryable")
        self.assertEqual(transient.exception.data["disposition"], "retryable")  # type: ignore[index]
        with StateStore(self.root) as store:
            self.assertIsNone(store.get_preflight(self.local_id, "task_1", "dispatch_1"))
            attempts = store.list_preflight_attempts(self.local_id, "task_1", "dispatch_1")
        self.assertEqual([(item["attemptOrdinal"], item["disposition"]) for item in attempts], [(1, "retryable")])

        native = {
            "terminalResourceId": "terminal-resource-1",
            "terminalHandle": "term_worker",
            "worktreeId": f"repo::{self.root.resolve()}",
        }
        with patch("orchestrate.admission._native_identity", return_value=native):
            admitted = self._run(PreflightClient(self.root, self.packet))
        self.assertEqual(admitted["editingGrant"], "fresh")
        with StateStore(self.root) as store:
            self.assertEqual(
                store.get_preflight(self.local_id, "task_1", "dispatch_1")["outcome"],  # type: ignore[index]
                "passed",
            )
            self.assertEqual(len(store.list_preflight_attempts(self.local_id, "task_1", "dispatch_1")), 1)

    def test_nonzero_git_show_is_retryable_and_recovers_to_a_fresh_grant(self) -> None:
        real_run = subprocess.run
        alternate_root_spelling = os.fspath(self.root) + os.sep + "."
        fault = ResolvedPathFault(alternate_root_spelling)

        def transient_show(
            arguments: tuple[str, ...],
            **keywords: object,
        ) -> subprocess.CompletedProcess[bytes]:
            if (
                len(arguments) >= 4
                and arguments[0] == "git"
                and arguments[1] == "-C"
                and fault.matches(arguments[2])
                and arguments[3] == "show"
            ):
                return subprocess.CompletedProcess(arguments, 128, b"", b"fatal: transient Git failure")
            return real_run(arguments, **keywords)

        self.assertNotEqual(alternate_root_spelling, os.fspath(self.root.resolve()))
        with patch("orchestrate.sources.subprocess.run", side_effect=transient_show):
            with self.assertRaises(OrchestrateError) as transient:
                self._run(PreflightClient(self.root, self.packet))
        self.assertGreaterEqual(fault.interceptions, 1)
        self.assertEqual(transient.exception.code, "preflight_retryable")
        self.assertEqual(transient.exception.data["cause"], "git_inspection_failed")  # type: ignore[index]
        with StateStore(self.root) as store:
            self.assertIsNone(store.get_preflight(self.local_id, "task_1", "dispatch_1"))
            attempts = store.list_preflight_attempts(self.local_id, "task_1", "dispatch_1")
        self.assertEqual([(item["attemptOrdinal"], item["disposition"]) for item in attempts], [(1, "retryable")])

        native = {
            "terminalResourceId": "terminal-resource-1",
            "terminalHandle": "term_worker",
            "worktreeId": f"repo::{self.root.resolve()}",
        }
        with patch("orchestrate.admission._native_identity", return_value=native):
            admitted = self._run(PreflightClient(self.root, self.packet))
        self.assertEqual(admitted["editingGrant"], "fresh")

    def test_untracked_ce_query_source_is_definitive_after_completed_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            query_script = "scripts/quality/project-log.mjs"
            real_run = subprocess.run
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def remove_query_source_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                git(root, "rm", "--cached", "--", query_script)
                return native

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            with (
                patch("orchestrate.admission._native_identity", side_effect=remove_query_source_after_completed_stage),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
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
            self.assertEqual(rejected.exception.data["cause"], "ce_query_source_changed")  # type: ignore[index]
            self.assertEqual(client.calls, [])
            with StateStore(root) as store:
                observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
            self.assertEqual(attempts[0]["disposition"], "definitive")

            git(root, "add", "--", query_script)
            with self.assertRaises(OrchestrateError) as restored:
                worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=PreflightClient(root, packet),
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            self.assertEqual(restored.exception.code, "preflight_already_rejected")

    def test_missing_manifest_selected_shard_is_definitive_after_completed_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, _query_result = self._prepare_ce_preflight(root)
            shard = root / "docs" / "project-log" / "shard-001.md"
            original = shard.read_bytes()
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def remove_shard_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                shard.unlink()
                return native

            client = PreflightClient(root, packet)
            with patch(
                "orchestrate.admission._native_identity",
                side_effect=remove_shard_after_completed_stage,
            ):
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
            self.assertEqual(client.calls, [])
            with StateStore(root) as store:
                observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
            self.assertEqual(attempts[0]["disposition"], "definitive")

            shard.write_bytes(original)
            with self.assertRaises(OrchestrateError) as restored:
                worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=PreflightClient(root, packet),
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            self.assertEqual(restored.exception.code, "preflight_already_rejected")

    def test_non_directory_shard_ancestor_is_definitive_after_completed_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, _query_result = self._prepare_ce_preflight(root)
            shard_parent = root / "docs" / "project-log"
            saved_parent = root / "docs" / "project-log-original"
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def replace_shard_parent_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                shard_parent.rename(saved_parent)
                shard_parent.write_bytes(b"not a directory\n")
                return native

            client = PreflightClient(root, packet)
            with patch(
                "orchestrate.admission._native_identity",
                side_effect=replace_shard_parent_after_completed_stage,
            ):
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
            self.assertEqual(client.calls, [])
            with StateStore(root) as store:
                observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
            self.assertEqual(attempts[0]["disposition"], "definitive")

            shard_parent.unlink()
            saved_parent.rename(shard_parent)
            with self.assertRaises(OrchestrateError) as restored:
                worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=PreflightClient(root, packet),
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            self.assertEqual(restored.exception.code, "preflight_already_rejected")

    def test_post_walk_non_directory_shard_race_is_definitive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            shard_parent = root / "docs" / "project-log"
            saved_parent = root / "docs" / "project-log-original"
            shard = shard_parent / "shard-001.md"
            fault = ResolvedPathFault(shard)
            real_lstat = Path.lstat
            real_run = subprocess.run
            armed = False
            replaced = False
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def arm_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                nonlocal armed
                armed = True
                return native

            def replace_parent_after_walk(path: Path) -> os.stat_result:
                nonlocal replaced
                info = real_lstat(path)
                if armed and not replaced and fault.matches(path):
                    shard_parent.rename(saved_parent)
                    shard_parent.write_bytes(b"not a directory\n")
                    replaced = True
                return info

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            with (
                patch(
                    "orchestrate.admission._native_identity",
                    side_effect=arm_after_completed_stage,
                ),
                patch("orchestrate.safeio.Path.lstat", new=replace_parent_after_walk),
            ):
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
            with StateStore(root) as store:
                observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")

            shard_parent.unlink()
            saved_parent.rename(shard_parent)
            restored_code: str | None = None
            restored_cause: object = None
            restored_grant: object = None
            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                try:
                    restored_report = worker_preflight(
                        root,
                        run_id="run_1",
                        task_id="task_1",
                        dispatch_id="dispatch_1",
                        packet_id=str(packet["packetId"]),
                        client=PreflightClient(root, packet),
                        environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                        platform="win32",
                    )
                    restored_grant = restored_report.get("editingGrant")
                except OrchestrateError as restored:
                    restored_code = restored.code
                    if isinstance(restored.data, dict):
                        restored_cause = restored.data.get("cause")

            self.assertGreaterEqual(fault.interceptions, 1)
            self.assertTrue(replaced)
            self.assertEqual(client.calls, [])
            self.assertEqual(
                (
                    rejected.exception.code,
                    rejected.exception.data.get("cause"),  # type: ignore[union-attr]
                    observed.get("outcome") if observed is not None else None,
                    attempts[0]["disposition"],
                    restored_code,
                    restored_cause,
                    restored_grant,
                ),
                (
                    "preflight_rejected",
                    "source_binding_changed",
                    "rejected",
                    "definitive",
                    "preflight_already_rejected",
                    None,
                    None,
                ),
            )

    def test_post_walk_self_referential_symlink_shard_race_is_definitive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            shard_parent = root / "docs" / "project-log"
            saved_parent = root / "docs" / "project-log-original"
            shard = shard_parent / "shard-001.md"
            fault = ResolvedPathFault(shard)
            real_lstat = Path.lstat
            real_run = subprocess.run
            armed = False
            replaced = False
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def arm_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                nonlocal armed
                armed = True
                return native

            def replace_parent_after_walk(path: Path) -> os.stat_result:
                nonlocal replaced
                info = real_lstat(path)
                if armed and not replaced and fault.matches(path):
                    shard_parent.rename(saved_parent)
                    shard_parent.symlink_to(shard_parent.name, target_is_directory=True)
                    replaced = True
                return info

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            with (
                patch(
                    "orchestrate.admission._native_identity",
                    side_effect=arm_after_completed_stage,
                ),
                patch("orchestrate.safeio.Path.lstat", new=replace_parent_after_walk),
            ):
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
            with StateStore(root) as store:
                observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")

            shard_parent.unlink()
            saved_parent.rename(shard_parent)
            restored_code: str | None = None
            restored_cause: object = None
            restored_grant: object = None
            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                try:
                    restored_report = worker_preflight(
                        root,
                        run_id="run_1",
                        task_id="task_1",
                        dispatch_id="dispatch_1",
                        packet_id=str(packet["packetId"]),
                        client=PreflightClient(root, packet),
                        environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                        platform="win32",
                    )
                    restored_grant = restored_report.get("editingGrant")
                except OrchestrateError as restored:
                    restored_code = restored.code
                    if isinstance(restored.data, dict):
                        restored_cause = restored.data.get("cause")

            self.assertGreaterEqual(fault.interceptions, 1)
            self.assertTrue(replaced)
            self.assertEqual(client.calls, [])
            self.assertEqual(
                (
                    rejected.exception.code,
                    rejected.exception.data.get("cause"),  # type: ignore[union-attr]
                    observed.get("outcome") if observed is not None else None,
                    attempts[0]["disposition"],
                    restored_code,
                    restored_cause,
                    restored_grant,
                ),
                (
                    "preflight_rejected",
                    "source_binding_changed",
                    "rejected",
                    "definitive",
                    "preflight_already_rejected",
                    None,
                    None,
                ),
            )

    @unittest.skipIf(os.name == "nt", "requires POSIX ENAMETOOLONG path resolution")
    def test_post_walk_overlong_symlink_shard_race_is_definitive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            shard_parent = root / "docs" / "project-log"
            saved_parent = root / "docs" / "project-log-original"
            shard = shard_parent / "shard-001.md"
            fault = ResolvedPathFault(shard)
            real_lstat = Path.lstat
            real_run = subprocess.run
            armed = False
            replaced = False
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def arm_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                nonlocal armed
                armed = True
                return native

            def replace_parent_after_walk(path: Path) -> os.stat_result:
                nonlocal replaced
                info = real_lstat(path)
                if armed and not replaced and fault.matches(path):
                    shard_parent.rename(saved_parent)
                    shard_parent.symlink_to("x" * 300, target_is_directory=True)
                    replaced = True
                return info

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            try:
                with (
                    patch(
                        "orchestrate.admission._native_identity",
                        side_effect=arm_after_completed_stage,
                    ),
                    patch("orchestrate.safeio.Path.lstat", new=replace_parent_after_walk),
                ):
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
                with StateStore(root) as store:
                    observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                    attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            finally:
                if shard_parent.is_symlink():
                    shard_parent.unlink()
                if saved_parent.exists():
                    saved_parent.rename(shard_parent)

            restored_code: str | None = None
            restored_cause: object = None
            restored_grant: object = None
            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                try:
                    restored_report = worker_preflight(
                        root,
                        run_id="run_1",
                        task_id="task_1",
                        dispatch_id="dispatch_1",
                        packet_id=str(packet["packetId"]),
                        client=PreflightClient(root, packet),
                        environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                        platform="win32",
                    )
                    restored_grant = restored_report.get("editingGrant")
                except OrchestrateError as restored:
                    restored_code = restored.code
                    if isinstance(restored.data, dict):
                        restored_cause = restored.data.get("cause")

            self.assertGreaterEqual(fault.interceptions, 1)
            self.assertTrue(replaced)
            self.assertEqual(client.calls, [])
            self.assertEqual(
                (
                    rejected.exception.code,
                    rejected.exception.data.get("cause"),  # type: ignore[union-attr]
                    observed.get("outcome") if observed is not None else None,
                    attempts[0]["disposition"],
                    restored_code,
                    restored_cause,
                    restored_grant,
                ),
                (
                    "preflight_rejected",
                    "source_binding_changed",
                    "rejected",
                    "definitive",
                    "preflight_already_rejected",
                    None,
                    None,
                ),
            )

    @unittest.skipIf(os.name == "nt", "requires POSIX dir-relative source opening")
    def test_query_source_absent_at_open_then_restored_is_definitive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            query_script = root / "scripts" / "quality" / "project-log.mjs"
            saved_script = query_script.with_name("project-log.mjs-open-race")
            real_open = os.open
            real_run = subprocess.run
            armed = False
            interrupted = False
            restored_before_return = False
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def arm_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                nonlocal armed
                armed = True
                return native

            def absent_then_restore_open(
                path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                nonlocal interrupted, restored_before_return
                if armed and not interrupted and path == query_script.name and dir_fd is not None:
                    interrupted = True
                    query_script.rename(saved_script)
                    try:
                        return real_open(path, flags, mode, dir_fd=dir_fd)
                    except FileNotFoundError:
                        saved_script.rename(query_script)
                        restored_before_return = True
                        raise
                return real_open(path, flags, mode, dir_fd=dir_fd)

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            try:
                with (
                    patch(
                        "orchestrate.admission._native_identity",
                        side_effect=arm_after_completed_stage,
                    ),
                    patch("orchestrate.safeio.os.open", new=absent_then_restore_open),
                ):
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
                with StateStore(root) as store:
                    observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                    attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            finally:
                if saved_script.exists() and not query_script.exists():
                    saved_script.rename(query_script)

            restored_code: str | None = None
            restored_cause: object = None
            restored_grant: object = None
            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                try:
                    restored_report = worker_preflight(
                        root,
                        run_id="run_1",
                        task_id="task_1",
                        dispatch_id="dispatch_1",
                        packet_id=str(packet["packetId"]),
                        client=PreflightClient(root, packet),
                        environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                        platform="win32",
                    )
                    restored_grant = restored_report.get("editingGrant")
                except OrchestrateError as restored:
                    restored_code = restored.code
                    if isinstance(restored.data, dict):
                        restored_cause = restored.data.get("cause")

            self.assertTrue(interrupted)
            self.assertTrue(restored_before_return)
            self.assertTrue(query_script.is_file())
            self.assertEqual(client.calls, [])
            self.assertEqual(
                (
                    rejected.exception.code,
                    rejected.exception.data.get("cause"),  # type: ignore[union-attr]
                    observed.get("outcome") if observed is not None else None,
                    attempts[0]["disposition"],
                    restored_code,
                    restored_cause,
                    restored_grant,
                ),
                (
                    "preflight_rejected",
                    "ce_query_source_changed",
                    "rejected",
                    "definitive",
                    "preflight_already_rejected",
                    None,
                    None,
                ),
            )

    def test_still_present_query_source_open_failure_is_retryable_after_native_readback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(root)
            query_script = "scripts/quality/project-log.mjs"
            fault = ResolvedPathFault(root / query_script)
            real_run = subprocess.run
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def transient_open(current_root: Path, relative: str) -> bytes:
                if fault.matches(relative, relative_to=current_root):
                    self.assertTrue((current_root / relative).is_file())
                    raise OrchestrateError(
                        "simulated sharing failure",
                        code="source_unavailable",
                    )
                return read_project_bytes(current_root, relative)

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.sources.read_project_bytes", side_effect=transient_open),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                with self.assertRaises(OrchestrateError) as transient:
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
            self.assertEqual(transient.exception.code, "preflight_retryable")
            self.assertEqual(transient.exception.data["cause"], "ce_query_source_unavailable")  # type: ignore[index]
            self.assertGreaterEqual(fault.interceptions, 1)
            self.assertEqual(client.calls, [])
            with StateStore(root) as store:
                self.assertIsNone(store.get_preflight(run.local_id, "task_1", "dispatch_1"))
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            self.assertEqual(
                [(item["attemptOrdinal"], item["disposition"]) for item in attempts],
                [(1, "retryable")],
            )

            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                admitted = worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=PreflightClient(root, packet),
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            self.assertEqual(admitted["status"], "admitted")
            self.assertEqual(admitted["editingGrant"], "fresh")
            with StateStore(root) as store:
                self.assertEqual(
                    store.get_preflight(run.local_id, "task_1", "dispatch_1")["outcome"],  # type: ignore[index]
                    "passed",
                )
                self.assertEqual(
                    len(store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")),
                    1,
                )

    @unittest.skipIf(os.name == "nt", "requires POSIX permission denial semantics")
    def test_actual_query_source_permission_denial_is_retryable_after_completed_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(
                root,
                executable_query=True,
            )
            query_script = root / "scripts" / "quality" / "project-log.mjs"
            original = query_script.read_bytes()
            original_mode = stat.S_IMODE(query_script.stat().st_mode)
            real_run = subprocess.run
            denied_errno: int | None = None
            status_after_denial = b""
            query_calls: list[tuple[str, ...]] = []
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def deny_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                nonlocal denied_errno, status_after_denial
                query_script.chmod(0)
                try:
                    descriptor = os.open(query_script, os.O_RDONLY)
                except OSError as exc:
                    denied_errno = exc.errno
                else:
                    os.close(descriptor)
                status_after_denial = real_run(
                    (
                        "git",
                        "-C",
                        os.fspath(root),
                        "status",
                        "--porcelain=v1",
                        "-z",
                        "--",
                        "scripts/quality/project-log.mjs",
                    ),
                    capture_output=True,
                    check=True,
                ).stdout
                return native

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    query_calls.append(arguments)
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            client = PreflightClient(root, packet)
            try:
                with (
                    patch(
                        "orchestrate.admission._native_identity",
                        side_effect=deny_after_completed_stage,
                    ),
                    patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
                ):
                    with self.assertRaises(OrchestrateError) as transient:
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
            finally:
                query_script.chmod(original_mode)

            self.assertEqual(denied_errno, errno.EACCES)
            self.assertTrue(status_after_denial)
            self.assertEqual(query_script.read_bytes(), original)
            self.assertEqual(transient.exception.code, "preflight_retryable")
            self.assertEqual(
                transient.exception.data["cause"],  # type: ignore[index]
                "ce_query_source_unavailable",
            )
            self.assertEqual(query_calls, [])
            self.assertEqual(client.calls, [])
            with StateStore(root) as store:
                self.assertIsNone(store.get_preflight(run.local_id, "task_1", "dispatch_1"))
                attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            self.assertEqual(
                [(item["attemptOrdinal"], item["disposition"]) for item in attempts],
                [(1, "retryable")],
            )

            with (
                patch("orchestrate.admission._native_identity", return_value=native),
                patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
            ):
                admitted = worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=PreflightClient(root, packet),
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            self.assertEqual(admitted["status"], "admitted")
            self.assertEqual(admitted["editingGrant"], "fresh")

    def _assert_query_source_mutation_is_definitive(
        self,
        mutate: Callable[[Path, bytes], None],
        *,
        executable_query: bool = False,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, run, packet, query_result = self._prepare_ce_preflight(
                root,
                executable_query=executable_query,
            )
            query_script = root / "scripts" / "quality" / "project-log.mjs"
            original = query_script.read_bytes()
            original_mode = stat.S_IMODE(query_script.stat().st_mode)
            real_run = subprocess.run
            native = {
                "terminalResourceId": "terminal-resource-1",
                "terminalHandle": "term_worker",
                "worktreeId": f"repo::{root.resolve()}",
            }

            def mutate_after_completed_stage(*_: object, **__: object) -> dict[str, str]:
                mutate(query_script, original)
                return native

            def synthetic_query(
                arguments: tuple[str, ...],
                **keywords: object,
            ) -> subprocess.CompletedProcess[bytes]:
                if arguments[0] == "pnpm":
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        json.dumps(query_result).encode("utf-8"),
                        b"",
                    )
                return real_run(arguments, **keywords)

            try:
                with (
                    patch(
                        "orchestrate.admission._native_identity",
                        side_effect=mutate_after_completed_stage,
                    ),
                    patch("orchestrate.readers.subprocess.run", side_effect=synthetic_query),
                ):
                    with self.assertRaises(OrchestrateError) as rejected:
                        worker_preflight(
                            root,
                            run_id="run_1",
                            task_id="task_1",
                            dispatch_id="dispatch_1",
                            packet_id=str(packet["packetId"]),
                            client=PreflightClient(root, packet),
                            environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                            platform="win32",
                        )
                with StateStore(root) as store:
                    observed = store.get_preflight(run.local_id, "task_1", "dispatch_1")
                    attempts = store.list_preflight_attempts(run.local_id, "task_1", "dispatch_1")
            finally:
                query_script.write_bytes(original)
                query_script.chmod(original_mode)

            self.assertEqual(rejected.exception.code, "preflight_rejected")
            self.assertEqual(
                rejected.exception.data["cause"],  # type: ignore[index]
                "ce_query_source_changed",
            )
            self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
            self.assertEqual(attempts[0]["disposition"], "definitive")
            with self.assertRaises(OrchestrateError) as restored:
                worker_preflight(
                    root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=PreflightClient(root, packet),
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
            self.assertEqual(restored.exception.code, "preflight_already_rejected")

    def test_query_source_byte_change_after_completed_stage_is_definitive(self) -> None:
        self._assert_query_source_mutation_is_definitive(
            lambda path, original: path.write_bytes(
                original + b"// changed after completed stage\n"
            )
        )

    @unittest.skipIf(os.name == "nt", "requires POSIX tracked executable-bit semantics")
    def test_tracked_query_source_executable_bit_change_is_definitive(self) -> None:
        self._assert_query_source_mutation_is_definitive(
            lambda path, _original: path.chmod(0o644),
            executable_query=True,
        )

    def test_packet_declared_repo_routing_cannot_authorize_a_byte_read(self) -> None:
        private_path = "private-notes.txt"
        (self.root / private_path).write_text("not reader-routed authority\n", encoding="utf-8")
        objective = "Implement bounded change"
        reader = read_project(self.profile, objective)
        sources = build_source_index(
            self.profile,
            extra_sources={*reader.consulted_paths, private_path},
        )
        tampered = make_packet(
            objective=objective,
            profile=self.profile,
            sources=sources,
            launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
            python_executable=str(Path(sys.executable).resolve()),
            run_id="run_1",
            reader=reader,
        )
        tampered["reader"]["routing"]["authority"].append(private_path)
        tampered["packetId"] = expected_packet_id(tampered)
        with StateStore(self.root) as store:
            store.connection.execute(
                "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                (canonical_packet_json(tampered), self.local_id, "task_1"),
            )

        client = PreflightClient(self.root, tampered)
        with patch("orchestrate.admission.read_project_bytes", wraps=read_project_bytes) as source_reads:
            with self.assertRaises(OrchestrateError) as rejected:
                worker_preflight(
                    self.root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(tampered["packetId"]),
                    client=client,
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )
        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "packet_identity_conflict")  # type: ignore[index]
        self.assertNotIn(private_path, [call.args[1] for call in source_reads.call_args_list])
        self.assertEqual(client.calls, [])

    def test_reader_and_index_consume_the_single_completed_source_stage(self) -> None:
        with StateStore(self.root) as store:
            run = store.get_run(self.local_id)
            packet_json = store.get_packet_json(self.local_id, "task_1")

        with (
            patch("orchestrate.admission.read_project_bytes", wraps=read_project_bytes) as source_reads,
            patch("orchestrate.sources.read_project_bytes", side_effect=AssertionError("source index reopened bytes")),
            patch("orchestrate.readers.read_project_text", side_effect=AssertionError("reader reopened bytes")),
            patch("orchestrate.admission.build_source_index", wraps=build_source_index) as index_builds,
        ):
            validation = validate_packet_sources(self.profile, run, packet_json)

        expected_paths = {
            record["path"]
            for record in self.packet["sources"]
            if record["sha256"] is not None
        }
        observed_paths = [call.args[1] for call in source_reads.call_args_list]
        self.assertCountEqual(observed_paths, expected_paths)
        self.assertEqual(len(observed_paths), len(expected_paths))
        self.assertEqual(index_builds.call_count, 1)
        self.assertEqual(validation.sources.digest, self.packet["sourceDigest"])

    def test_reference_only_source_created_after_check_is_never_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git(root, "init", "-q")
            git(root, "config", "user.email", "fixture@example.invalid")
            git(root, "config", "user.name", "Fixture")
            (root / "AGENTS.md").write_text("Bound instruction.\n", encoding="utf-8")
            (root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
            git(root, "add", ".")
            git(root, "commit", "-qm", "fixture")
            setup_project(root)
            profile_value = json.loads((root / ".orchestrate.json").read_text(encoding="utf-8"))
            profile_value["candidateSources"] = ["appearing.txt"]
            (root / ".orchestrate.json").write_text(
                json.dumps(profile_value, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            profile = setup_project(root, acknowledge_profile=True)
            reader = read_project(profile, "Inspect the bounded candidate")
            sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
            with StateStore(root) as store:
                run = store.create_run(
                    objective="Inspect the bounded candidate",
                    profile_digest=profile.digest,
                    source_digest=sources.digest,
                )
                run = store.update_run(run.local_id, native_run_id="run_1")
            packet = make_packet(
                objective=run.objective,
                profile=profile,
                sources=sources,
                launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                python_executable=str(Path(sys.executable).resolve()),
                run_id="run_1",
                reader=reader,
            )
            original_inventory = profile.require_instruction_acknowledgment

            def create_after_reference_check() -> tuple[str, ...]:
                result = original_inventory()
                (root / "appearing.txt").write_text("appeared after eligibility\n", encoding="utf-8")
                return result

            with (
                patch(
                    "orchestrate.admission.ProjectProfile.require_instruction_acknowledgment",
                    side_effect=create_after_reference_check,
                ),
                patch(
                    "orchestrate.sources.read_project_bytes",
                    side_effect=AssertionError("reference-only source bytes were consumed"),
                ),
                self.assertRaises(OrchestrateError) as rejected,
            ):
                validate_packet_sources(profile, run, canonical_packet_json(packet))
            self.assertEqual(rejected.exception.code, "source_binding_changed")

    def test_malformed_source_record_is_durably_rejected_before_profile_or_source_io(self) -> None:
        malformed = json.loads(json.dumps(self.packet))
        malformed["sources"][0].pop("authority")
        malformed["packetId"] = expected_packet_id(malformed)
        malformed_json = canonical_packet_json(malformed)
        with StateStore(self.root) as store:
            store.connection.execute(
                "UPDATE packets SET packet_json = ? WHERE run_local_id = ? AND task_id = ?",
                (malformed_json, self.local_id, "task_1"),
            )
        client = PreflightClient(self.root, malformed)

        with (
            patch("orchestrate.profile.read_project_bytes", side_effect=AssertionError("profile source read")) as profile_read,
            patch("orchestrate.admission.read_project_bytes", side_effect=AssertionError("project source read")) as source_read,
            patch("subprocess.run", side_effect=AssertionError("subprocess")) as subprocess_call,
            self.assertRaises(OrchestrateError) as rejected,
        ):
            worker_preflight(
                self.root,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                packet_id=str(malformed["packetId"]),
                client=client,
                environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                platform="win32",
            )

        self.assertEqual(rejected.exception.code, "preflight_rejected")
        self.assertEqual(rejected.exception.data["cause"], "packet_identity_conflict")  # type: ignore[index]
        profile_read.assert_not_called()
        source_read.assert_not_called()
        subprocess_call.assert_not_called()
        self.assertEqual(client.calls, [])
        with StateStore(self.root) as store:
            observed = store.get_preflight(self.local_id, "task_1", "dispatch_1")
        self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
        self.assertEqual(observed["mismatches"][0]["code"], "packet_identity_conflict")  # type: ignore[index]

    def test_acknowledged_selected_instruction_drift_rejects_before_any_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git(root, "init", "-q")
            git(root, "config", "user.email", "fixture@example.invalid")
            git(root, "config", "user.name", "Fixture")
            instruction = root / "AGENTS.md"
            instruction.write_text("Acknowledged instruction.\n", encoding="utf-8")
            (root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
            git(root, "add", ".")
            git(root, "commit", "-qm", "fixture")
            setup_project(root)
            profile = setup_project(root, acknowledge_profile=True)
            self.assertEqual(profile.selection_source, "explicit-configuration-acknowledgment")
            reader = read_project(profile, "Implement bounded change")
            sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
            with StateStore(root) as store:
                run = store.create_run(
                    objective="Implement bounded change",
                    profile_digest=profile.digest,
                    source_digest=sources.digest,
                )
                run = store.update_run(
                    run.local_id,
                    native_run_id="run_1",
                    task_id="task_1",
                    phase="awaiting_preflight",
                )
                local_id = run.local_id
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
                store.record_worker_resource_binding(
                    run.local_id,
                    dispatch_id="dispatch_1",
                    resource_id="terminal-resource-1",
                    terminal_handle="term_worker",
                    worktree_id=f"repo::{root.resolve()}",
                    readback={"fixture": "validated-start-readback"},
                )

            instruction.write_text("Drifted after acknowledgment.\n", encoding="utf-8")
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
            called.assert_not_called()
            self.assertEqual(client.calls, [])
            with StateStore(root) as store:
                observed = store.get_preflight(local_id, "task_1", "dispatch_1")
            self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
            self.assertEqual(
                observed["mismatches"][0]["code"],  # type: ignore[index]
                "source_binding_changed",
            )

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
                store.record_worker_resource_binding(
                    run.local_id,
                    dispatch_id="dispatch_1",
                    resource_id="terminal-resource-1",
                    terminal_handle="term_worker",
                    worktree_id=f"repo::{root.resolve()}",
                    readback={"fixture": "validated-start-readback"},
                )

            with patch("orchestrate.readers._run_ce_query", return_value=query_result):
                validated = validate_packet_sources(
                    profile,
                    run,
                    canonical_packet_json(packet),
                )
            self.assertEqual(validated.sources.digest, packet["sourceDigest"])

            private_path = "private-notes.txt"
            (root / private_path).write_text("not selected by the CE registry\n", encoding="utf-8")
            tampered_sources = build_source_index(
                profile,
                extra_sources={*reader.consulted_paths, private_path},
            )
            tampered = make_packet(
                objective=run.objective,
                profile=profile,
                sources=tampered_sources,
                launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                python_executable=str(Path(sys.executable).resolve()),
                run_id="run_1",
                reader=reader,
            )
            tampered["reader"]["routing"]["task"] = private_path
            tampered["packetId"] = expected_packet_id(tampered)
            with patch("orchestrate.admission.read_project_bytes", wraps=read_project_bytes) as source_reads:
                with self.assertRaises(OrchestrateError) as routed:
                    validate_packet_sources(profile, run, canonical_packet_json(tampered))
            self.assertEqual(routed.exception.code, "source_binding_changed")
            self.assertNotIn(private_path, [call.args[1] for call in source_reads.call_args_list])
            (root / private_path).unlink()

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
