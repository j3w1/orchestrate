from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest

from orchestrate.errors import OrchestrateError
from orchestrate.state import StateStore


class StateTests(unittest.TestCase):
    def test_project_contained_and_recognized_synchronized_state_roots_are_rejected_before_write(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as outside_dir:
            root = Path(project_dir)
            contained = root / ".private-state"
            with self.assertRaises(OrchestrateError) as contained_error:
                StateStore(root, home=contained)
            self.assertEqual(contained_error.exception.code, "state_storage_unsafe")
            self.assertFalse(contained.exists())

            synchronized = Path(outside_dir) / "OneDrive" / "state"
            with self.assertRaises(OrchestrateError) as synchronized_error:
                StateStore(root, home=synchronized)
            self.assertEqual(synchronized_error.exception.code, "state_storage_unsafe")
            self.assertFalse(synchronized.exists())

    def test_records_are_host_local_and_run_selection_is_explicit_when_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            root = Path(project_dir)
            with StateStore(root, home=Path(home_dir)) as store:
                first = store.create_run(objective="one", profile_digest="p", source_digest="s")
                second = store.create_run(objective="two", profile_digest="p", source_digest="s")
                self.assertNotEqual(first.local_id, second.local_id)
                self.assertTrue(store.path.is_relative_to(Path(home_dir)))
                self.assertFalse(store.path.is_relative_to(root))
                with self.assertRaisesRegex(OrchestrateError, "More than one"):
                    store.select_run(None)

    def test_os_lock_rejects_competing_controller(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                first = store.lock("run")
                second = store.lock("run")
                with first:
                    with self.assertRaises(OrchestrateError) as caught:
                        with second:
                            pass
                self.assertEqual(caught.exception.code, "controller_contention")

    def test_delivery_is_immutable_and_journaled_before_effects(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="one", profile_digest="p", source_digest="s")
                message = {"id": "msg_1", "type": "heartbeat", "payload": {"n": 1}}
                response = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
                store.journal_delivery(run.local_id, "delivery_1", response, [message])
                store.journal_delivery(run.local_id, "delivery_1", response, [message])
                with self.assertRaises(OrchestrateError) as caught:
                    store.journal_delivery(run.local_id, "delivery_1", {"different": True}, [message])
                row = store.connection.execute("SELECT response_json FROM deliveries").fetchone()
                self.assertIn("delivery_1", row["response_json"])
                self.assertEqual(store.messages(run.local_id, "delivery_1")[0]["effect_status"], "observed")
                self.assertEqual(caught.exception.code, "delivery_identity_conflict")

    def test_terminal_run_with_unacked_delivery_remains_resumable(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="one", profile_digest="p", source_digest="s")
                message = {
                    "id": "msg_done",
                    "type": "worker_done",
                    "payload": {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
                }
                store.journal_delivery(
                    run.local_id,
                    "delivery_1",
                    {"result": {"deliveryId": "delivery_1", "messages": [message]}},
                    [message],
                )
                store.update_run(run.local_id, phase="worker_succeeded", delivery_id="delivery_1")

                selected = store.select_run(None)

                self.assertEqual(selected.local_id, run.local_id)
                self.assertEqual(selected.delivery_id, "delivery_1")

    def test_blocked_managed_attempt_still_reserves_single_writer_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="one", profile_digest="p", source_digest="s")
                run = store.update_run(
                    run.local_id,
                    native_run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    phase="blocked",
                )

                self.assertEqual(store.select_run(None).local_id, run.local_id)

    def test_worker_resource_binding_is_immutable_and_keeps_validated_readback(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="one", profile_digest="p", source_digest="s")
                first = store.record_worker_resource_binding(
                    run.local_id,
                    dispatch_id="dispatch_1",
                    resource_id="resource_1",
                    terminal_handle="term_1",
                    worktree_id="worktree_1",
                    readback={"state": "owned"},
                )
                repeated = store.record_worker_resource_binding(
                    run.local_id,
                    dispatch_id="dispatch_1",
                    resource_id="resource_1",
                    terminal_handle="term_1",
                    worktree_id="worktree_1",
                    readback={"state": "released"},
                )
                self.assertEqual(first, repeated)
                self.assertEqual(first.readback_json, '{"state":"owned"}')
                with self.assertRaises(OrchestrateError) as caught:
                    store.record_worker_resource_binding(
                        run.local_id,
                        dispatch_id="dispatch_1",
                        resource_id="resource_other",
                        terminal_handle="term_1",
                        worktree_id="worktree_1",
                        readback={"state": "released"},
                    )
                self.assertEqual(caught.exception.code, "worker_resource_binding_mismatch")

    def test_worker_evidence_phase_and_message_effect_roll_back_as_one_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="one", profile_digest="p", source_digest="s")
                message = {
                    "id": "msg_done",
                    "type": "worker_done",
                    "payload": {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
                }
                delivery = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
                store.journal_delivery(run.local_id, "delivery_1", delivery, [message])
                store.update_run(run.local_id, phase="waiting", delivery_id="delivery_1")
                store.connection.execute(
                    """CREATE TRIGGER fail_message_effect BEFORE UPDATE ON delivery_messages
                       BEGIN SELECT RAISE(ABORT, 'synthetic boundary failure'); END"""
                )

                with self.assertRaises(sqlite3.IntegrityError):
                    store.finalize_worker_message(
                        run_local_id=run.local_id,
                        delivery_id="delivery_1",
                        message_id="msg_done",
                        outcome="succeeded",
                        subject="done",
                        payload=message,
                    )

                persisted = store.get_run(run.local_id)
                self.assertEqual(persisted.phase, "waiting")
                self.assertIsNone(persisted.worker_outcome)
                self.assertEqual(store.messages(run.local_id, "delivery_1")[0]["effect_status"], "observed")
                self.assertEqual(store.evidence(run.local_id), [])


if __name__ == "__main__":
    unittest.main()
