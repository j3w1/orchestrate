from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from orchestrate.errors import OrchestrateError
from orchestrate.state import StateStore


class StateTests(unittest.TestCase):
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
                store.journal_delivery(run.local_id, "delivery_1", {"different": True}, [message])
                row = store.connection.execute("SELECT response_json FROM deliveries").fetchone()
                self.assertIn("delivery_1", row["response_json"])
                self.assertEqual(store.messages(run.local_id, "delivery_1")[0]["effect_status"], "observed")


if __name__ == "__main__":
    unittest.main()
