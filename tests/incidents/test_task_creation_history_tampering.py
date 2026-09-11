from __future__ import annotations

import json
from unittest.mock import patch

from orchestrate.cli import main as cli_main
from orchestrate.controller import explain, implement, status
from orchestrate.state import StateStore
from test_controller import MilestoneClient, MilestoneRepo, ReportingClient


class TaskCreationHistoryTamperingIncident(MilestoneRepo):
    def test_accepted_review_fails_closed_until_exact_history_is_restored(self) -> None:
        objective = "Preserve exact Task creation history"
        plan = self._write_milestone_plan(objective)
        accepted = implement(
            self.root,
            objective,
            client=MilestoneClient(self.root),  # type: ignore[arg-type]
            milestone_plan=plan,
            wait_timeout_ms=60_000,
            require_context=False,
        )
        local_run_id = str(accepted["localRunId"])
        with StateStore(self.root) as store:
            task_creation_rows = [
                dict(row)
                for row in store.connection.execute(
                    "SELECT * FROM intentions WHERE run_local_id = ? ORDER BY created_at, id",
                    (local_run_id,),
                ).fetchall()
                if row["operation"] == "task-create"
                or row["operation"].startswith("milestone-task-create:")
            ]
        task_creation_by_operation = {row["operation"]: row for row in task_creation_rows}

        def restore() -> None:
            with StateStore(self.root) as store:
                store.connection.execute(
                    """DELETE FROM intentions
                       WHERE run_local_id = ?
                         AND (operation = 'task-create' OR operation GLOB 'milestone-task-create:*')""",
                    (local_run_id,),
                )
                for original in task_creation_rows:
                    store.connection.execute(
                        """INSERT INTO intentions(
                               id, run_local_id, operation, arguments_json, request_id, status,
                               returncode, response_json, error_json, created_at, updated_at
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        tuple(original.values()),
                    )

        def altered_followup_spec() -> None:
            original = task_creation_by_operation["milestone-task-create:verify"]
            arguments = json.loads(original["arguments_json"])
            arguments[arguments.index("--spec") + 1] = "Altered follow-up Task creation spec."
            with StateStore(self.root) as store:
                store.connection.execute(
                    "UPDATE intentions SET arguments_json = ? WHERE run_local_id = ? AND operation = ?",
                    (json.dumps(arguments), local_run_id, original["operation"]),
                )

        def duplicate_owner_row() -> None:
            original = task_creation_by_operation["task-create"]
            values = dict(original)
            values["id"] = f"{original['id']}_duplicate"
            with StateStore(self.root) as store:
                store.connection.execute(
                    """INSERT INTO intentions(
                           id, run_local_id, operation, arguments_json, request_id, status,
                           returncode, response_json, error_json, created_at, updated_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    tuple(values.values()),
                )

        for kind, corrupt in (
            ("altered_followup_spec", altered_followup_spec),
            ("duplicate_owner_row", duplicate_owner_row),
        ):
            with self.subTest(kind=kind):
                corrupt()
                try:
                    reports = (
                        status(self.root, local_run_id, client=ReportingClient()),  # type: ignore[arg-type]
                        explain(self.root, local_run_id),
                    )
                    for report in reports:
                        self.assertEqual(report["status"], "stale_review")
                        self.assertEqual(report["verification"], "review_stale")
                        self.assertIsNone(report["sourceBinding"]["current"])
                        self.assertEqual(report["sourceBinding"]["error"]["code"], "packet_identity_conflict")
                        self.assertEqual(report["staleEvidence"]["storedVerification"], "review_accepted")
                    for command, report in (("status", reports[0]), ("explain", reports[1])):
                        with patch("orchestrate.cli._execute", return_value=(report, True)), patch("builtins.print"):
                            self.assertEqual(
                                cli_main([command, "--project", str(self.root), "--run", local_run_id, "--json"]),
                                1,
                            )
                finally:
                    restore()

                restored = explain(self.root, local_run_id)
                self.assertEqual(restored["status"], "worker_succeeded")
                self.assertEqual(restored["verification"], "review_accepted")
                self.assertIs(restored["sourceBinding"]["unchanged"], True)
