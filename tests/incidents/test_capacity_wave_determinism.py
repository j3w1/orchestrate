from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from orchestrate.coordination import NativeDagScheduler
from orchestrate.state import StateStore
from test_coordination import FakeDagClient, milestone


class CapacityWaveDeterminismIncident(unittest.TestCase):
    def test_reversed_native_rows_preserve_deterministic_capacity_waves(self) -> None:
        plan = replace(milestone(), max_workers=1)
        client = FakeDagClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="deterministic waves", profile_digest="p", source_digest="s")
                scheduler = NativeDagScheduler(client, store, run.local_id)  # type: ignore[arg-type]
                bindings = scheduler.create("run_1", plan)
                for row in client.rows:
                    if row["id"] == bindings["owner"].task_id:
                        row["status"] = "completed"
                    elif row["id"] in {bindings["unit"].task_id, bindings["incident"].task_id}:
                        row["status"] = "ready"
                client.rows.reverse()

                first_wave = scheduler.ready_wave(
                    "run_1",
                    plan,
                    bindings,
                    active=set(),
                    finished={"owner"},
                    remaining_capacity=1,
                )
                for row in client.rows:
                    if row["id"] == bindings["unit"].task_id:
                        row["status"] = "dispatched"
                    elif row["id"] == bindings["incident"].task_id:
                        row["status"] = "pending"
                at_capacity = scheduler.ready_wave(
                    "run_1",
                    plan,
                    bindings,
                    active={"unit"},
                    finished={"owner"},
                    remaining_capacity=0,
                )
                for row in client.rows:
                    if row["id"] == bindings["unit"].task_id:
                        row["status"] = "completed"
                    elif row["id"] == bindings["incident"].task_id:
                        row["status"] = "ready"
                second_wave = scheduler.ready_wave(
                    "run_1",
                    plan,
                    bindings,
                    active=set(),
                    finished={"owner", "unit"},
                    remaining_capacity=1,
                )

        self.assertEqual([binding.key for binding in first_wave], ["unit"])
        self.assertEqual(at_capacity, ())
        self.assertEqual([binding.key for binding in second_wave], ["incident"])
