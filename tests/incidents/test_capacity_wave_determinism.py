from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from orchestrate.coordination import MilestoneTask, NativeDagScheduler
from orchestrate.state import StateStore
from test_coordination import FakeDagClient, milestone


class CapacityWaveDeterminismIncident(unittest.TestCase):
    def test_five_ready_tasks_preserve_deterministic_capacity_two_waves(self) -> None:
        base = milestone()
        owner = base.tasks[0]
        specialists = tuple(
            MilestoneTask(
                f"verify_{index}",
                f"Verify {index}",
                f"Run independent verification {index}",
                "specialist",
                dependencies=("owner",),
                gate="verification",
                independently_useful=True,
                candidate_digest=base.candidate_digest,
                contract_digest=base.contract.digest,
            )
            for index in range(5)
        )
        reviewer = replace(
            base.tasks[-1],
            dependencies=("owner", *(task.key for task in specialists)),
        )
        plan = replace(base, tasks=(owner, *specialists, reviewer), max_workers=2)
        client = FakeDagClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="deterministic waves", profile_digest="p", source_digest="s")
                scheduler = NativeDagScheduler(client, store, run.local_id)  # type: ignore[arg-type]
                bindings = scheduler.create("run_1", plan)
                client.rows.reverse()
                finished = {"owner"}
                waves: list[list[str]] = []
                while len(finished) < 6:
                    for row in client.rows:
                        key = next(key for key, binding in bindings.items() if binding.task_id == row["id"])
                        if key == "owner" or key in finished:
                            row["status"] = "completed"
                        elif key.startswith("verify_"):
                            row["status"] = "ready"
                        else:
                            row["status"] = "pending"
                    wave = scheduler.ready_wave(
                        "run_1",
                        plan,
                        bindings,
                        active=set(),
                        finished=finished,
                        remaining_capacity=2,
                    )
                    keys = [binding.key for binding in wave]
                    waves.append(keys)
                    finished.update(keys)

        self.assertEqual(
            waves,
            [["verify_0", "verify_1"], ["verify_2", "verify_3"], ["verify_4"]],
        )
