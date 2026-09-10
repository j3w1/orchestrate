from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import unittest

from orchestrate.errors import OrchestrateError
from orchestrate.intervention import InterventionLedger, InterventionRecord
from orchestrate.state import StateStore


class InterventionTests(unittest.TestCase):
    def test_same_correction_gets_exactly_one_bounded_diagnosis_without_new_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="fix", profile_digest="p", source_digest="s")
                ledger = InterventionLedger(store, run.local_id)
                record = InterventionRecord(
                    obligation="The parser must reject mixed aliases.",
                    failing_example="A mixed runId plus last_failure payload was accepted.",
                    hypothesis="The compatibility branch normalizes before shape selection.",
                    last_meaningful_evidence="fixture mixed_alias_case failed at assertion 4",
                    next_discriminating_check="Run only the version-shape matrix with normalization disabled.",
                )

                self.assertEqual(
                    ledger.consider_correction(task_key="parser", correction_key="move-shape-check", record=record),
                    "correction_allowed",
                )
                self.assertEqual(
                    ledger.consider_correction(task_key="parser", correction_key="move-shape-check", record=record),
                    "diagnosis_required",
                )
                self.assertEqual(ledger.read("parser")["diagnosisStatus"], "required")  # type: ignore[index]
                self.assertEqual(
                    ledger.finish_diagnosis(task_key="parser", diagnosis_evidence=None),
                    "unresolved",
                )
                self.assertEqual(
                    ledger.consider_correction(task_key="parser", correction_key="move-shape-check", record=record),
                    "unresolved",
                )
                with self.assertRaises(OrchestrateError) as repeated:
                    ledger.finish_diagnosis(task_key="parser", diagnosis_evidence="another try")
                self.assertEqual(repeated.exception.code, "diagnosis_not_authorized")

    def test_new_meaningful_evidence_allows_a_new_correction(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="fix", profile_digest="p", source_digest="s")
                ledger = InterventionLedger(store, run.local_id)
                first = InterventionRecord("obligation", "failure", "hypothesis", "evidence one", "check")
                second = InterventionRecord("obligation", "failure", "revised hypothesis", "evidence two", "next check")
                self.assertEqual(
                    ledger.consider_correction(task_key="task", correction_key="correction", record=first),
                    "correction_allowed",
                )
                self.assertEqual(
                    ledger.consider_correction(task_key="task", correction_key="correction", record=second),
                    "correction_allowed",
                )
                self.assertEqual(ledger.read("task")["correctionCount"], 2)  # type: ignore[index]

    def test_incomplete_intervention_record_fails_closed(self) -> None:
        with self.assertRaises(OrchestrateError) as caught:
            InterventionRecord("obligation", "", "hypothesis", "evidence", "check").validated()
        self.assertEqual(caught.exception.code, "intervention_record_incomplete")

    def test_concurrent_diagnosis_can_authorize_exactly_one_correction(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            project = Path(project_dir)
            home = Path(home_dir)
            with StateStore(project, home=home) as store:
                run = store.create_run(objective="fix", profile_digest="p", source_digest="s")
                ledger = InterventionLedger(store, run.local_id)
                record = InterventionRecord("obligation", "failure", "hypothesis", "evidence", "check")
                self.assertEqual(
                    ledger.consider_correction(task_key="task", correction_key="same", record=record),
                    "correction_allowed",
                )
                self.assertEqual(
                    ledger.consider_correction(task_key="task", correction_key="same", record=record),
                    "diagnosis_required",
                )

            barrier = threading.Barrier(2)
            outcomes: list[str] = []
            outcome_lock = threading.Lock()

            def finish() -> None:
                with StateStore(project, home=home) as concurrent_store:
                    concurrent = InterventionLedger(concurrent_store, run.local_id)
                    barrier.wait()
                    try:
                        outcome = concurrent.finish_diagnosis(
                            task_key="task",
                            diagnosis_evidence="new exact evidence",
                        )
                    except OrchestrateError as exc:
                        outcome = exc.code
                    with outcome_lock:
                        outcomes.append(outcome)

            threads = [threading.Thread(target=finish) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
                self.assertFalse(thread.is_alive())
            self.assertCountEqual(outcomes, ["correction_allowed", "diagnosis_not_authorized"])


if __name__ == "__main__":
    unittest.main()
