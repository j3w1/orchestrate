from __future__ import annotations

from pathlib import Path
import tempfile
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


if __name__ == "__main__":
    unittest.main()
