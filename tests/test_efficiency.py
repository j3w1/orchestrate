from __future__ import annotations

from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.coordination import load_stored_milestone_plan, next_session_action, WorkerSession
from orchestrate.cli import main as cli_main
from orchestrate.efficiency import (
    capacity_grant,
    efficiency_report,
    ensure_capacity_grant,
    initialize_efficiency_observation,
    occupied_milestone_tasks,
    record_efficiency_event,
    session_age_state,
)
from orchestrate.errors import OrchestrateError
from orchestrate.intervention import run_intervention
from orchestrate.state import StateStore


def plan_json(*, objective: str = "integrate", max_workers: int | None = None) -> str:
    value: dict[str, object] = {
        "schema": "orchestrate-milestone-plan/v1",
        "contract": {"interface": "fixed"},
        "tasks": [
            {"key": "owner", "title": "Owner", "spec": objective, "role": "owner", "dependencies": [], "gate": "integration"},
            {"key": "check", "title": "Check", "spec": "verify", "role": "specialist", "dependencies": ["owner"], "gate": "verification"},
            {"key": "review", "title": "Review", "spec": "review", "role": "reviewer", "dependencies": ["owner", "check"], "gate": "review"},
        ],
    }
    if max_workers is not None:
        value["maxWorkers"] = max_workers
    return json.dumps(value, ensure_ascii=False, indent=2)


class EfficiencyTests(unittest.TestCase):
    def test_omitted_plan_capacity_defaults_to_two_and_digest_binds_original_bytes(self) -> None:
        original = plan_json()
        loaded = load_stored_milestone_plan("plan.json", original, objective="integrate", candidate_digest="candidate")
        reformatted = json.dumps(json.loads(original), separators=(",", ":"))
        changed = load_stored_milestone_plan("plan.json", reformatted, objective="integrate", candidate_digest="candidate")
        self.assertEqual(loaded.plan.max_workers, 2)
        self.assertNotEqual(loaded.digest, changed.digest)
        self.assertEqual(loaded.canonical_json, original)

    def test_exceptional_grant_is_exact_per_run_and_plan(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="wide", profile_digest="p", source_digest="s")
                run = store.update_run(run.local_id, native_run_id="run_wide")
                with self.assertRaises(OrchestrateError) as missing:
                    ensure_capacity_grant(
                        store,
                        run,
                        plan_digest="plan_a",
                        requested_limit=5,
                        allow_exceptional_capacity=False,
                        capacity_reason=None,
                    )
                self.assertEqual(missing.exception.code, "exceptional_capacity_required")
                grant = ensure_capacity_grant(
                    store,
                    run,
                    plan_digest="plan_a",
                    requested_limit=5,
                    allow_exceptional_capacity=True,
                    capacity_reason="Five independent verification fixtures",
                )
                self.assertEqual(grant["limit"], 5)  # type: ignore[index]
                self.assertEqual(capacity_grant(store, run.local_id), grant)
                self.assertEqual(
                    ensure_capacity_grant(
                        store,
                        run,
                        plan_digest="plan_a",
                        requested_limit=5,
                        allow_exceptional_capacity=False,
                        capacity_reason=None,
                    ),
                    grant,
                )
                with self.assertRaises(OrchestrateError) as inherited:
                    ensure_capacity_grant(
                        store,
                        run,
                        plan_digest="plan_b",
                        requested_limit=5,
                        allow_exceptional_capacity=False,
                        capacity_reason=None,
                    )
                self.assertEqual(inherited.exception.code, "capacity_grant_conflict")

    def test_malformed_or_legacy_wide_capacity_never_grants_launches(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="legacy wide", profile_digest="p", source_digest="s")
                run = store.update_run(run.local_id, native_run_id="run_legacy")
                with self.assertRaises(OrchestrateError) as legacy:
                    ensure_capacity_grant(
                        store,
                        run,
                        plan_digest="plan_legacy",
                        requested_limit=8,
                        allow_exceptional_capacity=False,
                        capacity_reason=None,
                    )
                self.assertEqual(legacy.exception.code, "exceptional_capacity_required")
                store.add_evidence(
                    run.local_id,
                    kind="capacity-grant",
                    status="granted",
                    subject="plan_legacy",
                    payload={"limit": 8},
                )
                with self.assertRaises(OrchestrateError) as malformed:
                    capacity_grant(store, run.local_id)
                self.assertEqual(malformed.exception.code, "capacity_grant_conflict")

    def test_uncertain_launch_and_unresolved_release_hold_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="waves", profile_digest="p", source_digest="s")
                intention = store.prepare_intention(run.local_id, "milestone-worker-start:uncertain", ["worker-start"])
                store.mark_intention(intention, "uncertain", request_id="request_1")
                now = datetime.now(timezone.utc).isoformat()
                store.connection.execute(
                    "INSERT INTO milestone_worker_bindings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'uncertain', ?, ?, ?)",
                    (run.local_id, "releasing", "task_2", "dispatch_2", "specialist", "codex", "resource_2", "term_2", "wt_2", "{}", now, now),
                )
                self.assertEqual(occupied_milestone_tasks(store, run.local_id), {"uncertain", "releasing"})

    def test_efficiency_reports_unknown_history_and_observed_session_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                historical = store.create_run(objective="old", profile_digest="p", source_digest="s")
                historical_report = efficiency_report(store, historical)
                self.assertEqual(historical_report["sessionsCreated"], "unknown")
                self.assertEqual(historical_report["usage"]["controllerModelCalls"], "unknown")  # type: ignore[index]
                current = store.create_run(objective="new", profile_digest="p", source_digest="s")
                initialize_efficiency_observation(store, current)
                initialize_efficiency_observation(store, current)
                created = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
                record_efficiency_event(store, current.local_id, event="session_created", identity="dispatch_1", observed_at=created)
                record_efficiency_event(store, current.local_id, event="session_created", identity="dispatch_1", observed_at=created)
                report = efficiency_report(store, current)
                self.assertEqual(report["sessionsCreated"], 1)
                self.assertEqual(report["activeSessions"], 1)
                self.assertEqual(report["peakSessions"], 1)
                self.assertEqual(report["sessionCountBasis"], "OBSERVED")
                self.assertEqual(report["usage"]["workerTokens"], "unknown")  # type: ignore[index]
                self.assertEqual(report["usage"]["controllerModelCalls"]["value"], 0)  # type: ignore[index]
                self.assertEqual(len(report["events"]), 2)
        self.assertEqual(session_age_state(7199), "normal")
        self.assertEqual(session_age_state(7200), "warning")
        self.assertEqual(session_age_state(28799), "warning")
        self.assertEqual(session_age_state(28800), "fresh_session_required")

    def test_simulated_thirty_minute_healthy_wait_has_zero_controller_model_calls(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="healthy wait", profile_digest="p", source_digest="s")
                initialize_efficiency_observation(store, run)
                created = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
                record_efficiency_event(store, run.local_id, event="session_created", identity="dispatch_wait", observed_at=created)
                report = efficiency_report(store, run)
        self.assertEqual(report["usage"]["controllerModelCalls"], {"value": 0, "scope": "instrumented deterministic controller only"})  # type: ignore[index]
        self.assertEqual(report["repeatedLaunchAttempts"], 0)
        self.assertEqual(report["sessions"][0]["durationKind"], "observed wall time; not compute time")  # type: ignore[index]

    def test_every_new_task_requires_a_fresh_session(self) -> None:
        session = WorkerSession("dispatch", "task", "term", "resource", "worktree", "codex", "run")
        action = next_session_action(session, next_task_id="next", next_agent="codex")
        self.assertEqual(action.kind, "release")
        self.assertNotIn("--terminal", action.argv)

    def test_operator_intervention_binds_existing_task_and_strict_input(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            project = Path(project_dir)
            subprocess.run(("git", "init", "-q", project), check=True)
            with patch.dict(os.environ, {"ORCHESTRATE_HOME": home_dir}):
                with StateStore(project) as store:
                    run = store.create_run(objective="fix", profile_digest="p", source_digest="s")
                    run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1")
                record = project / "record.json"
                record.write_text(
                    json.dumps(
                        {
                            "obligation": "reject mixed aliases",
                            "failing_example": "mixed payload accepted",
                            "hypothesis": "normalization is early",
                            "last_meaningful_evidence": "fixture 4 failed",
                            "next_discriminating_check": "run the shape matrix",
                            "correction_key": "move-check",
                        }
                    ),
                    encoding="utf-8",
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    exit_code = cli_main(
                        [
                            "intervention",
                            "--project",
                            str(project),
                            "--run",
                            run.local_id,
                            "--task",
                            "task_1",
                            "--record",
                            str(record),
                            "--json",
                        ]
                    )
                first = json.loads(output.getvalue())
                second = run_intervention(project, run_id=run.local_id, task="task_1", record_path=record, diagnosis_path=None)
                malformed = project / "malformed.json"
                malformed.write_text(
                    '{"obligation":"one","obligation":"two"}',
                    encoding="utf-8",
                )
                with self.assertRaises(OrchestrateError) as rejected:
                    run_intervention(
                        project,
                        run_id=run.local_id,
                        task="task_1",
                        record_path=malformed,
                        diagnosis_path=None,
                    )
            self.assertEqual(exit_code, 0)
            self.assertEqual(first["status"], "correction_allowed")
            self.assertEqual(second["status"], "diagnosis_required")
            self.assertEqual(rejected.exception.code, "intervention_input_invalid")

    def test_public_intervention_consumes_productive_evidence_before_genuine_change(self) -> None:
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            project = Path(project_dir)
            subprocess.run(("git", "init", "-q", project), check=True)
            with patch.dict(os.environ, {"ORCHESTRATE_HOME": home_dir}):
                with StateStore(project) as store:
                    run = store.create_run(objective="fix", profile_digest="p", source_digest="s")
                    run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1")

                def write_record(path: Path, evidence: str) -> None:
                    path.write_text(
                        json.dumps(
                            {
                                "obligation": "reject mixed aliases",
                                "failing_example": "mixed payload accepted",
                                "hypothesis": "normalization is early",
                                "last_meaningful_evidence": evidence,
                                "next_discriminating_check": "run the shape matrix",
                                "correction_key": "move-check",
                            }
                        ),
                        encoding="utf-8",
                    )

                original = project / "original.json"
                productive = project / "productive.json"
                fresh = project / "fresh.json"
                diagnosis = project / "diagnosis.json"
                write_record(original, "fixture 4 failed")
                write_record(productive, "diagnosis isolated shape selection")
                write_record(fresh, "new fixture 7 failed")
                diagnosis.write_text(
                    json.dumps({"diagnosis_evidence": "diagnosis isolated shape selection"}),
                    encoding="utf-8",
                )

                decisions = [
                    run_intervention(project, run_id=run.local_id, task="task_1", record_path=original, diagnosis_path=None)["status"],
                    run_intervention(project, run_id=run.local_id, task="task_1", record_path=original, diagnosis_path=None)["status"],
                    run_intervention(project, run_id=run.local_id, task="task_1", record_path=None, diagnosis_path=diagnosis)["status"],
                    run_intervention(project, run_id=run.local_id, task="task_1", record_path=original, diagnosis_path=None)["status"],
                    run_intervention(project, run_id=run.local_id, task="task_1", record_path=productive, diagnosis_path=None)["status"],
                    run_intervention(project, run_id=run.local_id, task="task_1", record_path=fresh, diagnosis_path=None)["status"],
                ]
            self.assertEqual(
                decisions,
                [
                    "correction_allowed",
                    "diagnosis_required",
                    "correction_allowed",
                    "unresolved",
                    "unresolved",
                    "correction_allowed",
                ],
            )


if __name__ == "__main__":
    unittest.main()
