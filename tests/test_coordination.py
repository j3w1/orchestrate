from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from orchestrate.config import CONFIG_SCHEMA, load_role_roster
from orchestrate.coordination import (
    MilestonePlan,
    MilestoneTask,
    NativeDagScheduler,
    ReviewEvidence,
    SharedContract,
    WorkerSession,
    next_session_action,
    require_current_review,
    validate_effective_launch,
    validate_release_receipt,
)
from orchestrate.errors import OrchestrateError
from orchestrate.state import StateStore


def milestone() -> MilestonePlan:
    contract = SharedContract.draft(
        {"api": "v1", "owner": "implementation", "specialists": ["unit", "incident"]}
    ).settle()
    candidate = "candidate_sha256_abc"
    return MilestonePlan(
        contract=contract,
        candidate_digest=candidate,
        tasks=(
            MilestoneTask(
                "owner",
                "Integrate the milestone",
                "Own overlapping source changes and settle the candidate.",
                "owner",
                gate="integration",
                integration_owner=True,
                writes_shared_contract=True,
                candidate_digest=candidate,
                contract_digest=contract.digest,
            ),
            MilestoneTask(
                "unit",
                "Run focused unit verification",
                "Read-only focused verification with an exact outcome.",
                "specialist",
                dependencies=("owner",),
                gate="verification",
                independently_useful=True,
                candidate_digest=candidate,
                contract_digest=contract.digest,
            ),
            MilestoneTask(
                "incident",
                "Run incident verification",
                "Read-only incident verification with an exact outcome.",
                "specialist",
                dependencies=("owner",),
                gate="verification",
                independently_useful=True,
                candidate_digest=candidate,
                contract_digest=contract.digest,
            ),
            MilestoneTask(
                "review",
                "Independent candidate review",
                "Review only the exact candidate after both verification gates.",
                "reviewer",
                dependencies=("owner", "unit", "incident"),
                gate="review",
                independently_useful=True,
                candidate_digest=candidate,
                contract_digest=contract.digest,
            ),
        ),
    )


class FakeDagClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.rows: list[dict[str, object]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if arguments[:2] == ("orchestration", "task-create"):
            task_id = f"task_{len(self.rows) + 1}"
            run_id = arguments[arguments.index("--run") + 1]
            spec = arguments[arguments.index("--spec") + 1]
            deps = (
                json.loads(arguments[arguments.index("--deps") + 1])
                if "--deps" in arguments
                else []
            )
            status = "pending" if deps else "ready"
            self.rows.append({"id": task_id, "run_id": run_id, "spec": spec, "deps": deps, "status": status})
            return {
                "ok": True,
                "result": {
                    "task": {"id": task_id},
                    "mutation": {"requestId": f"request_{task_id}", "replayed": False},
                },
            }
        if arguments[:2] == ("orchestration", "task-list"):
            rows = self.rows
            if "--ready" in arguments:
                rows = [row for row in rows if row["status"] == "ready"]
            return {"ok": True, "result": {"tasks": rows}}
        raise AssertionError(arguments)


class CoordinationTests(unittest.TestCase):
    def test_independent_parallel_tasks_wait_for_the_integration_owner(self) -> None:
        plan = milestone().validated()
        self.assertEqual(plan.ready_keys(set()), ("owner",))
        self.assertEqual(plan.ready_keys({"owner"}), ("unit", "incident"))
        self.assertEqual(plan.ready_keys({"owner", "unit", "incident"}), ("review",))

    def test_shared_contract_must_settle_and_only_owner_may_write_it(self) -> None:
        settled = milestone()
        draft = MilestonePlan(
            SharedContract(settled.contract.digest, "draft", settled.contract.value),
            settled.candidate_digest,
            settled.tasks,
        )
        self.assertEqual(draft.ready_keys(set()), ("owner",))
        self.assertEqual(draft.ready_keys({"owner"}), ())

        client = FakeDagClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="settle", profile_digest="p", source_digest="s")
                bindings = NativeDagScheduler(client, store, run.local_id).create("run_contract", draft)  # type: ignore[arg-type]
        self.assertEqual(tuple(bindings), ("owner",))
        self.assertEqual(sum(call[:2] == ("orchestration", "task-create") for call in client.calls), 1)

        tasks = list(settled.tasks)
        tasks[1] = replace(tasks[1], writes_shared_contract=True)
        with self.assertRaises(OrchestrateError) as writer:
            MilestonePlan(settled.contract, settled.candidate_digest, tuple(tasks)).validated()
        self.assertEqual(writer.exception.code, "parallel_task_invalid")

    def test_native_creation_serializes_dependencies_and_validates_readback(self) -> None:
        client = FakeDagClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="milestone", profile_digest="p", source_digest="s")
                bindings = NativeDagScheduler(client, store, run.local_id).create("run_1", milestone())  # type: ignore[arg-type]
                # Simulate a crash after receipts but before derived projection.
                store.connection.execute(
                    "DELETE FROM milestone_task_bindings WHERE run_local_id = ?",
                    (run.local_id,),
                )
                # Applied receipts rebuild projection without duplicate Tasks.
                repeated = NativeDagScheduler(client, store, run.local_id).create("run_1", milestone())  # type: ignore[arg-type]
                self.assertEqual(repeated, bindings)

        self.assertEqual(bindings["unit"].dependencies, ("task_1",))
        self.assertEqual(bindings["incident"].dependencies, ("task_1",))
        self.assertEqual(bindings["review"].dependencies, ("task_1", "task_2", "task_3"))
        task_creates = [call for call in client.calls if call[:2] == ("orchestration", "task-create")]
        self.assertEqual(len(task_creates), 4)
        self.assertNotIn("--deps", task_creates[0])
        self.assertEqual(json.loads(task_creates[1][task_creates[1].index("--deps") + 1]), ["task_1"])

    def test_review_is_invalidated_by_candidate_or_contract_change(self) -> None:
        plan = milestone()
        review = ReviewEvidence("task_review", plan.candidate_digest, plan.contract.digest, "accepted")
        require_current_review(
            review,
            candidate_digest=plan.candidate_digest,
            contract_digest=plan.contract.digest,
        )
        for candidate, contract in (("candidate_new", plan.contract.digest), (plan.candidate_digest, "contract_new")):
            with self.subTest(candidate=candidate, contract=contract):
                with self.assertRaises(OrchestrateError) as caught:
                    require_current_review(review, candidate_digest=candidate, contract_digest=contract)
                self.assertEqual(caught.exception.code, "stale_review")

    def test_exact_session_is_reused_only_for_immediate_same_agent_work(self) -> None:
        session = WorkerSession("dispatch_1", "task_1", "term_exact", "resource_1", "worktree_1", "codex")
        reused = next_session_action(session, next_task_id="task_2", next_agent="codex")
        self.assertEqual(reused.kind, "reuse")
        self.assertEqual(reused.argv[-2:], ("--terminal", "term_exact"))
        released = next_session_action(session, next_task_id="task_2", next_agent="claude")
        self.assertEqual(released.kind, "release")
        self.assertEqual(released.argv[-1], "dispatch_1")

    def test_wsl_release_unknown_is_contained_without_a_second_release(self) -> None:
        session = WorkerSession("dispatch_wsl", "task_1", "term_wsl", "resource_wsl", "wt_wsl", "codex")
        decision = validate_release_receipt(
            session,
            {
                "result": {
                    "dispatchId": "dispatch_wsl",
                    "state": "release_unknown",
                    "processAction": "none",
                    "nextAction": ["orca-ide", "orchestration", "worker-show", "--dispatch", "dispatch_wsl", "--json"],
                }
            },
            worker_readback={
                "result": {
                    "dispatch": {
                        "id": "dispatch_wsl",
                        "runId": "run_1",
                        "taskId": "task_1",
                        "task_id": "task_1",
                        "lastFailure": None,
                    },
                    "worker": {
                        "dispatchId": "dispatch_wsl",
                        "worktreeId": "wt_wsl",
                        "agentTerminalHandle": "term_wsl",
                        "lastError": None,
                    },
                    "terminalResource": {
                        "id": "resource_wsl",
                        "terminalHandle": "term_wsl",
                        "worktreeId": "wt_wsl",
                        "originDispatchId": "dispatch_wsl",
                        "ownerDispatchId": "dispatch_wsl",
                    },
                }
            },
        )
        self.assertEqual(decision.state, "uncertain")
        self.assertFalse(decision.repeat_release)
        self.assertEqual(decision.recovery[0], "orca-ide")  # type: ignore[index]

    def test_role_roster_accepts_explicit_claude_provider_ids_without_probing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": CONFIG_SCHEMA,
                        "roles": {
                            "owner": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                            "specialist": {"agent": "claude", "model": "claude-provider-id"},
                            "reviewer": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "xhigh"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            roster = load_role_roster(path=path)
        self.assertEqual(roster.specialist.requested(), {"agent": "claude", "model": "claude-provider-id"})
        validate_effective_launch(
            roster.specialist,
            {
                "requested": {"agent": "claude", "model": "claude-provider-id"},
                "effective": {"agent": "claude", "model": "claude-provider-id"},
            },
        )
        with self.assertRaises(OrchestrateError) as unsupported:
            validate_effective_launch(roster.specialist, {"requested": roster.specialist.requested()})
        self.assertEqual(unsupported.exception.code, "worker_launch_unsupported")
        with self.assertRaises(OrchestrateError) as substituted:
            validate_effective_launch(
                roster.specialist,
                {"requested": roster.specialist.requested(), "effective": {"agent": "claude", "model": "other"}},
            )
        self.assertEqual(substituted.exception.code, "worker_launch_mismatch")

    def test_claude_role_without_provider_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps({"schema": CONFIG_SCHEMA, "roles": {"specialist": {"agent": "claude"}}}),
                encoding="utf-8",
            )
            with self.assertRaises(OrchestrateError) as caught:
                load_role_roster(path=path)
        self.assertEqual(caught.exception.code, "user_config_invalid")


if __name__ == "__main__":
    unittest.main()
