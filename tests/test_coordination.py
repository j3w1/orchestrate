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
from orchestrate.orca import OrcaCommandError, OrcaCommandResult
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
            task_title = arguments[arguments.index("--task-title") + 1]
            spec = arguments[arguments.index("--spec") + 1]
            deps = (
                json.loads(arguments[arguments.index("--deps") + 1])
                if "--deps" in arguments
                else []
            )
            status = "pending" if deps else "ready"
            self.rows.append(
                {
                    "id": task_id,
                    "run_id": run_id,
                    "task_title": task_title,
                    "spec": spec,
                    "deps": deps,
                    "status": status,
                }
            )
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


class RecoveringGateClient(FakeDagClient):
    def __init__(self) -> None:
        super().__init__()
        self.gates: list[dict[str, object]] = []
        self.lose_create = True
        self.lose_resolve = True

    def run_json(self, *arguments: str, **keywords: object) -> dict[str, object]:
        if arguments[:2] == ("orchestration", "gate-create"):
            self.calls.append(arguments)
            gate = {
                "id": f"gate_{len(self.gates) + 1}",
                "task_id": arguments[arguments.index("--task") + 1],
                "question": arguments[arguments.index("--question") + 1],
                "status": "pending",
                "resolution": None,
            }
            self.gates.append(gate)
            response = {
                "result": {
                    "gate": gate,
                    "mutation": {"requestId": f"request_{gate['id']}", "replayed": False},
                }
            }
            if self.lose_create:
                self.lose_create = False
                raise OrcaCommandError(
                    "lost create receipt",
                    OrcaCommandResult(arguments, -1, "", "lost", response),
                )
            return response
        if arguments[:2] == ("orchestration", "gate-list"):
            self.calls.append(arguments)
            task_id = arguments[arguments.index("--task") + 1]
            return {"result": {"gates": [dict(gate) for gate in self.gates if gate["task_id"] == task_id]}}
        if arguments[:2] == ("orchestration", "gate-resolve"):
            self.calls.append(arguments)
            gate_id = arguments[arguments.index("--id") + 1]
            gate = next(item for item in self.gates if item["id"] == gate_id)
            gate["status"] = "resolved"
            gate["resolution"] = arguments[arguments.index("--resolution") + 1]
            response = {
                "result": {
                    "gate": dict(gate),
                    "mutation": {"requestId": f"request_resolve_{gate_id}", "replayed": False},
                }
            }
            if self.lose_resolve:
                self.lose_resolve = False
                raise OrcaCommandError(
                    "lost resolve receipt",
                    OrcaCommandResult(arguments, -1, "", "lost", response),
                )
            return response
        return super().run_json(*arguments, **keywords)


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
                client.rows[1]["task_title"] = "Altered native Task title"
                with self.assertRaises(OrchestrateError) as altered_title:
                    NativeDagScheduler(client, store, run.local_id).create("run_1", milestone())  # type: ignore[arg-type]
                self.assertEqual(altered_title.exception.code, "native_task_binding_mismatch")

        self.assertEqual(bindings["unit"].dependencies, ("task_1",))
        self.assertEqual(bindings["incident"].dependencies, ("task_1",))
        self.assertEqual(bindings["review"].dependencies, ("task_1", "task_2", "task_3"))
        task_creates = [call for call in client.calls if call[:2] == ("orchestration", "task-create")]
        self.assertEqual(len(task_creates), 4)
        self.assertNotIn("--deps", task_creates[0])
        self.assertEqual(json.loads(task_creates[1][task_creates[1].index("--deps") + 1]), ["task_1"])

    def test_native_ready_frontier_uses_deterministic_remaining_capacity(self) -> None:
        plan = replace(milestone(), max_workers=1)
        client = FakeDagClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="bounded frontier", profile_digest="p", source_digest="s")
                scheduler = NativeDagScheduler(client, store, run.local_id)  # type: ignore[arg-type]
                bindings = scheduler.create("run_1", plan)
                for row in client.rows:
                    if row["id"] == bindings["owner"].task_id:
                        row["status"] = "completed"
                    elif row["id"] in {bindings["unit"].task_id, bindings["incident"].task_id}:
                        row["status"] = "ready"
                client.rows.reverse()

                selected = scheduler.ready_wave(
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

        self.assertEqual([binding.key for binding in selected], ["unit"])
        self.assertEqual(at_capacity, ())

    def test_native_ready_frontier_rejects_active_and_finished_ready_contradictions(self) -> None:
        plan = replace(milestone(), max_workers=1)
        for contradictory_key, active, finished in (
            ("unit", {"unit"}, {"owner"}),
            ("unit", set(), {"owner", "unit"}),
        ):
            with self.subTest(contradictory_key=contradictory_key, active=active):
                client = FakeDagClient()
                with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
                    with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                        run = store.create_run(objective="contradictory frontier", profile_digest="p", source_digest="s")
                        scheduler = NativeDagScheduler(client, store, run.local_id)  # type: ignore[arg-type]
                        bindings = scheduler.create("run_1", plan)
                        for row in client.rows:
                            row["status"] = (
                                "ready"
                                if row["id"] == bindings[contradictory_key].task_id
                                else "completed" if row["id"] == bindings["owner"].task_id else "pending"
                            )

                        with self.assertRaises(OrchestrateError) as caught:
                            scheduler.ready_wave(
                                "run_1",
                                plan,
                                bindings,
                                active=active,
                                finished=finished,
                                remaining_capacity=plan.max_workers - len(active),
                            )

                self.assertEqual(caught.exception.code, "native_ready_gate_mismatch")

    def test_native_ready_frontier_selects_valid_later_work_around_active_and_finished_tasks(self) -> None:
        plan = replace(milestone(), max_workers=2)
        client = FakeDagClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="mixed later wave", profile_digest="p", source_digest="s")
                scheduler = NativeDagScheduler(client, store, run.local_id)  # type: ignore[arg-type]
                bindings = scheduler.create("run_1", plan)
                for row in client.rows:
                    if row["id"] == bindings["owner"].task_id:
                        row["status"] = "completed"
                    elif row["id"] == bindings["unit"].task_id:
                        row["status"] = "dispatched"
                    elif row["id"] == bindings["incident"].task_id:
                        row["status"] = "ready"

                selected = scheduler.ready_wave(
                    "run_1",
                    plan,
                    bindings,
                    active={"unit"},
                    finished={"owner"},
                    remaining_capacity=1,
                )

        self.assertEqual([binding.key for binding in selected], ["incident"])

    def test_native_gate_intentions_recover_by_exact_readback_without_duplicate_mutation(self) -> None:
        client = RecoveringGateClient()
        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as home_dir:
            with StateStore(Path(project_dir), home=Path(home_dir)) as store:
                run = store.create_run(objective="milestone", profile_digest="p", source_digest="s")
                scheduler = NativeDagScheduler(client, store, run.local_id)  # type: ignore[arg-type]
                bindings = scheduler.create("run_1", milestone())
                with self.assertRaises(OrchestrateError) as create_lost:
                    scheduler.create_gates(milestone(), bindings)
                self.assertEqual(create_lost.exception.code, "mutation_outcome_uncertain")

                gates = scheduler.create_gates(milestone(), bindings)
                with self.assertRaises(OrchestrateError) as resolve_lost:
                    scheduler.resolve_gate(gates["unit"], resolution="accepted")
                self.assertEqual(resolve_lost.exception.code, "mutation_outcome_uncertain")

                recovered = scheduler.create_gates(milestone(), bindings)
                self.assertEqual(recovered["unit"].status, "resolved")
                self.assertEqual(recovered["unit"].resolution, "accepted")

        creates = [call for call in client.calls if call[:2] == ("orchestration", "gate-create")]
        resolves = [call for call in client.calls if call[:2] == ("orchestration", "gate-resolve")]
        self.assertEqual(len(creates), 3)
        self.assertEqual(len(resolves), 1)

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

        rejected = ReviewEvidence("task_review", plan.candidate_digest, plan.contract.digest, "rejected")
        with self.assertRaises(OrchestrateError) as rejected_error:
            require_current_review(
                rejected,
                candidate_digest=plan.candidate_digest,
                contract_digest=plan.contract.digest,
                task_id="task_review",
                settled=True,
            )
        self.assertEqual(rejected_error.exception.code, "review_rejected")

        with self.assertRaises(OrchestrateError) as wrong_task:
            require_current_review(
                review,
                candidate_digest=plan.candidate_digest,
                contract_digest=plan.contract.digest,
                task_id="task_other",
                settled=True,
            )
        self.assertEqual(wrong_task.exception.code, "review_gate_invalid")

    def test_shared_contract_is_deeply_immutable_and_digest_bound(self) -> None:
        source = {"nested": {"version": 1}, "items": [{"name": "fixed"}]}
        contract = SharedContract.draft(source).settle()
        digest = contract.digest
        source["nested"]["version"] = 2
        source["items"][0]["name"] = "changed"

        self.assertEqual(contract.value["nested"]["version"], 1)  # type: ignore[index]
        self.assertEqual(contract.value["items"][0]["name"], "fixed")  # type: ignore[index]
        self.assertEqual(contract.digest, digest)
        with self.assertRaises(TypeError):
            contract.value["added"] = True  # type: ignore[index]
        with self.assertRaises(TypeError):
            contract.value["nested"]["version"] = 3  # type: ignore[index]
        with self.assertRaises(OrchestrateError) as mismatch:
            SharedContract(digest, "settled", {"nested": {"version": 9}})
        self.assertEqual(mismatch.exception.code, "shared_contract_digest_mismatch")

    def test_exact_session_is_reused_only_for_immediate_same_agent_work(self) -> None:
        session = WorkerSession(
            "dispatch_1", "task_1", "term_exact", "resource_1", "worktree_1", "codex", "run_1"
        )
        reused = next_session_action(session, next_task_id="task_2", next_agent="codex")
        self.assertEqual(reused.kind, "reuse")
        self.assertEqual(
            reused.argv[-4:],
            ("--terminal", "term_exact", "--worktree", "id:worktree_1"),
        )
        released = next_session_action(session, next_task_id="task_2", next_agent="claude")
        self.assertEqual(released.kind, "release")
        self.assertEqual(released.argv[-1], "dispatch_1")

    def test_wsl_release_unknown_is_contained_without_a_second_release(self) -> None:
        session = WorkerSession(
            "dispatch_wsl", "task_1", "term_wsl", "resource_wsl", "wt_wsl", "codex", "run_1"
        )
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
                        "status": "completed",
                        "lastFailure": None,
                    },
                    "worker": {
                        "dispatchId": "dispatch_wsl",
                        "worktreeId": "wt_wsl",
                        "agentTerminalHandle": "term_wsl",
                        "state": "succeeded",
                        "stage": "settled",
                        "lastError": None,
                    },
                    "terminalResource": {
                        "id": "resource_wsl",
                        "terminalHandle": "term_wsl",
                        "worktreeId": "wt_wsl",
                        "originDispatchId": "dispatch_wsl",
                        "ownerDispatchId": "dispatch_wsl",
                        "ownershipState": "owned",
                        "releaseState": "not_requested",
                        "retainedReason": None,
                    },
                }
            },
        )
        self.assertEqual(decision.state, "uncertain")
        self.assertFalse(decision.repeat_release)
        self.assertEqual(decision.recovery[0], "orca-ide")  # type: ignore[index]
        self.assertEqual(decision.recovery_metadata["processAction"], "none")  # type: ignore[index]

    def test_identity_only_release_readback_cannot_report_settlement(self) -> None:
        session = WorkerSession(
            "dispatch_1", "task_1", "term_1", "resource_1", "wt_1", "codex", "run_1"
        )
        readback = {
            "result": {
                "dispatch": {
                    "id": "dispatch_1",
                    "runId": "run_1",
                    "taskId": "task_1",
                    "task_id": "task_1",
                    "status": "completed",
                    "lastFailure": None,
                },
                "worker": {
                    "dispatchId": "dispatch_1",
                    "worktreeId": "wt_1",
                    "agentTerminalHandle": "term_1",
                    "state": "succeeded",
                    "stage": "settled",
                    "lastError": None,
                },
                "terminal": {"handle": "term_1"},
                "terminalResource": {
                    "id": "resource_1",
                    "terminalHandle": "term_1",
                    "worktreeId": "wt_1",
                    "originDispatchId": "dispatch_1",
                    "ownerDispatchId": "dispatch_1",
                    "ownershipState": "owned",
                    "releaseState": "not_requested",
                    "retainedReason": None,
                    "releaseError": None,
                    "releaseRequestedAt": None,
                    "releaseCompletedAt": None,
                    "archive": {"source": None, "status": None},
                },
            }
        }
        with self.assertRaises(OrchestrateError) as caught:
            validate_release_receipt(
                session,
                {"result": {"dispatchId": "dispatch_1", "state": "released"}},
                worker_readback=readback,
            )
        self.assertEqual(caught.exception.code, "release_unconfirmed")

    def test_stored_pending_release_converges_from_exact_released_readback(self) -> None:
        session = WorkerSession(
            "dispatch_1", "task_1", "term_1", "resource_1", "wt_1", "codex", "run_1"
        )
        decision = validate_release_receipt(
            session,
            {
                "result": {
                    "dispatchId": "dispatch_1",
                    "state": "release_pending",
                    "processAction": "none",
                    "recovery": "wait for Orca's committed release recovery, then read worker-show",
                }
            },
            worker_readback={
                "result": {
                    "dispatch": {
                        "id": "dispatch_1",
                        "runId": "run_1",
                        "taskId": "task_1",
                        "task_id": "task_1",
                        "status": "completed",
                        "lastFailure": None,
                    },
                    "worker": {
                        "dispatchId": "dispatch_1",
                        "worktreeId": "wt_1",
                        "agentTerminalHandle": "term_1",
                        "state": "succeeded",
                        "stage": "settled",
                        "lastError": None,
                    },
                    "terminal": None,
                    "terminalResource": {
                        "id": "resource_1",
                        "terminalHandle": "term_1",
                        "worktreeId": "wt_1",
                        "originDispatchId": "dispatch_1",
                        "ownerDispatchId": "dispatch_1",
                        "ownershipState": "released",
                        "releaseState": "released",
                        "retainedReason": None,
                        "releaseError": None,
                        "releaseRequestedAt": "2026-01-01T00:00:00Z",
                        "releaseCompletedAt": "2026-01-01T00:00:01Z",
                        "archive": {"source": "transcript", "status": "captured"},
                    },
                }
            },
        )

        self.assertEqual(decision.state, "released")
        self.assertFalse(decision.repeat_release)
        self.assertEqual(decision.recovery_metadata["processAction"], "none")  # type: ignore[index]

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

        inherited = replace(roster.specialist, agent="codex", model=None, effort=None)
        with self.assertRaises(OrchestrateError) as hidden_effective:
            validate_effective_launch(
                inherited,
                {
                    "requested": {"agent": "codex"},
                    "effective": {"agent": "codex", "model": "substituted", "effort": "low"},
                },
            )
        self.assertEqual(hidden_effective.exception.code, "worker_launch_mismatch")

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
