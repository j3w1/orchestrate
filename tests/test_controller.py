from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.controller import _mutation, _process_delivery, _request_id, answer, implement, reconcile_intentions, resume
from orchestrate.errors import OrchestrateError
from orchestrate.profile import setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from orchestrate.state import StateStore


def git(root: Path, *arguments: str) -> bytes:
    return subprocess.run(("git", "-C", str(root), *arguments), check=True, capture_output=True).stdout


class FakeClient:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if not self.responses:
            raise AssertionError(f"Unexpected Orca call: {arguments}")
        return self.responses.pop(0)


def completion_responses(outcome: str = "succeeded") -> list[dict[str, object]]:
    native_status = "completed" if outcome == "succeeded" else "failed"
    return [
        {"id": "request_run", "result": {"run": {"id": "run_1"}}},
        {"id": "request_task", "result": {"task": {"id": "task_1"}}},
        {"id": "request_worker", "result": {"dispatch": {"id": "dispatch_1"}}},
        {
            "result": {
                "deliveryId": "delivery_1",
                "messages": [
                    {
                        "id": "message_done",
                        "type": "worker_done",
                        "subject": "done",
                        "payload": {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": outcome},
                    }
                ],
            }
        },
        {
            "result": {
                "dispatch": {
                    "id": "dispatch_1",
                    "run_id": "run_1",
                    "task_id": "task_1",
                    "status": native_status,
                }
            }
        },
        {"result": {"tasks": [{"id": "task_1", "run_id": "run_1", "status": native_status}]}},
        {"id": "request_release", "result": {"releaseState": "released"}},
        {"result": {"terminalResource": {"releaseState": "released"}}},
        {"id": "request_ack", "result": {"messages": []}},
    ]


class ControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.project_temp = tempfile.TemporaryDirectory()
        self.state_temp = tempfile.TemporaryDirectory()
        self.root = Path(self.project_temp.name)
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Preserve WIP.\n", encoding="utf-8")
        (self.root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")
        setup_project(self.root)
        self.environment = patch.dict(
            os.environ,
            {
                "ORCHESTRATE_HOME": self.state_temp.name,
                "ORCA_TERMINAL_HANDLE": "term_plain",
                "ORCA_AGENT_HOOK_TOKEN": "",
                "ORCA_AGENT_HOOK_ENDPOINT": "",
                "ORCA_AGENT_LAUNCH_TOKEN": "",
            },
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()
        self.project_temp.cleanup()
        self.state_temp.cleanup()

    def test_single_worker_happy_path_releases_before_ack_and_preserves_wip(self) -> None:
        before = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        client = FakeClient(completion_responses())
        report = implement(self.root, "Make the bounded change", client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        after = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")

        self.assertEqual(before, after)
        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "pending")
        self.assertIn("independent verification", report["nextObligation"])
        release_index = next(i for i, call in enumerate(client.calls) if "worker-release" in call)
        ack_index = next(i for i, call in enumerate(client.calls) if "--ack" in call)
        self.assertLess(release_index, ack_index)
        worker_start = next(call for call in client.calls if "worker-start" in call)
        self.assertIn("gpt-5.6-sol", worker_start)
        self.assertIn("high", worker_start)

    def test_controller_refuses_agent_context_even_with_legacy_bypass_variable(self) -> None:
        client = FakeClient([])
        with patch.dict(
            os.environ,
            {"ORCA_AGENT_HOOK_TOKEN": "agent", "ORCHESTRATE_ALLOW_AGENT_CONTROLLER": "1"},
        ):
            with self.assertRaises(OrchestrateError) as caught:
                implement(self.root, "Do not dispatch", client=client)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "agent_terminal_not_controller")
        self.assertEqual(client.calls, [])

    def test_controller_requires_exact_current_worktree_before_mutation(self) -> None:
        client = FakeClient([{"result": {"worktree": {"path": str(self.root.parent)}}}])
        with self.assertRaises(OrchestrateError) as caught:
            implement(self.root, "Do not dispatch", client=client)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "controller_worktree_mismatch")
        self.assertEqual(len(client.calls), 1)

    def test_failed_worker_is_not_reported_as_success_or_verification(self) -> None:
        client = FakeClient(completion_responses("failed"))
        report = implement(self.root, "Fail honestly", client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertEqual(report["verification"], "not_run")

    def test_confirmed_external_terminal_retention_is_valid_release_disposition(self) -> None:
        responses = completion_responses()
        responses[7] = {
            "result": {
                "terminalResource": {
                    "releaseState": "retained",
                    "ownershipKind": "external_terminal",
                }
            }
        }
        report = implement(self.root, "Retain reused terminal", client=FakeClient(responses), wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "worker_succeeded")

    def test_question_delivery_remains_unacknowledged(self) -> None:
        responses = completion_responses()[:3]
        responses.append(
            {
                "result": {
                    "deliveryId": "delivery_q",
                    "messages": [
                        {
                            "id": "question_1",
                            "type": "question",
                            "body": "Choose A or B",
                            "payload": {"taskId": "task_1", "dispatchId": "dispatch_1"},
                        }
                    ],
                }
            }
        )
        client = FakeClient(responses)
        report = implement(self.root, "Ask when blocked", client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(report["pendingQuestions"], ["question_1"])
        self.assertFalse(any("--ack" in call for call in client.calls))

        answer_client = FakeClient(
            [
                {"id": "request_use", "result": {"run": {"id": "run_1"}}},
                {"id": "request_reply", "result": {"message": {"id": "reply_1"}}},
                {"id": "request_ack", "result": {"messages": []}},
            ]
        )
        answered = answer(
            self.root,
            str(report["runId"]),
            "question_1",
            "Choose A",
            client=answer_client,  # type: ignore[arg-type]
            require_context=False,
        )
        self.assertEqual(answered["pendingQuestions"], [])
        self.assertIn("run-use", answer_client.calls[0])
        self.assertIn("reply", answer_client.calls[1])
        self.assertIn("--ack", answer_client.calls[2])

    def test_uncertain_request_replays_exact_identity(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create", "--objective", "recover"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            client = FakeClient(
                [
                    {"result": {"status": "completed"}},
                    {"id": "request_1", "result": {"run": {"id": "run_recovered"}}},
                ]
            )
            recovered = reconcile_intentions(client, store, run)  # type: ignore[arg-type]
            self.assertEqual(recovered.native_run_id, "run_recovered")
            self.assertIn("--retry-request", client.calls[1])
            self.assertIn("request_1", client.calls[1])

    def test_native_mutation_request_id_wins_over_transport_correlation(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            first = FakeClient(
                [
                    {
                        "id": "transport-correlation",
                        "result": {
                            "run": {"id": "run_recovered"},
                            "mutation": {"requestId": "native-mutation", "replayed": False},
                        },
                    }
                ]
            )
            _mutation(first, store, run, "run-create", ["orchestration", "run-create"])  # type: ignore[arg-type]
            row = store.connection.execute("SELECT * FROM intentions").fetchone()
            self.assertEqual(row["request_id"], "native-mutation")
            store.mark_intention(row["id"], "uncertain", request_id="native-mutation")
            recovery = FakeClient(
                [
                    {"result": {"status": "completed"}},
                    {
                        "id": "different-transport-correlation",
                        "result": {
                            "run": {"id": "run_recovered"},
                            "mutation": {"requestId": "native-mutation", "replayed": True},
                        },
                    },
                ]
            )

            recovered = reconcile_intentions(recovery, store, run)  # type: ignore[arg-type]

            self.assertEqual(recovered.native_run_id, "run_recovered")
            self.assertIn("native-mutation", recovery.calls[0])
            self.assertNotIn("transport-correlation", recovery.calls[0])
            self.assertIn("native-mutation", recovery.calls[1])
            self.assertNotIn("transport-correlation", recovery.calls[1])

    def test_error_data_mutation_receipt_is_recoverable(self) -> None:
        payload = {
            "id": "transport-correlation",
            "error": {"data": {"mutation": {"requestId": "native-error-request"}}},
        }
        self.assertEqual(_request_id(payload), "native-error-request")

    def test_prepared_but_never_invoked_intention_is_safe_to_start(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create", "--objective", "recover"])
            client = FakeClient([{"id": "request_1", "result": {"run": {"id": "run_recovered"}}}])

            recovered = reconcile_intentions(client, store, run)  # type: ignore[arg-type]

            self.assertEqual(recovered.native_run_id, "run_recovered")
            self.assertNotIn("--retry-request", client.calls[0])

    def test_replaying_old_applied_bindings_never_regresses_terminal_phase(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="complete", profile_digest="p", source_digest="s")
            receipts = (
                ("run-create", ["orchestration", "run-create"], {"result": {"run": {"id": "run_1"}}}),
                ("task-create", ["orchestration", "task-create"], {"result": {"task": {"id": "task_1"}}}),
                ("worker-start", ["orchestration", "worker-start"], {"result": {"dispatch": {"id": "dispatch_1"}}}),
            )
            current = run
            for operation, arguments, response in receipts:
                intention = store.prepare_intention(run.local_id, operation, arguments)
                store.mark_intention(intention, "applied", response=response)
                current = reconcile_intentions(FakeClient([]), store, current)  # type: ignore[arg-type]
            current = store.update_run(current.local_id, phase="worker_succeeded", worker_outcome="succeeded")

            replayed = reconcile_intentions(FakeClient([]), store, current)  # type: ignore[arg-type]

            self.assertEqual(replayed.phase, "worker_succeeded")
            self.assertEqual(replayed.worker_outcome, "succeeded")

    def test_missing_request_history_preserves_uncertainty(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create"])
            store.mark_intention(intention, "uncertain")
            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "unknown_external_effect")

    def test_replayed_processed_delivery_does_not_release_twice(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="replay", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            message = {
                "id": "message_done",
                "type": "worker_done",
                "subject": "done",
                "payload": {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
            }
            payload = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
            store.journal_delivery(run.local_id, "delivery_1", payload, [message])
            client = FakeClient(completion_responses()[4:8])
            completed = _process_delivery(client, store, run, "delivery_1", [message])  # type: ignore[arg-type]
            replay_client = FakeClient([])
            replayed = _process_delivery(replay_client, store, completed, "delivery_1", [message])  # type: ignore[arg-type]

            self.assertEqual(replayed.phase, "worker_succeeded")
            self.assertEqual(replay_client.calls, [])
            self.assertEqual(len(store.evidence(run.local_id)), 1)

    def test_escalation_cannot_be_overwritten_by_done_in_same_delivery(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="escalate", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            binding = {"taskId": "task_1", "dispatchId": "dispatch_1"}
            messages = [
                {"id": "message_escalation", "type": "escalation", "subject": "blocked", "payload": binding},
                {
                    "id": "message_done",
                    "type": "worker_done",
                    "subject": "done",
                    "payload": {**binding, "outcome": "succeeded"},
                },
            ]
            payload = {"result": {"deliveryId": "delivery_1", "messages": messages}}
            store.journal_delivery(run.local_id, "delivery_1", payload, messages)
            result = _process_delivery(
                FakeClient(completion_responses()[4:8]),  # type: ignore[arg-type]
                store,
                run,
                "delivery_1",
                messages,
            )

            self.assertEqual(result.phase, "blocked")
            self.assertEqual(result.worker_outcome, "succeeded")

    def test_resume_recovers_missing_packet_before_worker_start(self) -> None:
        profile = setup_project(self.root)
        objective = "Recover the exact task packet"
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(
                objective=objective,
                profile_digest=profile.digest,
                source_digest=sources.digest,
            )
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                phase="task_created",
            )
            local_id = run.local_id
        client = FakeClient(
            [
                {"id": "request_use", "result": {"run": {"id": "run_1"}}},
                {"id": "request_worker", "result": {"dispatch": {"id": "dispatch_1"}}},
                {
                    "result": {
                        "deliveryId": "delivery_q",
                        "messages": [
                            {
                                "id": "question_1",
                                "type": "question",
                                "body": "Continue?",
                                "payload": {"taskId": "task_1", "dispatchId": "dispatch_1"},
                            }
                        ],
                    }
                },
            ]
        )

        result = resume(self.root, local_id, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]

        self.assertEqual(result["pendingQuestions"], ["question_1"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            recovered = store.get_packet(local_id, "task_1")
        self.assertEqual(recovered["native"]["taskId"], "task_1")
        worker_start = next(call for call in client.calls if "worker-start" in call)
        self.assertIn(f"path:{self.root.resolve()}", worker_start)


if __name__ == "__main__":
    unittest.main()
