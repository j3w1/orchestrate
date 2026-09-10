from __future__ import annotations

from collections.abc import Callable, Mapping
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.controller import (
    _mutation,
    _process_delivery,
    _release_disposition,
    _request_id,
    answer,
    implement,
    reconcile_intentions,
    resume,
)
from orchestrate.errors import OrchestrateError
from orchestrate.identity import require_plain_controller
from orchestrate.packets import canonical_packet_json, make_packet, packet_spec
from orchestrate.profile import setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from orchestrate.state import StateStore


def git(root: Path, *arguments: str) -> bytes:
    return subprocess.run(("git", "-C", str(root), *arguments), check=True, capture_output=True).stdout


Response = dict[str, object] | Callable[["FakeClient", tuple[str, ...]], dict[str, object]]


class FakeClient:
    def __init__(self, responses: list[Response]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, ...]] = []

    def run_json(self, *arguments: str, **_: object) -> dict[str, object]:
        self.calls.append(arguments)
        if not self.responses:
            raise AssertionError(f"Unexpected Orca call: {arguments}")
        response = self.responses.pop(0)
        return response(self, arguments) if callable(response) else response


def mutation(request_id: str, **result: object) -> dict[str, object]:
    return {
        "result": {
            **result,
            "mutation": {"requestId": request_id, "replayed": False},
        }
    }


def request_show(
    request_id: str,
    state: str = "completed",
    method: str = "orchestration.run-create",
) -> dict[str, object]:
    return {
        "result": {
            "requestId": request_id,
            "state": state,
            "method": method,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:01Z",
            "receipt": {},
            "interpretation": "synthetic",
        }
    }


def _task_readback(client: FakeClient, _: tuple[str, ...]) -> dict[str, object]:
    call = next(item for item in client.calls if "task-create" in item)
    spec = call[call.index("--spec") + 1]
    title = call[call.index("--task-title") + 1]
    return {
        "result": {
            "tasks": [
                {
                    "id": "task_1",
                    "run_id": "run_1",
                    "task_title": title,
                    "spec": spec,
                    "status": "ready",
                }
            ]
        }
    }


def _worker_start(root: Path) -> dict[str, object]:
    worktree_id = f"repo::{root.resolve()}"
    terminal_id = "term_worker"
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": terminal_id}
    response = mutation(
        "request_worker",
        runId="run_1",
        taskId="task_1",
        dispatchId="dispatch_1",
        state="ready",
        stage="input_accepted",
        setup={"state": "not_applicable"},
        launch=launch,
        effects=[
            {"kind": "worktree", "action": "reused", "id": worktree_id},
            {"kind": "setup", "action": "not_applicable", "state": "not_applicable"},
            terminal_effect,
            {"kind": "dispatch_input", "role": "agent", "id": terminal_id, "state": "accepted"},
        ],
        residualResources=[terminal_effect],
    )
    response["_meta"] = {"runtimeId": "runtime_test"}
    return response


def _worker_start_readback(root: Path) -> dict[str, object]:
    worktree_id = f"repo::{root.resolve()}"
    launch = {
        "requested": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
        "effective": {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
    }
    terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": "term_worker"}
    return {
        "result": {
            "dispatch": {"id": "dispatch_1", "run_id": "run_1", "task_id": "task_1", "status": "dispatched"},
            "worker": {
                "state": "ready",
                "stage": "input_accepted",
                "worktree_id": worktree_id,
                "agent_terminal_handle": "term_worker",
                "effects": [],
                "residualResources": [terminal_effect],
                "startOptions": {
                    "worktree": f"path:{root.resolve()}",
                    "resolvedWorktreeId": worktree_id,
                    "terminal": None,
                    "agent": "codex",
                    "launch": launch,
                    "setup": "not_applicable",
                    "setupSource": "existing_worktree",
                },
            },
        },
        "_meta": {"runtimeId": "runtime_test"},
    }


def _worker_start_with_preflight(root: Path) -> Response:
    def respond(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
        response = _worker_start(root)
        with StateStore(root) as store:
            run = store.select_run(None)
            packet_json = store.get_packet_json(run.local_id, str(run.task_id))
            packet = store.get_packet(run.local_id, str(run.task_id))
            launch = packet["admission"]["launch"]
            reader = packet["reader"]
            observation = {
                "schema": "orchestrate-worker-preflight/v1",
                "outcome": "passed",
                "runId": "run_1",
                "taskId": "task_1",
                "dispatchId": "dispatch_1",
                "packetId": packet["packetId"],
                "packetJsonSha256": hashlib.sha256(packet_json.encode("utf-8")).hexdigest(),
                "expectedSourceDigest": run.source_digest,
                "observedSourceDigest": run.source_digest,
                "profileDigest": packet["profileDigest"],
                "routingDigest": hashlib.sha256(
                    json.dumps(reader, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                "candidate": packet["candidate"],
                "native": {
                    "runtimeId": "runtime_test",
                    "actor": launch["agent"],
                    "terminalHandle": "term_worker",
                    "executionHostId": "local",
                    "hostPlatform": "win32",
                    "worktreeId": f"repo::{root.resolve()}",
                    "worktreeRoot": str(root.resolve()),
                    "launch": launch,
                },
                "observedAt": "2026-01-01T00:00:00Z",
                "limitations": [],
                "mismatches": [],
            }
            store.record_preflight(
                run.local_id,
                run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                observation=observation,
            )
        return response

    return respond


def settlement_responses(outcome: str = "succeeded") -> list[Response]:
    native_status = "completed" if outcome == "succeeded" else "failed"
    return [
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
        mutation(
            "request_release",
            dispatchId="dispatch_1",
            state="released",
            processAction="closed",
            archive=None,
        ),
        {
            "result": {
                "dispatch": {"id": "dispatch_1"},
                "terminalResource": {
                    "id": "terminal-resource-1",
                    "ownershipState": "released",
                    "releaseState": "released",
                    "retainedReason": None,
                    "originDispatchId": "dispatch_1",
                    "ownerDispatchId": "dispatch_1",
                    "terminalHandle": "term_worker",
                    "worktreeId": "repo::fixture",
                    "releaseRequestedAt": "2026-01-01T00:00:00Z",
                    "releaseCompletedAt": "2026-01-01T00:00:01Z",
                    "releaseError": None,
                    "archive": {"source": "transcript", "status": "captured"},
                },
            }
        },
    ]


def completion_responses(root: Path, objective: str, outcome: str = "succeeded") -> list[Response]:
    delivery = {
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
    }
    return [
        mutation("request_run", run={"id": "run_1"}),
        {"result": {"run": {"id": "run_1", "objective": objective}}},
        mutation("request_task", task={"id": "task_1"}),
        _task_readback,
        _worker_start_with_preflight(root),
        _worker_start_readback(root),
        delivery,
        *settlement_responses(outcome),
        mutation("request_ack", messages=[]),
    ]


def identity_responses(
    root: Path,
    *,
    agent: str | None = None,
    workers: list[dict[str, object]] | None = None,
    run_id: str | None = None,
) -> list[Response]:
    terminal: dict[str, object] = {
        "handle": "term_plain",
        "worktreeId": "worktree_1",
        "worktreePath": str(root),
        "executionHostId": "local",
        "connected": True,
        "writable": True,
    }
    if agent is not None:
        terminal["agentIdentity"] = agent
    return [
        {"result": {"terminal": terminal}},
        {"result": {"worktree": {"id": "worktree_1", "path": str(root)}}},
        {"result": {"workers": [] if workers is None else workers}},
        {"result": {"run": None if run_id is None else {"id": run_id}}},
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
        self.environment = patch.dict(
            os.environ,
            {
                "ORCHESTRATE_HOME": self.state_temp.name,
                "ORCA_TERMINAL_HANDLE": "term_plain",
                "ORCA_AGENT_HOOK_TOKEN": "ordinary-hook-credential",
                "ORCA_AGENT_HOOK_ENDPOINT": "ordinary-hook-endpoint",
                "ORCA_AGENT_LAUNCH_TOKEN": "",
            },
        )
        self.environment.start()
        setup_project(self.root)

    def tearDown(self) -> None:
        self.environment.stop()
        self.project_temp.cleanup()
        self.state_temp.cleanup()

    def test_single_worker_happy_path_releases_before_ack_and_preserves_wip(self) -> None:
        objective = "Make the bounded change"
        before = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        client = FakeClient(completion_responses(self.root, objective))
        report = implement(self.root, objective, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        after = git(self.root, "status", "--porcelain=v1", "-z", "--untracked-files=all")

        self.assertEqual(before, after)
        self.assertEqual(report["status"], "worker_succeeded")
        self.assertEqual(report["verification"], "pending")
        release_index = next(i for i, call in enumerate(client.calls) if "worker-release" in call)
        ack_index = next(i for i, call in enumerate(client.calls) if "--ack" in call)
        self.assertLess(release_index, ack_index)

    def test_hook_credentials_do_not_make_an_ordinary_terminal_an_agent(self) -> None:
        client = FakeClient(identity_responses(self.root))
        identity = require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(identity.terminal_handle, "term_plain")

    def test_native_agent_identity_is_rejected_before_mutation(self) -> None:
        client = FakeClient(identity_responses(self.root, agent="codex")[:1])
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "agent_terminal_not_controller")

    def test_active_context_dispatch_is_rejected_by_dispatch_status(self) -> None:
        workers = [
            {
                "dispatchId": "dispatch_active",
                "agentTerminalHandle": "term_plain",
                "dispatchStatus": "dispatched",
                "terminalState": "retained",
            }
        ]
        client = FakeClient(identity_responses(self.root, workers=workers)[:3])
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "agent_terminal_not_controller")

    def test_controller_requires_exact_current_worktree_before_mutation(self) -> None:
        responses = identity_responses(self.root)
        terminal = responses[0]["result"]["terminal"]  # type: ignore[index]
        terminal["worktreePath"] = str(self.root.parent)  # type: ignore[index]
        client = FakeClient(responses[:1])
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "controller_worktree_mismatch")

    def test_conflicting_native_run_binding_is_rejected(self) -> None:
        client = FakeClient(identity_responses(self.root, run_id="run_other"))
        with self.assertRaises(OrchestrateError) as caught:
            require_plain_controller(client, self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "controller_run_conflict")

    def test_failed_worker_is_not_reported_as_success_or_verification(self) -> None:
        objective = "Fail honestly"
        report = implement(
            self.root,
            objective,
            client=FakeClient(completion_responses(self.root, objective, "failed")),  # type: ignore[arg-type]
            wait_timeout_ms=100,
            require_context=False,
        )
        self.assertEqual(report["status"], "worker_failed")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertEqual(report["verification"], "not_run")

    def test_opaque_release_text_is_not_a_structured_disposition(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="release", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="waiting",
            )
            responses: list[Response] = [
                mutation("request_release", dispatchId="dispatch_1", state="retained"),
                {
                    "result": {
                        "dispatch": {"id": "dispatch_1"},
                        "terminalResource": {
                            "releaseState": "retained",
                            "detail": "not external_terminal; owned resource is still live",
                        },
                    }
                },
            ]
            with self.assertRaises(OrchestrateError) as caught:
                _release_disposition(FakeClient(responses), store, run)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "release_unconfirmed")

    def test_exact_user_takeover_is_preserved_but_unproven_external_shape_holds(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="user_takeover", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_takeover",
                task_id="task_takeover",
                dispatch_id="dispatch_takeover",
                phase="waiting",
            )
            dispatch_id = str(run.dispatch_id)
            resource = {
                "id": "terminal-resource-takeover",
                "ownershipState": "user_owned",
                "releaseState": "retained",
                "retainedReason": "user_takeover",
                "originDispatchId": dispatch_id,
                "ownerDispatchId": dispatch_id,
                "terminalHandle": "term_exact",
                "worktreeId": "worktree_exact",
                "releaseRequestedAt": None,
                "releaseCompletedAt": None,
                "releaseError": None,
                "archive": {"source": None, "status": None},
            }
            _release_disposition(
                FakeClient(
                    [
                        mutation("request_release", dispatchId=dispatch_id, state="retained"),
                        {"result": {"dispatch": {"id": dispatch_id}, "terminalResource": resource}},
                    ]
                ),
                store,
                run,
            )  # type: ignore[arg-type]

        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="external", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_external",
                task_id="task_external",
                dispatch_id="dispatch_external",
                phase="waiting",
            )
            dispatch_id = str(run.dispatch_id)
            external = {
                **resource,
                "id": "terminal-resource-external",
                "ownershipState": "external",
                "releaseState": "not_requested",
                "retainedReason": "external_terminal",
                "originDispatchId": dispatch_id,
                "ownerDispatchId": dispatch_id,
            }
            with self.assertRaises(OrchestrateError) as caught:
                _release_disposition(
                    FakeClient(
                        [
                            mutation("request_external", dispatchId=dispatch_id, state="retained"),
                            {"result": {"dispatch": {"id": dispatch_id}, "terminalResource": external}},
                        ]
                    ),
                    store,
                    run,
                )  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "release_unconfirmed")

    def test_question_delivery_remains_unacknowledged_then_answers_exactly(self) -> None:
        objective = "Ask when blocked"
        responses = completion_responses(self.root, objective)[:6]
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
        report = implement(self.root, objective, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(report["pendingQuestions"], ["question_1"])

        answer_client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                mutation("request_reply", message={"id": "reply_1"}),
                mutation("request_ack", messages=[]),
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

    def test_uncertain_request_replays_only_exact_native_shape(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create", "--objective", "recover"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            client = FakeClient(
                [
                    request_show("request_1"),
                    {
                        "result": {
                            "run": {"id": "run_recovered"},
                            "mutation": {"requestId": "request_1", "replayed": True},
                        }
                    },
                ]
            )
            recovered = reconcile_intentions(client, store, run)  # type: ignore[arg-type]
            self.assertEqual(recovered.native_run_id, "run_recovered")
            self.assertIn("--retry-request", client.calls[1])

    def test_generic_request_status_is_rejected(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([{"result": {"status": "completed"}}]), store, run)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "orca_contract_error")

    def test_request_receipt_for_a_different_method_is_rejected(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            intention = store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create"])
            store.mark_intention(intention, "uncertain", request_id="request_1")
            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(
                    FakeClient([request_show("request_1", method="orchestration.task-create")]),  # type: ignore[arg-type]
                    store,
                    run,
                )
            self.assertEqual(caught.exception.code, "orca_contract_error")

    def test_native_mutation_request_id_wins_over_transport_correlation(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            response = {
                "id": "transport-correlation",
                "result": {
                    "run": {"id": "run_recovered"},
                    "mutation": {"requestId": "native-mutation", "replayed": False},
                },
            }
            _mutation(FakeClient([response]), store, run, "run-create", ["orchestration", "run-create"])  # type: ignore[arg-type]
            row = store.connection.execute("SELECT * FROM intentions").fetchone()
            self.assertEqual(row["request_id"], "native-mutation")

    def test_error_data_mutation_receipt_is_recoverable_but_generic_id_is_not(self) -> None:
        payload = {
            "id": "transport-correlation",
            "error": {"data": {"mutation": {"requestId": "native-error-request"}, "requestId": "generic"}},
        }
        self.assertEqual(_request_id(payload), "native-error-request")
        self.assertIsNone(_request_id({"error": {"data": {"requestId": "generic"}}}))

    def test_prepared_but_never_invoked_intention_is_safe_to_start(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover", profile_digest="p", source_digest="s")
            store.prepare_intention(run.local_id, "run-create", ["orchestration", "run-create", "--objective", "recover"])
            recovered = reconcile_intentions(
                FakeClient([mutation("request_1", run={"id": "run_recovered"})]),  # type: ignore[arg-type]
                store,
                run,
            )
            self.assertEqual(recovered.native_run_id, "run_recovered")

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
            completed = _process_delivery(FakeClient(settlement_responses()), store, run, "delivery_1", [message])  # type: ignore[arg-type]
            replay_client = FakeClient([])
            replayed = _process_delivery(replay_client, store, completed, "delivery_1", [message])  # type: ignore[arg-type]
            self.assertEqual(replayed.phase, "worker_unadmitted")
            self.assertEqual(replay_client.calls, [])

    def test_release_receipt_survives_failed_readback_without_reissuing_release(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="release recovery", profile_digest="p", source_digest="s")
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
            delivery = {"result": {"deliveryId": "delivery_1", "messages": [message]}}
            store.journal_delivery(run.local_id, "delivery_1", delivery, [message])
            first_responses = settlement_responses()
            first_responses[-1] = {
                "result": {
                    "dispatch": {"id": "dispatch_1"},
                    "terminalResource": {"releaseState": "release_pending"},
                }
            }
            with self.assertRaises(OrchestrateError) as initial:
                _process_delivery(FakeClient(first_responses), store, run, "delivery_1", [message])  # type: ignore[arg-type]
            self.assertEqual(initial.exception.code, "release_unconfirmed")

            exact = settlement_responses()
            recovery_client = FakeClient([exact[0], exact[1], exact[-1]])
            recovered = _process_delivery(
                recovery_client,
                store,
                store.get_run(run.local_id),
                "delivery_1",
                [message],
            )  # type: ignore[arg-type]

            self.assertEqual(recovered.phase, "worker_unadmitted")
            self.assertFalse(any("worker-release" in call for call in recovery_client.calls))
            self.assertEqual(len(store.evidence(run.local_id)), 2)

    def test_resume_reprocesses_terminal_phase_observed_delivery_and_acks(self) -> None:
        profile = setup_project(self.root)
        objective = "Complete"
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        message = {
            "id": "message_done",
            "type": "worker_done",
            "subject": "done",
            "payload": {"taskId": "task_1", "dispatchId": "dispatch_1", "outcome": "succeeded"},
        }
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                dispatch_id="dispatch_1",
                phase="worker_succeeded",
                worker_outcome="succeeded",
                delivery_id="delivery_1",
            )
            store.journal_delivery(
                run.local_id,
                "delivery_1",
                {"result": {"deliveryId": "delivery_1", "messages": [message]}},
                [message],
            )
            release = store.prepare_intention(
                run.local_id,
                "worker-release",
                ["orchestration", "worker-release", "--dispatch", "dispatch_1"],
            )
            store.mark_intention(
                release,
                "applied",
                request_id="request_release",
                response=mutation("request_release", dispatchId="dispatch_1", state="released"),
            )
            local_id = run.local_id
        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            *settlement_responses()[:2],
            settlement_responses()[3],
            mutation("request_ack", messages=[]),
        ]
        report = resume(
            self.root,
            local_id,
            client=FakeClient(responses),  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )
        self.assertEqual(report["status"], "worker_unadmitted")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            persisted = store.get_run(local_id)
            self.assertIsNone(persisted.delivery_id)
            self.assertEqual(store.messages(local_id, "delivery_1")[0]["effect_status"], "processed")

    def test_dispatched_and_stored_packet_have_one_truthful_identity(self) -> None:
        objective = "Inspect packet identity"
        responses = completion_responses(self.root, objective)[:6]
        responses.append(
            {
                "result": {
                    "deliveryId": "delivery_q",
                    "messages": [
                        {
                            "id": "question_1",
                            "type": "question",
                            "body": "pause",
                            "payload": {"taskId": "task_1", "dispatchId": "dispatch_1"},
                        }
                    ],
                }
            }
        )
        client = FakeClient(responses)
        report = implement(self.root, objective, client=client, wait_timeout_ms=100, require_context=False)  # type: ignore[arg-type]
        task_call = next(call for call in client.calls if "task-create" in call)
        spec = task_call[task_call.index("--spec") + 1]
        embedded_json = spec[spec.index("{"):]
        dispatched = json.loads(embedded_json)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            stored = store.get_packet(str(report["localRunId"]), "task_1")
            stored_bytes = store.connection.execute(
                "SELECT CAST(packet_json AS BLOB) AS packet_bytes FROM packets WHERE run_local_id = ? AND task_id = ?",
                (str(report["localRunId"]), "task_1"),
            ).fetchone()["packet_bytes"]
        self.assertEqual(stored_bytes, embedded_json.encode("utf-8"))
        self.assertEqual(dispatched, stored)
        self.assertEqual(dispatched["schema"], "orchestrate-worker-packet/v3")
        self.assertEqual(dispatched["admission"]["commandTemplate"][1:4], ["-I", "-m", "orchestrate"])
        self.assertEqual(
            Path(dispatched["admission"]["commandTemplate"][0]).resolve(),
            Path(sys.executable).resolve(),
        )
        self.assertEqual(dispatched["native"]["taskIdSource"], "orca-injected-task-and-dispatch-preamble")
        self.assertNotIn("taskId", dispatched["native"])
        self.assertEqual(dispatched["operationalProfile"]["selectedDigest"], dispatched["profileDigest"])
        self.assertEqual(dispatched["operationalProfile"]["selectionSource"], "initial-setup-selection")

    def test_legacy_packet_json_is_not_rewritten_and_still_requires_task_readback(self) -> None:
        objective = "Resume a legacy packet row"
        profile = setup_project(self.root)
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
        ]
        client = FakeClient(responses)
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1", phase="task_created")
            packet = make_packet(
                objective=objective,
                profile=profile,
                sources=sources,
                launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                python_executable=str(Path(sys.executable).resolve()),
                run_id="run_1",
                reader=reader,
            )
            legacy_json = json.dumps(packet, sort_keys=True)
            store.connection.execute(
                "INSERT INTO packets VALUES (?, ?, ?, datetime('now'))",
                (run.local_id, "task_1", legacy_json),
            )
            local_id = run.local_id

        responses.append(
            {
                "result": {
                    "tasks": [
                        {
                            "id": "task_1",
                            "run_id": "run_1",
                            "task_title": objective,
                            "spec": packet_spec(packet),
                            "status": "ready",
                        }
                    ]
                }
            }
        )
        responses.extend(
            [
                _worker_start(self.root),
                _worker_start_readback(self.root),
            ]
        )
        resume(
            self.root,
            local_id,
            client=client,  # type: ignore[arg-type]
            wait_timeout_ms=1,
            require_context=False,
        )
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            self.assertEqual(store.get_packet_json(local_id, "task_1"), legacy_json)
            self.assertNotEqual(legacy_json, canonical_packet_json(packet))

    def test_worker_launch_substitution_holds_before_waiting(self) -> None:
        objective = "Reject substituted model"
        responses = completion_responses(self.root, objective)[:4]
        substituted = _worker_start(self.root)
        substituted["result"]["launch"]["effective"]["model"] = "different"  # type: ignore[index]
        responses.append(substituted)
        with self.assertRaises(OrchestrateError) as caught:
            implement(
                self.root,
                objective,
                client=FakeClient(responses),  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(caught.exception.code, "worker_launch_mismatch")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.select_run(None)
            intention = store.connection.execute(
                "SELECT status FROM intentions WHERE run_local_id = ? AND operation = 'worker-start'",
                (run.local_id,),
            ).fetchone()
            self.assertEqual(intention["status"], "receipt_confirmed")
            with self.assertRaises(OrchestrateError) as replay:
                reconcile_intentions(FakeClient([]), store, run)  # type: ignore[arg-type]
            self.assertEqual(replay.exception.code, "worker_launch_mismatch")
            self.assertEqual(store.get_run(run.local_id).phase, "task_created")

    def test_last_prelaunch_source_validation_stops_before_worker_start(self) -> None:
        objective = "Reject prelaunch drift"
        responses = completion_responses(self.root, objective)[:3]

        def drift_after_task_readback(client: FakeClient, arguments: tuple[str, ...]) -> dict[str, object]:
            response = _task_readback(client, arguments)
            (self.root / "AGENTS.md").write_text("Changed before worker-start.\n", encoding="utf-8")
            return response

        responses.append(drift_after_task_readback)
        client = FakeClient(responses)
        with self.assertRaises(OrchestrateError) as caught:
            implement(self.root, objective, client=client, wait_timeout_ms=1, require_context=False)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "source_binding_changed")
        self.assertFalse(any(call[:2] == ("orchestration", "worker-start") for call in client.calls))

    def test_admitted_output_before_launch_receipt_is_preserved_through_resume(self) -> None:
        objective = "Preserve post-admission output"
        responses = completion_responses(self.root, objective)[:4]
        admitted_start = _worker_start_with_preflight(self.root)

        def start_then_output(client: FakeClient, arguments: tuple[str, ...]) -> dict[str, object]:
            response = admitted_start(client, arguments)  # type: ignore[operator]
            (self.root / "output.txt").write_text("legitimate worker output\n", encoding="utf-8")
            return response

        responses.extend([start_then_output, _worker_start_readback(self.root)])
        initial = implement(
            self.root, objective, client=FakeClient(responses), wait_timeout_ms=1, require_context=False,  # type: ignore[arg-type]
        )
        self.assertEqual(initial["status"], "waiting")
        resume_responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            completion_responses(self.root, objective)[6],
            *settlement_responses(),
            mutation("request_ack", messages=[]),
        ]
        final = resume(
            self.root, str(initial["localRunId"]), client=FakeClient(resume_responses),
            wait_timeout_ms=100, require_context=False,  # type: ignore[arg-type]
        )
        self.assertEqual(final["status"], "worker_succeeded")
        self.assertEqual((self.root / "output.txt").read_text(encoding="utf-8").strip(), "legitimate worker output")

    def test_prepared_worker_start_crash_recovery_still_validates_effective_launch(self) -> None:
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective="recover prepared start", profile_digest="p", source_digest="s")
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                phase="task_created",
            )
            arguments = [
                "orchestration",
                "worker-start",
                "--run",
                "run_1",
                "--task",
                "task_1",
                "--worktree",
                f"path:{self.root.resolve()}",
                "--agent",
                "codex",
                "--model",
                "gpt-5.6-sol",
                "--effort",
                "high",
            ]
            intention = store.prepare_intention(run.local_id, "worker-start", arguments)
            substituted = _worker_start(self.root)
            substituted["result"]["launch"]["effective"]["effort"] = "low"  # type: ignore[index]

            with self.assertRaises(OrchestrateError) as caught:
                reconcile_intentions(FakeClient([substituted]), store, run)  # type: ignore[arg-type]

            self.assertEqual(caught.exception.code, "worker_launch_mismatch")
            persisted = store.connection.execute(
                "SELECT status FROM intentions WHERE id = ?",
                (intention,),
            ).fetchone()
            self.assertEqual(persisted["status"], "receipt_confirmed")
            self.assertEqual(store.get_run(run.local_id).phase, "task_created")

    def test_resume_revalidates_exact_task_packet_before_worker_start(self) -> None:
        objective = "Reject substituted Task spec"
        responses = completion_responses(self.root, objective)[:3]
        responses.append(
            {
                "result": {
                    "tasks": [
                        {
                            "id": "task_1",
                            "run_id": "run_1",
                            "task_title": objective,
                            "spec": "substituted",
                            "status": "ready",
                        }
                    ]
                }
            }
        )
        with self.assertRaises(OrchestrateError) as initial:
            implement(
                self.root,
                objective,
                client=FakeClient(responses),  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(initial.exception.code, "orca_contract_error")
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.select_run(None)
            local_id = run.local_id
            store.get_packet(local_id, "task_1")
        replay_client = FakeClient(
            [
                mutation("request_use", run={"id": "run_1"}),
                {"result": {"run": {"id": "run_1"}}},
                {"result": {"run": {"id": "run_1", "objective": objective}}},
                {
                    "result": {
                        "tasks": [
                            {
                                "id": "task_1",
                                "run_id": "run_1",
                                "task_title": objective,
                                "spec": "persistently substituted",
                                "status": "ready",
                            }
                        ]
                    }
                },
            ]
        )
        with self.assertRaises(OrchestrateError) as replay:
            resume(
                self.root,
                local_id,
                client=replay_client,  # type: ignore[arg-type]
                wait_timeout_ms=1,
                require_context=False,
            )
        self.assertEqual(replay.exception.code, "orca_contract_error")
        self.assertFalse(any("worker-start" in call for call in replay_client.calls))

    def test_resume_recovers_missing_packet_before_worker_start(self) -> None:
        profile = setup_project(self.root)
        objective = "Recover the exact task packet"
        reader = read_project(profile, objective)
        sources = build_source_index(profile, extra_sources=set(reader.consulted_paths))
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            run = store.create_run(objective=objective, profile_digest=profile.digest, source_digest=sources.digest)
            run = store.update_run(run.local_id, native_run_id="run_1", task_id="task_1", phase="task_created")
            local_id = run.local_id
        recovered_packet = make_packet(
            objective=objective,
            profile=profile,
            sources=sources,
            launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
            python_executable=str(Path(sys.executable).resolve()),
            run_id="run_1",
            reader=reader,
        )

        def recovered_task_readback(_: FakeClient, __: tuple[str, ...]) -> dict[str, object]:
            with StateStore(self.root, home=Path(self.state_temp.name)) as recovered_store:
                with self.assertRaises(OrchestrateError) as missing:
                    recovered_store.get_packet(local_id, "task_1")
                self.assertEqual(missing.exception.code, "packet_not_found")
            return {
                "result": {
                    "tasks": [
                        {
                            "id": "task_1",
                            "run_id": "run_1",
                            "task_title": objective,
                            "spec": packet_spec(recovered_packet),
                            "status": "ready",
                        }
                    ]
                }
            }

        responses: list[Response] = [
            mutation("request_use", run={"id": "run_1"}),
            {"result": {"run": {"id": "run_1"}}},
            {"result": {"run": {"id": "run_1", "objective": objective}}},
            recovered_task_readback,
            _worker_start(self.root),
            _worker_start_readback(self.root),
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
        result = resume(
            self.root,
            local_id,
            client=FakeClient(responses),  # type: ignore[arg-type]
            wait_timeout_ms=100,
            require_context=False,
        )
        self.assertEqual(result["pendingQuestions"], ["question_1"])
        with StateStore(self.root, home=Path(self.state_temp.name)) as store:
            recovered = store.get_packet(local_id, "task_1")
        self.assertEqual(recovered["native"]["taskIdSource"], "orca-injected-task-and-dispatch-preamble")


if __name__ == "__main__":
    unittest.main()
