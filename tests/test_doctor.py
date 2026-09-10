from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from typing import Any

from orchestrate.doctor import (
    ProbeContractError,
    collect_doctor_report,
    create_run_probe,
    error_report,
    run_worker_probe,
)
from orchestrate.orca import OrcaCommandError, OrcaCommandResult


class FakeClient:
    def __init__(self, responses: list[dict[str, Any] | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, ...]] = []
        self.command = ("orca",)

    def executable_available(self) -> bool:
        return True

    def run_text(self, *arguments: str, **_: object) -> Any:
        self.calls.append(arguments)
        return type("Result", (), {"stdout": "1.4.198\n"})()

    def run_json(self, *arguments: str, **_: object) -> dict[str, Any]:
        self.calls.append(arguments)
        if not self.responses:
            raise AssertionError(f"unexpected command: {arguments}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def git(root: Path, *arguments: str) -> None:
    subprocess.run(("git", "-C", str(root), *arguments), check=True, capture_output=True)


class DoctorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.project_temp = tempfile.TemporaryDirectory()
        self.state_temp = tempfile.TemporaryDirectory()
        self.root = Path(self.project_temp.name)
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / ".orchestrate-disposable").write_text("orchestrate-disposable/v1\n", encoding="utf-8")
        (self.root / "README.md").write_text("fixture\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")
        self.environment = patch.dict(
            os.environ,
            {
                "ORCHESTRATE_HOME": self.state_temp.name,
                "ORCA_TERMINAL_HANDLE": "term_plain",
                "ORCA_AGENT_HOOK_TOKEN": "",
                "ORCA_AGENT_LAUNCH_TOKEN": "",
            },
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()
        self.project_temp.cleanup()
        self.state_temp.cleanup()

    def worktree_response(self) -> dict[str, Any]:
        return {"ok": True, "result": {"worktree": {"id": "worktree_1", "path": str(self.root)}}}

    def caller_responses(self, run_id: str | None = None) -> list[dict[str, Any]]:
        return [
            {
                "ok": True,
                "result": {
                    "terminal": {
                        "handle": "term_plain",
                        "worktreeId": "worktree_1",
                        "worktreePath": str(self.root),
                        "executionHostId": "local",
                        "connected": True,
                        "writable": True,
                    }
                },
            },
            self.worktree_response(),
            {"ok": True, "result": {"workers": []}},
            {"ok": True, "result": {"run": None if run_id is None else {"id": run_id}}},
            self.worktree_response(),
        ]

    def mint_probe(self) -> dict[str, Any]:
        client = FakeClient(
            [*self.caller_responses(),
                {"ok": True, "result": {"run": None}},
                {
                    "ok": True,
                    "result": {
                        "run": {"id": "run_probe"},
                        "mutation": {"requestId": "request_run", "replayed": False},
                    },
                },
            ]
        )
        return create_run_probe(client, project=self.root)  # type: ignore[arg-type]

    def worker_responses(
        self,
        objective: str,
        messages: list[Any],
        *,
        outcome: str = "succeeded",
        current_worker_shape: bool = False,
    ) -> list[dict[str, Any]]:
        native = "completed" if outcome == "succeeded" else "failed"
        launch = {
            "requested": {"agent": "codex", "model": None, "effort": None},
            "effective": {"agent": "codex", "model": None, "effort": None},
        }
        terminal_effect = {"kind": "terminal", "role": "agent", "action": "created", "id": "term_probe"}
        settled_dispatch = (
            {
                "id": "dispatch_probe",
                "runId": "run_probe",
                "taskId": "task_probe",
                "task_id": "task_probe",
                "status": native,
                "lastFailure": None if outcome == "succeeded" else "worker_failed",
            }
            if current_worker_shape
            else {
                "id": "dispatch_probe",
                "run_id": "run_probe",
                "task_id": "task_probe",
                "status": native,
                "last_failure": None if outcome == "succeeded" else "worker_failed",
            }
        )
        started_dispatch = (
            {
                "id": "dispatch_probe",
                "runId": "run_probe",
                "taskId": "task_probe",
                "task_id": "task_probe",
                "status": "dispatched",
                "lastFailure": None,
            }
            if current_worker_shape
            else {
                "id": "dispatch_probe",
                "run_id": "run_probe",
                "task_id": "task_probe",
                "status": "dispatched",
                "last_failure": None,
            }
        )
        started_worker = (
            {
                "dispatchId": "dispatch_probe",
                "worktreeId": "worktree_1",
                "agentTerminalHandle": "term_probe",
                "lastError": None,
                "state": "ready",
                "stage": "input_accepted",
            }
            if current_worker_shape
            else {
                "worktree_id": "worktree_1",
                "agent_terminal_handle": "term_probe",
                "last_error": None,
                "state": "ready",
                "stage": "input_accepted",
            }
        )
        released_worker = (
            {
                "dispatchId": "dispatch_probe",
                "worktreeId": "worktree_1",
                "agentTerminalHandle": None,
                "lastError": None if outcome == "succeeded" else "worker_failed",
                "state": outcome,
                "stage": "released",
            }
            if current_worker_shape
            else {
                "worktree_id": "worktree_1",
                "agent_terminal_handle": None,
                "last_error": None if outcome == "succeeded" else "worker_failed",
                "state": outcome,
                "stage": "released",
            }
        )
        terminal_resource = {
            "id": "terminal-resource-probe",
            "ownershipState": "owned",
            "releaseState": "not_requested",
            "retainedReason": None,
            "originDispatchId": "dispatch_probe",
            "ownerDispatchId": "dispatch_probe",
            "terminalHandle": "term_probe",
            "worktreeId": "worktree_1",
        }
        return [
            *self.caller_responses("run_probe"),
            {"ok": True, "result": {"run": {"id": "run_probe", "objective": objective}}},
            {"ok": True, "result": {"tasks": []}},
            {"ok": True, "result": {"run": {"id": "run_probe"}}},
            {
                "ok": True,
                "result": {
                    "run": {"id": "run_probe"},
                    "mutation": {"requestId": "request_use", "replayed": False},
                },
            },
            {
                "ok": True,
                "result": {
                    "task": {"id": "task_probe"},
                    "mutation": {"requestId": "request_task", "replayed": False},
                },
            },
            {
                "ok": True,
                "result": {
                    "runId": "run_probe",
                    "taskId": "task_probe",
                    "dispatchId": "dispatch_probe",
                    "state": "ready",
                    "stage": "input_accepted",
                    "setup": {"state": "not_applicable"},
                    "launch": launch,
                    "effects": [
                        {"kind": "worktree", "action": "reused", "id": "worktree_1"},
                        {"kind": "setup", "action": "not_applicable", "state": "not_applicable"},
                        terminal_effect,
                        {"kind": "dispatch_input", "role": "agent", "id": "term_probe", "state": "accepted"},
                    ],
                    "residualResources": [terminal_effect],
                    "mutation": {"requestId": "request_worker", "replayed": False},
                },
            },
            {
                "ok": True,
                "result": {
                    "dispatch": started_dispatch,
                    "worker": started_worker,
                    "terminalResource": terminal_resource,
                },
            },
            {"ok": True, "result": {"deliveryId": "delivery_probe", "messages": messages}},
            {
                "ok": True,
                "result": {
                    "dispatch": settled_dispatch,
                },
            },
            {
                "ok": True,
                "result": {
                    "tasks": [{"id": "task_probe", "run_id": "run_probe", "status": native}]
                },
            },
            {
                "ok": True,
                "result": {
                    "dispatchId": "dispatch_probe",
                    "state": "released",
                    "mutation": {"requestId": "request_release", "replayed": False},
                },
            },
            {
                "ok": True,
                "result": {
                    "dispatch": settled_dispatch,
                    "worker": released_worker,
                    "terminalResource": {
                        "id": "terminal-resource-probe",
                        "ownershipState": "released",
                        "releaseState": "released",
                        "retainedReason": None,
                        "originDispatchId": "dispatch_probe",
                        "ownerDispatchId": "dispatch_probe",
                        "terminalHandle": "term_probe",
                        "worktreeId": "worktree_1",
                        "releaseRequestedAt": "2026-01-01T00:00:00Z",
                        "releaseCompletedAt": "2026-01-01T00:00:01Z",
                        "releaseError": None,
                        "archive": {"source": "transcript", "status": "captured"},
                    },
                },
            },
            {
                "ok": True,
                "result": {
                    "messages": [],
                    "mutation": {"requestId": "request_ack", "replayed": False},
                },
            },
        ]

    def done_message(self, *, outcome: str = "succeeded", files: list[str] | None = None) -> dict[str, Any]:
        return {
            "id": "message_done",
            "type": "worker_done",
            "payload": {
                "taskId": "task_probe",
                "dispatchId": "dispatch_probe",
                "outcome": outcome,
                "filesModified": [] if files is None else files,
            },
        }

    def test_error_report_retains_structured_recovery_data(self) -> None:
        payload = {
            "ok": False,
            "error": {"code": "agent_prompt_stalled", "message": "stalled", "data": {"stage": "dispatch_input"}},
        }
        report = error_report(OrcaCommandError("stalled", OrcaCommandResult(("orca",), 1, "", "", payload)), operation="worker-probe")
        self.assertEqual(report["error"]["data"]["stage"], "dispatch_input")

    def test_read_only_doctor_requires_both_capabilities(self) -> None:
        client = FakeClient(
            [
                {
                    "ok": True,
                    "result": {
                        "runtime": {
                            "state": "ready",
                            "reachable": True,
                            "capabilities": [
                                "orchestration.contract.v1",
                                "orchestration.worker-launch-preferences.v1",
                            ],
                        }
                    },
                }
            ]
        )
        self.assertEqual(collect_doctor_report(client)["status"], "pass")  # type: ignore[arg-type]

    def test_run_probe_mints_one_use_receipt_with_full_success_receipt(self) -> None:
        report = self.mint_probe()
        self.assertTrue(report["probeToken"].startswith("probe_"))
        self.assertEqual(
            report["receipts"]["runCreate"]["result"]["mutation"]["requestId"],
            "request_run",
        )
        receipt = json.loads((Path(self.state_temp.name) / "probes" / f"{report['probeToken']}.json").read_text())
        self.assertEqual(receipt["state"], "ready")
        self.assertEqual(receipt["baseline"]["root"], str(self.root.resolve()))

    def test_bare_run_id_cannot_mutate_any_run(self) -> None:
        client = FakeClient([])
        with self.assertRaisesRegex(ProbeContractError, "minted probe token"):
            run_worker_probe(client, "run_unrelated", project=self.root, wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertEqual(client.calls, [])

    def test_agent_caller_is_rejected_before_native_mutation(self) -> None:
        with patch.dict(os.environ, {"ORCA_AGENT_HOOK_TOKEN": "present"}):
            terminal = self.caller_responses()[0]
            terminal["result"]["terminal"]["agentIdentity"] = "codex"
            client = FakeClient([terminal])
            with self.assertRaisesRegex(ProbeContractError, "reasoning-agent"):
                create_run_probe(client, project=self.root)  # type: ignore[arg-type]
            self.assertEqual(len(client.calls), 1)

    def test_worker_probe_releases_before_acknowledging(self) -> None:
        minted = self.mint_probe()
        receipt = json.loads((Path(self.state_temp.name) / "probes" / f"{minted['probeToken']}.json").read_text())
        client = FakeClient(
            self.worker_responses(
                receipt["objective"],
                [self.done_message()],
                current_worker_shape=True,
            )
        )
        report = run_worker_probe(client, minted["probeToken"], project=self.root, wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "pass")
        release_index = next(i for i, call in enumerate(client.calls) if "worker-release" in call)
        ack_index = next(i for i, call in enumerate(client.calls) if "--ack" in call)
        self.assertLess(release_index, ack_index)
        start = next(call for call in client.calls if "worker-start" in call)
        self.assertIn(f"path:{self.root.resolve()}", start)

    def test_nonempty_files_modified_releases_and_acks_but_blocks_pass(self) -> None:
        minted = self.mint_probe()
        receipt = json.loads((Path(self.state_temp.name) / "probes" / f"{minted['probeToken']}.json").read_text())
        client = FakeClient(self.worker_responses(receipt["objective"], [self.done_message(files=["README.md"])]))
        report = run_worker_probe(client, minted["probeToken"], project=self.root, wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "blocked")
        self.assertIn("filesModified", report["noEditValidation"])
        self.assertTrue(any("worker-release" in call for call in client.calls))
        self.assertTrue(any("--ack" in call for call in client.calls))

    def test_malformed_fifo_entry_is_not_filtered_or_acknowledged(self) -> None:
        minted = self.mint_probe()
        receipt = json.loads((Path(self.state_temp.name) / "probes" / f"{minted['probeToken']}.json").read_text())
        client = FakeClient(self.worker_responses(receipt["objective"], [17, self.done_message()])[:13])
        with self.assertRaisesRegex(ProbeContractError, "delivery id or messages"):
            run_worker_probe(client, minted["probeToken"], project=self.root, wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertFalse(any("--ack" in call or "worker-release" in call for call in client.calls))

    def test_active_doctor_release_rejects_each_changed_resource_identity(self) -> None:
        for changed_field, changed_value in (
            ("id", "terminal-resource-other"),
            ("terminalHandle", "term_other"),
            ("worktreeId", "worktree_other"),
        ):
            with self.subTest(changed_field=changed_field):
                minted = self.mint_probe()
                receipt = json.loads(
                    (Path(self.state_temp.name) / "probes" / f"{minted['probeToken']}.json").read_text()
                )
                responses = self.worker_responses(
                    receipt["objective"],
                    [self.done_message()],
                    current_worker_shape=True,
                )
                released = next(
                    response
                    for response in responses
                    if isinstance(response.get("result"), dict)
                    and isinstance(response["result"].get("terminalResource"), dict)
                    and response["result"]["terminalResource"].get("releaseState") == "released"
                )
                released["result"]["terminalResource"][changed_field] = changed_value
                client = FakeClient(responses)
                with self.assertRaises(ProbeContractError):
                    run_worker_probe(
                        client,
                        minted["probeToken"],
                        project=self.root,
                        wait_timeout_ms=10,
                    )  # type: ignore[arg-type]
                self.assertFalse(any("--ack" in call for call in client.calls))

    def test_active_doctor_binds_released_worker_semantics_for_both_shapes_and_outcomes(self) -> None:
        for current_shape in (False, True):
            for outcome in ("succeeded", "failed"):
                with self.subTest(current_shape=current_shape, outcome=outcome):
                    minted = self.mint_probe()
                    receipt = json.loads(
                        (Path(self.state_temp.name) / "probes" / f"{minted['probeToken']}.json").read_text()
                    )
                    client = FakeClient(
                        self.worker_responses(
                            receipt["objective"],
                            [self.done_message(outcome=outcome)],
                            outcome=outcome,
                            current_worker_shape=current_shape,
                        )
                    )

                    report = run_worker_probe(
                        client,  # type: ignore[arg-type]
                        minted["probeToken"],
                        project=self.root,
                        wait_timeout_ms=10,
                    )

                    expected_status = "pass" if outcome == "succeeded" else "blocked"
                    self.assertEqual(report["status"], expected_status)
                    self.assertEqual(report["workerOutcome"], outcome)
                    self.assertTrue(any("worker-release" in call for call in client.calls))
                    self.assertTrue(any("--ack" in call for call in client.calls))

    def test_active_doctor_rejects_each_released_worker_semantic_contradiction(self) -> None:
        contradiction_fields = (
            "dispatch_status",
            "dispatch_last_failure",
            "worker_state",
            "worker_stage",
            "worker_last_error",
            "worker_terminal_handle",
        )
        for current_shape in (False, True):
            for outcome in ("succeeded", "failed"):
                for field in contradiction_fields:
                    with self.subTest(current_shape=current_shape, outcome=outcome, field=field):
                        minted = self.mint_probe()
                        receipt = json.loads(
                            (
                                Path(self.state_temp.name)
                                / "probes"
                                / f"{minted['probeToken']}.json"
                            ).read_text()
                        )
                        responses = self.worker_responses(
                            receipt["objective"],
                            [self.done_message(outcome=outcome)],
                            outcome=outcome,
                            current_worker_shape=current_shape,
                        )
                        released_index = next(
                            index
                            for index, response in enumerate(responses)
                            if isinstance(response.get("result"), dict)
                            and isinstance(response["result"].get("terminalResource"), dict)
                            and response["result"]["terminalResource"].get("releaseState") == "released"
                        )
                        released = deepcopy(responses[released_index])
                        responses[released_index] = released
                        dispatch = released["result"]["dispatch"]
                        worker = released["result"]["worker"]
                        if field == "dispatch_status":
                            dispatch["status"] = "dispatched"
                        elif field == "dispatch_last_failure":
                            dispatch[
                                "lastFailure" if current_shape else "last_failure"
                            ] = "contradictory_failure"
                        elif field == "worker_state":
                            worker["state"] = "ready"
                        elif field == "worker_stage":
                            worker["stage"] = "input_accepted"
                        elif field == "worker_last_error":
                            worker[
                                "lastError" if current_shape else "last_error"
                            ] = "contradictory_failure"
                        elif field == "worker_terminal_handle":
                            worker[
                                "agentTerminalHandle" if current_shape else "agent_terminal_handle"
                            ] = "term_probe"
                        client = FakeClient(responses)

                        with self.assertRaises(ProbeContractError):
                            run_worker_probe(
                                client,  # type: ignore[arg-type]
                                minted["probeToken"],
                                project=self.root,
                                wait_timeout_ms=10,
                            )

                        self.assertEqual(
                            sum("worker-start" in call for call in client.calls),
                            1,
                        )
                        self.assertEqual(
                            sum("worker-release" in call for call in client.calls),
                            1,
                        )
                        self.assertFalse(any(call[:2] == ("terminal", "close") for call in client.calls))
                        self.assertFalse(any("--ack" in call for call in client.calls))

    def test_run_creation_timeout_preserves_unknown_receipt_and_prior_reads(self) -> None:
        timeout = OrcaCommandError(
            "timed out",
            OrcaCommandResult(("orca", "orchestration", "run-create"), -1, "", "keepalive", None),
        )
        client = FakeClient([*self.caller_responses(), {"ok": True, "result": {"run": None}}, timeout])
        with self.assertRaises(ProbeContractError) as caught:
            create_run_probe(client, project=self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.data["stage"], "runCreate")
        self.assertIn("worktreeCurrent", caught.exception.data["receipts"])
        self.assertEqual(caught.exception.data["failedCommand"]["returncode"], -1)


if __name__ == "__main__":
    unittest.main()
