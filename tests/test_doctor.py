from __future__ import annotations

import unittest
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
    command = ("orca",)

    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, ...]] = []

    def executable_available(self) -> bool:
        return True

    def run_text(self, *arguments: str, **_: object) -> Any:
        self.calls.append(arguments)
        return type("Result", (), {"stdout": "1.4.198\n"})()

    def run_json(self, *arguments: str, **_: object) -> dict[str, Any]:
        self.calls.append(arguments)
        if not self.responses:
            raise AssertionError("unexpected command")
        return self.responses.pop(0)


class DoctorTests(unittest.TestCase):
    def test_error_report_retains_structured_recovery_data(self) -> None:
        payload = {
            "ok": False,
            "error": {
                "code": "agent_prompt_stalled",
                "message": "prompt delivery stalled",
                "data": {"stage": "dispatch_input", "recovery": ["inspect worker"]},
            },
        }
        result = OrcaCommandResult(("orca",), 1, "", "", payload)
        report = error_report(OrcaCommandError("prompt delivery stalled", result), operation="worker-probe")
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
        report = collect_doctor_report(client)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["activeProbe"], "NOT_RUN")

    def test_create_probe_returns_run_id_for_separate_resume_process(self) -> None:
        client = FakeClient([{"ok": True, "result": {"run": {"id": "run_probe"}}}])
        report = create_run_probe(client)  # type: ignore[arg-type]
        self.assertEqual(report["runId"], "run_probe")
        self.assertEqual(report["activeProbe"], "run-created-and-controller-exited")

    def test_worker_probe_releases_before_acknowledging_completion(self) -> None:
        client = FakeClient(
            [
                {"ok": True, "result": {"run": {"id": "run_probe"}}},
                {"ok": True, "result": {"task": {"id": "task_probe"}}},
                {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
                {
                    "ok": True,
                    "result": {
                        "deliveryId": "delivery_probe",
                        "messages": [
                                {
                                    "type": "heartbeat",
                                    "payload": {
                                        "taskId": "task_probe",
                                        "dispatchId": "dispatch_probe",
                                    },
                                },
                                {
                                    "type": "worker_done",
                                    "payload": {
                                        "taskId": "task_probe",
                                        "dispatchId": "dispatch_probe",
                                        "outcome": "succeeded",
                                    },
                                },
                        ],
                    },
                },
                {
                    "ok": True,
                    "result": {
                        "dispatch": {
                            "id": "dispatch_probe",
                            "run_id": "run_probe",
                            "task_id": "task_probe",
                            "status": "completed",
                        }
                    },
                },
                {
                    "ok": True,
                    "result": {
                        "tasks": [
                            {"id": "task_probe", "run_id": "run_probe", "status": "completed"}
                        ]
                    },
                },
                {"ok": True, "result": {"messages": []}},
                {
                    "ok": True,
                    "result": {
                        "dispatch": {"id": "dispatch_probe"},
                        "terminalResource": {"releaseState": "released"},
                    },
                },
                {"ok": True, "result": {"messages": []}},
            ]
        )
        report = run_worker_probe(client, "run_probe", wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "pass")
        release_index = next(i for i, call in enumerate(client.calls) if "worker-release" in call)
        ack_index = next(i for i, call in enumerate(client.calls) if "--ack" in call)
        self.assertLess(release_index, ack_index)

    def test_worker_probe_does_not_ack_question_delivery(self) -> None:
        client = FakeClient(
            [
                {"ok": True, "result": {}},
                {"ok": True, "result": {"task": {"id": "task_probe"}}},
                {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
                {
                    "ok": True,
                    "result": {
                        "deliveryId": "delivery_probe",
                        "messages": [{"type": "question"}],
                    },
                },
            ]
        )
        with self.assertRaisesRegex(ProbeContractError, "unacknowledged"):
            run_worker_probe(client, "run_probe", wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertFalse(any("--ack" in call for call in client.calls))

    def test_worker_probe_does_not_ignore_question_batched_with_completion(self) -> None:
        client = FakeClient(
            [
                {"ok": True, "result": {}},
                {"ok": True, "result": {"task": {"id": "task_probe"}}},
                {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
                {
                    "ok": True,
                    "result": {
                        "deliveryId": "delivery_probe",
                        "messages": [
                                {"type": "question"},
                                {
                                    "type": "worker_done",
                                    "payload": {
                                        "dispatchId": "dispatch_probe",
                                        "taskId": "task_probe",
                                        "outcome": "succeeded",
                                    },
                                },
                        ],
                    },
                },
            ]
        )
        with self.assertRaisesRegex(ProbeContractError, "intervention mail"):
            run_worker_probe(client, "run_probe", wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertFalse(any("worker-release" in call or "--ack" in call for call in client.calls))

    def test_worker_probe_holds_stale_worker_done(self) -> None:
        client = FakeClient(
            [
                {"ok": True, "result": {}},
                {"ok": True, "result": {"task": {"id": "task_probe"}}},
                {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
                {
                    "ok": True,
                    "result": {
                        "deliveryId": "delivery_probe",
                        "messages": [
                                {
                                    "type": "worker_done",
                                    "payload": {
                                        "taskId": "task_stale",
                                        "dispatchId": "dispatch_probe",
                                        "outcome": "succeeded",
                                    },
                                }
                        ],
                    },
                },
            ]
        )
        with self.assertRaisesRegex(ProbeContractError, "not bound"):
            run_worker_probe(client, "run_probe", wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertFalse(any("worker-release" in call or "--ack" in call for call in client.calls))

    def test_failed_worker_is_released_and_acknowledged_but_probe_is_blocked(self) -> None:
        client = FakeClient(
            [
                {"ok": True, "result": {}},
                {"ok": True, "result": {"task": {"id": "task_probe"}}},
                {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
                {
                    "ok": True,
                    "result": {
                        "deliveryId": "delivery_probe",
                        "messages": [
                                {
                                    "type": "worker_done",
                                    "payload": {
                                        "taskId": "task_probe",
                                        "dispatchId": "dispatch_probe",
                                        "outcome": "failed",
                                    },
                                }
                        ],
                    },
                },
                {
                    "ok": True,
                    "result": {
                        "dispatch": {
                            "id": "dispatch_probe",
                            "run_id": "run_probe",
                            "task_id": "task_probe",
                            "status": "failed",
                        }
                    },
                },
                {
                    "ok": True,
                    "result": {
                        "tasks": [{"id": "task_probe", "run_id": "run_probe", "status": "failed"}]
                    },
                },
                {"ok": True, "result": {"messages": []}},
                {
                    "ok": True,
                    "result": {
                        "dispatch": {"id": "dispatch_probe"},
                        "terminalResource": {"releaseState": "released"},
                    },
                },
                {"ok": True, "result": {"messages": []}},
            ]
        )
        report = run_worker_probe(client, "run_probe", wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["workerOutcome"], "failed")
        self.assertTrue(any("worker-release" in call for call in client.calls))
        self.assertTrue(any("--ack" in call for call in client.calls))

    def test_unconfirmed_release_holds_delivery_and_preserves_receipts(self) -> None:
        client = FakeClient(
            [
                {"ok": True, "result": {}},
                {"ok": True, "result": {"task": {"id": "task_probe"}}},
                {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
                {
                    "ok": True,
                    "result": {
                        "deliveryId": "delivery_probe",
                        "messages": [
                                {
                                    "type": "worker_done",
                                    "payload": {
                                        "taskId": "task_probe",
                                        "dispatchId": "dispatch_probe",
                                        "outcome": "succeeded",
                                    },
                                }
                        ],
                    },
                },
                {
                    "ok": True,
                    "result": {
                        "dispatch": {
                            "id": "dispatch_probe",
                            "run_id": "run_probe",
                            "task_id": "task_probe",
                            "status": "completed",
                        }
                    },
                },
                {
                    "ok": True,
                    "result": {
                        "tasks": [
                            {"id": "task_probe", "run_id": "run_probe", "status": "completed"}
                        ]
                    },
                },
                {"ok": True, "result": {"releaseState": "release_pending"}},
                {
                    "ok": True,
                    "result": {
                        "dispatch": {"id": "dispatch_probe"},
                        "terminalResource": {"releaseState": "release_pending"},
                    },
                },
            ]
        )
        with self.assertRaises(ProbeContractError) as caught:
            run_worker_probe(client, "run_probe", wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.data["stage"], "releaseDisposition")
        self.assertIn("workerRelease", caught.exception.data["receipts"])
        self.assertFalse(any("--ack" in call for call in client.calls))


if __name__ == "__main__":
    unittest.main()
