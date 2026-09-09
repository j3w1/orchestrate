from __future__ import annotations

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

    def mint_probe(self) -> dict[str, Any]:
        client = FakeClient(
            [
                self.worktree_response(),
                {"ok": True, "result": {"run": None}},
                {"ok": True, "id": "request_run", "result": {"run": {"id": "run_probe"}}},
            ]
        )
        return create_run_probe(client, project=self.root)  # type: ignore[arg-type]

    def worker_responses(self, objective: str, messages: list[Any], *, outcome: str = "succeeded") -> list[dict[str, Any]]:
        native = "completed" if outcome == "succeeded" else "failed"
        return [
            self.worktree_response(),
            {"ok": True, "result": {"run": {"id": "run_probe", "objective": objective}}},
            {"ok": True, "result": {"tasks": []}},
            {"ok": True, "result": {"run": {"id": "run_probe"}}},
            {"ok": True, "result": {"run": {"id": "run_probe"}}},
            {"ok": True, "result": {"task": {"id": "task_probe"}}},
            {"ok": True, "result": {"dispatch": {"id": "dispatch_probe"}}},
            {"ok": True, "result": {"deliveryId": "delivery_probe", "messages": messages}},
            {
                "ok": True,
                "result": {
                    "dispatch": {
                        "id": "dispatch_probe",
                        "run_id": "run_probe",
                        "task_id": "task_probe",
                        "status": native,
                    }
                },
            },
            {
                "ok": True,
                "result": {
                    "tasks": [{"id": "task_probe", "run_id": "run_probe", "status": native}]
                },
            },
            {"ok": True, "result": {"releaseState": "released"}},
            {
                "ok": True,
                "result": {
                    "dispatch": {"id": "dispatch_probe"},
                    "terminalResource": {"releaseState": "released"},
                },
            },
            {"ok": True, "result": {"messages": []}},
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
        self.assertEqual(report["receipts"]["runCreate"]["id"], "request_run")
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
            client = FakeClient([])
            with self.assertRaisesRegex(ProbeContractError, "reasoning-agent"):
                create_run_probe(client, project=self.root)  # type: ignore[arg-type]
            self.assertEqual(client.calls, [])

    def test_worker_probe_releases_before_acknowledging(self) -> None:
        minted = self.mint_probe()
        receipt = json.loads((Path(self.state_temp.name) / "probes" / f"{minted['probeToken']}.json").read_text())
        client = FakeClient(self.worker_responses(receipt["objective"], [self.done_message()]))
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
        client = FakeClient(self.worker_responses(receipt["objective"], [17, self.done_message()])[:8])
        with self.assertRaisesRegex(ProbeContractError, "delivery id or messages"):
            run_worker_probe(client, minted["probeToken"], project=self.root, wait_timeout_ms=10)  # type: ignore[arg-type]
        self.assertFalse(any("--ack" in call or "worker-release" in call for call in client.calls))

    def test_run_creation_timeout_preserves_unknown_receipt_and_prior_reads(self) -> None:
        timeout = OrcaCommandError(
            "timed out",
            OrcaCommandResult(("orca", "orchestration", "run-create"), -1, "", "keepalive", None),
        )
        client = FakeClient([self.worktree_response(), {"ok": True, "result": {"run": None}}, timeout])
        with self.assertRaises(ProbeContractError) as caught:
            create_run_probe(client, project=self.root)  # type: ignore[arg-type]
        self.assertEqual(caught.exception.data["stage"], "runCreate")
        self.assertIn("worktreeCurrent", caught.exception.data["receipts"])
        self.assertEqual(caught.exception.data["failedCommand"]["returncode"], -1)


if __name__ == "__main__":
    unittest.main()
