from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrate.coordination import SharedContract
from orchestrate.errors import OrchestrateError
from orchestrate.packets import (
    canonical_packet_json,
    decode_packet,
    expected_packet_id,
    make_milestone_packet,
    make_packet,
)
from orchestrate.profile import setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index


def git(root: Path, *arguments: str) -> None:
    subprocess.run(("git", "-C", os.fspath(root), *arguments), check=True, capture_output=True)


class CanonicalPacketVariantTests(unittest.TestCase):
    """Host-neutral producer/strict-decoder compatibility for every controller packet."""

    def setUp(self) -> None:
        self.project = tempfile.TemporaryDirectory()
        self.state = tempfile.TemporaryDirectory()
        self.root = Path(self.project.name)
        self.environment = patch.dict(os.environ, {"ORCHESTRATE_HOME": self.state.name})
        self.environment.start()
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Bound packet authority.\n", encoding="utf-8")
        (self.root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")
        self.profile = setup_project(self.root)
        self.objective = "Integrate the canonical packet variants"
        self.reader = read_project(self.profile, self.objective)
        self.launch = {"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"}
        self.default_sources = build_source_index(
            self.profile,
            extra_sources=set(self.reader.consulted_paths),
        )
        self.plan_path = "milestone-plan.json"
        (self.root / self.plan_path).write_text(
            json.dumps({"schema": "orchestrate-milestone-plan/v1"}),
            encoding="utf-8",
        )
        git(self.root, "add", self.plan_path)
        git(self.root, "commit", "-qm", "add packet fixture plan")
        self.milestone_sources = build_source_index(
            self.profile,
            extra_sources=set(self.reader.consulted_paths),
            milestone_sources={self.plan_path},
        )
        self.contract = SharedContract.draft(
            {"interface": "frozen-v1", "checks": ["focused"]},
        ).settle()

    def tearDown(self) -> None:
        self.environment.stop()
        self.project.cleanup()
        self.state.cleanup()

    def _assert_strict_round_trip(self, packet: dict[str, object]) -> None:
        packet_json = canonical_packet_json(packet)
        decoded = decode_packet(packet_json)
        self.assertEqual(decoded.value, packet)
        self.assertEqual(canonical_packet_json(decoded.value), packet_json)

    def _followup_packet(self, *, task_key: str, role: str) -> dict[str, object]:
        return make_milestone_packet(
            objective=self.objective,
            task_key=task_key,
            task_spec=f"Execute the exact {role} task.",
            role=role,
            candidate_digest=self.milestone_sources.digest,
            contract_digest=self.contract.digest,
            contract=json.loads(self.contract.canonical_json),
            result_path=f"{task_key}-result.json",
            max_workers=2,
            profile=self.profile,
            sources=self.milestone_sources,
            launch=self.launch,
            python_executable=os.fspath(Path(sys.executable).resolve()),
            run_id="run_packet_variants",
            reader=self.reader,
        )

    def test_default_owner_packet_strictly_decodes(self) -> None:
        packet = make_packet(
            objective=self.objective,
            profile=self.profile,
            sources=self.default_sources,
            launch=self.launch,
            python_executable=os.fspath(Path(sys.executable).resolve()),
            run_id="run_packet_variants",
            reader=self.reader,
        )
        self._assert_strict_round_trip(packet)

    def test_milestone_owner_packet_strictly_decodes(self) -> None:
        packet = make_packet(
            objective=self.objective,
            profile=self.profile,
            sources=self.milestone_sources,
            launch=self.launch,
            python_executable=os.fspath(Path(sys.executable).resolve()),
            run_id="run_packet_variants",
            reader=self.reader,
        )
        self.assertIn(self.plan_path, {item["path"] for item in packet["sources"]})
        self._assert_strict_round_trip(packet)

    def test_specialist_followup_packet_strictly_decodes(self) -> None:
        packet = self._followup_packet(task_key="verify", role="specialist")
        self.assertEqual(packet["milestone"]["contractDigest"], self.contract.digest)
        self._assert_strict_round_trip(packet)

    def test_reviewer_followup_packet_strictly_decodes(self) -> None:
        packet = self._followup_packet(task_key="review", role="reviewer")
        self.assertEqual(packet["milestone"]["contractDigest"], self.contract.digest)
        self._assert_strict_round_trip(packet)

    def test_milestone_contract_digest_rejects_bare_hex(self) -> None:
        packet = self._followup_packet(task_key="verify", role="specialist")
        packet["milestone"]["contractDigest"] = self.contract.digest.removeprefix("contract_sha256_")
        packet["packetId"] = expected_packet_id(packet)
        with self.assertRaises(OrchestrateError) as rejected:
            decode_packet(canonical_packet_json(packet))
        self.assertEqual(rejected.exception.code, "packet_identity_conflict")


if __name__ == "__main__":
    unittest.main()
