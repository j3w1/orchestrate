from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from orchestrate.errors import OrchestrateError
from orchestrate.profile import PROFILE_NAME, ProjectProfile, setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index


def git(root: Path, *arguments: str) -> None:
    subprocess.run(("git", "-C", str(root), *arguments), check=True, capture_output=True)


class DisposableRepo(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Do no harm.\n", encoding="utf-8")
        (self.root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")

    def tearDown(self) -> None:
        self.temporary.cleanup()


class ProfileAndSourceTests(DisposableRepo):
    def test_setup_is_shallow_and_never_runs_manifest_commands(self) -> None:
        marker = self.root / "must-not-exist"
        (self.root / "Makefile").write_text(f"all:\n\t@echo bad > {marker}\n", encoding="utf-8")
        profile = setup_project(self.root)
        self.assertFalse(marker.exists())
        self.assertEqual(profile.value["schema"], "orchestrate-profile/v1")
        self.assertEqual(profile.value["instructions"], ["AGENTS.md"])
        self.assertIn("Makefile", profile.value["commandManifests"])

    def test_profile_rejects_path_escape(self) -> None:
        value = {
            "schema": "orchestrate-profile/v1",
            "project": {"kind": "git", "root": "."},
            "reader": {"kind": "repo"},
            "instructions": ["../AGENTS.md"],
            "taskEntrypoints": [],
            "commandManifests": [],
            "checks": [],
            "strategy": {"kind": "single-owner-first", "maxWorkers": 3},
        }
        (self.root / PROFILE_NAME).write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(OrchestrateError, "escapes"):
            ProjectProfile.load(self.root)

    def test_staged_unstaged_untracked_and_deleted_instructions_invalidate(self) -> None:
        profile = setup_project(self.root)
        initial = build_source_index(profile).digest

        (self.root / "AGENTS.md").write_text("Restricted.\n", encoding="utf-8")
        unstaged = build_source_index(profile).digest
        self.assertNotEqual(initial, unstaged)

        git(self.root, "add", "AGENTS.md")
        staged = build_source_index(profile).digest
        self.assertNotEqual(unstaged, staged)

        nested = self.root / "pkg"
        nested.mkdir()
        (nested / "AGENTS.md").write_text("Nested restriction.\n", encoding="utf-8")
        held = build_source_index(profile)
        self.assertFalse(held.value["candidate"]["coverageComplete"])
        self.assertEqual(held.value["candidate"]["uncoveredChanges"][0]["path"], "pkg/AGENTS.md")
        profile.value["candidateSources"].append("pkg/AGENTS.md")
        untracked = build_source_index(profile)
        self.assertNotEqual(staged, untracked.digest)
        record = next(item for item in untracked.value["sources"] if item["path"] == "pkg/AGENTS.md")
        self.assertEqual(record["authority"], "candidate-restrict-only")

        (self.root / "AGENTS.md").unlink()
        deleted = build_source_index(profile).digest
        self.assertNotEqual(untracked.digest, deleted)

    def test_two_staged_revisions_with_same_status_have_distinct_index_identity(self) -> None:
        profile = setup_project(self.root)
        (self.root / "AGENTS.md").write_text("staged one\n", encoding="utf-8")
        git(self.root, "add", "AGENTS.md")
        first = build_source_index(profile)
        first_change = next(item for item in first.value["candidate"]["coveredChanges"] if item["path"] == "AGENTS.md")

        (self.root / "AGENTS.md").write_text("staged two\n", encoding="utf-8")
        git(self.root, "add", "AGENTS.md")
        second = build_source_index(profile)
        second_change = next(item for item in second.value["candidate"]["coveredChanges"] if item["path"] == "AGENTS.md")

        self.assertEqual(first_change["state"], second_change["state"])
        self.assertNotEqual(first_change["indexSha256"], second_change["indexSha256"])
        self.assertNotEqual(first.digest, second.digest)

    def test_renamed_source_requires_explicit_destination_coverage(self) -> None:
        profile = setup_project(self.root)
        git(self.root, "mv", "AGENTS.md", "POLICY.md")

        held = build_source_index(profile)

        self.assertFalse(held.value["candidate"]["coverageComplete"])
        self.assertEqual(
            [item["path"] for item in held.value["candidate"]["uncoveredChanges"]],
            ["POLICY.md"],
        )
        profile.value["candidateSources"].append("POLICY.md")
        covered = build_source_index(profile)
        self.assertTrue(covered.value["candidate"]["coverageComplete"])
        paths = [item["path"] for item in covered.value["candidate"]["coveredChanges"]]
        self.assertIn("AGENTS.md", paths)
        self.assertIn("POLICY.md", paths)

    def test_link_ancestor_is_held_before_source_read(self) -> None:
        profile = setup_project(self.root)
        outside = self.root.parent / f"{self.root.name}-outside"
        outside.mkdir()
        self.addCleanup(lambda: outside.rmdir() if outside.exists() else None)
        (outside / "AGENTS.md").write_text("outside\n", encoding="utf-8")
        self.addCleanup(lambda: (outside / "AGENTS.md").unlink() if (outside / "AGENTS.md").exists() else None)
        try:
            (self.root / "linked").symlink_to(outside, target_is_directory=True)
        except OSError as exc:
            if sys.platform != "win32":
                self.skipTest(f"directory symlink unavailable: {exc}")
            created = subprocess.run(
                (
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "New-Item -ItemType Junction -Path $env:ORCHESTRATE_TEST_LINK -Target $env:ORCHESTRATE_TEST_TARGET | Out-Null",
                ),
                env={
                    **os.environ,
                    "ORCHESTRATE_TEST_LINK": str(self.root / "linked"),
                    "ORCHESTRATE_TEST_TARGET": str(outside),
                },
                capture_output=True,
                check=False,
            )
            if created.returncode:
                self.skipTest(f"directory junction unavailable: {created.stderr!r}")
        with self.assertRaises(OrchestrateError) as caught:
            build_source_index(profile, extra_sources={"linked/AGENTS.md"})
        self.assertEqual(caught.exception.code, "source_boundary_unresolved")

    def test_env_variants_are_excluded_before_content_hashing(self) -> None:
        profile = setup_project(self.root)
        (self.root / ".env.production").write_text("TOP_SECRET=do-not-hash\n", encoding="utf-8")
        index = build_source_index(profile)
        change = next(item for item in index.value["candidate"]["uncoveredChanges"] if item["path"] == ".env.production")
        self.assertEqual(change["state"], "??")
        self.assertFalse(index.value["candidate"]["coverageComplete"])
        self.assertTrue(any(".env.production" in item for item in index.value["limitations"]))

    def test_secret_bearing_instruction_is_excluded(self) -> None:
        profile = setup_project(self.root)
        profile.value["instructions"].append("secrets.json")
        (self.root / "secrets.json").write_text("do not read", encoding="utf-8")
        with self.assertRaisesRegex(OrchestrateError, "excluded"):
            build_source_index(profile)

    def test_ce_reader_requires_and_uses_manifest_first_routing(self) -> None:
        (self.root / "docs" / "tasks").mkdir(parents=True)
        (self.root / "docs" / "project-log").mkdir(parents=True)
        (self.root / "docs" / "context").mkdir(parents=True)
        (self.root / "docs" / "tasks" / "README.md").write_text(
            "| [CE-1234](CE-1234.md) | active |\n| [CE-12345](CE-12345.md) | unrelated |\n",
            encoding="utf-8",
        )
        (self.root / "docs" / "tasks" / "CE-1234.md").write_text(
            "Context packet — Must read: [packet](../context/CE-1234.md)\n",
            encoding="utf-8",
        )
        (self.root / "docs" / "context" / "CE-1234.md").write_text("bounded context\n", encoding="utf-8")
        (self.root / "docs" / "tasks" / "CE-12345.md").write_text("unrelated task\n", encoding="utf-8")
        (self.root / "docs" / "project-log" / "CE-1234.md").write_text("historical log\n", encoding="utf-8")
        manifest = {
            "entries": [
                {"taskId": "CE-1234", "record": "CE-1234.md"},
                {"taskId": "CE-12345", "record": "unrelated.md"},
            ]
        }
        (self.root / "docs" / "project-log" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        profile = setup_project(self.root, force=True)
        result = read_project(profile, "Implement CE-1234 within approved scope")
        self.assertEqual(result.kind, "ce-gd")
        self.assertEqual(result.routing["registry"], "docs/tasks/README.md")
        self.assertEqual(result.routing["task"], "docs/tasks/CE-1234.md")
        self.assertEqual(result.routing["contextPackets"], ["docs/context/CE-1234.md"])
        self.assertEqual(result.routing["logs"], ["docs/project-log/CE-1234.md"])

        with self.assertRaises(OrchestrateError) as caught:
            read_project(profile, "Implement an unspecified maintenance task")
        self.assertEqual(caught.exception.code, "ce_task_missing")


if __name__ == "__main__":
    unittest.main()
