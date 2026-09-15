from __future__ import annotations

import errno
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import orchestrate.profile as profile_module
from orchestrate.errors import OrchestrateError
from orchestrate.controller import _require_profile_selection
from orchestrate.profile import (
    PROFILE_NAME,
    ProjectProfile,
    _selection_path,
    instruction_inventory,
    setup_project,
)
from orchestrate.readers import _run_ce_query, read_project
from orchestrate.safeio import ProjectSourceState, project_source_state, read_project_bytes
from orchestrate.sources import build_source_index


def git(root: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
    ).stdout


class DisposableRepo(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.state_temporary = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {"ORCHESTRATE_HOME": self.state_temporary.name})
        self.environment.start()
        self.root = Path(self.temporary.name)
        git(self.root, "init", "-q")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "config", "user.name", "Fixture")
        (self.root / "AGENTS.md").write_text("Do no harm.\n", encoding="utf-8")
        (self.root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "fixture")

    def tearDown(self) -> None:
        self.environment.stop()
        self.temporary.cleanup()
        self.state_temporary.cleanup()

    def write_large_ignored_dependency_tree(self) -> None:
        (self.root / ".gitignore").write_text("node_modules/\n", encoding="utf-8")
        git(self.root, "add", ".gitignore")
        git(self.root, "commit", "-qm", "ignore dependencies")
        dependency = self.root / "vendor" / "node_modules"
        for index in range(49):
            package = dependency / f"dependency-{index:03d}-with-a-deliberately-long-name"
            package.mkdir(parents=True)
            (package / "metadata-with-a-deliberately-long-name.json").write_text("{}\n", encoding="utf-8")
        (dependency / "dependency-000-with-a-deliberately-long-name" / "AGENTS.md").write_text(
            "Ignored dependency text.\n",
            encoding="utf-8",
        )


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
        untracked = build_source_index(profile)
        self.assertTrue(untracked.value["candidate"]["coverageComplete"])
        self.assertIn("pkg/AGENTS.md", untracked.value["instructionInventory"])
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

    def test_source_state_distinguishes_non_directory_ancestor_from_environmental_failure(self) -> None:
        ancestor = self.root / "nested"
        ancestor.write_text("not a directory\n", encoding="utf-8")
        self.assertEqual(
            project_source_state(self.root, "nested/source.txt"),
            ProjectSourceState.CHANGED,
        )

        ancestor.unlink()
        ancestor.mkdir()
        source = ancestor / "source.txt"
        source.write_text("still present\n", encoding="utf-8")
        real_lstat = Path.lstat

        def unavailable_lstat(path: Path) -> os.stat_result:
            if path == source:
                raise PermissionError(errno.EACCES, "simulated metadata failure", path)
            return real_lstat(path)

        with patch("orchestrate.safeio.Path.lstat", new=unavailable_lstat):
            self.assertEqual(
                project_source_state(self.root, "nested/source.txt"),
                ProjectSourceState.UNAVAILABLE,
            )

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

    def test_committed_candidate_profile_cannot_change_operational_selection(self) -> None:
        profile = setup_project(self.root)
        profile.value["checks"] = ["approved-check"]
        (self.root / PROFILE_NAME).write_text(json.dumps(profile.value), encoding="utf-8")
        setup_project(self.root, acknowledge_profile=True)
        candidate = self.root / "candidate.py"
        candidate.write_text("candidate = True\n", encoding="utf-8")
        changed = json.loads((self.root / PROFILE_NAME).read_text(encoding="utf-8"))
        changed["reader"] = {"kind": "ce-gd"}
        changed["checks"] = []
        changed["candidateSources"] = ["candidate.py"]
        (self.root / PROFILE_NAME).write_text(json.dumps(changed), encoding="utf-8")
        git(self.root, "add", PROFILE_NAME)
        git(self.root, "commit", "-qm", "candidate attempts authority change")

        loaded = ProjectProfile.load(self.root)

        self.assertEqual(loaded.value["reader"], {"kind": "repo"})
        self.assertEqual(loaded.value["checks"], ["approved-check"])
        self.assertEqual(loaded.value["candidateSources"], [])
        with self.assertRaises(OrchestrateError) as caught:
            _require_profile_selection(loaded)
        self.assertEqual(caught.exception.code, "profile_selection_changed")

        acknowledged = setup_project(self.root, acknowledge_profile=True)
        self.assertFalse(acknowledged.candidate_changed)
        self.assertEqual(acknowledged.selection_source, "explicit-configuration-acknowledgment")
        self.assertEqual(acknowledged.value["reader"], {"kind": "ce-gd"})

    def test_missing_operational_selection_history_holds(self) -> None:
        setup_project(self.root)
        _selection_path(self.root).unlink()

        with self.assertRaises(OrchestrateError) as caught:
            ProjectProfile.load(self.root)

        self.assertEqual(caught.exception.code, "profile_selection_missing")
        with self.assertRaises(OrchestrateError) as setup_caught:
            setup_project(self.root)
        self.assertEqual(setup_caught.exception.code, "profile_selection_missing")
        recovered = setup_project(self.root, acknowledge_profile=True)
        self.assertEqual(recovered.selection_source, "explicit-configuration-acknowledgment")

    def test_profile_selection_rejects_project_contained_state_before_binding_write(self) -> None:
        unsafe = self.root / ".orchestrate-private"
        with patch.dict(os.environ, {"ORCHESTRATE_HOME": str(unsafe)}):
            with self.assertRaises(OrchestrateError) as caught:
                setup_project(self.root)

        self.assertEqual(caught.exception.code, "state_storage_unsafe")
        self.assertFalse(unsafe.exists())

    def test_ignored_nested_instruction_is_excluded_by_default(self) -> None:
        (self.root / ".gitignore").write_text("nested/AGENTS.md\n", encoding="utf-8")
        git(self.root, "add", ".gitignore")
        git(self.root, "commit", "-qm", "ignore nested authority")
        (self.root / "nested").mkdir()
        (self.root / "nested" / "AGENTS.md").write_text("Nested authority.\n", encoding="utf-8")
        profile = setup_project(self.root)

        result = read_project(profile, "Apply project instructions")
        indexed = build_source_index(profile)

        self.assertNotIn("nested/AGENTS.md", indexed.value["instructionInventory"])
        self.assertNotIn("nested/AGENTS.md", [item["path"] for item in indexed.value["sources"]])
        self.assertTrue(indexed.value["candidate"]["coverageComplete"])
        self.assertEqual(result.routing["authority"], ["AGENTS.md"])

    def test_explicitly_selected_ignored_instruction_is_read_as_candidate_restriction(self) -> None:
        (self.root / ".gitignore").write_text("nested/AGENTS.md\n", encoding="utf-8")
        git(self.root, "add", ".gitignore")
        git(self.root, "commit", "-qm", "ignore nested authority")
        (self.root / "nested").mkdir()
        (self.root / "nested" / "AGENTS.md").write_text("Nested restriction.\n", encoding="utf-8")
        profile = setup_project(self.root)
        profile.value["instructions"].append("nested/AGENTS.md")
        (self.root / PROFILE_NAME).write_text(json.dumps(profile.value), encoding="utf-8")
        selected = setup_project(self.root, acknowledge_profile=True)

        result = read_project(selected, "Apply project instructions")
        indexed = build_source_index(selected, extra_sources=set(result.consulted_paths))

        self.assertEqual(result.routing["authority"], ["AGENTS.md", "nested/AGENTS.md"])
        record = next(item for item in indexed.value["sources"] if item["path"] == "nested/AGENTS.md")
        self.assertEqual(record["authority"], "candidate-restrict-only")
        self.assertIsNone(record["headSha256"])
        self.assertEqual(len(record["sha256"]), 64)
        self.assertTrue(indexed.value["candidate"]["coverageComplete"])

    def test_large_ignored_dependency_tree_stays_within_bounded_inventory_output(self) -> None:
        self.write_large_ignored_dependency_tree()

        self.assertEqual(profile_module.MAX_GIT_PATH_BYTES, 4 * 1024 * 1024)
        with patch.object(profile_module, "MAX_GIT_PATH_BYTES", 256):
            with self.assertRaises(OrchestrateError) as legacy:
                profile_module._git_bytes(
                    self.root,
                    "ls-files",
                    "-z",
                    "--others",
                    "--ignored",
                    "--exclude-standard",
                )
            self.assertEqual(legacy.exception.code, "instruction_inventory_too_large")
            self.assertEqual(instruction_inventory(self.root), ("AGENTS.md",))
            profile = setup_project(self.root)

        self.assertEqual(profile.value["instructions"], ["AGENTS.md"])

    def test_more_than_256_conventional_instructions_fail_closed(self) -> None:
        profile = setup_project(self.root)
        for index in range(256):
            directory = self.root / f"pkg{index:03d}"
            directory.mkdir()
            (directory / "AGENTS.md").write_text("Nested instruction.\n", encoding="utf-8")

        for operation in (lambda: instruction_inventory(self.root), lambda: build_source_index(profile)):
            with self.subTest(operation=operation):
                with self.assertRaises(OrchestrateError) as caught:
                    operation()
                self.assertEqual(caught.exception.code, "instruction_inventory_too_large")

    def test_untracked_nested_instruction_is_inventoried_beside_excluded_ignored_sibling(self) -> None:
        (self.root / ".gitignore").write_text("pkg/node_modules/\n", encoding="utf-8")
        git(self.root, "add", ".gitignore")
        git(self.root, "commit", "-qm", "ignore nested dependencies")
        (self.root / "pkg" / "node_modules" / "dep").mkdir(parents=True)
        (self.root / "pkg" / "AGENTS.md").write_text("Package restriction.\n", encoding="utf-8")
        (self.root / "pkg" / "node_modules" / "dep" / "AGENTS.md").write_text(
            "Ignored dependency text.\n",
            encoding="utf-8",
        )
        profile = setup_project(self.root)

        indexed = build_source_index(profile)

        self.assertEqual(indexed.value["instructionInventory"], ["AGENTS.md", "pkg/AGENTS.md"])
        record = next(item for item in indexed.value["sources"] if item["path"] == "pkg/AGENTS.md")
        self.assertEqual(record["authority"], "candidate-restrict-only")
        self.assertNotIn(
            "pkg/node_modules/dep/AGENTS.md",
            [item["path"] for item in indexed.value["sources"]],
        )
        self.assertTrue(indexed.value["candidate"]["coverageComplete"])

    def test_selected_non_conventional_instruction_is_read_and_routed_as_authority(self) -> None:
        git(self.root, "config", "core.autocrlf", "false")
        (self.root / "POLICY.md").write_text("Selected policy.\n", encoding="utf-8")
        git(self.root, "add", "POLICY.md")
        git(self.root, "commit", "-qm", "add selected policy")
        self.assertEqual(
            git(self.root, "show", "HEAD:POLICY.md"),
            (self.root / "POLICY.md").read_bytes(),
        )
        profile = setup_project(self.root)
        profile.value["instructions"].append("POLICY.md")
        (self.root / PROFILE_NAME).write_text(json.dumps(profile.value), encoding="utf-8")
        selected = setup_project(self.root, acknowledge_profile=True)

        result = read_project(selected, "Apply the selected policy")
        indexed = build_source_index(selected, extra_sources=set(result.consulted_paths))

        self.assertEqual(result.instructions["POLICY.md"].splitlines(), ["Selected policy."])
        self.assertIn("POLICY.md", result.consulted_paths)
        self.assertIn("POLICY.md", result.routing["authority"])
        record = next(item for item in indexed.value["sources"] if item["path"] == "POLICY.md")
        self.assertEqual(record["authority"], "consulted")

        (self.root / "POLICY.md").write_text("Candidate restriction.\n", encoding="utf-8")
        restricted_result = read_project(selected, "Apply the selected policy")
        restricted = build_source_index(selected, extra_sources=set(restricted_result.consulted_paths))
        restricted_record = next(item for item in restricted.value["sources"] if item["path"] == "POLICY.md")
        self.assertEqual(restricted_result.instructions["POLICY.md"].splitlines(), ["Candidate restriction."])
        self.assertEqual(restricted_record["authority"], "candidate-restrict-only")
        self.assertIn(
            "Candidate instruction changes can restrict but cannot grant execution authority.",
            restricted.value["limitations"],
        )

    def test_conventional_credential_store_is_excluded_before_hashing(self) -> None:
        (self.root / ".aws").mkdir()
        (self.root / ".aws" / "credentials").write_text("synthetic-placeholder\n", encoding="utf-8")
        profile = setup_project(self.root)
        profile.value["candidateSources"].append(".aws/credentials")
        with self.assertRaises(OrchestrateError) as caught:
            build_source_index(profile)
        self.assertEqual(caught.exception.code, "secret_source_excluded")

    def test_source_content_is_read_from_the_verified_handle_not_a_reopened_path(self) -> None:
        expected = (self.root / "AGENTS.md").read_bytes()
        with patch.object(Path, "read_bytes", side_effect=AssertionError("pathname reopened")):
            self.assertEqual(read_project_bytes(self.root, "AGENTS.md"), expected)

    def _write_ce_fixture(self, shard_tasks: list[list[str]]) -> tuple[ProjectProfile, list[dict[str, object]]]:
        (self.root / "docs" / "tasks").mkdir(parents=True)
        (self.root / "docs" / "project-log").mkdir(parents=True)
        (self.root / "docs" / "context").mkdir(parents=True)
        (self.root / "scripts" / "quality").mkdir(parents=True)
        (self.root / "docs" / "tasks" / "README.md").write_text(
            "| [CE-1234](CE-1234.md) | active |\n| [CE-12345](CE-12345.md) | unrelated |\n",
            encoding="utf-8",
        )
        (self.root / "docs" / "tasks" / "CE-1234.md").write_text(
            "Context packet — Must read: [packet](../context/CE-1234.md)\n",
            encoding="utf-8",
        )
        (self.root / "docs" / "tasks" / "CE-12345.md").write_text("unrelated\n", encoding="utf-8")
        (self.root / "docs" / "context" / "CE-1234.md").write_text("bounded context\n", encoding="utf-8")
        (self.root / "package.json").write_text(
            json.dumps({"scripts": {"project-log:query": "node scripts/quality/project-log.mjs query"}}),
            encoding="utf-8",
        )
        (self.root / "scripts" / "quality" / "project-log.mjs").write_text(
            "// synthetic query adapter fixture; execution is mocked\n",
            encoding="utf-8",
        )
        shards: list[dict[str, object]] = []
        for sequence, task_ids in enumerate(shard_tasks, start=1):
            relative = f"docs/project-log/shard-{sequence:03d}.md"
            content = f"# Synthetic shard {sequence}\n"
            (self.root / relative).write_text(content, encoding="utf-8")
            shards.append(
                {
                    "sequence": sequence,
                    "path": relative,
                    "state": "closed",
                    "bytes": len(content.encode("utf-8")),
                    "sha256": f"{sequence:x}" * 64,
                    "git_blob_sha": f"{sequence:x}" * 40,
                    "entry_count": 1,
                    "first_heading": f"Synthetic {sequence}",
                    "last_heading": f"Synthetic {sequence}",
                    "task_ids": task_ids,
                    "legacy_start_byte": sequence - 1,
                    "legacy_end_byte_exclusive": sequence,
                }
            )
        manifest = {
            "schema_version": 1,
            "kind": "ce-systems-project-log-manifest",
            "agent_access": {
                "default_loading": "manifest-only",
                "closed_shards_preloaded": False,
                "query_command": "pnpm run project-log:query -- <term>",
            },
            "shards": shards,
        }
        (self.root / "docs" / "project-log" / "manifest.json").write_text(
            json.dumps(manifest),
            encoding="utf-8",
        )
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "synthetic CE query fixture")
        return setup_project(self.root, force=True), shards

    @staticmethod
    def _query_result(shards: list[dict[str, object]], *, omitted: int = 0) -> dict[str, object]:
        selected = [item for item in shards if "CE-1234" in item["task_ids"]]
        return {
            "term": "CE-1234",
            "exact_task": "CE-1234",
            "selected_shards": [item["path"] for item in selected],
            "matches": [
                {
                    "path": item["path"],
                    "sequence": item["sequence"],
                    "heading": f"Synthetic {item['sequence']}",
                    "text": "sanitized synthetic match",
                }
                for item in selected
            ],
            "omitted_matches": omitted,
        }

    def test_ce_reader_requires_and_uses_manifest_first_routing(self) -> None:
        profile, shards = self._write_ce_fixture([["CE-1234"], ["CE-9999"]])
        with patch("orchestrate.readers._run_ce_query", return_value=self._query_result(shards)):
            result = read_project(profile, "Implement CE-1234 within selected scope")
        self.assertEqual(result.kind, "ce-gd")
        self.assertEqual(result.routing["registry"], "docs/tasks/README.md")
        self.assertEqual(result.routing["task"], "docs/tasks/CE-1234.md")
        self.assertEqual(result.routing["contextPackets"], ["docs/context/CE-1234.md"])
        self.assertEqual(result.routing["logs"], ["docs/project-log/shard-001.md"])
        self.assertNotIn("docs/project-log/shard-001.md", result.consulted_paths)
        self.assertIn("scripts/quality/project-log.mjs", result.consulted_paths)

        with self.assertRaises(OrchestrateError) as caught:
            read_project(profile, "Implement an unspecified maintenance task")
        self.assertEqual(caught.exception.code, "ce_task_missing")
        with self.assertRaises(OrchestrateError) as five_digits:
            read_project(profile, "Implement CE-12345")
        self.assertEqual(five_digits.exception.code, "ce_task_missing")

    def test_ce_manifest_allows_multiple_declared_selected_shards_without_unrelated_siblings(self) -> None:
        profile, shards = self._write_ce_fixture([["CE-1234"], ["CE-9999"], ["CE-1234"]])
        with patch("orchestrate.readers._run_ce_query", return_value=self._query_result(shards)):
            result = read_project(profile, "Implement CE-1234")

        self.assertEqual(
            result.routing["logs"],
            ["docs/project-log/shard-001.md", "docs/project-log/shard-003.md"],
        )

    def test_ce_manifest_and_query_hold_unknown_cross_task_or_incomplete_shapes(self) -> None:
        profile, shards = self._write_ce_fixture([["CE-1234"], ["CE-9999"]])
        manifest_path = self.root / "docs" / "project-log" / "manifest.json"
        original = manifest_path.read_text(encoding="utf-8")
        manifest_path.write_text(json.dumps({"CE-1234": {"record": "shard-001.md"}}), encoding="utf-8")
        with self.assertRaises(OrchestrateError) as malformed:
            read_project(profile, "Implement CE-1234")
        self.assertEqual(malformed.exception.code, "ce_manifest_invalid")
        manifest_path.write_text(original, encoding="utf-8")

        unrelated = self._query_result(shards)
        unrelated["selected_shards"] = ["docs/project-log/shard-002.md"]
        with patch("orchestrate.readers._run_ce_query", return_value=unrelated):
            with self.assertRaises(OrchestrateError) as cross_task:
                read_project(profile, "Implement CE-1234")
        self.assertEqual(cross_task.exception.code, "ce_query_invalid")

        with patch("orchestrate.readers._run_ce_query", return_value=self._query_result(shards, omitted=1)):
            with self.assertRaises(OrchestrateError) as incomplete:
                read_project(profile, "Implement CE-1234")
        self.assertEqual(incomplete.exception.code, "ce_query_incomplete")

    def test_ce_query_wrapper_uses_only_the_fixed_bounded_machine_interface(self) -> None:
        profile, shards = self._write_ce_fixture([["CE-1234"]])
        manifest_text = (self.root / "docs" / "project-log" / "manifest.json").read_text(encoding="utf-8")
        package_text = (self.root / "package.json").read_text(encoding="utf-8")
        script_text = (self.root / "scripts" / "quality" / "project-log.mjs").read_bytes().decode("utf-8")
        expected = self._query_result(shards)
        real_run = subprocess.run
        query_calls: list[tuple[str, ...]] = []

        def synthetic_run(arguments: tuple[str, ...], **keywords: object) -> subprocess.CompletedProcess[bytes]:
            if arguments[0] == "pnpm":
                query_calls.append(arguments)
                return subprocess.CompletedProcess(arguments, 0, json.dumps(expected).encode("utf-8"), b"")
            return real_run(arguments, **keywords)

        with patch("orchestrate.readers.subprocess.run", side_effect=synthetic_run):
            result = _run_ce_query(
                profile,
                "CE-1234",
                manifest_text=manifest_text,
                package_text=package_text,
                script_text=script_text,
            )

        self.assertEqual(result, expected)
        self.assertEqual(
            query_calls,
            [
                (
                    "pnpm",
                    "--silent",
                    "run",
                    "project-log:query",
                    "--",
                    "CE-1234",
                    "--json",
                    "--max-entries",
                    "12",
                    "--max-bytes",
                    "32768",
                )
            ],
        )

    def test_ce_query_wrapper_revalidates_packet_bound_sources_before_pnpm(self) -> None:
        profile, shards = self._write_ce_fixture([["CE-1234"]])
        manifest_text = (self.root / "docs" / "project-log" / "manifest.json").read_text(encoding="utf-8")
        package_text = (self.root / "package.json").read_text(encoding="utf-8")
        script_path = self.root / "scripts" / "quality" / "project-log.mjs"
        script_text = script_path.read_text(encoding="utf-8")
        captured = {
            "package.json": package_text.encode("utf-8"),
            "scripts/quality/project-log.mjs": script_text.encode("utf-8"),
            "docs/project-log/manifest.json": manifest_text.encode("utf-8"),
        }
        expected_identities = {
            path: {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
            for path, raw in captured.items()
        }
        script_path.write_text("// clean post-prepass replacement\n", encoding="utf-8")
        git(self.root, "add", "scripts/quality/project-log.mjs")
        git(self.root, "commit", "-qm", "replace query after prepass")
        real_run = subprocess.run
        query_calls: list[tuple[str, ...]] = []

        def synthetic_run(arguments: tuple[str, ...], **keywords: object) -> subprocess.CompletedProcess[bytes]:
            if arguments[0] == "pnpm":
                query_calls.append(arguments)
                return subprocess.CompletedProcess(
                    arguments,
                    0,
                    json.dumps(self._query_result(shards)).encode("utf-8"),
                    b"",
                )
            return real_run(arguments, **keywords)

        with patch("orchestrate.readers.subprocess.run", side_effect=synthetic_run):
            with self.assertRaises(OrchestrateError) as changed:
                _run_ce_query(
                    profile,
                    "CE-1234",
                    manifest_text=manifest_text,
                    package_text=package_text,
                    script_text=script_text,
                    expected_source_identities=expected_identities,
                )
        self.assertEqual(changed.exception.code, "ce_query_source_changed")
        self.assertEqual(query_calls, [])


if __name__ == "__main__":
    unittest.main()
