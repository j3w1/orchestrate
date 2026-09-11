from __future__ import annotations

from pathlib import Path
import sys
from unittest.mock import patch

import orchestrate.sources as sources_module
from orchestrate.admission import worker_preflight
from orchestrate.errors import OrchestrateError
from orchestrate.packets import canonical_packet_json, make_packet
from orchestrate.profile import (
    ProjectProfile,
    _selection_path,
    instruction_inventory,
    setup_project,
)
from orchestrate.readers import read_project
from orchestrate.safeio import read_project_bytes
from orchestrate.sources import build_source_index
from orchestrate.state import StateStore
from test_admission import PreflightClient
from test_profile_sources import DisposableRepo, git


class SelectedBeforeIgnoredAuthorityAcknowledgmentIncident(DisposableRepo):
    def test_selected_before_ignored_holds_until_same_digest_acknowledgment(self) -> None:
        ignored = "dependencies/generated/AGENTS.md"
        ignored_bytes = b"Dependency-local instructions.\n"
        path = self.root / ignored
        path.parent.mkdir(parents=True)
        path.write_bytes(ignored_bytes)

        initially_selected = setup_project(self.root)
        self.assertEqual(initially_selected.value["instructions"], ["AGENTS.md", ignored])
        self.assertEqual(initially_selected.selection_source, "initial-setup-selection")

        (self.root / ".gitignore").write_text("dependencies/\n", encoding="utf-8")
        git(self.root, "add", ".orchestrate.json", ".gitignore")
        git(self.root, "commit", "-qm", "ignore previously selected dependency authority")
        self.assertEqual(git(self.root, "status", "--porcelain=v1", "--untracked-files=all"), b"")
        git(self.root, "check-ignore", ignored)
        self.assertEqual(instruction_inventory(self.root), ("AGENTS.md",))
        selection_before_hold = _selection_path(self.root).read_bytes()

        with (
            patch("orchestrate.profile.read_project_bytes", wraps=read_project_bytes) as profile_reads,
            patch("orchestrate.sources.read_project_bytes", wraps=read_project_bytes) as source_reads,
            patch("orchestrate.sources._sha256", wraps=sources_module._sha256) as source_hashes,
        ):
            for operation in (
                lambda: ProjectProfile.load(self.root),
                lambda: setup_project(self.root),
                lambda: read_project(initially_selected, "Respect repository authority"),
                lambda: build_source_index(initially_selected),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaises(OrchestrateError) as caught:
                        operation()
                    self.assertEqual(caught.exception.code, "instruction_acknowledgment_required")
                    self.assertEqual(caught.exception.data, {"selectedPaths": [ignored]})
                    self.assertIn("setup --acknowledge-profile", str(caught.exception))

        read_paths = [
            call.args[1]
            for call in (*profile_reads.call_args_list, *source_reads.call_args_list)
        ]
        self.assertNotIn(ignored, read_paths)
        self.assertNotIn(ignored_bytes, [call.args[0] for call in source_hashes.call_args_list])
        self.assertEqual(_selection_path(self.root).read_bytes(), selection_before_hold)

        acknowledged = setup_project(self.root, acknowledge_profile=True)
        self.assertEqual(acknowledged.digest, initially_selected.digest)
        self.assertNotEqual(
            acknowledged.selection_history_digest,
            initially_selected.selection_history_digest,
        )
        self.assertEqual(
            acknowledged.selection_source,
            "explicit-configuration-acknowledgment",
        )
        durably_selected = ProjectProfile.load(self.root)
        self.assertEqual(
            durably_selected.selection_source,
            "explicit-configuration-acknowledgment",
        )

        result = read_project(durably_selected, "Respect repository authority")
        indexed = build_source_index(durably_selected)
        self.assertFalse(indexed.value["candidate"]["dirty"])
        self.assertTrue(indexed.value["candidate"]["coverageComplete"])
        self.assertEqual(result.routing["authority"], ["AGENTS.md", ignored])
        self.assertEqual(result.instructions[ignored].encode("utf-8"), ignored_bytes)
        record = next(item for item in indexed.value["sources"] if item["path"] == ignored)
        self.assertEqual(record["authority"], "candidate-restrict-only")
        self.assertEqual(record["bytes"], len(ignored_bytes))

        path.unlink()
        with self.assertRaises(OrchestrateError) as removed:
            ProjectProfile.load(self.root)
        self.assertEqual(removed.exception.code, "source_unavailable")
        with self.assertRaises(OrchestrateError) as stale_reader:
            read_project(durably_selected, "Respect repository authority")
        self.assertEqual(stale_reader.exception.code, "source_unavailable")

    def test_post_profile_discovery_packet_holds_before_ignored_instruction_readback(self) -> None:
        ignored = "dependencies/generated/AGENTS.md"
        ignored_bytes = b"Late-discovered dependency instructions.\n"
        initially_selected = setup_project(self.root)
        self.assertEqual(initially_selected.value["instructions"], ["AGENTS.md"])
        self.assertEqual(initially_selected.selection_source, "initial-setup-selection")

        path = self.root / ignored
        path.parent.mkdir(parents=True)
        path.write_bytes(ignored_bytes)
        reader = read_project(initially_selected, "Respect repository authority")
        self.assertEqual(reader.routing["authority"], ["AGENTS.md", ignored])
        sources = build_source_index(
            initially_selected,
            extra_sources=set(reader.consulted_paths),
        )
        self.assertIn(ignored, [record["path"] for record in sources.value["sources"]])
        with StateStore(self.root) as store:
            run = store.create_run(
                objective="Respect repository authority",
                profile_digest=initially_selected.digest,
                source_digest=sources.digest,
            )
            run = store.update_run(
                run.local_id,
                native_run_id="run_1",
                task_id="task_1",
                phase="awaiting_preflight",
            )
            local_id = run.local_id
            packet = make_packet(
                objective=run.objective,
                profile=initially_selected,
                sources=sources,
                launch={"agent": "codex", "model": "gpt-5.6-sol", "effort": "high"},
                python_executable=str(Path(sys.executable).resolve()),
                run_id="run_1",
                reader=reader,
            )
            store.save_packet(run.local_id, "task_1", canonical_packet_json(packet))
            store.record_worker_resource_binding(
                run.local_id,
                dispatch_id="dispatch_1",
                resource_id="terminal-resource-1",
                terminal_handle="term_worker",
                worktree_id=f"repo::{self.root.resolve()}",
                readback={"fixture": "validated-start-readback"},
            )

        (self.root / ".gitignore").write_text("dependencies/\n", encoding="utf-8")
        git(self.root, "add", ".orchestrate.json", ".gitignore")
        git(self.root, "commit", "-qm", "hide late-discovered dependency authority")
        self.assertEqual(git(self.root, "status", "--porcelain=v1", "--untracked-files=all"), b"")
        git(self.root, "check-ignore", ignored)
        self.assertEqual(instruction_inventory(self.root), ("AGENTS.md",))

        client = PreflightClient(self.root, packet)
        with (
            patch("orchestrate.profile.read_project_bytes", wraps=read_project_bytes) as profile_reads,
            patch("orchestrate.admission.read_project_bytes", wraps=read_project_bytes) as admission_reads,
            patch("orchestrate.sources.read_project_bytes", wraps=read_project_bytes) as source_reads,
        ):
            with self.assertRaises(OrchestrateError) as caught:
                worker_preflight(
                    self.root,
                    run_id="run_1",
                    task_id="task_1",
                    dispatch_id="dispatch_1",
                    packet_id=str(packet["packetId"]),
                    client=client,
                    environment={"ORCA_TERMINAL_HANDLE": "term_worker"},
                    platform="win32",
                )

        self.assertEqual(caught.exception.code, "preflight_rejected")
        self.assertEqual(caught.exception.data["cause"], "instruction_acknowledgment_required")  # type: ignore[index]
        mismatch = caught.exception.data["mismatches"][0]  # type: ignore[index]
        self.assertEqual(mismatch["code"], "instruction_acknowledgment_required")
        self.assertEqual(mismatch["details"], {"packetInstructionPaths": [ignored]})
        read_paths = [
            call.args[1]
            for call in (
                *profile_reads.call_args_list,
                *admission_reads.call_args_list,
                *source_reads.call_args_list,
            )
        ]
        self.assertNotIn(ignored, read_paths)
        self.assertEqual(client.calls, [])
        with StateStore(self.root) as store:
            observed = store.get_preflight(local_id, "task_1", "dispatch_1")
        self.assertEqual(observed["outcome"], "rejected")  # type: ignore[index]
        self.assertIsNone(observed["native"])  # type: ignore[index]
        self.assertEqual(
            observed["mismatches"][0]["code"],  # type: ignore[index]
            "instruction_acknowledgment_required",
        )
