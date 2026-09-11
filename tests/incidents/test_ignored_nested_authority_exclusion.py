from __future__ import annotations

import json

from orchestrate.errors import OrchestrateError
from orchestrate.profile import PROFILE_NAME, setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from test_profile_sources import DisposableRepo, git


class IgnoredNestedAuthorityExclusionIncident(DisposableRepo):
    def test_ignored_nested_authority_requires_acknowledged_explicit_selection(self) -> None:
        ignored = "services/auth/node_modules/fastify/AGENTS.md"
        (self.root / ".gitignore").write_text("node_modules/\n", encoding="utf-8")
        git(self.root, "add", ".gitignore")
        git(self.root, "commit", "-qm", "ignore dependencies")
        path = self.root / ignored
        path.parent.mkdir(parents=True)
        path.write_text("Dependency-local instructions.\n", encoding="utf-8")
        profile = setup_project(self.root)

        default_read = read_project(profile, "Respect repository authority")
        default_index = build_source_index(profile)
        self.assertEqual(default_read.routing["authority"], ["AGENTS.md"])
        self.assertNotIn(ignored, default_index.value["instructionInventory"])

        profile.value["instructions"].append(ignored)
        (self.root / PROFILE_NAME).write_text(json.dumps(profile.value), encoding="utf-8")
        with self.assertRaises(OrchestrateError) as changed:
            setup_project(self.root)
        self.assertEqual(changed.exception.code, "profile_selection_changed")

        selected = setup_project(self.root, acknowledge_profile=True)
        selected_read = read_project(selected, "Respect repository authority")
        selected_index = build_source_index(selected)
        self.assertEqual(selected_read.routing["authority"], ["AGENTS.md", ignored])
        record = next(item for item in selected_index.value["sources"] if item["path"] == ignored)
        self.assertEqual(record["authority"], "candidate-restrict-only")
