from __future__ import annotations

from unittest.mock import patch

import orchestrate.profile as profile_module
from orchestrate.profile import instruction_inventory, setup_project
from orchestrate.readers import read_project
from orchestrate.sources import build_source_index
from test_profile_sources import DisposableRepo


class LargeIgnoredDependencyInventoryIncident(DisposableRepo):
    def test_ignored_dependency_inventory_does_not_enter_discovery(self) -> None:
        self.write_large_ignored_dependency_tree()

        with patch.object(profile_module, "MAX_GIT_PATH_BYTES", 256):
            profile = setup_project(self.root)
            result = read_project(profile, "Respect the repository authority")
            indexed = build_source_index(profile)

        self.assertEqual(instruction_inventory(self.root), ("AGENTS.md",))
        self.assertEqual(result.routing["authority"], ["AGENTS.md"])
        self.assertEqual(indexed.value["instructionInventory"], ["AGENTS.md"])
        self.assertTrue(indexed.value["candidate"]["coverageComplete"])
