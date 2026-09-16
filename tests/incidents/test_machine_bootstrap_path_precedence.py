from __future__ import annotations

from pathlib import Path
import unittest

from orchestrate.machine_bootstrap import register_user_path


class MemoryUserPath:
    def __init__(self, value: str) -> None:
        self.value = value

    def read(self) -> str:
        return self.value

    def write(self, value: str) -> None:
        self.value = value


class MachineBootstrapPathPrecedenceIncident(unittest.TestCase):
    def test_repair_moves_one_canonical_entry_ahead_of_foreign_command_locations(self) -> None:
        entry = Path(r"C:\Users\fixture\AppData\Local\orchestrate\bin")
        store = MemoryUserPath(
            ";".join(
                (
                    r"C:\foreign-command",
                    str(entry).upper(),
                    r"C:\unrelated",
                    str(entry),
                )
            )
        )

        self.assertTrue(register_user_path(store, entry))
        self.assertEqual(
            store.value.split(";"),
            [str(entry), r"C:\foreign-command", r"C:\unrelated"],
        )
        self.assertFalse(register_user_path(store, entry))


if __name__ == "__main__":
    unittest.main()
