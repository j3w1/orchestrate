"""Shared spelling-robust matching for path-sensitive fault injection."""

from __future__ import annotations

from os import PathLike
from pathlib import Path


class ResolvedPathFault:
    """Match one intended path identity and count each intercepted call."""

    def __init__(self, expected: str | PathLike[str]) -> None:
        self.expected = Path(expected).resolve(strict=False)
        self.interceptions = 0

    def matches(
        self,
        candidate: str | PathLike[str],
        *,
        relative_to: str | PathLike[str] | None = None,
    ) -> bool:
        actual = Path(candidate)
        if relative_to is not None and not actual.is_absolute():
            actual = Path(relative_to) / actual
        if actual.resolve(strict=False) != self.expected:
            return False
        self.interceptions += 1
        return True
