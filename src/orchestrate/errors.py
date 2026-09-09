"""Typed, user-facing failures for orchestrate."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class OrchestrateError(RuntimeError):
    """A bounded failure with a stable machine-readable code."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "orchestrate_error",
        data: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.data = dict(data) if data is not None else None
