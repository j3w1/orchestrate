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
        if data is None and code.startswith("machine_bootstrap_") and (
            "identity" in code or code == "machine_bootstrap_verification_failed"
        ):
            data = {
                "component": "identity-proof",
                "expected": "available-and-matching",
                "observed": "unavailable-or-divergent",
            }
        self.data = dict(data) if data is not None else None
