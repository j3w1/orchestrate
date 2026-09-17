"""Reviewed-checkout first entry point for native Windows and Linux setup."""

from __future__ import annotations

from pathlib import Path
import sys


CHECKOUT = Path(__file__).resolve().parent
sys.path.insert(0, str(CHECKOUT / "src"))

from orchestrate.cli import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
