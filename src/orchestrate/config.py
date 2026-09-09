"""User-level model roster; never stored in a project profile."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from .errors import OrchestrateError
from .state import state_home


CONFIG_SCHEMA = "orchestrate-user-config/v1"


@dataclass(frozen=True, slots=True)
class ModelChoice:
    agent: str
    model: str
    effort: str


def load_owner_model(*, path: Path | None = None) -> ModelChoice:
    """Load the named owner slot, with the approved Sol/high default."""

    selected = state_home() / "config.json" if path is None else path
    if not selected.exists():
        return ModelChoice("codex", "gpt-5.6-sol", "high")
    try:
        value = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError(f"Cannot read user model roster: {exc}", code="user_config_unreadable") from exc
    roster = value.get("models") if isinstance(value, dict) and value.get("schema") == CONFIG_SCHEMA else None
    owner = roster.get("owner") if isinstance(roster, dict) else None
    if not isinstance(owner, dict):
        raise OrchestrateError("User model roster has no 'owner' slot", code="user_config_invalid")
    agent, model, effort = owner.get("agent"), owner.get("model"), owner.get("effort")
    if (
        agent != "codex"
        or not all(isinstance(item, str) and item for item in (model, effort))
        or effort not in {"low", "medium", "high", "xhigh", "max", "ultra"}
    ):
        raise OrchestrateError("The owner slot must name a Codex model and effort", code="user_config_invalid")
    return ModelChoice(agent, model, effort)
