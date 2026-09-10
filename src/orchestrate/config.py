"""Host-local role and launch roster; never stored in a project profile."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Literal

from .errors import OrchestrateError
from .state import state_home


CONFIG_SCHEMA = "orchestrate-user-config/v1"


@dataclass(frozen=True, slots=True)
class ModelChoice:
    agent: str
    model: str
    effort: str


RoleName = Literal["owner", "specialist", "reviewer"]
ROLE_NAMES: tuple[RoleName, ...] = ("owner", "specialist", "reviewer")
EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max", "ultra"})


@dataclass(frozen=True, slots=True)
class RoleChoice:
    """One requested launch, kept distinct from Orca's effective receipt."""

    role: RoleName
    agent: Literal["codex", "claude"]
    model: str | None
    effort: str | None

    def requested(self) -> dict[str, str]:
        value = {"agent": self.agent}
        if self.model is not None:
            value["model"] = self.model
        if self.effort is not None:
            value["effort"] = self.effort
        return value


@dataclass(frozen=True, slots=True)
class RoleRoster:
    owner: RoleChoice
    specialist: RoleChoice
    reviewer: RoleChoice

    def choice(self, role: RoleName) -> RoleChoice:
        return getattr(self, role)


def _default_roster() -> RoleRoster:
    return RoleRoster(
        owner=RoleChoice("owner", "codex", "gpt-5.6-sol", "high"),
        specialist=RoleChoice("specialist", "codex", "gpt-5.6-sol", "high"),
        reviewer=RoleChoice("reviewer", "codex", "gpt-5.6-sol", "xhigh"),
    )


def _load_config(path: Path | None) -> Mapping[str, Any] | None:
    selected = state_home() / "config.json" if path is None else path
    if not selected.exists():
        return None
    try:
        value = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError(f"Cannot read user model roster: {exc}", code="user_config_unreadable") from exc
    if not isinstance(value, Mapping) or value.get("schema") != CONFIG_SCHEMA:
        raise OrchestrateError("User model roster has an unsupported schema", code="user_config_invalid")
    return value


def _role_choice(role: RoleName, raw: object, default: RoleChoice | None) -> RoleChoice:
    if raw is None and default is not None:
        return default
    if not isinstance(raw, Mapping):
        raise OrchestrateError(f"User model roster has no '{role}' slot", code="user_config_invalid")
    agent = raw.get("agent")
    model = raw.get("model")
    effort = raw.get("effort")
    if agent not in {"codex", "claude"}:
        raise OrchestrateError(
            f"The {role} slot must name the supported codex or claude agent",
            code="user_config_invalid",
        )
    if model is not None and (not isinstance(model, str) or not model.strip()):
        raise OrchestrateError(f"The {role} model must be a non-empty provider ID", code="user_config_invalid")
    if effort is not None and (not isinstance(effort, str) or effort not in EFFORTS):
        raise OrchestrateError(f"The {role} effort is unsupported", code="user_config_invalid")
    if effort is not None and model is None:
        raise OrchestrateError(f"The {role} effort requires an explicit model", code="user_config_invalid")
    if agent == "claude" and model is None:
        raise OrchestrateError(
            f"The {role} Claude slot requires a user-configured provider model ID",
            code="user_config_invalid",
        )
    return RoleChoice(role, agent, model.strip() if isinstance(model, str) else None, effort)


def load_role_roster(*, path: Path | None = None) -> RoleRoster:
    """Load the three bounded host roles without probing or guessing providers."""

    defaults = _default_roster()
    value = _load_config(path)
    if value is None:
        return defaults
    raw_roles = value.get("roles")
    if raw_roles is None:
        # Preserve the accepted v1 owner-only config spelling. The absent new
        # slots take reviewed defaults; no project profile receives this data.
        raw_roles = value.get("models")
    if not isinstance(raw_roles, Mapping):
        raise OrchestrateError("User model roster has no role map", code="user_config_invalid")
    unknown = set(raw_roles) - set(ROLE_NAMES)
    if unknown:
        raise OrchestrateError(
            f"User model roster has unsupported roles: {sorted(unknown)}",
            code="user_config_invalid",
        )
    return RoleRoster(
        owner=_role_choice("owner", raw_roles.get("owner"), defaults.owner),
        specialist=_role_choice("specialist", raw_roles.get("specialist"), defaults.specialist),
        reviewer=_role_choice("reviewer", raw_roles.get("reviewer"), defaults.reviewer),
    )


def load_owner_model(*, path: Path | None = None) -> ModelChoice:
    """Load the named owner slot, with the approved Sol/high default."""

    owner = load_role_roster(path=path).owner
    if owner.agent != "codex" or owner.model is None or owner.effort is None:
        raise OrchestrateError(
            "The accepted first-increment owner path requires an explicit Codex model and effort",
            code="user_config_invalid",
        )
    return ModelChoice(owner.agent, owner.model, owner.effort)
