"""Bounded native DAG, role-launch, review, and worker-session contracts.

The first increment's single-owner controller remains the default writer path.
This module supplies the second increment's deterministic coordination layer:
it prepares native Orca Tasks and dependencies, validates ready waves, and
keeps review and terminal-resource identity bound to the exact candidate.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from .config import RoleChoice
from .errors import OrchestrateError
from .orca import OrcaClient, OrcaCommandError, orca_task_title
from .orca_compat import (
    WorkerShowShapeError,
    worker_execution_identity,
    worker_terminal_resource_identity,
)
from .safeio import approved_project_path, read_project_bytes
from .sources import SourceIndex
from .state import StateStore, utc_now


MAX_MILESTONE_TASKS = 12
MILESTONE_PLAN_SCHEMA = "orchestrate-milestone-plan/v1"
TaskRole = Literal["owner", "specialist", "reviewer"]
GateKind = Literal["none", "integration", "verification", "review"]


def _json_tree(value: object) -> object:
    """Copy one JSON value without retaining caller-owned containers."""

    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise OrchestrateError("Shared-contract object keys must be strings", code="shared_contract_invalid")
        return {key: _json_tree(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_tree(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise OrchestrateError("Shared contracts must contain only JSON values", code="shared_contract_invalid")


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            _json_tree(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OrchestrateError("Shared contract is not canonical JSON", code="shared_contract_invalid") from exc


def _canonical_digest_from_json(canonical_json: str, prefix: str) -> str:
    return f"{prefix}_sha256_" + hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _canonical_digest(value: object, prefix: str) -> str:
    return _canonical_digest_from_json(_canonical_json(value), prefix)


def _deeply_immutable(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _deeply_immutable(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_deeply_immutable(item) for item in value)
    return value


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


@dataclass(frozen=True, slots=True, init=False)
class SharedContract:
    """One immutable interface snapshot that must settle before a parallel wave."""

    digest: str
    state: Literal["draft", "settled"]
    _canonical_json: str

    def __init__(self, digest: str, state: Literal["draft", "settled"], value: Mapping[str, Any]) -> None:
        canonical = _canonical_json(value)
        expected = _canonical_digest_from_json(canonical, "contract")
        if state not in {"draft", "settled"}:
            raise OrchestrateError("Shared contract has an unsupported state", code="shared_contract_invalid")
        if digest != expected:
            raise OrchestrateError(
                "Shared-contract digest does not match its exact canonical nested bytes",
                code="shared_contract_digest_mismatch",
            )
        object.__setattr__(self, "digest", digest)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "_canonical_json", canonical)

    @property
    def value(self) -> Mapping[str, Any]:
        decoded = json.loads(self._canonical_json)
        immutable = _deeply_immutable(decoded)
        if not isinstance(immutable, Mapping):
            raise AssertionError("shared-contract root stopped being an object")
        return immutable

    @property
    def canonical_json(self) -> str:
        return self._canonical_json

    @classmethod
    def draft(cls, value: Mapping[str, Any]) -> "SharedContract":
        return cls(_canonical_digest(value, "contract"), "draft", dict(value))

    def settle(self) -> "SharedContract":
        return SharedContract(self.digest, "settled", json.loads(self._canonical_json))


@dataclass(frozen=True, slots=True)
class MilestoneTask:
    key: str
    title: str
    spec: str
    role: TaskRole
    dependencies: tuple[str, ...] = ()
    gate: GateKind = "none"
    integration_owner: bool = False
    independently_useful: bool = False
    writes_shared_contract: bool = False
    candidate_digest: str | None = None
    contract_digest: str | None = None


@dataclass(frozen=True, slots=True)
class NativeTaskBinding:
    key: str
    task_id: str
    dependencies: tuple[str, ...]
    spec: str


@dataclass(frozen=True, slots=True)
class NativeGateBinding:
    task_key: str
    task_id: str
    gate_id: str
    gate_kind: Literal["verification", "review"]
    question: str
    status: Literal["pending", "resolved"]
    resolution: str | None = None


@dataclass(frozen=True, slots=True)
class ReviewEvidence:
    task_id: str
    candidate_digest: str
    contract_digest: str
    outcome: Literal["accepted", "rejected"]

    def current_for(self, *, candidate_digest: str, contract_digest: str) -> bool:
        return self.candidate_digest == candidate_digest and self.contract_digest == contract_digest


@dataclass(frozen=True, slots=True)
class MilestonePlan:
    contract: SharedContract
    candidate_digest: str
    tasks: tuple[MilestoneTask, ...]
    max_workers: int = 2

    def validated(self) -> "MilestonePlan":
        if not self.candidate_digest:
            raise OrchestrateError("Milestone candidate identity is missing", code="candidate_identity_missing")
        if not 1 <= self.max_workers <= 8:
            raise OrchestrateError("max_workers must be between 1 and 8", code="milestone_plan_invalid")
        if not self.tasks or len(self.tasks) > MAX_MILESTONE_TASKS:
            raise OrchestrateError("Milestone task count is outside the bounded range", code="milestone_plan_invalid")
        by_key = {task.key: task for task in self.tasks}
        if len(by_key) != len(self.tasks) or any(not task.key or not task.title.strip() or not task.spec.strip() for task in self.tasks):
            raise OrchestrateError("Milestone Tasks need unique non-empty identities and specs", code="milestone_plan_invalid")
        owners = [task for task in self.tasks if task.role == "owner"]
        integration = [task for task in self.tasks if task.integration_owner]
        if len(owners) != 1 or integration != owners:
            raise OrchestrateError(
                "A milestone requires exactly one implementation and integration owner",
                code="integration_owner_invalid",
            )
        owner = owners[0]
        if owner.dependencies or not owner.writes_shared_contract:
            raise OrchestrateError(
                "The integration owner must be the root and sole shared-contract writer",
                code="integration_owner_invalid",
            )
        for task in self.tasks:
            if task.contract_digest != self.contract.digest:
                raise OrchestrateError(
                    f"Task {task.key} is not bound to the settled shared contract",
                    code="shared_contract_binding_mismatch",
                )
            if task.candidate_digest != self.candidate_digest:
                raise OrchestrateError(
                    f"Task {task.key} is not bound to the exact candidate",
                    code="candidate_binding_mismatch",
                )
            if len(set(task.dependencies)) != len(task.dependencies) or task.key in task.dependencies:
                raise OrchestrateError(f"Task {task.key} has invalid dependencies", code="milestone_plan_invalid")
            if any(dependency not in by_key for dependency in task.dependencies):
                raise OrchestrateError(f"Task {task.key} names an unknown dependency", code="milestone_plan_invalid")
            if task is not owner and (not task.independently_useful or task.writes_shared_contract):
                raise OrchestrateError(
                    "Additional workers must be bounded, independently useful, and read-only over shared contracts",
                    code="parallel_task_invalid",
                )
            if (task.integration_owner or task.gate == "integration") != (task is owner):
                raise OrchestrateError(
                    "The integration gate belongs only to the exact implementation owner",
                    code="integration_owner_invalid",
                )
            if (task.role == "reviewer") != (task.gate == "review"):
                raise OrchestrateError(
                    "The single reviewer Task must own the review gate",
                    code="review_gate_invalid",
                )
            if task.gate == "verification" and task.role != "specialist":
                raise OrchestrateError(
                    "Verification gates belong only to specialist Tasks",
                    code="verification_gate_invalid",
                )
            if task.role == "reviewer" and owner.key not in task.dependencies:
                raise OrchestrateError(
                    "Independent review must depend on the integration owner",
                    code="review_gate_invalid",
                )
            if task.gate in {"verification", "review"} and not task.dependencies:
                raise OrchestrateError("Verification and review are dependent gates", code="verification_gate_invalid")

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(key: str) -> None:
            if key in visiting:
                raise OrchestrateError("Milestone dependencies contain a cycle", code="milestone_plan_invalid")
            if key in visited:
                return
            visiting.add(key)
            for dependency in by_key[key].dependencies:
                visit(dependency)
            visiting.remove(key)
            visited.add(key)

        for key in by_key:
            visit(key)
        return self

    def topological(self) -> tuple[MilestoneTask, ...]:
        self.validated()
        remaining = {task.key: task for task in self.tasks}
        ordered: list[MilestoneTask] = []
        settled: set[str] = set()
        while remaining:
            ready = [task for task in self.tasks if task.key in remaining and set(task.dependencies) <= settled]
            if not ready:
                raise AssertionError("validated DAG did not yield a ready Task")
            for task in ready:
                ordered.append(task)
                settled.add(task.key)
                del remaining[task.key]
        return tuple(ordered)

    def ready_keys(self, completed: set[str]) -> tuple[str, ...]:
        self.validated()
        owner = next(task for task in self.tasks if task.integration_owner)
        if self.contract.state == "draft":
            # The integration owner is the serialized contract-settlement wave.
            return (owner.key,) if owner.key not in completed else ()
        return tuple(
            task.key
            for task in self.tasks
            if task.key not in completed and set(task.dependencies) <= completed
        )[: self.max_workers]


@dataclass(frozen=True, slots=True)
class LoadedMilestonePlan:
    relative_path: str
    digest: str
    canonical_json: str
    plan: MilestonePlan


def milestone_plan_relative_path(root: Path, value: str) -> str:
    raw = Path(value)
    if raw.is_absolute():
        raise OrchestrateError("Milestone plan path must be project-relative", code="milestone_plan_invalid")
    relative = raw.as_posix()
    approved_project_path(root, relative)
    return relative


def load_milestone_plan(
    root: Path,
    relative_path: str,
    *,
    objective: str,
    candidate_digest: str,
    sources: SourceIndex,
) -> LoadedMilestonePlan:
    """Read one tracked, explicit v1 plan and bind it to the observed candidate."""

    relative = milestone_plan_relative_path(root, relative_path)
    source_rows = sources.value.get("sources")
    matching = [
        row
        for row in source_rows or []
        if isinstance(row, Mapping) and row.get("path") == relative
    ]
    if (
        len(matching) != 1
        or not isinstance(matching[0].get("indexSha256"), str)
        or not matching[0]["indexSha256"]
    ):
        raise OrchestrateError(
            "The explicit milestone plan must be a tracked project file",
            code="milestone_plan_untracked",
        )
    raw = read_project_bytes(root, relative)
    try:
        original_json = raw.decode("utf-8")
        decoded = json.loads(original_json)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestrateError("Milestone plan is not strict UTF-8 JSON", code="milestone_plan_invalid") from exc
    plan = _validated_milestone_plan_value(
        decoded,
        objective=objective,
        candidate_digest=candidate_digest,
    )
    digest = _canonical_digest_from_json(original_json, "plan")
    return LoadedMilestonePlan(relative, digest, original_json, plan)


def load_stored_milestone_plan(
    relative_path: str,
    plan_json: str,
    *,
    objective: str,
    candidate_digest: str,
) -> LoadedMilestonePlan:
    """Revalidate one stored plan from its exact canonical bytes and bound candidate."""

    try:
        decoded = json.loads(plan_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise OrchestrateError("Stored milestone plan is not strict JSON", code="milestone_plan_invalid") from exc
    plan = _validated_milestone_plan_value(
        decoded,
        objective=objective,
        candidate_digest=candidate_digest,
    )
    return LoadedMilestonePlan(
        relative_path,
        _canonical_digest_from_json(plan_json, "plan"),
        plan_json,
        plan,
    )


def _validated_milestone_plan_value(
    decoded: object,
    *,
    objective: str,
    candidate_digest: str,
) -> MilestonePlan:
    """Validate the complete v1 schema, contract, Tasks, roles, dependencies, and gates."""

    if not isinstance(decoded, Mapping) or set(decoded) not in (
        {"schema", "contract", "tasks"},
        {"schema", "contract", "maxWorkers", "tasks"},
    ):
        raise OrchestrateError("Milestone plan has unsupported or missing fields", code="milestone_plan_invalid")
    if decoded.get("schema") != MILESTONE_PLAN_SCHEMA:
        raise OrchestrateError("Milestone plan schema is unsupported", code="milestone_plan_invalid")
    max_workers = decoded.get("maxWorkers", 2)
    raw_tasks = decoded.get("tasks")
    raw_contract = decoded.get("contract")
    if type(max_workers) is not int or not isinstance(raw_tasks, list) or not isinstance(raw_contract, Mapping):
        raise OrchestrateError("Milestone plan fields have unsupported shapes", code="milestone_plan_invalid")
    contract = SharedContract.draft(raw_contract).settle()
    tasks: list[MilestoneTask] = []
    required_task_fields = {"key", "title", "spec", "role", "dependencies", "gate"}
    for raw_task in raw_tasks:
        if not isinstance(raw_task, Mapping) or set(raw_task) != required_task_fields:
            raise OrchestrateError("Milestone Task has unsupported or missing fields", code="milestone_plan_invalid")
        key = raw_task.get("key")
        title = raw_task.get("title")
        spec = raw_task.get("spec")
        role = raw_task.get("role")
        dependencies = raw_task.get("dependencies")
        gate = raw_task.get("gate")
        if (
            not isinstance(key, str)
            or not isinstance(title, str)
            or not isinstance(spec, str)
            or role not in {"owner", "specialist", "reviewer"}
            or gate not in {"integration", "verification", "review", "none"}
            or not isinstance(dependencies, list)
            or not all(isinstance(item, str) for item in dependencies)
        ):
            raise OrchestrateError("Milestone Task fields have unsupported shapes", code="milestone_plan_invalid")
        is_owner = role == "owner"
        if is_owner and (spec != objective or gate != "integration"):
            raise OrchestrateError(
                "The explicit integration-owner Task must exactly match the authorized objective",
                code="integration_owner_invalid",
            )
        tasks.append(
            MilestoneTask(
                key=key,
                title=title,
                spec=spec,
                role=role,
                dependencies=tuple(dependencies),
                gate=gate,
                integration_owner=is_owner,
                independently_useful=not is_owner,
                writes_shared_contract=is_owner,
                candidate_digest=candidate_digest,
                contract_digest=contract.digest,
            )
        )
    plan = MilestonePlan(contract, candidate_digest, tuple(tasks), max_workers).validated()
    verification_keys = {task.key for task in plan.tasks if task.gate == "verification"}
    reviewers = [task for task in plan.tasks if task.role == "reviewer"]
    reviewer_dependencies = {
        task.key
        for task in plan.tasks
        if not task.integration_owner and task.role != "reviewer"
    }
    if (
        not verification_keys
        or len(reviewers) != 1
        or not reviewer_dependencies <= set(reviewers[0].dependencies)
    ):
        raise OrchestrateError(
            "A production milestone requires one final reviewer gated by every specialist Task",
            code="review_gate_invalid",
        )
    return plan


def milestone_gate_question(task: MilestoneTask, plan: MilestonePlan) -> str:
    """Return the immutable native-gate question bound to one planned Task."""

    return (
        f"Accept {task.gate} prerequisites for planned Task {task.key} at "
        f"{plan.candidate_digest} / {plan.contract.digest}?"
    )


def native_task_create_arguments(
    *,
    run_id: str,
    title: str,
    spec: str,
    dependency_ids: Sequence[str] = (),
) -> list[str]:
    """Return the exact public Orca argv used to create one native Task."""

    arguments = [
        "orchestration",
        "task-create",
        "--run",
        run_id,
        "--task-title",
        orca_task_title(title),
        "--spec",
        spec,
    ]
    if dependency_ids:
        arguments.extend(("--deps", json.dumps(list(dependency_ids), separators=(",", ":"))))
    return arguments


def validate_effective_launch(choice: RoleChoice, launch: object) -> Mapping[str, str]:
    """Require Orca to echo the exact requested and effective launch values."""

    expected = choice.requested()
    if not isinstance(launch, Mapping):
        raise OrchestrateError("worker-start omitted launch validation", code="worker_launch_unsupported")
    requested, effective = launch.get("requested"), launch.get("effective")
    if requested != expected:
        raise OrchestrateError("Orca did not retain the exact requested role launch", code="worker_launch_mismatch")
    if not isinstance(effective, Mapping):
        raise OrchestrateError("Orca did not report effective launch capabilities", code="worker_launch_unsupported")
    if dict(effective) != expected:
        raise OrchestrateError(
            "Orca substituted or could not provide the requested role launch",
            code="worker_launch_mismatch",
            data={"role": choice.role, "requested": expected, "effective": dict(effective)},
        )
    return expected


class NativeDagScheduler:
    """Create and read back one bounded plan using Orca-native Tasks/deps."""

    def __init__(self, client: OrcaClient, store: StateStore, run_local_id: str) -> None:
        self.client = client
        self.store = store
        self.run_local_id = run_local_id

    def _stored_binding(
        self,
        task: MilestoneTask,
        dependency_ids: list[str],
        arguments: list[str],
    ) -> NativeTaskBinding | None:
        row = self.store.connection.execute(
            "SELECT * FROM milestone_task_bindings WHERE run_local_id = ? AND task_key = ?",
            (self.run_local_id, task.key),
        ).fetchone()
        if row is None:
            # A crash may occur after the exact receipt was journaled but before
            # its derived binding row. Rebuild only from that immutable applied
            # receipt; never issue task-create again merely because projection
            # state is missing.
            operation = f"milestone-task-create:{task.key}"
            receipt = self.store.connection.execute(
                """SELECT arguments_json, response_json FROM intentions
                   WHERE run_local_id = ? AND operation = ? AND status = 'applied'
                   ORDER BY created_at""",
                (self.run_local_id, operation),
            ).fetchall()
            if not receipt:
                return None
            if len(receipt) != 1:
                raise OrchestrateError("Milestone Task has conflicting applied receipts", code="native_task_binding_mismatch")
            try:
                stored_arguments = json.loads(receipt[0]["arguments_json"])
                response = json.loads(receipt[0]["response_json"])
                native = _result(response).get("task")
            except (json.JSONDecodeError, TypeError, OrchestrateError) as exc:
                raise OrchestrateError("Applied milestone Task receipt is malformed", code="native_task_binding_mismatch") from exc
            if stored_arguments != arguments or not isinstance(native, Mapping) or not isinstance(native.get("id"), str):
                raise OrchestrateError("Applied milestone Task receipt changed identity", code="native_task_binding_mismatch")
            return self._record_binding(task, native["id"], dependency_ids)
        expected = (
            task.candidate_digest,
            task.contract_digest,
            task.spec,
            json.dumps(dependency_ids, separators=(",", ":")),
        )
        actual = (
            row["candidate_digest"],
            row["contract_digest"],
            row["spec"],
            row["dependencies_json"],
        )
        if actual != expected:
            raise OrchestrateError(
                "Stored milestone Task binding conflicts with the selected plan",
                code="native_task_binding_mismatch",
            )
        return NativeTaskBinding(task.key, row["task_id"], tuple(dependency_ids), task.spec)

    def _record_binding(
        self,
        task: MilestoneTask,
        task_id: str,
        dependency_ids: list[str],
    ) -> NativeTaskBinding:
        self.store.connection.execute(
            "INSERT INTO milestone_task_bindings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.run_local_id,
                task.key,
                task_id,
                task.candidate_digest,
                task.contract_digest,
                task.spec,
                json.dumps(dependency_ids, separators=(",", ":")),
                utc_now(),
            ),
        )
        return NativeTaskBinding(task.key, task_id, tuple(dependency_ids), task.spec)

    def _create_selected(
        self,
        run_id: str,
        plan: MilestonePlan,
        selected: Sequence[MilestoneTask],
        bindings: dict[str, NativeTaskBinding],
    ) -> dict[str, NativeTaskBinding]:
        for task in selected:
            dependency_ids = [bindings[key].task_id for key in task.dependencies]
            arguments = native_task_create_arguments(
                run_id=run_id,
                title=task.title,
                spec=task.spec,
                dependency_ids=dependency_ids,
            )
            stored = self._stored_binding(task, dependency_ids, arguments)
            if stored is not None:
                bindings[task.key] = stored
                continue
            operation = f"milestone-task-create:{task.key}"
            unsettled = self.store.connection.execute(
                "SELECT id, request_id, status FROM intentions WHERE run_local_id = ? AND operation = ? AND status != 'applied'",
                (self.run_local_id, operation),
            ).fetchone()
            if unsettled is not None:
                raise OrchestrateError(
                    "A prior native milestone Task creation is unresolved; no duplicate was issued",
                    code="unknown_external_effect",
                    data={
                        "taskKey": task.key,
                        "intentionId": unsettled["id"],
                        "requestId": unsettled["request_id"],
                        "status": unsettled["status"],
                    },
                )
            intention_id = self.store.prepare_intention(self.run_local_id, operation, arguments)
            self.store.mark_intention(intention_id, "invoking")
            try:
                response = self.client.run_json(*arguments, "--json")
            except OrcaCommandError as exc:
                response_payload = exc.result.payload if exc.result is not None else None
                request_id = None
                if isinstance(response_payload, Mapping):
                    result = response_payload.get("result")
                    mutation = result.get("mutation") if isinstance(result, Mapping) else None
                    request_id = mutation.get("requestId") if isinstance(mutation, Mapping) else None
                self.store.mark_intention(
                    intention_id,
                    "uncertain",
                    request_id=request_id if isinstance(request_id, str) else None,
                    error=response_payload or {"code": exc.code, "message": str(exc)},
                )
                raise OrchestrateError(
                    "Native milestone Task creation is uncertain; no duplicate will be issued",
                    code="mutation_outcome_uncertain",
                    data={"taskKey": task.key, "requestId": request_id},
                ) from exc
            result = _result(response)
            native = result.get("task")
            mutation = result.get("mutation")
            if (
                not isinstance(native, Mapping)
                or not isinstance(native.get("id"), str)
                or not isinstance(mutation, Mapping)
                or not isinstance(mutation.get("requestId"), str)
                or not isinstance(mutation.get("replayed"), bool)
            ):
                self.store.mark_intention(intention_id, "uncertain", error=response)
                raise OrchestrateError("task-create omitted its exact native receipt", code="orca_contract_error")
            self.store.mark_intention(
                intention_id,
                "applied",
                request_id=mutation["requestId"],
                response=response,
            )
            bindings[task.key] = self._record_binding(task, native["id"], dependency_ids)
        self._validate_readback(
            run_id,
            plan,
            {task.key: bindings[task.key] for task in selected},
        )
        return bindings

    def create(self, run_id: str, plan: MilestonePlan) -> dict[str, NativeTaskBinding]:
        plan.validated()
        ordered = plan.topological()
        selected = tuple(task for task in ordered if task.integration_owner) if plan.contract.state == "draft" else ordered
        return self._create_selected(run_id, plan, selected, {})

    def create_followups(
        self,
        run_id: str,
        plan: MilestonePlan,
        *,
        integration_owner: NativeTaskBinding,
    ) -> dict[str, NativeTaskBinding]:
        """Bind the accepted first-increment owner, then create only follow-up Tasks."""

        plan.validated()
        owner = next(task for task in plan.tasks if task.integration_owner)
        if integration_owner.key != owner.key or integration_owner.dependencies:
            raise OrchestrateError(
                "The existing implementation Task is not the exact integration owner",
                code="integration_owner_invalid",
            )
        selected = tuple(task for task in plan.topological() if not task.integration_owner)
        return self._create_selected(
            run_id,
            plan,
            selected,
            {owner.key: integration_owner},
        )

    def _validate_readback(
        self,
        run_id: str,
        plan: MilestonePlan,
        bindings: Mapping[str, NativeTaskBinding],
    ) -> None:
        response = self.client.run_json("orchestration", "task-list", "--run", run_id, "--json")
        rows = _result(response).get("tasks")
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise OrchestrateError("task-list returned an unknown native shape", code="orca_contract_error")
        by_id = {row.get("id"): row for row in rows if isinstance(row.get("id"), str)}
        task_map = {task.key: task for task in plan.tasks}
        for key, binding in bindings.items():
            task = task_map[key]
            row = by_id.get(binding.task_id)
            worker = self.store.connection.execute(
                """SELECT outcome FROM milestone_worker_bindings
                   WHERE run_local_id = ? AND task_key = ? AND task_id = ?""",
                (self.run_local_id, key, binding.task_id),
            ).fetchone()
            if worker is None:
                allowed_statuses = {"ready", "pending"}
            elif worker["outcome"] is None:
                allowed_statuses = {"dispatched"}
            else:
                allowed_statuses = {"completed" if worker["outcome"] == "succeeded" else "failed"}
            if (
                not isinstance(row, Mapping)
                or row.get("run_id") != run_id
                or row.get("task_title") != orca_task_title(task.title)
                or row.get("spec") != task.spec
                or row.get("deps", []) != list(binding.dependencies)
                or row.get("status") not in allowed_statuses
            ):
                raise OrchestrateError(
                    f"Native Task readback changed {task.key}'s identity, title, spec, dependencies, or gate state",
                    code="native_task_binding_mismatch",
                )

    def ready_wave(
        self,
        run_id: str,
        plan: MilestonePlan,
        bindings: Mapping[str, NativeTaskBinding],
        *,
        active: set[str],
        finished: set[str],
        remaining_capacity: int,
    ) -> tuple[NativeTaskBinding, ...]:
        plan.validated()
        task_keys = {task.key for task in plan.tasks}
        if (
            type(remaining_capacity) is not int
            or remaining_capacity < 0
            or remaining_capacity > plan.max_workers
            or not active <= task_keys
            or not finished <= task_keys
            or bool(active & finished)
            or remaining_capacity != plan.max_workers - len(active)
        ):
            raise OrchestrateError(
                "Active, finished, and remaining worker capacity do not form one bounded roster",
                code="native_ready_gate_mismatch",
            )
        response = self.client.run_json(
            "orchestration", "task-list", "--run", run_id, "--ready", "--brief", "--json"
        )
        rows = _result(response).get("tasks")
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise OrchestrateError("ready Task view has an unknown native shape", code="orca_contract_error")
        by_id = {binding.task_id: binding for binding in bindings.values()}
        ineligible_ids = {
            bindings[key].task_id
            for key in active | finished
            if key in bindings
        }
        if len(ineligible_ids) != len(active | finished):
            raise OrchestrateError(
                "Active or finished work lost its exact native Task binding",
                code="native_ready_gate_mismatch",
            )
        ready_ids: set[str] = set()
        for row in rows:
            task_id = row.get("id")
            if (
                not isinstance(task_id, str)
                or task_id not in by_id
                or task_id in ready_ids
                or task_id in ineligible_ids
                or row.get("status") != "ready"
            ):
                raise OrchestrateError("ready Task view escaped the bound milestone", code="native_ready_gate_mismatch")
            ready_ids.add(task_id)
        frontier = tuple(
            bindings[task.key]
            for task in plan.tasks
            if task.key in bindings and bindings[task.key].task_id in ready_ids
        )
        if len(frontier) != len(ready_ids):
            raise OrchestrateError("ready Task view escaped the bound milestone", code="native_ready_gate_mismatch")
        if plan.contract.state != "settled" and any(
            not next(task for task in plan.tasks if task.key == binding.key).integration_owner
            for binding in frontier
        ):
            raise OrchestrateError(
                "Shared contracts must settle before parallel dispatch",
                code="shared_contract_unsettled",
            )
        return frontier[:remaining_capacity]

    @staticmethod
    def _gate_question(task: MilestoneTask, plan: MilestonePlan) -> str:
        return milestone_gate_question(task, plan)

    @staticmethod
    def _gate_task_id(row: Mapping[str, Any]) -> object:
        camel = row.get("taskId")
        snake = row.get("task_id")
        if camel is not None and snake is not None and camel != snake:
            raise OrchestrateError("Native gate returned conflicting Task aliases", code="native_gate_mismatch")
        return camel if camel is not None else snake

    def _validate_gate_row(
        self,
        row: Mapping[str, Any],
        binding: NativeGateBinding,
    ) -> None:
        if (
            row.get("id") != binding.gate_id
            or self._gate_task_id(row) != binding.task_id
            or row.get("question") != binding.question
            or row.get("status") != binding.status
            or row.get("resolution") != binding.resolution
        ):
            raise OrchestrateError(
                "Native gate readback changed its exact Task, question, status, or resolution",
                code="native_gate_mismatch",
            )

    def _gate_readback(self, binding: NativeGateBinding) -> Mapping[str, Any]:
        payload = self.client.run_json("orchestration", "gate-list", "--task", binding.task_id, "--json")
        rows = _result(payload).get("gates")
        matching = [row for row in rows or [] if isinstance(row, Mapping) and row.get("id") == binding.gate_id]
        if len(matching) != 1:
            raise OrchestrateError("Native gate readback omitted the exact planned gate", code="native_gate_mismatch")
        self._validate_gate_row(matching[0], binding)
        return matching[0]

    def _gate_rows_for_task(self, task_id: str) -> list[Mapping[str, Any]]:
        payload = self.client.run_json("orchestration", "gate-list", "--task", task_id, "--json")
        rows = _result(payload).get("gates")
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise OrchestrateError("Native gate readback has an unknown shape", code="native_gate_mismatch")
        return rows

    def _stored_gate(self, task: MilestoneTask, task_id: str, question: str) -> NativeGateBinding | None:
        row = self.store.connection.execute(
            "SELECT * FROM milestone_gate_bindings WHERE run_local_id = ? AND task_key = ?",
            (self.run_local_id, task.key),
        ).fetchone()
        if row is None:
            return None
        expected = (task_id, task.gate, question)
        actual = (row["task_id"], row["gate_kind"], row["question"])
        if actual != expected:
            raise OrchestrateError("Stored gate conflicts with the selected plan", code="native_gate_mismatch")
        binding = NativeGateBinding(
            task.key,
            task_id,
            row["gate_id"],
            task.gate,  # type: ignore[arg-type]
            question,
            row["status"],
            row["resolution"],
        )
        try:
            self._gate_readback(binding)
            return binding
        except OrchestrateError as exc:
            if binding.status != "pending" or exc.code != "native_gate_mismatch":
                raise
        live = [row for row in self._gate_rows_for_task(task_id) if row.get("id") == binding.gate_id]
        if len(live) != 1 or live[0].get("status") != "resolved" or live[0].get("resolution") not in {"accepted", "rejected"}:
            raise OrchestrateError("Native gate readback conflicts with local pending state", code="native_gate_mismatch")
        recovered = NativeGateBinding(
            task.key,
            task_id,
            binding.gate_id,
            binding.gate_kind,
            question,
            "resolved",
            live[0]["resolution"],  # type: ignore[arg-type]
        )
        self._validate_gate_row(live[0], recovered)
        operation = f"milestone-gate-resolve:{task.key}"
        intentions = self.store.connection.execute(
            """SELECT * FROM intentions WHERE run_local_id = ? AND operation = ? AND status != 'applied'
               ORDER BY created_at""",
            (self.run_local_id, operation),
        ).fetchall()
        expected_arguments = [
            "orchestration",
            "gate-resolve",
            "--id",
            binding.gate_id,
            "--resolution",
            recovered.resolution,
        ]
        if (
            len(intentions) != 1
            or not intentions[0]["request_id"]
            or json.loads(intentions[0]["arguments_json"]) != expected_arguments
        ):
            raise OrchestrateError("Resolved native gate lacks its exact recovery intention", code="unknown_external_effect")
        self.store.mark_intention(
            intentions[0]["id"],
            "applied",
            request_id=intentions[0]["request_id"],
            response={"result": {"gate": dict(live[0]), "recoveredBy": "gate-list"}},
        )
        self.store.connection.execute(
            """UPDATE milestone_gate_bindings SET status = 'resolved', resolution = ?, updated_at = ?
               WHERE run_local_id = ? AND task_key = ? AND status = 'pending'""",
            (recovered.resolution, utc_now(), self.run_local_id, task.key),
        )
        return recovered

    def create_gates(
        self,
        plan: MilestonePlan,
        bindings: Mapping[str, NativeTaskBinding],
    ) -> dict[str, NativeGateBinding]:
        """Create one journaled native gate for every verification/review label."""

        plan.validated()
        gates: dict[str, NativeGateBinding] = {}
        for task in plan.topological():
            if task.gate not in {"verification", "review"}:
                continue
            native_task = bindings.get(task.key)
            if native_task is None:
                raise OrchestrateError("Gate Task has no exact native binding", code="native_gate_mismatch")
            question = self._gate_question(task, plan)
            stored = self._stored_gate(task, native_task.task_id, question)
            if stored is not None:
                gates[task.key] = stored
                continue
            operation = f"milestone-gate-create:{task.key}"
            unresolved = self.store.connection.execute(
                "SELECT id, request_id, status FROM intentions WHERE run_local_id = ? AND operation = ? AND status != 'applied'",
                (self.run_local_id, operation),
            ).fetchone()
            if unresolved is not None:
                live = [
                    row
                    for row in self._gate_rows_for_task(native_task.task_id)
                    if row.get("question") == question
                    and self._gate_task_id(row) == native_task.task_id
                ]
                if (
                    len(live) != 1
                    or not unresolved["request_id"]
                    or live[0].get("status") != "pending"
                    or live[0].get("resolution") is not None
                    or not isinstance(live[0].get("id"), str)
                ):
                    raise OrchestrateError(
                        "A prior native gate creation is unresolved; no duplicate was issued",
                        code="unknown_external_effect",
                        data={"taskKey": task.key, "requestId": unresolved["request_id"], "status": unresolved["status"]},
                    )
                binding = NativeGateBinding(
                    task.key,
                    native_task.task_id,
                    live[0]["id"],  # type: ignore[arg-type]
                    task.gate,
                    question,
                    "pending",
                    None,
                )  # type: ignore[arg-type]
                self._validate_gate_row(live[0], binding)
                self.store.mark_intention(
                    unresolved["id"],
                    "applied",
                    request_id=unresolved["request_id"],
                    response={"result": {"gate": dict(live[0]), "recoveredBy": "gate-list"}},
                )
                now = utc_now()
                self.store.connection.execute(
                    "INSERT INTO milestone_gate_bindings VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)",
                    (
                        self.run_local_id,
                        task.key,
                        native_task.task_id,
                        binding.gate_id,
                        task.gate,
                        question,
                        now,
                        now,
                    ),
                )
                gates[task.key] = binding
                continue
            arguments = [
                "orchestration",
                "gate-create",
                "--task",
                native_task.task_id,
                "--question",
                question,
                "--options",
                '["accepted","rejected"]',
            ]
            intention_id = self.store.prepare_intention(self.run_local_id, operation, arguments)
            self.store.mark_intention(intention_id, "invoking")
            try:
                response = self.client.run_json(*arguments, "--json")
            except OrcaCommandError as exc:
                payload = exc.result.payload if exc.result is not None else None
                raw_result = payload.get("result") if isinstance(payload, Mapping) else None
                mutation = raw_result.get("mutation") if isinstance(raw_result, Mapping) else None
                request_id = mutation.get("requestId") if isinstance(mutation, Mapping) else None
                self.store.mark_intention(
                    intention_id,
                    "uncertain",
                    request_id=request_id if isinstance(request_id, str) else None,
                    error=payload or {"code": exc.code, "message": str(exc)},
                )
                raise OrchestrateError(
                    "Native gate creation is uncertain; no duplicate will be issued",
                    code="mutation_outcome_uncertain",
                    data={"taskKey": task.key, "requestId": request_id},
                ) from exc
            result = _result(response)
            gate = result.get("gate")
            mutation = result.get("mutation")
            if (
                not isinstance(gate, Mapping)
                or not isinstance(gate.get("id"), str)
                or not isinstance(mutation, Mapping)
                or not isinstance(mutation.get("requestId"), str)
                or not isinstance(mutation.get("replayed"), bool)
            ):
                self.store.mark_intention(intention_id, "uncertain", error=response)
                raise OrchestrateError("gate-create omitted its exact native receipt", code="orca_contract_error")
            binding = NativeGateBinding(
                task.key,
                native_task.task_id,
                gate["id"],
                task.gate,
                question,
                "pending",
                None,
            )  # type: ignore[arg-type]
            self._validate_gate_row(gate, binding)
            self.store.mark_intention(
                intention_id,
                "applied",
                request_id=mutation["requestId"],
                response=response,
            )
            now = utc_now()
            self.store.connection.execute(
                "INSERT INTO milestone_gate_bindings VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)",
                (
                    self.run_local_id,
                    task.key,
                    native_task.task_id,
                    binding.gate_id,
                    task.gate,
                    question,
                    now,
                    now,
                ),
            )
            self._gate_readback(binding)
            gates[task.key] = binding
        return gates

    def resolve_gate(self, binding: NativeGateBinding, *, resolution: Literal["accepted", "rejected"]) -> NativeGateBinding:
        if binding.status == "resolved":
            if binding.resolution != resolution:
                raise OrchestrateError("Native gate was already resolved differently", code="native_gate_mismatch")
            self._gate_readback(binding)
            return binding
        operation = f"milestone-gate-resolve:{binding.task_key}"
        unresolved = self.store.connection.execute(
            "SELECT id, request_id, status FROM intentions WHERE run_local_id = ? AND operation = ? AND status != 'applied'",
            (self.run_local_id, operation),
        ).fetchone()
        if unresolved is not None:
            raise OrchestrateError(
                "A prior native gate resolution is unresolved; no duplicate was issued",
                code="unknown_external_effect",
                data={"taskKey": binding.task_key, "requestId": unresolved["request_id"], "status": unresolved["status"]},
            )
        arguments = [
            "orchestration",
            "gate-resolve",
            "--id",
            binding.gate_id,
            "--resolution",
            resolution,
        ]
        intention_id = self.store.prepare_intention(self.run_local_id, operation, arguments)
        self.store.mark_intention(intention_id, "invoking")
        try:
            response = self.client.run_json(*arguments, "--json")
        except OrcaCommandError as exc:
            payload = exc.result.payload if exc.result is not None else None
            raw_result = payload.get("result") if isinstance(payload, Mapping) else None
            mutation = raw_result.get("mutation") if isinstance(raw_result, Mapping) else None
            request_id = mutation.get("requestId") if isinstance(mutation, Mapping) else None
            self.store.mark_intention(
                intention_id,
                "uncertain",
                request_id=request_id if isinstance(request_id, str) else None,
                error=payload or {"code": exc.code, "message": str(exc)},
            )
            raise OrchestrateError(
                "Native gate resolution is uncertain; no duplicate will be issued",
                code="mutation_outcome_uncertain",
                data={"taskKey": binding.task_key, "requestId": request_id},
            ) from exc
        result = _result(response)
        gate = result.get("gate")
        mutation = result.get("mutation")
        resolved = NativeGateBinding(
            binding.task_key,
            binding.task_id,
            binding.gate_id,
            binding.gate_kind,
            binding.question,
            "resolved",
            resolution,
        )
        if (
            not isinstance(gate, Mapping)
            or not isinstance(mutation, Mapping)
            or not isinstance(mutation.get("requestId"), str)
            or not isinstance(mutation.get("replayed"), bool)
        ):
            self.store.mark_intention(intention_id, "uncertain", error=response)
            raise OrchestrateError("gate-resolve omitted its exact native receipt", code="orca_contract_error")
        self._validate_gate_row(gate, resolved)
        self.store.mark_intention(
            intention_id,
            "applied",
            request_id=mutation["requestId"],
            response=response,
        )
        self.store.connection.execute(
            """UPDATE milestone_gate_bindings SET status = 'resolved', resolution = ?, updated_at = ?
               WHERE run_local_id = ? AND task_key = ? AND status = 'pending'""",
            (resolution, utc_now(), self.run_local_id, binding.task_key),
        )
        self._gate_readback(resolved)
        return resolved


def require_current_review(
    review: ReviewEvidence,
    *,
    candidate_digest: str,
    contract_digest: str,
    task_id: str | None = None,
    settled: bool = True,
) -> None:
    if not review.current_for(candidate_digest=candidate_digest, contract_digest=contract_digest):
        raise OrchestrateError(
            "Independent review is stale for the current candidate or shared contract",
            code="stale_review",
            data={
                "reviewCandidate": review.candidate_digest,
                "currentCandidate": candidate_digest,
                "reviewContract": review.contract_digest,
                "currentContract": contract_digest,
            },
        )
    if review.outcome != "accepted":
        raise OrchestrateError(
            "Independent review rejected the current candidate",
            code="review_rejected",
            data={"taskId": review.task_id},
        )
    if not settled or (task_id is not None and review.task_id != task_id):
        raise OrchestrateError(
            "Independent review does not bind the exact settled planned native Task",
            code="review_gate_invalid",
            data={"reviewTaskId": review.task_id, "plannedTaskId": task_id, "settled": settled},
        )


@dataclass(frozen=True, slots=True)
class WorkerSession:
    dispatch_id: str
    task_id: str
    terminal_handle: str
    resource_id: str
    worktree_id: str
    agent: str
    run_id: str
    outcome: Literal["succeeded", "failed"] = "succeeded"


@dataclass(frozen=True, slots=True)
class SessionAction:
    kind: Literal["reuse", "release"]
    argv: tuple[str, ...]


def next_session_action(
    session: WorkerSession,
    *,
    next_task_id: str | None,
    next_agent: str | None,
) -> SessionAction:
    """Choose exactly one post-settlement owner for the bound terminal."""

    # A Task boundary is a session boundary. Existing Dispatch recovery and
    # answers retain their session, but a newly created Task always starts a
    # fresh agent session even when its role selects the same agent.
    return SessionAction(
        "release",
        ("orchestration", "worker-release", "--dispatch", session.dispatch_id),
    )


@dataclass(frozen=True, slots=True)
class ReleaseDecision:
    state: Literal["released", "retained", "uncertain"]
    recovery: str | tuple[str, ...] | None
    repeat_release: bool = False
    recovery_metadata: Mapping[str, Any] | None = None


def validate_session_readback(session: WorkerSession, payload: Mapping[str, Any]) -> None:
    result = _result(payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    resource = result.get("terminalResource")
    if not all(isinstance(item, Mapping) for item in (dispatch, worker, resource)):
        raise OrchestrateError("worker-show omitted the exact session resource", code="release_unconfirmed")
    try:
        identity = worker_execution_identity(  # type: ignore[arg-type]
            dispatch,
            worker,
            semantics=session.outcome,
            terminal_handle=session.terminal_handle,
        )
        resource_identity = worker_terminal_resource_identity(resource, dispatch_id=session.dispatch_id)  # type: ignore[arg-type]
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="release_unconfirmed") from exc
    if (
        dispatch.get("id") != session.dispatch_id  # type: ignore[union-attr]
        or identity.dispatch_id != session.dispatch_id
        or identity.dispatch.run_id != session.run_id
        or identity.dispatch.task_id != session.task_id
        or identity.worktree_id != session.worktree_id
        or identity.terminal_handle != session.terminal_handle
        or resource_identity.resource_id != session.resource_id
        or resource_identity.terminal_handle != session.terminal_handle
        or resource_identity.worktree_id != session.worktree_id
    ):
        raise OrchestrateError("worker-show changed the exact session resource identity", code="release_unconfirmed")


def validate_release_receipt(
    session: WorkerSession,
    payload: Mapping[str, Any],
    *,
    worker_readback: Mapping[str, Any],
) -> ReleaseDecision:
    """Contain pending/unknown WSL cleanup without changing terminal identity."""

    validate_session_readback(session, worker_readback)
    result = _result(payload)
    if result.get("dispatchId") != session.dispatch_id:
        raise OrchestrateError("Worker release belongs to another Dispatch", code="release_unconfirmed")
    state = result.get("state")
    readback_result = _result(worker_readback)
    resource = readback_result.get("terminalResource")
    if not isinstance(resource, Mapping):
        raise OrchestrateError("worker-show omitted terminal release state", code="release_unconfirmed")
    archive = resource.get("archive")
    released = (
        resource.get("ownershipState") == "released"
        and resource.get("releaseState") == "released"
        and resource.get("retainedReason") is None
        and resource.get("releaseError") is None
        and isinstance(resource.get("releaseRequestedAt"), str)
        and bool(resource.get("releaseRequestedAt"))
        and isinstance(resource.get("releaseCompletedAt"), str)
        and bool(resource.get("releaseCompletedAt"))
        and isinstance(archive, Mapping)
        and archive.get("source") == "transcript"
        and archive.get("status") == "captured"
        and readback_result.get("terminal") is None
    )
    retained = (
        resource.get("ownershipState") == "user_owned"
        and resource.get("releaseState") == "retained"
        and resource.get("retainedReason") == "user_takeover"
        and resource.get("releaseError") is None
        and resource.get("releaseRequestedAt") is None
        and resource.get("releaseCompletedAt") is None
        and isinstance(archive, Mapping)
        and archive.get("source") is None
        and archive.get("status") is None
    )
    if state in {"released", "already_released"}:
        if not released:
            raise OrchestrateError(
                "Worker release receipt is not confirmed by exact released-resource readback",
                code="release_unconfirmed",
            )
        return ReleaseDecision("released", None)
    if state == "retained":
        if not retained:
            raise OrchestrateError(
                "Worker retention receipt is not confirmed by exact retained-resource readback",
                code="release_unconfirmed",
            )
        return ReleaseDecision("retained", None)
    if state not in {"release_pending", "release_unknown"}:
        raise OrchestrateError("Worker release returned an unsupported state", code="release_unconfirmed")
    if result.get("processAction") != "none":
        raise OrchestrateError("Uncertain release attempted an unbound process action", code="release_unconfirmed")
    recovery: str | tuple[str, ...] | None = None
    raw_recovery = result.get("recovery")
    if isinstance(raw_recovery, str) and raw_recovery.strip():
        recovery = raw_recovery
    raw_next = result.get("nextAction")
    if raw_next is not None:
        if (
            not isinstance(raw_next, Sequence)
            or isinstance(raw_next, (str, bytes))
            or not raw_next
            or not all(isinstance(part, str) and part for part in raw_next)
        ):
            raise OrchestrateError("Uncertain release nextAction is malformed", code="release_unconfirmed")
        recovery = tuple(raw_next)
    if recovery is None:
        raise OrchestrateError("Uncertain release omitted its exact recovery action", code="release_unconfirmed")
    recovery_fields = {
        key: _json_tree(value)
        for key, value in result.items()
        if key in {"archive", "lastError", "processAction", "recovery", "nextAction"}
    }
    frozen_metadata = _deeply_immutable(recovery_fields)
    if not isinstance(frozen_metadata, Mapping):
        raise AssertionError("release recovery metadata stopped being an object")
    if released:
        return ReleaseDecision(
            "released",
            recovery,
            repeat_release=False,
            recovery_metadata=frozen_metadata,
        )
    return ReleaseDecision("uncertain", recovery, repeat_release=False, recovery_metadata=frozen_metadata)
