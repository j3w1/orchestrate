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
from typing import Any, Literal

from .config import RoleChoice
from .errors import OrchestrateError
from .orca import OrcaClient, OrcaCommandError, orca_task_title
from .orca_compat import WorkerShowShapeError, worker_show_identity, worker_terminal_resource_identity
from .state import StateStore, utc_now


MAX_MILESTONE_TASKS = 12
TaskRole = Literal["owner", "specialist", "reviewer"]
GateKind = Literal["none", "integration", "verification", "review"]


def _canonical_digest(value: object, prefix: str) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return f"{prefix}_sha256_" + hashlib.sha256(raw).hexdigest()


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise OrchestrateError("Orca response omitted result", code="orca_contract_error")
    return result


@dataclass(frozen=True, slots=True)
class SharedContract:
    """One immutable interface snapshot that must settle before a parallel wave."""

    digest: str
    state: Literal["draft", "settled"]
    value: Mapping[str, Any]

    @classmethod
    def draft(cls, value: Mapping[str, Any]) -> "SharedContract":
        return cls(_canonical_digest(value, "contract"), "draft", dict(value))

    def settle(self) -> "SharedContract":
        return SharedContract(self.digest, "settled", dict(self.value))


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
    max_workers: int = 3

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
    effective_selected = {key: effective.get(key) for key in expected}
    if effective_selected != expected or any(key in effective and effective.get(key) != value for key, value in expected.items()):
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

    def create(self, run_id: str, plan: MilestonePlan) -> dict[str, NativeTaskBinding]:
        plan.validated()
        bindings: dict[str, NativeTaskBinding] = {}
        ordered = plan.topological()
        selected = tuple(task for task in ordered if task.integration_owner) if plan.contract.state == "draft" else ordered
        for task in selected:
            dependency_ids = [bindings[key].task_id for key in task.dependencies]
            arguments = [
                "orchestration",
                "task-create",
                "--run",
                run_id,
                "--task-title",
                orca_task_title(task.title),
                "--spec",
                task.spec,
            ]
            if dependency_ids:
                arguments.extend(("--deps", json.dumps(dependency_ids, separators=(",", ":"))))
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
        self._validate_readback(run_id, plan, bindings)
        return bindings

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
            if (
                not isinstance(row, Mapping)
                or row.get("run_id") != run_id
                or row.get("spec") != task.spec
                or row.get("deps", []) != list(binding.dependencies)
                or row.get("status") not in {"ready", "pending"}
            ):
                raise OrchestrateError(
                    f"Native Task readback changed {task.key}'s identity, spec, dependencies, or gate state",
                    code="native_task_binding_mismatch",
                )

    def ready_wave(
        self,
        run_id: str,
        plan: MilestonePlan,
        bindings: Mapping[str, NativeTaskBinding],
    ) -> tuple[NativeTaskBinding, ...]:
        plan.validated()
        response = self.client.run_json(
            "orchestration", "task-list", "--run", run_id, "--ready", "--brief", "--json"
        )
        rows = _result(response).get("tasks")
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise OrchestrateError("ready Task view has an unknown native shape", code="orca_contract_error")
        by_id = {binding.task_id: binding for binding in bindings.values()}
        selected: list[NativeTaskBinding] = []
        for row in rows:
            task_id = row.get("id")
            if task_id not in by_id or row.get("status") != "ready":
                raise OrchestrateError("ready Task view escaped the bound milestone", code="native_ready_gate_mismatch")
            selected.append(by_id[task_id])
        if plan.contract.state != "settled" and any(
            not next(task for task in plan.tasks if task.key == binding.key).integration_owner
            for binding in selected
        ):
            raise OrchestrateError(
                "Shared contracts must settle before parallel dispatch",
                code="shared_contract_unsettled",
            )
        if len(selected) > plan.max_workers:
            raise OrchestrateError("Native ready wave exceeds the bounded worker roster", code="native_ready_gate_mismatch")
        return tuple(selected)


def require_current_review(
    review: ReviewEvidence,
    *,
    candidate_digest: str,
    contract_digest: str,
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


@dataclass(frozen=True, slots=True)
class WorkerSession:
    dispatch_id: str
    task_id: str
    terminal_handle: str
    resource_id: str
    worktree_id: str
    agent: str


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

    if next_task_id is not None and next_agent == session.agent:
        return SessionAction(
            "reuse",
            (
                "orchestration",
                "worker-start",
                "--task",
                next_task_id,
                "--terminal",
                session.terminal_handle,
            ),
        )
    return SessionAction(
        "release",
        ("orchestration", "worker-release", "--dispatch", session.dispatch_id),
    )


@dataclass(frozen=True, slots=True)
class ReleaseDecision:
    state: Literal["released", "retained", "uncertain"]
    recovery: str | tuple[str, ...] | None
    repeat_release: bool = False


def validate_session_readback(session: WorkerSession, payload: Mapping[str, Any]) -> None:
    result = _result(payload)
    dispatch = result.get("dispatch")
    worker = result.get("worker")
    resource = result.get("terminalResource")
    if not all(isinstance(item, Mapping) for item in (dispatch, worker, resource)):
        raise OrchestrateError("worker-show omitted the exact session resource", code="release_unconfirmed")
    try:
        identity = worker_show_identity(dispatch, worker)  # type: ignore[arg-type]
        resource_identity = worker_terminal_resource_identity(resource, dispatch_id=session.dispatch_id)  # type: ignore[arg-type]
    except WorkerShowShapeError as exc:
        raise OrchestrateError(str(exc), code="release_unconfirmed") from exc
    if (
        dispatch.get("id") != session.dispatch_id  # type: ignore[union-attr]
        or identity.dispatch_id != session.dispatch_id
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
    if state in {"released", "already_released"}:
        return ReleaseDecision("released", None)
    if state == "retained":
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
    return ReleaseDecision("uncertain", recovery, repeat_release=False)
