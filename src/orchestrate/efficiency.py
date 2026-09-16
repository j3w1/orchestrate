"""Per-Run capacity grants and deterministic efficiency observations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

from .errors import OrchestrateError
from .state import RunRecord, StateStore, utc_now


EFFICIENCY_EVENT_SCHEMA = "orchestrate-efficiency-event/v1"
CAPACITY_GRANT_SCHEMA = "orchestrate-capacity-grant/v1"
ORDINARY_CAPACITY_MAX = 3
EXCEPTIONAL_CAPACITY_MAX = 8
SESSION_WARNING_SECONDS = 2 * 60 * 60
SESSION_HANDOFF_SECONDS = 8 * 60 * 60


def session_age_state(observed_seconds: int) -> str:
    if type(observed_seconds) is not int or observed_seconds < 0:
        raise ValueError("Observed session age must be a non-negative integer")
    if observed_seconds >= SESSION_HANDOFF_SECONDS:
        return "fresh_session_required"
    if observed_seconds >= SESSION_WARNING_SECONDS:
        return "warning"
    return "normal"


def _event_id(run_local_id: str, event: str, identity: str) -> str:
    framed = f"{run_local_id}\0{event}\0{identity}".encode("utf-8")
    return "evidence_efficiency_" + hashlib.sha256(framed).hexdigest()


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def record_efficiency_event(
    store: StateStore,
    run_local_id: str,
    *,
    event: str,
    identity: str,
    details: Mapping[str, object] | None = None,
    observed_at: str | None = None,
) -> str:
    evidence_id = _event_id(run_local_id, event, identity)
    existing = store.connection.execute(
        "SELECT kind, subject, payload_json FROM evidence WHERE id = ?",
        (evidence_id,),
    ).fetchone()
    if existing is not None:
        try:
            previous = json.loads(existing["payload_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise OrchestrateError(
                "Efficiency event identity conflicts with existing evidence",
                code="evidence_identity_conflict",
            ) from exc
        expected_details = dict(details) if details else None
        expected_keys = {"schema", "eventId", "event", "identity", "observedAt"}
        if details:
            expected_keys.add("details")
        if (
            existing["kind"] != "efficiency-event"
            or existing["subject"] != event
            or not isinstance(previous, dict)
            or previous.get("schema") != EFFICIENCY_EVENT_SCHEMA
            or previous.get("eventId") != evidence_id
            or previous.get("event") != event
            or previous.get("identity") != identity
            or _parse_time(previous.get("observedAt")) is None
            or previous.get("details") != expected_details
            or set(previous) != expected_keys
        ):
            raise OrchestrateError("Efficiency event identity conflicts with existing evidence", code="evidence_identity_conflict")
        return evidence_id
    timestamp = observed_at or utc_now()
    if _parse_time(timestamp) is None:
        raise OrchestrateError("Efficiency event timestamp is invalid", code="efficiency_evidence_conflict")
    payload: dict[str, object] = {
        "schema": EFFICIENCY_EVENT_SCHEMA,
        "eventId": evidence_id,
        "event": event,
        "identity": identity,
        "observedAt": timestamp,
    }
    if details:
        payload["details"] = dict(details)
    store.record_stable_evidence(
        run_local_id,
        evidence_id=evidence_id,
        kind="efficiency-event",
        status="observed",
        subject=event,
        payload=payload,
        created_at=timestamp,
    )
    return evidence_id


def initialize_efficiency_observation(store: StateStore, run: RunRecord) -> None:
    row = store.connection.execute(
        "SELECT created_at FROM runs WHERE local_id = ?",
        (run.local_id,),
    ).fetchone()
    if row is None or not isinstance(row["created_at"], str):
        raise OrchestrateError("Run start time is unavailable", code="state_schema_migration_required")
    record_efficiency_event(
        store,
        run.local_id,
        event="run_started",
        identity=run.local_id,
        details={"scope": "deterministic-controller"},
        observed_at=row["created_at"],
    )


def _capacity_grant_rows(store: StateStore, run_local_id: str) -> list[Any]:
    return store.connection.execute(
        """SELECT id, subject, payload_json, created_at FROM evidence
           WHERE run_local_id = ? AND kind = 'capacity-grant' ORDER BY created_at, id""",
        (run_local_id,),
    ).fetchall()


def capacity_grant(store: StateStore, run_local_id: str) -> dict[str, object] | None:
    rows = _capacity_grant_rows(store, run_local_id)
    if not rows:
        return None
    if len(rows) != 1:
        raise OrchestrateError("Run has conflicting exceptional-capacity grants", code="capacity_grant_conflict")
    try:
        value = json.loads(rows[0]["payload_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise OrchestrateError("Capacity grant record is malformed", code="capacity_grant_conflict") from exc
    required = {"schema", "localRunId", "runId", "planDigest", "limit", "reason", "grantedAt"}
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value.get("schema") != CAPACITY_GRANT_SCHEMA
        or value.get("localRunId") != run_local_id
        or not isinstance(value.get("runId"), str)
        or not value["runId"]
        or not isinstance(value.get("planDigest"), str)
        or type(value.get("limit")) is not int
        or not 4 <= value["limit"] <= EXCEPTIONAL_CAPACITY_MAX
        or not isinstance(value.get("reason"), str)
        or not value["reason"].strip()
        or len(value["reason"]) > 1000
        or not isinstance(value.get("grantedAt"), str)
        or _parse_time(value["grantedAt"]) is None
        or rows[0]["subject"] != value["planDigest"]
    ):
        raise OrchestrateError("Capacity grant record is malformed", code="capacity_grant_conflict")
    return value


def ensure_capacity_grant(
    store: StateStore,
    run: RunRecord,
    *,
    plan_digest: str | None,
    requested_limit: int,
    allow_exceptional_capacity: bool,
    capacity_reason: str | None,
) -> dict[str, object] | None:
    """Validate ordinary capacity or persist one exact exceptional per-Run grant."""

    has_reason = isinstance(capacity_reason, str) and bool(capacity_reason.strip())
    if allow_exceptional_capacity != has_reason:
        raise OrchestrateError(
            "Exceptional capacity requires both --allow-exceptional-capacity and --capacity-reason",
            code="capacity_grant_incomplete",
        )
    existing = capacity_grant(store, run.local_id)
    if requested_limit <= ORDINARY_CAPACITY_MAX:
        if allow_exceptional_capacity or existing is not None:
            raise OrchestrateError(
                "Exceptional capacity applies only to a selected plan limit from 4 through 8",
                code="capacity_grant_not_applicable",
            )
        return None
    if plan_digest is None or not 4 <= requested_limit <= EXCEPTIONAL_CAPACITY_MAX:
        raise OrchestrateError("Requested Run capacity is outside the supported range", code="capacity_limit_invalid")
    if not isinstance(run.native_run_id, str) or not run.native_run_id:
        raise OrchestrateError("Exceptional capacity requires the exact native Run identity", code="capacity_run_unbound")
    if existing is not None:
        if (
            existing["runId"] != run.native_run_id
            or existing["planDigest"] != plan_digest
            or existing["limit"] != requested_limit
        ):
            raise OrchestrateError(
                "Exceptional capacity grant does not bind the selected plan and limit",
                code="capacity_grant_conflict",
            )
        if allow_exceptional_capacity and existing["reason"] != capacity_reason:
            raise OrchestrateError("Exceptional capacity was already granted for a different reason", code="capacity_grant_conflict")
        return existing
    if not allow_exceptional_capacity or not has_reason:
        raise OrchestrateError(
            "Plan capacity above 3 is held until an explicit per-Run exceptional grant is recorded",
            code="exceptional_capacity_required",
            data={"planDigest": plan_digest, "requestedLimit": requested_limit},
        )
    assert capacity_reason is not None
    reason = capacity_reason.strip()
    if len(reason) > 1000:
        raise OrchestrateError("Capacity reason exceeds the bounded 1000-character limit", code="capacity_reason_too_large")
    granted_at = utc_now()
    value: dict[str, object] = {
        "schema": CAPACITY_GRANT_SCHEMA,
        "localRunId": run.local_id,
        "runId": run.native_run_id,
        "planDigest": plan_digest,
        "limit": requested_limit,
        "reason": reason,
        "grantedAt": granted_at,
    }
    store.record_stable_evidence(
        run.local_id,
        evidence_id=_event_id(run.local_id, "capacity_grant", plan_digest),
        kind="capacity-grant",
        status="granted",
        subject=plan_digest,
        payload=value,
        created_at=granted_at,
    )
    return value


def effective_capacity(
    store: StateStore,
    run_local_id: str,
    *,
    plan_digest: str | None,
    requested_limit: int,
) -> int:
    if requested_limit <= ORDINARY_CAPACITY_MAX:
        return requested_limit
    grant = capacity_grant(store, run_local_id)
    if grant is not None and grant["planDigest"] == plan_digest and grant["limit"] == requested_limit:
        return requested_limit
    return ORDINARY_CAPACITY_MAX


def occupied_milestone_tasks(store: StateStore, run_local_id: str) -> set[str]:
    """Treat uncertain launches and every unresolved release as occupied."""

    worker_rows = store.connection.execute(
        """SELECT task_key, release_state FROM milestone_worker_bindings
           WHERE run_local_id = ? AND release_state NOT IN ('released', 'retained')""",
        (run_local_id,),
    ).fetchall()
    occupied = {row["task_key"] for row in worker_rows}
    bound = {
        row["task_key"]
        for row in store.connection.execute(
            "SELECT task_key FROM milestone_worker_bindings WHERE run_local_id = ?",
            (run_local_id,),
        ).fetchall()
    }
    intentions = store.connection.execute(
        """SELECT operation FROM intentions
           WHERE run_local_id = ? AND operation LIKE 'milestone-worker-start:%'""",
        (run_local_id,),
    ).fetchall()
    occupied.update(
        row["operation"].split(":", 1)[1]
        for row in intentions
        if row["operation"].split(":", 1)[1] not in bound
    )
    return occupied


def _efficiency_events(store: StateStore, run_local_id: str) -> list[dict[str, object]]:
    rows = store.connection.execute(
        """SELECT id, payload_json, created_at FROM evidence
           WHERE run_local_id = ? AND kind = 'efficiency-event' ORDER BY created_at, id""",
        (run_local_id,),
    ).fetchall()
    events: list[dict[str, object]] = []
    for row in rows:
        try:
            value = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise OrchestrateError("Efficiency event record is malformed", code="efficiency_evidence_conflict") from exc
        expected_keys = {"schema", "eventId", "event", "identity", "observedAt"}
        if isinstance(value, dict) and "details" in value:
            expected_keys.add("details")
        if (
            not isinstance(value, dict)
            or set(value) != expected_keys
            or value.get("schema") != EFFICIENCY_EVENT_SCHEMA
            or value.get("eventId") != row["id"]
            or not isinstance(value.get("event"), str)
            or not isinstance(value.get("identity"), str)
            or _parse_time(value.get("observedAt")) is None
            or ("details" in value and not isinstance(value["details"], dict))
        ):
            raise OrchestrateError("Efficiency event record is malformed", code="efficiency_evidence_conflict")
        events.append(value)
    return events


def efficiency_report(store: StateStore, run: RunRecord) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    run_row = store.connection.execute(
        "SELECT created_at FROM runs WHERE local_id = ?",
        (run.local_id,),
    ).fetchone()
    started = _parse_time(run_row["created_at"] if run_row else None)
    plan_row = store.connection.execute(
        "SELECT plan_digest, plan_json FROM milestone_plan_bindings WHERE run_local_id = ?",
        (run.local_id,),
    ).fetchone()
    requested: int | None = 1
    plan_digest: str | None = None
    if plan_row is not None:
        plan_digest = plan_row["plan_digest"]
        try:
            plan_value = json.loads(plan_row["plan_json"])
            candidate_limit = plan_value.get("maxWorkers", 2) if isinstance(plan_value, dict) else None
        except (TypeError, json.JSONDecodeError):
            candidate_limit = None
        requested = candidate_limit if type(candidate_limit) is int and 1 <= candidate_limit <= 8 else None
    grant = capacity_grant(store, run.local_id)
    if grant is not None and grant["runId"] != run.native_run_id:
        raise OrchestrateError("Capacity grant belongs to another native Run", code="capacity_grant_conflict")
    events = _efficiency_events(store, run.local_id)
    instrumented = any(event["event"] == "run_started" for event in events)
    sessions: dict[str, dict[str, object]] = {}
    released_at: dict[str, str] = {}
    for event in events:
        identity = str(event["identity"])
        if event["event"] == "session_created":
            sessions.setdefault(identity, {"sessionId": identity, "createdAt": event["observedAt"]})
        elif event["event"] == "session_released":
            released_at[identity] = str(event["observedAt"])
    historical_counts_complete = instrumented and all(identity in sessions for identity in released_at)
    observations: list[dict[str, object]] = []
    timeline: list[tuple[datetime, int]] = []
    for identity, session in sorted(sessions.items(), key=lambda item: str(item[1]["createdAt"])):
        created = _parse_time(session["createdAt"])
        released_text = released_at.get(identity)
        released = _parse_time(released_text)
        end = released or now
        duration = max(0, int((end - created).total_seconds())) if created is not None else "unknown"
        age_state = session_age_state(duration) if isinstance(duration, int) else "unknown"
        observations.append(
            {
                **session,
                "releasedAt": released_text,
                "observedDurationSeconds": duration,
                "durationKind": "observed wall time; not compute time",
                "ageState": age_state,
                "followOnReuseAllowed": False,
                "freshSessionHandoffRequired": age_state == "fresh_session_required",
            }
        )
        if created is not None:
            timeline.append((created, 1))
            if released is not None:
                timeline.append((released, -1))
    active = 0
    peak = 0
    for _, delta in sorted(timeline, key=lambda item: (item[0], -item[1])):
        active += delta
        peak = max(peak, active)
    launch_groups: dict[str, int] = defaultdict(int)
    for row in store.connection.execute(
        """SELECT operation FROM intentions WHERE run_local_id = ?
           AND (operation = 'worker-start' OR operation LIKE 'milestone-worker-start:%')""",
        (run.local_id,),
    ).fetchall():
        launch_groups[row["operation"]] += 1
    diagnosis_rows = store.connection.execute(
        "SELECT task_key, diagnosis_status FROM interventions WHERE run_local_id = ? ORDER BY task_key",
        (run.local_id,),
    ).fetchall()
    unknown = "unknown"
    return {
        "effectiveCapacity": (
            effective_capacity(
                store,
                run.local_id,
                plan_digest=plan_digest,
                requested_limit=requested,
            )
            if requested is not None
            else unknown
        ),
        "requestedCapacity": requested if requested is not None else unknown,
        "capacityGrant": grant,
        "runElapsedSeconds": max(0, int((now - started).total_seconds())) if started else unknown,
        "sessionsCreated": len(sessions) if historical_counts_complete else unknown,
        "activeSessions": active if historical_counts_complete else unknown,
        "peakSessions": peak if historical_counts_complete else unknown,
        "sessionCountBasis": "OBSERVED" if historical_counts_complete else unknown,
        "sessions": observations,
        "repeatedLaunchAttempts": sum(max(0, count - 1) for count in launch_groups.values()),
        "diagnosisState": [
            {"task": row["task_key"], "status": row["diagnosis_status"]}
            for row in diagnosis_rows
        ],
        "historicalCountsComplete": historical_counts_complete,
        "events": [
            {
                "eventId": event["eventId"],
                "event": event["event"],
                "observedAt": event["observedAt"],
            }
            for event in events
        ],
        "usage": {
            "controllerModelCalls": (
                {"value": 0, "scope": "instrumented deterministic controller only"}
                if instrumented
                else unknown
            ),
            "workerTokens": unknown,
            "modelTurns": unknown,
            "externalCoordinatorUsage": unknown,
        },
    }
