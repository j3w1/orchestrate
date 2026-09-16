"""One-shot repeated-failure intervention records.

This is deliberately not a retry engine. It records why a correction is being
considered and consumes at most one bounded diagnosis when the same correction
would otherwise repeat without new meaningful evidence.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Literal

from .errors import OrchestrateError
from .profile import find_project_root
from .state import StateStore, utc_now


InterventionDecision = Literal["correction_allowed", "diagnosis_required", "unresolved"]


def _digest(value: str) -> str:
    return "evidence_sha256_" + hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class InterventionRecord:
    obligation: str
    failing_example: str
    hypothesis: str
    last_meaningful_evidence: str
    next_discriminating_check: str

    def validated(self) -> "InterventionRecord":
        for name, value in asdict(self).items():
            if not isinstance(value, str) or not value.strip():
                raise OrchestrateError(
                    f"Intervention record requires {name.replace('_', ' ')}",
                    code="intervention_record_incomplete",
                )
        return self

    @property
    def evidence_digest(self) -> str:
        return _digest(self.last_meaningful_evidence)


class InterventionLedger:
    def __init__(self, store: StateStore, run_local_id: str) -> None:
        self.store = store
        self.run_local_id = run_local_id

    def consider_correction(
        self,
        *,
        task_key: str,
        correction_key: str,
        record: InterventionRecord,
    ) -> InterventionDecision:
        """Authorize new evidence, otherwise require exactly one diagnosis."""

        record.validated()
        if not task_key.strip() or not correction_key.strip():
            raise OrchestrateError("Intervention identity is incomplete", code="intervention_record_incomplete")
        encoded = json.dumps(asdict(record), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        now = utc_now()
        with self.store.transaction():
            row = self.store.connection.execute(
                "SELECT * FROM interventions WHERE run_local_id = ? AND task_key = ?",
                (self.run_local_id, task_key),
            ).fetchone()
            if row is None:
                self.store.connection.execute(
                    "INSERT INTO interventions VALUES (?, ?, ?, ?, ?, 1, 'not_needed', ?, ?)",
                    (
                        self.run_local_id,
                        task_key,
                        encoded,
                        correction_key,
                        record.evidence_digest,
                        now,
                        now,
                    ),
                )
                return "correction_allowed"
            same_attempt = (
                row["correction_key"] == correction_key
                and row["evidence_digest"] == record.evidence_digest
            )
            if not same_attempt:
                self.store.connection.execute(
                    """UPDATE interventions
                       SET record_json = ?, correction_key = ?, evidence_digest = ?,
                           correction_count = correction_count + 1,
                           diagnosis_status = 'not_needed', updated_at = ?
                       WHERE run_local_id = ? AND task_key = ?""",
                    (
                        encoded,
                        correction_key,
                        record.evidence_digest,
                        now,
                        self.run_local_id,
                        task_key,
                    ),
                )
                return "correction_allowed"
            if row["diagnosis_status"] in {"not_needed", "required"}:
                if row["diagnosis_status"] == "not_needed":
                    self.store.connection.execute(
                        """UPDATE interventions SET record_json = ?, diagnosis_status = 'required', updated_at = ?
                           WHERE run_local_id = ? AND task_key = ?""",
                        (encoded, now, self.run_local_id, task_key),
                    )
                return "diagnosis_required"
            return "unresolved"

    def finish_diagnosis(
        self,
        *,
        task_key: str,
        diagnosis_evidence: str | None,
    ) -> InterventionDecision:
        """Consume the one diagnosis; only genuinely new evidence reopens correction."""

        with self.store.transaction():
            row = self.store.connection.execute(
                "SELECT * FROM interventions WHERE run_local_id = ? AND task_key = ?",
                (self.run_local_id, task_key),
            ).fetchone()
            if row is None or row["diagnosis_status"] != "required":
                raise OrchestrateError(
                    "No bounded diagnosis is currently required for this Task",
                    code="diagnosis_not_authorized",
                )
            new_digest = (
                _digest(diagnosis_evidence)
                if isinstance(diagnosis_evidence, str) and diagnosis_evidence.strip()
                else None
            )
            productive = new_digest is not None and new_digest != row["evidence_digest"]
            changed = self.store.connection.execute(
                """UPDATE interventions
                   SET evidence_digest = COALESCE(?, evidence_digest),
                       diagnosis_status = ?, updated_at = ?
                   WHERE run_local_id = ? AND task_key = ? AND diagnosis_status = 'required'""",
                (
                    new_digest,
                    "productive" if productive else "unresolved",
                    utc_now(),
                    self.run_local_id,
                    task_key,
                ),
            )
            if changed.rowcount != 1:
                raise OrchestrateError(
                    "The bounded diagnosis was already consumed concurrently",
                    code="diagnosis_not_authorized",
                )
        return "correction_allowed" if productive else "unresolved"

    def read(self, task_key: str) -> dict[str, object] | None:
        row = self.store.connection.execute(
            "SELECT * FROM interventions WHERE run_local_id = ? AND task_key = ?",
            (self.run_local_id, task_key),
        ).fetchone()
        if row is None:
            return None
        return {
            "taskKey": row["task_key"],
            "record": json.loads(row["record_json"]),
            "correctionKey": row["correction_key"],
            "evidenceDigest": row["evidence_digest"],
            "correctionCount": row["correction_count"],
            "diagnosisStatus": row["diagnosis_status"],
        }


MAX_INTERVENTION_FILE_BYTES = 64 * 1024


def _read_bounded_json(path: Path) -> object:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(os.fspath(path), flags)
        opened = os.fstat(descriptor)
        linked = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
            or opened.st_size > MAX_INTERVENTION_FILE_BYTES
        ):
            raise OrchestrateError("Intervention input must be one bounded regular file", code="intervention_input_invalid")
        chunks: list[bytes] = []
        remaining = MAX_INTERVENTION_FILE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    except (OSError, UnicodeError) as exc:
        raise OrchestrateError("Intervention input cannot be read safely", code="intervention_input_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if len(raw) > MAX_INTERVENTION_FILE_BYTES:
        raise OrchestrateError("Intervention input exceeds 64 KiB", code="intervention_input_invalid")

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON value {value}")

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key {key}")
            value[key] = item
        return value

    try:
        return json.loads(
            raw.decode("utf-8"),
            parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise OrchestrateError("Intervention input must be strict UTF-8 JSON", code="intervention_input_invalid") from exc


def _bound_task(store: StateStore, run_local_id: str, task: str) -> tuple[str, str]:
    run = store.get_run(run_local_id)
    if run.task_id == task:
        return task, "implementation-owner"
    rows = store.connection.execute(
        """SELECT task_key, task_id FROM milestone_task_bindings
           WHERE run_local_id = ? AND (task_id = ? OR task_key = ?)""",
        (run.local_id, task, task),
    ).fetchall()
    if len(rows) != 1:
        raise OrchestrateError("Intervention Task does not bind uniquely to the selected Run", code="intervention_task_not_found")
    return rows[0]["task_id"], rows[0]["task_key"]


def run_intervention(
    root: Path,
    *,
    run_id: str,
    task: str,
    record_path: Path | None,
    diagnosis_path: Path | None,
) -> dict[str, Any]:
    """Apply one operator-led ledger transition without launching or retrying work."""

    if (record_path is None) == (diagnosis_path is None):
        raise OrchestrateError("Choose exactly one intervention record or diagnosis", code="intervention_input_invalid")
    with StateStore(find_project_root(root)) as store:
        run = store.get_run(run_id)
        task_id, task_key = _bound_task(store, run.local_id, task)
        ledger = InterventionLedger(store, run.local_id)
        if record_path is not None:
            value = _read_bounded_json(record_path)
            required = {
                "obligation",
                "failing_example",
                "hypothesis",
                "last_meaningful_evidence",
                "next_discriminating_check",
                "correction_key",
            }
            if not isinstance(value, dict) or set(value) != required or not all(
                isinstance(value[key], str) for key in required
            ):
                raise OrchestrateError(
                    "Intervention record must contain exactly the five record fields and correction_key",
                    code="intervention_input_invalid",
                )
            record = InterventionRecord(
                value["obligation"],
                value["failing_example"],
                value["hypothesis"],
                value["last_meaningful_evidence"],
                value["next_discriminating_check"],
            )
            decision = ledger.consider_correction(
                task_key=task_id,
                correction_key=value["correction_key"],
                record=record,
            )
        else:
            assert diagnosis_path is not None
            value = _read_bounded_json(diagnosis_path)
            if (
                not isinstance(value, dict)
                or set(value) != {"diagnosis_evidence"}
                or (
                    value["diagnosis_evidence"] is not None
                    and (
                        not isinstance(value["diagnosis_evidence"], str)
                        or not value["diagnosis_evidence"].strip()
                    )
                )
            ):
                raise OrchestrateError(
                    "Diagnosis input must contain exactly diagnosis_evidence as a string or null",
                    code="intervention_input_invalid",
                )
            decision = ledger.finish_diagnosis(
                task_key=task_id,
                diagnosis_evidence=value["diagnosis_evidence"],
            )
        return {
            "schema": "orchestrate-report/v1",
            "status": decision,
            "runId": run.native_run_id,
            "localRunId": run.local_id,
            "taskId": task_id,
            "taskKey": task_key,
            "intervention": ledger.read(task_id),
            "nextObligation": (
                "perform only the recorded discriminating diagnosis"
                if decision == "diagnosis_required"
                else "operator review remains required; this decision does not authorize a Dispatch or retry"
            ),
        }
