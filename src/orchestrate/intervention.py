"""One-shot repeated-failure intervention records.

This is deliberately not a retry engine. It records why a correction is being
considered and consumes at most one bounded diagnosis when the same correction
would otherwise repeat without new meaningful evidence.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Literal

from .errors import OrchestrateError
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
        if not task_key or not correction_key:
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

        row = self.store.connection.execute(
            "SELECT * FROM interventions WHERE run_local_id = ? AND task_key = ?",
            (self.run_local_id, task_key),
        ).fetchone()
        if row is None or row["diagnosis_status"] != "required":
            raise OrchestrateError(
                "No bounded diagnosis is currently required for this Task",
                code="diagnosis_not_authorized",
            )
        new_digest = _digest(diagnosis_evidence) if isinstance(diagnosis_evidence, str) and diagnosis_evidence.strip() else None
        productive = new_digest is not None and new_digest != row["evidence_digest"]
        self.store.connection.execute(
            """UPDATE interventions
               SET evidence_digest = COALESCE(?, evidence_digest),
                   diagnosis_status = ?, updated_at = ?
               WHERE run_local_id = ? AND task_key = ?""",
            (
                new_digest,
                "productive" if productive else "unproductive",
                utc_now(),
                self.run_local_id,
                task_key,
            ),
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
