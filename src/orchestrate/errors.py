"""Typed, user-facing failures for orchestrate."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import ntpath
import os
import re
import sys
from typing import Any


_DIAGNOSTIC_FIELD_LIMIT = 256
_DIAGNOSTIC_MESSAGE_LIMIT = 4096
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SAFE_TOKEN = re.compile(r"[A-Za-z0-9_.:+-]{1,128}\Z")
_SAFE_IDENTITY_VALUES = {
    "absent",
    "ambiguous",
    "available",
    "available-descriptor-path",
    "available-and-matching",
    "canonical-object",
    "complete",
    "dict",
    "directory",
    "empty-string",
    "existing-unreceipted-archive",
    "exclusive-owned-regular-file",
    "file",
    "incomplete-or-unsupported",
    "invalid-handle",
    "invalid-json",
    "incomplete",
    "invalid",
    "duplicate-or-nonfinite-json",
    "matching",
    "matching-receipt",
    "nonnegative-integer",
    "nonempty-string",
    "non-utf8",
    "numeric-major-minor",
    "NoneType",
    "noncanonical-json",
    "os-error",
    "owned-single-link-regular-file",
    "owned-regular-file",
    "receipt-absent",
    "redirected",
    "reservation-exhausted",
    "regular-file",
    "regular-file-or-directory",
    "regular-file;links:1;opened-equals-linked",
    "success",
    "string",
    "unavailable",
    "unavailable-or-divergent",
    "unavailable-or-redirected",
    "unexpected-node",
    "unique-key-value-record",
    "utf8-key-value-record",
    "valid-handle",
    "valid",
}
_RECEIPT_FIELDS = {
    "anchorSha256",
    "commandSha256",
    "commandSize",
    "installationId",
    "pyvenvConfigSha256",
    "pythonPath",
    "receiptSha256",
    "schema",
    "sourceArchiveSha256",
    "sourceArchiveSize",
    "sourceRoot",
    "sourceTreeSha256",
    "venvRoot",
}


def _bootstrap_provider_name() -> str:
    selected = os.environ.get("ORCHESTRATE_BOOTSTRAP_PROVIDER", "").strip()
    if selected == "simulated-win32":
        return selected
    return "native-win32" if sys.platform == "win32" else "posix"


def _diagnostic_hash(value: object) -> str:
    try:
        raw = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: type(item).__name__,
        )
    except (TypeError, ValueError):
        raw = type(value).__name__
    return f"sha256:{hashlib.sha256(raw.encode('utf-8', errors='replace')).hexdigest()}"


def _bounded_text(value: object) -> str:
    text = str(value)
    if len(text) <= _DIAGNOSTIC_FIELD_LIMIT:
        return text
    digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
    return f"{text[:160]}...<truncated-text-id:sha256:{digest}>"


def _redacted_string_identity(value: object) -> str:
    return f"redacted-string-id:{_diagnostic_hash(value)}"


def _sanitize_identity_value(component: str, value: object) -> object:
    if value is None or type(value) in {bool, int}:
        return value
    if isinstance(value, (list, tuple)):
        if component.endswith(".fields"):
            return [
                item
                if isinstance(item, str) and item in _RECEIPT_FIELDS
                else f"unknown-field-string-id:{_diagnostic_hash(item)}"
                for item in value[:16]
            ]
        return [_sanitize_identity_value(component, item) for item in value[:16]]
    if not isinstance(value, str):
        return {
            "type": type(value).__name__,
            "redactedValueIdentity": _diagnostic_hash(value),
        }

    if value in _SAFE_IDENTITY_VALUES or value.startswith("machine_bootstrap_"):
        return value
    if value.startswith(("redacted-string-id:sha256:", "invalid-digest-string-id:sha256:")):
        return value
    if value.startswith("redacted:sha256:"):
        return value.replace("redacted:sha256:", "redacted-string-id:sha256:", 1)
    if value.startswith("invalid:sha256:"):
        return value.replace("invalid:sha256:", "invalid-digest-string-id:sha256:", 1)
    lowered = component.casefold()
    if any(marker in lowered for marker in ("path", "root", "resolution")):
        if value == "absent" or os.path.isabs(value) or ntpath.isabs(value):
            return _bounded_text(value)
        return _redacted_string_identity(value)
    if "sha256" in lowered or "digest" in lowered:
        return (
            value
            if _SHA256.fullmatch(value)
            else f"invalid-digest-string-id:{_diagnostic_hash(value)}"
        )
    if "installationid" in lowered:
        return _redacted_string_identity(value)
    if "fileidentity" in lowered:
        return _bounded_text(value)
    if re.fullmatch(r"(?:0\.\.[0-9]+|mode:0x[0-9a-f]+(?:;links:[0-9]+)?)", value):
        return value
    if re.fullmatch(
        r"(?:O_NOFOLLOW\|O_NONBLOCK|"
        r"O_NOFOLLOW:(?:True|False);O_NONBLOCK:(?:True|False)|"
        r"regular-file;size:0\.\.[0-9]+|mode:0x[0-9a-f]+;size:[0-9]+)",
        value,
    ):
        return value
    if value.startswith(("orchestrate-machine-install/", "orchestrate-machine-install-anchor/")):
        return _bounded_text(value)
    return _redacted_string_identity(value)


def _sanitize_operation(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {"form": "unavailable", "detailSha256": _diagnostic_hash(value)}
    result: dict[str, object] = {}
    for key in (
        "operation",
        "form",
        "path",
        "pathForm",
        "explicitApplicationName",
        "access",
        "share",
    ):
        item = value.get(key)
        if item is None or type(item) in {bool, int}:
            if item is not None:
                result[key] = item
        elif isinstance(item, str):
            result[key] = _bounded_text(item)
    return result


def _sanitize_archive_stage(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {"stage": "unavailable", "detailSha256": _diagnostic_hash(value)}
    result: dict[str, object] = {}
    for key in ("stage", "form", "path", "zipStructure"):
        item = value.get(key)
        if isinstance(item, str):
            result[key] = _bounded_text(item)
    size = value.get("size")
    if type(size) is int:
        result["size"] = size
    digest = value.get("sha256")
    if isinstance(digest, str):
        result["sha256"] = (
            digest
            if _SHA256.fullmatch(digest)
            else f"invalid-digest-string-id:{_diagnostic_hash(digest)}"
        )
    return result


def _sanitize_machine_diagnostic(code: str, data: Mapping[str, Any] | None) -> dict[str, object]:
    incoming = {} if data is None else dict(data)
    component_value = incoming.get("component")
    component = (
        component_value
        if isinstance(component_value, str) and _SAFE_TOKEN.fullmatch(component_value)
        else code.removeprefix("machine_bootstrap_").replace("_", ".")
    )
    result: dict[str, object] = {
        "provider": _bootstrap_provider_name(),
        "component": component,
        "expected": _sanitize_identity_value(
            component, incoming.get("expected", "available-and-matching")
        ),
        "observed": _sanitize_identity_value(
            component, incoming.get("observed", "unavailable-or-divergent")
        ),
    }
    for key in (
        "phase",
        "path",
        "pathForm",
        "operation",
        "form",
        "access",
        "share",
        "attributes",
        "nodeType",
        "handleIdentity",
        "linkedIdentity",
        "receiptWriteCause",
    ):
        value = incoming.get(key)
        if isinstance(value, str):
            result[key] = _bounded_text(value)
    for key in ("errno", "winerror", "handleSize", "explicitApplicationName"):
        value = incoming.get(key)
        if value is None or type(value) in {bool, int}:
            if value is not None:
                result[key] = value
    operations = incoming.get("operations")
    if isinstance(operations, (list, tuple)):
        result["operations"] = [_sanitize_operation(item) for item in operations[:8]]
    archive_stages = incoming.get("archiveStages")
    if isinstance(archive_stages, (list, tuple)):
        result["archiveStages"] = [
            _sanitize_archive_stage(item) for item in archive_stages[:8]
        ]
    ancestry = incoming.get("anchorAncestry")
    if isinstance(ancestry, (list, tuple)):
        result["anchorAncestry"] = [
            {
                key: (_bounded_text(item[key]) if isinstance(item.get(key), str) else item[key])
                for key in ("path", "result", "attributes")
                if isinstance(item, Mapping) and key in item
            }
            for item in ancestry[:8]
        ]
    comparison = incoming.get("pathComparison")
    if isinstance(comparison, Mapping):
        result["pathComparison"] = {
            key: (_bounded_text(value) if isinstance(value, str) else value)
            for key, value in comparison.items()
            if key in {"expectedNormalized", "observedNormalized", "matched"}
            and (isinstance(value, str) or type(value) is bool)
        }
    return result


def _render_machine_diagnostic(data: Mapping[str, object]) -> str:
    rendered = json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if len(rendered) <= _DIAGNOSTIC_MESSAGE_LIMIT:
        return rendered
    core = {
        key: data[key]
        for key in ("provider", "phase", "component", "expected", "observed")
        if key in data
    }
    core["detailSha256"] = _diagnostic_hash(data)
    core["truncated"] = True
    return json.dumps(core, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class OrchestrateError(RuntimeError):
    """A bounded failure with a stable machine-readable code."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "orchestrate_error",
        data: Mapping[str, Any] | None = None,
    ) -> None:
        self.code = code
        self._base_message = message
        self._machine_diagnostic = code.startswith("machine_bootstrap_") and (
            "identity" in code
            or code
            in {
                "machine_bootstrap_safe_io_unavailable",
                "machine_bootstrap_verification_failed",
                "machine_bootstrap_receipt_write_failed",
            }
        )
        if self._machine_diagnostic:
            self.data = _sanitize_machine_diagnostic(code, data)
        else:
            self.data = dict(data) if data is not None else None
        self._refresh_message()

    def _refresh_message(self) -> None:
        if self._machine_diagnostic and self.data is not None:
            message = f"{self._base_message} [diagnostic={_render_machine_diagnostic(self.data)}]"
        else:
            message = self._base_message
        RuntimeError.__init__(self, message)

    def add_diagnostic(self, data: Mapping[str, Any]) -> None:
        """Attach bounded machine-bootstrap evidence without changing the failure."""

        if not self._machine_diagnostic:
            return
        combined = {} if self.data is None else dict(self.data)
        for key, value in data.items():
            if key in {"operations", "archiveStages", "anchorAncestry"}:
                prior = combined.get(key)
                combined[key] = [
                    *(prior if isinstance(prior, list) else []),
                    *(value if isinstance(value, (list, tuple)) else [value]),
                ]
            else:
                combined[key] = value
        self.data = _sanitize_machine_diagnostic(self.code, combined)
        self._refresh_message()
