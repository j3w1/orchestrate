"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
from typing import Any, Sequence

from .doctor import (
    ProbeContractError,
    collect_doctor_report,
    create_run_probe,
    error_report,
    run_worker_probe,
)
from .orca import OrcaClient, OrcaCommandError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orchestrate")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="check the public Orca CLI contract")
    doctor.add_argument("--json", action="store_true", help="emit one JSON document")
    active = doctor.add_mutually_exclusive_group()
    active.add_argument(
        "--active-run-probe",
        action="store_true",
        help="create a disposable Run; requires explicit coordinator authority",
    )
    active.add_argument(
        "--active-worker-probe",
        metavar="RUN_ID",
        help="resume a probe Run and exercise worker_done/release/ack; root coordinators only",
    )
    doctor.add_argument(
        "--wait-timeout-ms",
        type=int,
        default=300_000,
        help="worker completion wait for --active-worker-probe (default: 300000)",
    )
    return parser


def _print_human(report: dict[str, Any]) -> None:
    print(f"orchestrate doctor: {report['status']}")
    if "checks" in report:
        for check in report["checks"]:
            print(f"  {check['status'].upper():11} {check['name']}: {check['detail']}")
    if "runId" in report:
        print(f"  Run: {report['runId']}")
    error = report.get("error")
    if isinstance(error, dict):
        print(f"  {error.get('code', 'error')}: {error.get('message', '')}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = OrcaClient()

    if args.command != "doctor":
        raise AssertionError(f"unhandled command: {args.command}")

    try:
        if args.active_run_probe:
            report = create_run_probe(client)
        elif args.active_worker_probe:
            if args.wait_timeout_ms <= 0:
                raise ProbeContractError("--wait-timeout-ms must be positive")
            report = run_worker_probe(
                client,
                args.active_worker_probe,
                wait_timeout_ms=args.wait_timeout_ms,
            )
        else:
            report = collect_doctor_report(client)
    except (OrcaCommandError, ProbeContractError) as exc:
        operation = "worker-probe" if args.active_worker_probe else "run-create"
        report = error_report(exc, operation=operation)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_human(report)
    return 0 if report["status"] in {"pass", "created"} else 1

