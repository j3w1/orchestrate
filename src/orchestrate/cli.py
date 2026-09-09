"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Sequence

from .bootstrap import decode_payload, launch_controller
from .controller import answer, explain, implement, packet, resume, status
from .doctor import ProbeContractError, collect_doctor_report, create_run_probe, error_report, run_worker_probe
from .errors import OrchestrateError
from .orca import OrcaClient, OrcaCommandError
from .profile import setup_project
from .state import StateStore


def _project_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project", default=".", help="project path (default: current directory)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orchestrate")
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup = subparsers.add_parser("setup", help="discover and write the small project profile")
    _project_argument(setup)
    setup.add_argument("--force", action="store_true", help="refresh bounded name-based discovery")
    setup.add_argument(
        "--acknowledge-profile",
        action="store_true",
        help="select reviewed profile changes as the host-local operational configuration",
    )
    setup.add_argument("--json", action="store_true")

    run = subparsers.add_parser("implement", help="prepare and supervise one implementation owner")
    run.add_argument("objective", nargs="?", help="authorized objective; omit only to resume one unambiguous Run")
    _project_argument(run)
    run.add_argument("--wait-timeout-ms", type=int, default=300_000)
    run.add_argument("--json", action="store_true")

    for name, help_text in (
        ("status", "show native state, evidence, and the next obligation"),
        ("resume", "reconcile intentions and continue one Run"),
        ("explain", "explain local state without a model call"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        _project_argument(command)
        command.add_argument("--run")
        if name == "resume":
            command.add_argument("--wait-timeout-ms", type=int, default=300_000)
        command.add_argument("--json", action="store_true")

    packet_parser = subparsers.add_parser("packet", help="print one immutable Task packet")
    _project_argument(packet_parser)
    packet_parser.add_argument("--run", required=True)
    packet_parser.add_argument("--task", required=True)
    packet_parser.add_argument("--json", action="store_true")

    answer_parser = subparsers.add_parser("answer", help="reply to one exact worker question")
    _project_argument(answer_parser)
    answer_parser.add_argument("--run", required=True)
    answer_parser.add_argument("--question", required=True)
    answer_parser.add_argument("--text", required=True)
    answer_parser.add_argument("--json", action="store_true")

    bootstrap = subparsers.add_parser("bootstrap", help="run controller arguments in a dedicated ordinary Orca terminal")
    _project_argument(bootstrap)
    bootstrap.add_argument("arguments", nargs=argparse.REMAINDER)

    inner = subparsers.add_parser("_controller", help=argparse.SUPPRESS)
    inner.add_argument("--payload", required=True)
    inner.add_argument("--result-path")

    doctor = subparsers.add_parser("doctor", help="check the public Orca CLI contract")
    doctor.add_argument("--json", action="store_true", help="emit one JSON document")
    active = doctor.add_mutually_exclusive_group()
    active.add_argument("--active-run-probe", action="store_true")
    active.add_argument("--active-worker-probe", metavar="PROBE_TOKEN")
    doctor.add_argument(
        "--disposable-project",
        help="exact clean Orca worktree containing a committed .orchestrate-disposable marker",
    )
    doctor.add_argument("--wait-timeout-ms", type=int, default=300_000)
    return parser


def _print_human(report: dict[str, Any]) -> None:
    print(f"orchestrate: {report.get('status', 'ok')}")
    if "checks" in report:
        for check in report["checks"]:
            print(f"  {check['status'].upper():11} {check['name']}: {check['detail']}")
    for label, key in (("Run", "runId"), ("Task", "taskId"), ("Dispatch", "dispatchId")):
        if report.get(key):
            print(f"  {label}: {report[key]}")
    if report.get("nextObligation"):
        print(f"  Next: {report['nextObligation']}")
    error = report.get("error")
    if isinstance(error, dict):
        print(f"  {error.get('code', 'error')}: {error.get('message', '')}")


def _human_text(report: dict[str, Any]) -> str:
    from contextlib import redirect_stdout
    import io

    output = io.StringIO()
    with redirect_stdout(output):
        _print_human(report)
    return output.getvalue()


def _error(exc: Exception) -> dict[str, Any]:
    code = exc.code if isinstance(exc, (OrchestrateError, OrcaCommandError)) else "orchestrate_error"
    result: dict[str, Any] = {
        "schema": "orchestrate-report/v1",
        "status": "blocked",
        "error": {"code": code, "message": str(exc)},
    }
    if isinstance(exc, OrchestrateError) and exc.data is not None:
        result["error"]["data"] = exc.data
    return result


def _needs_bootstrap(command: str) -> bool:
    return command in {"implement", "resume", "answer"} and not os.environ.get("ORCA_TERMINAL_HANDLE")


def _execute(args: argparse.Namespace, raw_argv: list[str]) -> tuple[dict[str, Any], bool]:
    client = OrcaClient()
    root = Path(getattr(args, "project", ".")).resolve()
    if _needs_bootstrap(args.command):
        exit_code = launch_controller(root, raw_argv, client=client)
        return {"_bootstrapPassthrough": True, "controllerExitCode": exit_code}, False
    if args.command == "setup":
        profile = setup_project(
            root,
            force=args.force,
            acknowledge_profile=args.acknowledge_profile,
        )
        with StateStore(profile.root):
            pass
        return {
            "schema": "orchestrate-report/v1",
            "status": "configured",
            "profile": os.fspath(profile.path),
            "profileSelection": {
                "digest": profile.digest,
                "source": profile.selection_source,
                "historyDigest": profile.selection_history_digest,
            },
            "reader": profile.value["reader"]["kind"],
            "instructions": profile.value["instructions"],
            "taskEntrypoints": profile.value["taskEntrypoints"],
            "checks": "NOT_RUN",
        }, args.json
    if args.command == "implement":
        return implement(root, args.objective, client=client, wait_timeout_ms=args.wait_timeout_ms), args.json
    if args.command == "resume":
        return resume(root, args.run, client=client, wait_timeout_ms=args.wait_timeout_ms), args.json
    if args.command == "status":
        return status(root, args.run, client=client), args.json
    if args.command == "explain":
        return explain(root, args.run), args.json
    if args.command == "packet":
        return packet(root, args.run, args.task), args.json
    if args.command == "answer":
        return answer(root, args.run, args.question, args.text, client=client), args.json
    if args.command == "bootstrap":
        inner_args = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
        if not inner_args:
            raise OrchestrateError("bootstrap requires orchestrate arguments after --", code="bootstrap_arguments_missing")
        exit_code = launch_controller(root, inner_args, client=client)
        return {"_bootstrapPassthrough": True, "controllerExitCode": exit_code}, False
    if args.command == "doctor":
        try:
            if args.active_run_probe:
                if not args.disposable_project:
                    raise ProbeContractError("--active-run-probe requires --disposable-project")
                report = create_run_probe(client, project=Path(args.disposable_project).resolve())
            elif args.active_worker_probe:
                if args.wait_timeout_ms <= 0:
                    raise ProbeContractError("--wait-timeout-ms must be positive")
                if not args.disposable_project:
                    raise ProbeContractError("--active-worker-probe requires --disposable-project")
                report = run_worker_probe(
                    client,
                    args.active_worker_probe,
                    project=Path(args.disposable_project).resolve(),
                    wait_timeout_ms=args.wait_timeout_ms,
                )
            else:
                report = collect_doctor_report(client)
        except (OrcaCommandError, ProbeContractError) as exc:
            operation = "worker-probe" if args.active_worker_probe else "run-create"
            report = error_report(exc, operation=operation)
        return report, args.json
    raise AssertionError(f"unhandled command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(argv) if argv is not None else list(os.sys.argv[1:])
    args = build_parser().parse_args(raw)
    result_path: Path | None = None
    if args.command == "_controller":
        result_path = Path(args.result_path).resolve() if args.result_path else None
        raw = decode_payload(args.payload)
        args = build_parser().parse_args(raw)
    try:
        report, emit_json = _execute(args, raw)
    except KeyboardInterrupt:
        report = {
            "schema": "orchestrate-report/v1",
            "status": "interrupted",
            "error": {
                "code": "controller_interrupted",
                "message": "Controller waiting stopped; active workers were not killed",
            },
        }
        emit_json = bool(getattr(args, "json", False))
    except (OrchestrateError, OrcaCommandError) as exc:
        report = _error(exc)
        emit_json = bool(getattr(args, "json", False)) or (
            args.command == "bootstrap" and "--json" in getattr(args, "arguments", [])
        )
    if report.get("_bootstrapPassthrough") is True:
        exit_code = report.get("controllerExitCode")
        return exit_code if isinstance(exit_code, int) else 1
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n" if emit_json else _human_text(report)
    exit_code = 0 if report.get("status") not in {"blocked", "worker_failed"} else 1
    if report.get("status") == "interrupted":
        exit_code = 130
    if isinstance(report.get("controllerExitCode"), int):
        exit_code = report["controllerExitCode"]
    if result_path is not None:
        result_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = result_path.with_suffix(result_path.suffix + ".tmp")
        temporary.write_text(
            json.dumps({"schema": "orchestrate-bootstrap/v1", "exitCode": exit_code, "stdout": rendered}),
            encoding="utf-8",
        )
        temporary.replace(result_path)
    print(rendered, end="")
    return exit_code
