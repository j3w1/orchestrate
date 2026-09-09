# Orca public CLI compatibility

This document records the bounded first-milestone observations. It is not a general Orca certification and does not establish hosted, WSL, connected-server, model-provider, or release behavior.

## Observed environment

- Windows host
- Python 3.13.15
- Orca 1.4.198
- Public capabilities `orchestration.contract.v1` and `orchestration.worker-launch-preferences.v1` advertised
- Version-matched guides loaded with `orca skills get orca-cli` and `orca skills get orchestration`

The compact orchestration guide mentions a `--reference` discovery path, but this installed CLI rejects `--references` and its `skills get --help` exposes `--full` instead. The compact guide itself was available and read in full.

## Caller identity results

Three caller contexts produced materially different results.

First, a plain Python 3.13 subprocess was launched through the dispatched worker's command-execution surface with every `ORCA_*` environment entry removed. It did not declare a terminal with `--from`. Its public `orca orchestration run-create` request reached the installed CLI and failed closed:

```text
code: no_active_sender_terminal
message: Could not determine the sender terminal for this orchestration command. Pass --from <terminal-handle> or run the command inside a live Orca terminal with ORCA_TERMINAL_HANDLE set.
```

No Run was created by that attempt. The executable probe reproduces the result with:

```powershell
py -3.13 -c "import os, pathlib, subprocess; env={k:v for k,v in os.environ.items() if not k.startswith('ORCA_')}; env['PYTHONPATH']=str(pathlib.Path('src').resolve()); p=subprocess.run(['py','-3.13','-m','orchestrate','doctor','--active-run-probe','--json'], env=env, capture_output=True, text=True); print(p.stdout, end=''); raise SystemExit(p.returncode)"
```

Second, removing Orca variables from a subprocess launched directly by an existing coordinator agent did not remove its caller association: Orca bound the resulting Run to that existing agent terminal. That is inherited terminal evidence, not standalone-host support.

Third, a fresh ordinary Orca shell terminal, with no reasoning agent in it, ran plain Python and successfully created a Run under the shell's own terminal identity. A later plain-Python process in that shell successfully rebound the Run and created a native Task. This proves that a reasoning agent is not required for the Python coordinator, provided it runs in an Orca-owned terminal.

The direct public command still has no demonstrated outside-Orca sender identity. A caller with neither an Orca terminal association nor an explicit `--from` fails with `no_active_sender_terminal`; passing another terminal's handle would impersonate it and is not a supported fallback. A source-backed architecture assessment found that a thin user-entry bootstrap into an ordinary Orca terminal fits the approved Orca-native plan and requires no established Orca API change. That bootstrap remains unimplemented and must eventually preserve foreground output, exit status, Ctrl-C behavior, exact workspace selection, and Windows/WSL argument boundaries.

The active implementation worker is already an Orca agent terminal at dispatch depth 1. Its inherited lifecycle authority is not evidence for a standalone controller, and it did not attempt a nested dispatch.

## Current gate state

| Gate | Result | Evidence |
| --- | --- | --- |
| Resolve and invoke one Orca executable | Verified | Read-only `doctor` client and focused tests |
| Runtime ready and required capabilities advertised | Verified | Live `status --json` |
| Plain Python in an ordinary Orca shell creates/binds a Run | Verified | Coordinator live probe; shell identity owned the Run |
| Fresh-process Run rebind and Task creation in that shell | Verified | Coordinator live probe |
| Direct caller without Orca terminal association creates/binds a Run | Unsupported in tested contract | `no_active_sender_terminal` |
| Thin bootstrap into an ordinary Orca coordinator terminal | Architecture-compatible; implementation NOT_RUN | Must preserve foreground process and host/argv boundaries |
| Real worker launch | Partial failure | Native Dispatch created, then `agent_prompt_stalled` at `dispatch_input` |
| `worker_done`, release, and Delivery acknowledgment | NOT_RUN | Prompt never reached the worker; no blind retry |
| Release-before-ack completion handling | Locally tested with synthetic responses; live NOT_RUN | Coordinator-only probe prepared |
| WSL and connected-server behavior | NOT_RUN | Outside this milestone |

The plain-shell evidence removes a blanket Python-controller blocker, but the first vertical slice is not complete because real prompt delivery, completion, release, and acknowledgment did not converge. Dependent controller, setup, packet, reader, bookkeeping, and execution features remain unimplemented. There is no fallback coordinator, hidden terminal impersonation, or parallel task store.

## Coordinator-only live probe

Run these commands only from a fresh, otherwise unused ordinary Orca shell terminal, not from a dispatched worker and not from a coordinator terminal that is supervising another Run. The first command creates a disposable Run and exits; the second starts in a fresh Python process, rebinds that Run, launches a no-edit worker, waits for `worker_done`, releases the worker, and only then acknowledges the Delivery.

```powershell
$env:PYTHONPATH = (Resolve-Path .\src).Path
py -3.13 -m orchestrate doctor --active-run-probe --json
py -3.13 -m orchestrate doctor --active-worker-probe <run-id-from-first-command> --json
```

The active probes mutate Orca coordination state and are never run by ordinary `doctor`. If a worker asks a question or escalates instead of completing, the probe fails and deliberately leaves that Delivery unacknowledged for coordinator inspection. The first coordinator exercise already reached `agent_prompt_stalled`; do not repeat it unchanged without new diagnostic evidence or an explicit recovery decision.

Before acknowledgment, the prepared probe requires one exact Task- and Dispatch-bound `worker_done`, a recognized outcome, matching native Task and Dispatch settlement, and a confirmed released terminal resource. It holds intervention, malformed, stale, unknown, and release-pending cases without acknowledgment, and includes prior command receipts plus structured failure data in its JSON report for recovery.
