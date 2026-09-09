# orchestrate

orchestrate is an early Orca-native execution layer for governed software work. It is being built to help a small Python controller prepare work, supervise Orca-owned workers, and explain what remains without becoming a second scheduler or governance system.

The first compatibility milestone is intentionally narrow. The package can inspect the public Orca contract and run explicit compatibility probes; the full `setup`, `implement`, `status`, `resume`, `explain`, `packet`, and `answer` workflow is not implemented yet.

## Table of Contents

- [How it works](#how-it-works)
- [Installation](#installation)
  - [Windows](#windows)
  - [Agent skill](#agent-skill)
  - [WSL](#wsl)
- [The Basic Workflow](#the-basic-workflow)
- [What's Inside](#whats-inside)
- [Philosophy](#philosophy)
- [Contributing](#contributing)
- [Updating](#updating)
- [License](#license)

## How it works

Right now, orchestrate starts with compatibility instead of pretending the controller already exists.

`orchestrate doctor` resolves one Orca executable using the installed guide's platform rules, invokes it with argument arrays, keeps JSON stdout separate from stderr keepalives, and verifies the required public orchestration capabilities. It makes no Orca coordination mutations by default.

Two explicit active modes exercise the first vertical slice. One creates a disposable Run and exits. A later process can rebind that Run and, when executed by a root Orca coordinator, launch a no-edit worker, receive `worker_done`, release the worker, and acknowledge the Delivery in the required order.

The live evidence draws a sharp host boundary. Plain Python in an ordinary Orca shell can create and rebind a Run without a reasoning agent, while a caller with no Orca terminal association fails with `no_active_sender_terminal`. A thin bootstrap into that ordinary Orca terminal fits the approved architecture but is not implemented yet. A first real worker launch created a native Dispatch but stalled while delivering the prompt, so completion and acknowledgment are still unverified. The exact evidence and remaining gates are in [Orca public CLI compatibility](docs/orca-compatibility.md).

## Installation

orchestrate is not published to PyPI. Install this checkout in a Python 3.13 virtual environment while it is under development.

### Windows

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python -m pip install --editable .
.\.venv\Scripts\orchestrate doctor
```

The active probes require a running Orca desktop runtime. Worker launch additionally requires a root coordinator terminal; a dispatched worker must not use it to route around Orca's nesting limit.

### Agent skill

An installable orchestrate agent skill is planned but is not included in this milestone. There are no hidden edits to `AGENTS.md` or another agent's instruction files.

### WSL

The thin WSL launcher is not implemented or certified yet. Until it exists, this repository makes no claim that Windows coordinator state, Linux path forwarding, or WSL exit-code behavior works.

## The Basic Workflow

Run the read-only diagnostics first:

```powershell
orchestrate doctor
orchestrate doctor --json
```

From a fresh, otherwise unused root Orca terminal, create the disposable compatibility Run. Do not replace the Run binding of a coordinator that is supervising other work:

```powershell
orchestrate doctor --active-run-probe --json
```

Copy the returned Run ID into a fresh process to test resume and the real completion path:

```powershell
orchestrate doctor --active-worker-probe <run-id> --json
```

These active commands create Orca coordination records and may launch a real Codex worker. They are compatibility probes, not implementation commands. The first live worker-start exercise stalled at prompt delivery; do not rerun it unchanged without new evidence or an explicit recovery decision.

## What's Inside

- A Python 3.13 package using the standard `src` layout.
- A strict standard-library subprocess client for the public Orca CLI.
- Read-only compatibility diagnostics with human and JSON output.
- Explicit Run-create and coordinator-only worker completion probes.
- Focused unit tests for executable resolution, JSON contract failures, resume ordering, release-before-ack, and unacknowledged intervention.
- A sanitized compatibility record that keeps live runtime identities out of the repository.

Everything after this foundation remains unimplemented while the first live completion path is unresolved. There is no project profile, SQLite bookkeeping, task preparation, reader, packet builder, WSL launcher, provider adapter, or dashboard in this milestone.

## Philosophy

- **Orca owns lifecycle** — Runs, Tasks, Dispatches, environments, workers, and UI stay native to Orca.
- **Authority before action** — a process without authenticated terminal authority fails closed.
- **Evidence over inference** — inherited agent authority is not standalone-host proof, and synthetic tests are not live completion evidence.
- **One controller, no shadow scheduler** — orchestrate prepares and supervises; it does not recreate Orca's task system.
- **Unknown stays unknown** — unavailable WSL, hosted, model, and provider gates remain `NOT_RUN`.

## Contributing

Read [AGENTS.md](AGENTS.md) and the [approved implementation plan](docs/implementation-plan.md) before changing code. Preserve unrelated work and keep active probes disposable.

Run the focused checks with Python 3.13:

```powershell
$env:PYTHONPATH = (Resolve-Path .\src).Path
py -3.13 -m unittest discover -s tests -v
py -3.13 -m compileall -q src tests
```

Live Orca probes are separate from unit tests. Do not run the worker probe from a dispatched worker, do not pass another terminal through `--from`, and do not publish from this repository during the current milestone.

## Updating

Pull the desired reviewed revision, then reinstall the editable package in the same virtual environment:

```powershell
git pull --ff-only
.\.venv\Scripts\python -m pip install --editable .
```

Re-run `orchestrate doctor` after Orca upgrades because the public CLI contract and capabilities are live compatibility inputs.

## License

orchestrate is available under the MIT License. See [LICENSE](LICENSE).
