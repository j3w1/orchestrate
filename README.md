# orchestrate

orchestrate gives governed software work a small, restartable controller without giving it a second scheduler.

You describe one authorized objective. orchestrate reads the project entrypoints, binds the exact working candidate, prepares a compact packet, and asks Orca to run one implementation owner. Orca still owns the Run, Task, Dispatch, worker, terminal, and mailbox. Your project still owns its checks, review, acceptance, merge, and release.

The first increment is deliberately single-worker. It is enough to execute real work and survive a controller interruption, while keeping every larger claim honest.

## Table of Contents

- [How it works](#how-it-works)
- [Installation](#installation)
  - [Windows](#windows)
  - [Agent skill](#agent-skill)
  - [Linux and WSL](#linux-and-wsl)
- [The Basic Workflow](#the-basic-workflow)
- [What's Inside](#whats-inside)
- [Philosophy](#philosophy)
- [Contributing](#contributing)
- [Updating](#updating)
- [License](#license)

## How it works

The project keeps one small tracked file and one host-local database.

`.orchestrate.json` points at instructions, task sources, command manifests, optional approved `candidateSources`, and checks. `orchestrate setup` discovers the conventional entrypoint names but runs nothing. Before dispatch, the reader identifies the exact non-secret bytes it consulted, including relevant staged bytes, while Git status remains an opaque candidate identity. Any dirty path outside that approved coverage produces a hold instead of being opened speculatively. The packet sent to the worker carries those bindings rather than a vague description of HEAD.

Every Orca mutation starts as a durable intention. Every Delivery is journaled whole before its messages have effects. Completion is accepted only for the exact Task and Dispatch, release is settled before acknowledgment, and missing request history stays uncertain.

Mutating commands need an ordinary Orca controller terminal. If you start from an outside PowerShell, orchestrate opens one in the exact workspace, keeps the command in the foreground, forwards Ctrl-C, reproduces the child result and exit code, and closes that dedicated tab. It refuses to use a reasoning-agent terminal as a dispatch-depth shortcut.

Read [First-increment contracts and recovery](docs/contracts-and-recovery.md) for the detailed state machine.

## Installation

orchestrate requires Python 3.13 or newer. It is not published to PyPI.

### Windows

Install a reviewed checkout into a virtual environment:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python -m pip install --editable .
.\.venv\Scripts\orchestrate doctor
```

`doctor` is read-only unless you explicitly select an active probe. The active compatibility probe has additional disposable-project requirements and is not needed for normal work.

### Agent skill

The short skill lives at [`skills/orchestrate/SKILL.md`](skills/orchestrate/SKILL.md). Install it explicitly after reviewing it:

```powershell
$skill = Join-Path $env:USERPROFILE ".codex\skills\orchestrate"
New-Item -ItemType Directory -Force $skill | Out-Null
Copy-Item -Recurse -Force .\skills\orchestrate\* $skill
```

On Linux, use the same explicit copy boundary:

```bash
skill_root="${CODEX_HOME:-$HOME/.codex}/skills/orchestrate"
mkdir -p "$skill_root"
cp -R skills/orchestrate/. "$skill_root/"
```

The package never edits `AGENTS.md`, user prompts, or another agent's configuration behind your back.

### Linux and WSL

Unit, incident, wheel-build, and isolated-install checks run on Linux CI. The Orca CLI resolver uses `ORCA_CLI_COMMAND` in a managed forwarded session, `orca-dev` in a dev checkout, `orca-ide` on Linux outside Orca, and `orca` on packaged Windows.

The canonical Windows controller and a real WSL-native worker lifecycle are not certified by this first increment. Do not turn Linux CI into a WSL/provider claim. The planned WSL launcher remains a later increment.

## The Basic Workflow

Start in the project you want to configure:

```powershell
orchestrate setup --json
```

Review `.orchestrate.json`, especially its instruction and task entrypoints, required checks, and any exact dirty paths you deliberately add to `candidateSources`. Then give one concrete, already-authorized objective:

```powershell
orchestrate implement "Fix the parser regression and run the profile checks" --json
```

The command waits in the foreground. Ctrl-C stops controller waiting; it does not pretend the active worker stopped. Continue with the Run ID:

```powershell
orchestrate status --run <run-id> --json
orchestrate explain --run <run-id>
orchestrate resume --run <run-id> --json
```

When a worker asks a question, answer that exact message and resume:

```powershell
orchestrate answer --run <run-id> --question <message-id> --text "Use the existing public interface" --json
orchestrate resume --run <run-id> --json
```

Inspect the immutable Task packet without consuming mail or calling a model:

```powershell
orchestrate packet --run <run-id> --task <task-id> --json
```

Omitting an objective resumes only when one local Run is unambiguous. Multiple Runs always require an explicit selection. A succeeded worker leaves verification pending by design.

For a copyable disposable live exercise, use [Live first-increment exercise](docs/live-first-increment.md).

## What's Inside

- A Python 3.13 standard-library CLI in a conventional `src` layout.
- A small `orchestrate-profile/v1` project profile.
- Repository and CE task-registry/context/manifest-first reader boundaries.
- Exact Git/source identities and `orchestrate-worker-packet/v1` packets.
- Host-local SQLite intentions, Deliveries, questions, evidence, and OS locks.
- Deterministic Run creation, Task creation, one-worker launch, supervision, answer, release, acknowledgment, and resume.
- Non-consuming `status`, model-free `explain`, and exact `packet` output.
- A focused ordinary-terminal bootstrap with durable result and exit receipts.
- Passive compatibility diagnostics and a separately contained no-edit active probe.
- Windows/Linux unit, incident, build, and install-smoke CI.

There is no parallel task database, worktree manager, provider API, policy engine, automatic retry campaign, dashboard, PyPI release, or governance replacement.

## Philosophy

- **Orca owns lifecycle.** Runs, Tasks, Dispatches, workers, environments, and UI remain native.
- **Projects own authority.** A candidate policy edit cannot grant itself more power.
- **One writer first.** Parallelism waits until work is independently useful and shared interfaces are settled.
- **Effects are replayed, not guessed.** Unknown external effects stop repetition.
- **Every message counts.** FIFO Deliveries are processed in full and acknowledged as a whole.
- **Evidence keeps its label.** Worker success, local verification, hosted proof, independent review, acceptance, merge, and release are different things.
- **WIP is a candidate, not clutter.** Dirty status is bound opaquely; only explicitly approved, consulted paths are read and hashed, and uncovered paths hold dispatch.

## Contributing

Read [AGENTS.md](AGENTS.md) and the [approved implementation plan](docs/implementation-plan.md) before changing code. The tracked plan remains until final project completion.

Run the first-increment gates with Python 3.13:

```powershell
$env:PYTHONPATH = (Resolve-Path .\src).Path
py -3.13 -m unittest discover -s tests -v
py -3.13 -m compileall -q src tests
git diff --check
```

Build and install into an isolated environment for the packaging smoke test:

```powershell
py -3.13 -m pip install build
py -3.13 -m build
py -3.13 -m venv $env:TEMP\orchestrate-smoke
& $env:TEMP\orchestrate-smoke\Scripts\python -m pip install (Get-ChildItem .\dist\*.whl | Select-Object -First 1)
& $env:TEMP\orchestrate-smoke\Scripts\orchestrate --help
```

Live Orca exercises are separate. Use only disposable projects for mutations, never run them from a dispatched worker, and keep CE/application trials read-only.

## Updating

Pull the reviewed revision and reinstall it in the same environment:

```powershell
git pull --ff-only
.\.venv\Scripts\python -m pip install --editable .
orchestrate doctor
```

Re-run `doctor` after an Orca update. Public contracts and effective launch behavior are live compatibility inputs, not assumptions frozen into this repository.

## License

orchestrate is available under the MIT License. See [LICENSE](LICENSE).
