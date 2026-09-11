# orchestrate

orchestrate gives governed software work a small, restartable controller without adding a second scheduler.

You describe one authorized objective. orchestrate reads the project entrypoints, binds the exact working candidate, prepares compact packets, and starts with one implementation owner. When a bounded milestone has genuinely independent follow-up work, its coordination layer creates Orca-native dependencies and gates instead of building a second scheduler. Orca still owns the Run, Tasks, Dispatches, workers, terminals, and mailbox. Your project still owns its checks, review, acceptance, merge, and release.

The ordinary path is deliberately single-owner-first. When a reviewed milestone really does need specialist and review waves, orchestrate uses a bounded plan and Orca's own lifecycle instead of making parallelism the default.

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

The project keeps one small tracked profile and one host-local database.

First, `orchestrate setup` discovers familiar project entrypoints without running hooks, checks, or models. Conventional `AGENTS.md` and `CLAUDE.md` discovery is a single case-sensitive Git pathspec-filtered inventory: at most 256 tracked and ordinary untracked matches at any depth, with strict UTF-8 and byte limits. Ignored dependency trees do not become authority by accident. An ignored or non-conventional instruction participates only when you select its repository-relative path in `instructions`, review the profile, and run `setup --acknowledge-profile`.

Next, `orchestrate implement` reads the selected sources, binds the exact candidate, and prepares a compact worker packet. Dirty paths it was not authorized to read remain opaque and hold dispatch. The worker must pass a Dispatch-bound preflight before its result can count.

From there, Orca stays in charge of Runs, Tasks, Dispatches, workers, gates, and messages. orchestrate journals mutation intentions and whole Deliveries, checks exact public readbacks, and stops on uncertain effects instead of guessing or retrying blindly. `status`, `explain`, and `resume` use that record after an interruption.

The default workflow has one implementation owner. An explicitly selected milestone plan can add independent verification and review work in deterministic, capacity-bounded waves. Worker success, accepted verification, independent review, hosted proof, project acceptance, merge, and release keep separate labels throughout.

Read [Execution contracts and recovery](docs/contracts-and-recovery.md) for the detailed state machine and [Validation evidence](docs/validation.md) for the evidence matrix.

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

The CI workflow is configured to run unit, incident, wheel-build, and isolated-install checks on Windows and Linux. A workflow definition is not proof that a particular candidate passed; [Validation evidence](docs/validation.md) keeps that distinction explicit. The Orca CLI resolver uses `ORCA_CLI_COMMAND` in a managed forwarded session, `orca-dev` in a dev checkout, `orca-ide` on Linux outside Orca, and `orca` on packaged Windows.

`orchestrate-wsl` is a transport-only launcher. Configure its one machine-local pointer as a JSON argument array naming the reviewed Windows entry point, then pass ordinary CLI arguments:

```bash
export ORCHESTRATE_WINDOWS_COMMAND_JSON='["/mnt/c/path/to/windows/.venv/Scripts/orchestrate.exe"]'
orchestrate-wsl status --json
```

It forwards the exact `WSL_DISTRO_NAME`, absolute Linux working directory, Unicode-safe argument array, inherited streams, and exit code to the Windows receiver. It creates no Linux state directory; the Windows process remains the only state owner. The payload does not choose a worker host, translate an Orca recovery command, or replace Orca placement and lifecycle. A real Windows-coordinated WSL worker lifecycle is still `NOT_RUN` for this candidate, so Linux unit or CI success is not a WSL/provider claim.

## The Basic Workflow

Start in the project you want to configure:

```powershell
orchestrate setup --json
```

Review `.orchestrate.json`, especially its instruction and task entrypoints, required checks, and any exact dirty paths you deliberately add to `candidateSources`. The initial setup invocation selects the generated bytes operationally. If you later edit the profile, review the diff and explicitly update only this tool's host-local selection:

```powershell
orchestrate setup --acknowledge-profile --json
```

Committing the file is ordinary candidate history, not a configuration acknowledgment. Then give one concrete, already-authorized objective:

```powershell
orchestrate implement "Fix the parser regression and run the profile checks" --json
```

That command keeps the one-owner default. For an already-authorized bounded milestone, pass a reviewed, tracked `orchestrate-milestone-plan/v1` file explicitly:

```powershell
orchestrate implement "Integrate the exact bounded milestone" --plan milestone-plan.json --json
```

The plan names the exact owner objective, specialist outputs, final reviewer, dependencies, gate kinds, settled shared-contract value, and worker limit. orchestrate does not infer that decomposition from objective prose. Follow-up packets bind each native Task to the post-owner candidate and contract digests and name a host-local result-file contract; only an exact admitted, natively settled worker plus an `accepted` result can satisfy verification or review. Resume may reassert the same path with `--plan`, but cannot select a different plan for the Run. The complete strict JSON shape and recovery rules are in [Execution contracts and recovery](docs/contracts-and-recovery.md).

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

Omitting an objective resumes only when one local Run is unambiguous. Multiple Runs always require an explicit selection. A succeeded default owner leaves verification pending by design; a planned milestone reaches `review_accepted` only after every planned verification result and the exact final review are accepted.

For a copyable disposable live exercise, use [Live first-increment exercise](docs/live-first-increment.md).

## What's Inside

- A Python 3.13 standard-library CLI in a conventional `src` layout.
- A small `orchestrate-profile/v1` project profile.
- Repository and CE task-registry/context/manifest-first reader boundaries.
- Exact Git/source identities and one immutable `orchestrate-worker-packet/v3` packet per Task.
- One immutable managed worker-preflight observation, joined to the native launch before worker claims can count.
- Strict current-Delivery wire parsing with Dispatch-bound question senders and exact worker-terminal lifecycle senders.
- Host-local SQLite intentions, Deliveries, questions, evidence, controller locks, and a separate admission/effect fence.
- A host-local operational-profile selection history that cannot live inside the project or a recognized synchronized root.
- Deterministic Run creation, Task creation, one-worker launch, supervision, answer, release, acknowledgment, and resume.
- An explicitly selected bounded native-DAG path for one integration owner, settled shared contracts, independent specialists, journaled native verification/review gates, exact result artifacts, and stale-review invalidation.
- Host-local owner/specialist/reviewer launch choices with explicit Claude provider IDs and requested/effective receipt validation.
- Exact-terminal reuse or Dispatch release decisions, including fail-closed `release_unknown` containment.
- Durable intervention records and one bounded diagnosis before an unchanged correction can repeat without new evidence.
- A state-free `orchestrate-wsl` argument/exit forwarding boundary to the canonical Windows installation.
- Non-consuming `status`, model-free `explain`, and exact `packet` output.
- A focused ordinary-terminal bootstrap with durable result, exit, and exact uncertain-close reconciliation receipts.
- Passive compatibility diagnostics and a separately contained no-edit active probe.
- Synthetic practical regressions under `tests/incidents`, including ignored dependency authority, Task-history tampering, and capacity waves.
- A Windows/Linux CI workflow for unit, explicit incident-discovery, build, and isolated-install smoke checks.
- A sanitized evidence matrix in [`docs/validation.md`](docs/validation.md).

Detailed role, dependency, review-invalidation, intervention, and uncertain-release shapes live in [Execution contracts and recovery](docs/contracts-and-recovery.md).

There is no parallel task database, worktree manager, provider API, policy engine, automatic retry campaign, dashboard, PyPI release, or governance replacement.

## Philosophy

- **Orca owns lifecycle.** Runs, Tasks, Dispatches, workers, environments, and UI remain native.
- **Projects own authority.** A candidate policy edit cannot grant itself more power.
- **One writer first.** Parallelism waits until work is independently useful and shared interfaces are settled.
- **Admission is provenance, not a sandbox.** Managed workers prove exact preflight identity; hostile shell bypass and unrelated external writers remain outside this guarantee.
- **Effects are replayed, not guessed.** Unknown external effects stop repetition.
- **Every message counts.** FIFO Deliveries are processed in full and acknowledged as a whole.
- **Accepted input is not invented progress.** An unproven turn start becomes a bounded diagnostic, never duplicate input.
- **Evidence keeps its label.** Worker success, local verification, hosted proof, independent review, acceptance, merge, and release are different things.
- **WIP is a candidate, not clutter.** Dirty status is bound opaquely; only operationally selected or reader-consulted paths are read and hashed, and uncovered paths hold dispatch.

## Contributing

Read [AGENTS.md](AGENTS.md) and the [approved implementation plan](docs/implementation-plan.md) before changing code. The tracked plan remains until final project completion.

Run the repository gates with Python 3.13:

```powershell
$env:PYTHONPATH = (Resolve-Path .\src).Path
py -3.13 -m unittest discover -s tests -v
py -3.13 -m unittest discover -s tests/incidents -t tests -v
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

Live Orca exercises are separate. Use only disposable projects for mutations, never run them from a dispatched worker, and keep CE/application trials read-only. Record each gate using the fields and boundaries in [Validation evidence](docs/validation.md); a local pass does not fill a hosted or live row.

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
