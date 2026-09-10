# orchestrate

orchestrate gives governed software work a small, restartable controller without giving it a second scheduler.

You describe one authorized objective. orchestrate reads the project entrypoints, binds the exact working candidate, prepares compact packets, and starts with one implementation owner. When a bounded milestone has genuinely independent follow-up work, its coordination layer creates Orca-native dependencies and gates instead of building a second scheduler. Orca still owns the Run, Tasks, Dispatches, workers, terminals, and mailbox. Your project still owns its checks, review, acceptance, merge, and release.

The default command path remains deliberately single-owner-first. The second increment adds the reusable native-DAG, role, session, intervention, and WSL forwarding boundaries needed for bounded specialist and review waves without turning parallelism into a default.

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

`.orchestrate.json` points at instructions, task sources, command manifests, optional selected `candidateSources`, and checks. The explicitly invoked initial `orchestrate setup` discovers conventional entrypoint names, runs nothing, and records that exact profile as a host-local operational selection outside the candidate. Committing a later profile edit does not select it and cannot grant itself new sources, checks, or reader behavior; the controller holds until the operator reviews the change and uses the explicit acknowledgment option. Before dispatch, the reader inventories bounded tracked, untracked, and ignored conventional instruction files and identifies the exact non-secret bytes it consulted, including relevant staged bytes, while Git status remains an opaque candidate identity. Any dirty path outside that selected coverage produces a hold instead of being opened speculatively. The packet sent to the worker carries those bindings rather than a vague description of HEAD. Project instructions and governance still decide whether the work itself is authorized and accepted.

Every Orca orchestration mutation starts as a durable intention. Exact native request receipts, Task/Run readback, requested-versus-effective launch values, worker placement, and structured release state are checked before the local state advances. A dedicated host-local admission/effect fence orders managed worker-preflight evidence against reply, Delivery, retry, release, and acknowledgement intervals without blocking the first preflight behind the controller's broader Run lock. Every Delivery is journaled whole before its messages have effects. Current Orca lifecycle rows retain their raw JSON-string payloads in that journal, then undergo strict Run, Task, Dispatch, sender, and alias validation before any effect. Completion is accepted only for the exact Task and Dispatch, release is settled before acknowledgment, and missing request history stays uncertain.

Bounded multi-worker plans have exactly one implementation/integration owner. A draft shared contract admits only that serialized owner wave; the snapshot must be settled and bound before specialist Tasks or parallel dispatch. Only the owner may write the contract. Specialist Tasks must be independently useful, verification and review use journaled Orca-native gates, and an accepted review becomes stale as soon as either the candidate or contract digest changes. A worker success without the exact accepted result artifact cannot open the review gate. Settled terminals have one explicit next action: immediate same-agent reuse by exact handle and captured worktree, or Orca-native release by exact Dispatch. `release_pending` and the observed WSL `release_unknown` shape preserve Orca's literal recovery action and prohibit an unchanged second release.

The host-local role roster has owner, specialist, and independent-reviewer slots. Defaults remain Codex/Sol; an optional Claude slot must carry a user-supplied provider model ID. The tool never probes provider APIs or guesses that a configured model exists—each launch still has to prove that Orca's requested and effective values are identical.

`worker-start` reporting `ready` at `input_accepted` proves accepted input, not a started model turn. If the managed preflight still has not appeared when foreground observation ends, orchestrate performs one exact read-only `worker-show`, records an unresolved compatibility diagnostic, and returns without resending the task text, pressing a UI key, launching a replacement, or claiming failure.

Mutating commands need an ordinary Orca controller terminal. Native terminal, worktree, Dispatch, and current-Run readback distinguishes that caller from an agent; hook credentials alone are not treated as agent identity. If you start from an outside PowerShell, orchestrate opens one in the exact workspace, journals each terminal lifecycle intention, keeps the command in the foreground, forwards Ctrl-C, reproduces the child result and exit code, and closes that dedicated tab. A lost create, interrupt, or close response without a public recovery identity stays uncertain and cannot launch a replacement. It refuses to use a reasoning-agent terminal as a dispatch-depth shortcut.

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
- Windows/Linux unit, incident, build, and install-smoke CI.

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
