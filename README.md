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

First, `orchestrate setup` discovers familiar project entrypoints without running hooks, checks, or models. Conventional `AGENTS.md` and `CLAUDE.md` discovery is a single case-sensitive Git pathspec-filtered inventory: at most 256 tracked and ordinary untracked matches at any depth, with strict UTF-8 and byte limits. Ignored dependency trees do not become authority by accident. Any selected instruction outside the current conventional inventory—including one hidden by a later ignore rule—holds before its bytes are read until you review the profile and run `setup --acknowledge-profile`.

Next, `orchestrate implement` reads the selected sources, binds the exact candidate, and prepares a compact worker packet. Dirty paths it was not authorized to read remain opaque and hold dispatch. The worker must pass a Dispatch-bound preflight before its result can count.

From there, Orca stays in charge of Runs, Tasks, Dispatches, workers, gates, and messages. orchestrate journals mutation intentions and whole Deliveries, checks exact public readbacks, and stops on uncertain effects instead of guessing or retrying blindly. `status`, `explain`, and `resume` use that record after an interruption.

The default workflow has one implementation owner. An explicitly selected milestone plan can add independent verification and review work in deterministic, capacity-bounded waves. Capacity belongs to one Run: plans default to two workers, ordinary limits are one through three, and limits four through eight need an explicit reasoned grant for that exact plan. Worker success, accepted verification, independent review, hosted proof, project acceptance, merge, and release keep separate labels throughout.

Read [Execution contracts and recovery](docs/contracts-and-recovery.md) for the detailed state machine and [Validation evidence](docs/validation.md) for the evidence matrix.

## Installation

orchestrate requires Python 3.13 or newer and a reviewed checkout. It is not published to PyPI. You do not need to create a virtual environment, run pip, or edit PATH yourself.

### Windows

From the reviewed orchestrate checkout, use its small first-entry script and name the project you want to configure:

```powershell
py -3.13 .\bootstrap.py setup --project C:\path\to\your-project --json
```

That one command verifies Python 3.13 and retains a no-follow component chain while it creates installation ancestry. Later installation-root directory creation and file writes stay anchored to that same retained root; POSIX test providers mutate through parent directory descriptors, while native Windows retains a no-delete-shared handle for every component. Child phases receive a verified working directory and bound effect inputs, and a bounded whole-tree scan after each phase rejects redirects that remain. This is containment of orchestrate's paths and acceptance checks, not a sandbox for a child process that deliberately accesses some other absolute path. The command builds a bounded archive from the exact reviewed source bytes and gives that content-bound input—not the mutable checkout pathname—to pip. It then installs the Windows and WSL-facing launchers and puts `%LOCALAPPDATA%\orchestrate\bin` first on the **user** PATH. Windows holds the interpreter and source archive for actual read access without write/delete sharing, passes the interpreter's exact path as the process application name, and checks the same identities before and after every effect; the synthetic provider models read/write/delete share admission. It then continues the requested project setup. It never edits the system PATH and never falls through to another Python environment.

Open a new terminal after the first run. The normal command is now available:

```powershell
orchestrate doctor
orchestrate setup --project C:\path\to\another-project --json
```

Every later `setup` begins with one user-PATH lookup, one command lookup, fixed launcher/receipt reads, and a bounded identity scan of the reviewed checkout. The installed package first loads the reviewed checkout path from the canonical v5 receipt, verifies the separate host-local anchor that authenticates that receipt path and source-tree digest together, and only then opens the recorded checkout; it never guesses from the installed package's `site-packages` location. The private v5 receipt binds its canonical payload, checkout tree, exact source archive, environment, and installed command. The archive builder computes the same framed tree digest from the bytes it writes and refuses any transient mismatch. Every Windows named stage remains a normal addressable node through commit; abandoned stages are closed and explicitly unlinked. Its retained commit record carries exact identity, size, digest, and change token into the archive binding; native Windows retains the archive temporary until a delete-sharing bridge can close the writer, remove only that name, and open the strict read pin without an identity gap. Non-native tests use a sealed `memfd` when available; the named fallback is checked before and after pip for bytes, identity, mode, and change token, and a mutation prevents the install from being receipted even if the bytes are restored. A separate canonical anchor under `%LOCALAPPDATA%\orchestrate-state` binds that receipt digest, source/archive digest, installation nonce, and command digest outside the checkout; its complete directory ancestry is physically pinned and redirected ancestors are refused. Recomputing only checkout-controlled receipt bytes therefore cannot launder a stale command or source path. When the checks are healthy setup performs no environment creation, pip command, install, upgrade, write, or model call and prints no bootstrap message. Missing launchers can be created, while a mismatching existing launcher is preserved and refused for inspection. PATH repair preserves the existing `REG_SZ` or `REG_EXPAND_SZ` kind and unrelated entry text through one transactional compare-and-replace plus read-back. The healthy path requires the one dedicated PATH entry to be first. Concurrent setup commands share one machine-bootstrap lock, so source installation is performed once and the follower reuses it.

Receipt recording preserves the original proof component and expected/observed identity payload when it wraps an anchor-write failure.

Diagnostic values labeled `redacted-string-id:sha256:` identify redacted arbitrary strings. Only a bare `sha256:` value in an explicit digest component represents a content digest.

If bootstrap stops, its error names the failed phase and identity failures report the expected and observed proof component. Correct the reported Python, filesystem, pip/network, or user-registry problem and run the same checkout command again. Safe completed phases are reused; an unreceipted command or source archive left by an interrupted installer is deliberately not guessed to be owned. An incomplete, redirected, or identity-mismatched dedicated venv is never silently overwritten—move that one `%LOCALAPPDATA%\orchestrate\venv` directory aside after inspection, then rerun. Likewise, inspect and move aside an unrecognized receipt, command, anchor, archive, or launcher rather than asking bootstrap to overwrite it. Every included file in the recorded checkout uses the same recovery rule: a temporary access, sharing, or I/O failure is retryable and performs no repair, while a moved, removed, structurally replaced, or updated source is definitive and never falls through to a silent reinstall or fresh source grant. Present files are read through proved handles, and their relative names, executable bits, and exact bytes form the aggregate source digest authenticated by the anchor. When the initial ready scan establishes either classification—including from that aggregate digest—it returns the failure before installation-parent or installation-root preparation and before mutation-lock construction or acquisition. For a retryable result, correct the temporary problem and rerun the installed command; after reviewing a definitive change, move aside `%LOCALAPPDATA%\orchestrate\install.json`, `%LOCALAPPDATA%\orchestrate\installed-source.zip`, `%LOCALAPPDATA%\orchestrate\venv\Scripts\orchestrate.exe`, and `%LOCALAPPDATA%\orchestrate-state\machine-install.json`, then rerun that checkout's `bootstrap.py` first-entry path. The source scan excludes only `.git`, `.venv`, `venv`, names beginning `.venv-` or `venv-`, names ending `-venv` or `.egg-info`, `__pycache__`, `.pytest_cache`, `build`, `dist`, and files ending `.pyc` or `.pyo`. `doctor` is read-only unless you explicitly select an active probe; the active compatibility probe has additional disposable-project requirements and is not needed for normal work.

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

The CI workflow runs the complete unit and incident suite on Windows, including the managed-admission and controller-lifecycle fixtures that require native Windows. Linux runs every host-neutral test and explicitly skips only tests whose intended assertion requires the win32-only managed worker admission path. Both jobs run explicit incident discovery, wheel build, isolated install, and CLI help smokes. A workflow definition is not proof that a particular candidate passed; [Validation evidence](docs/validation.md) keeps that distinction explicit. The Orca CLI resolver uses `ORCA_CLI_COMMAND` in a managed forwarded session, `orca-dev` in a dev checkout, `orca-ide` on Linux outside Orca, and `orca` on packaged Windows.

Machine bootstrap places an extensionless `orchestrate` shell launcher beside the Windows shim. With WSL's normal Windows-PATH import enabled, it is available in a new WSL shell without a Linux installation:

```bash
orchestrate status --json
```

The launcher uses WSL's `python3` only to encode the bounded transport payload. It forwards the exact `WSL_DISTRO_NAME`, absolute Linux working directory, Unicode-safe argument array, inherited streams, and exit code to the Windows executable. It installs no Linux package and creates no Linux state directory; Windows remains the only installation and state owner. Environments that deliberately disable Windows-PATH import must expose the mounted `%LOCALAPPDATA%\orchestrate\bin` directory through their own WSL policy; bootstrap does not edit shell startup files.

The packaged `orchestrate-wsl` entry point remains the same transport implementation for already configured environments using `ORCHESTRATE_WINDOWS_COMMAND_JSON`. Neither launcher chooses a worker host, translates an Orca recovery command, or replaces Orca placement and lifecycle. A real Windows PATH bootstrap and Windows-coordinated WSL lifecycle are still `NOT_RUN` for this candidate, so synthetic or Linux test success is not a live Windows/WSL claim.

## The Basic Workflow

After the one-time checkout entry above, start in any project you want to configure:

```powershell
orchestrate setup --json
```

Review `.orchestrate.json`, especially its instruction and task entrypoints, required checks, and any exact dirty paths you deliberately add to `candidateSources`. The initial setup invocation selects the generated bytes operationally. If you later edit the profile, review the diff and explicitly update only this tool's host-local selection:

```powershell
orchestrate setup --acknowledge-profile --json
```

Committing the file is ordinary candidate history, not a configuration acknowledgment. The same acknowledgment command records reviewed provenance when the selected profile bytes are unchanged, which is the cure when an existing selected instruction becomes ignored. Then give one concrete, already-authorized objective:

```powershell
orchestrate implement "Fix the parser regression and run the profile checks" --json
```

That command keeps the one-owner default. For an already-authorized bounded milestone, pass a reviewed, tracked `orchestrate-milestone-plan/v1` file explicitly:

```powershell
orchestrate implement "Integrate the exact bounded milestone" --plan milestone-plan.json --json
```

The plan names the exact owner objective, specialist outputs, final reviewer, dependencies, gate kinds, settled shared-contract value, and worker limit. orchestrate does not infer that decomposition from objective prose. Follow-up packets bind each native Task to the post-owner candidate and contract digests and name a host-local result-file contract; only an exact admitted, natively settled worker plus an `accepted` result can satisfy verification or review. Resume may reassert the same path with `--plan`, but cannot select a different plan for the Run. The complete strict JSON shape and recovery rules are in [Execution contracts and recovery](docs/contracts-and-recovery.md).

`maxWorkers` may be omitted for the default of two. Values one through three need no extra flag. A reviewed plan that genuinely needs four through eight records its operational grant before the first launch:

```powershell
orchestrate implement "Integrate the exact bounded milestone" --plan milestone-plan.json `
  --allow-exceptional-capacity --capacity-reason "Five independent platform fixtures" --json
```

If that command stops before launch, `resume` accepts the same two flags and binds the grant to the existing Run and exact plan digest. A grant never transfers to a changed plan. Uncertain launches and unresolved releases keep occupying their slots; orchestrate does not launch a replacement merely because local completion was observed.

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

`status` and `explain` include an `efficiency` object with the effective Run capacity, any exceptional grant, elapsed wall time, observed session counts and durations, repeated launch attempts, and intervention state. Session duration is observed wall time, not provider compute time. Historical counts, tokens, model turns, and enclosing-coordinator usage stay `unknown` when orchestrate did not observe them; only the deterministic controller's own instrumented model-call count is reported as zero.

When a correction would repeat without new evidence, record the operator's exact proposal instead of starting another Dispatch:

```powershell
orchestrate intervention --run <run-id> --task <task-id> --record intervention.json --json
orchestrate intervention --run <run-id> --task <task-id> --diagnosis diagnosis.json --json
```

The record contains exactly `obligation`, `failing_example`, `hypothesis`, `last_meaningful_evidence`, `next_discriminating_check`, and `correction_key`. A diagnosis file contains exactly `diagnosis_evidence`, as a string when it found new evidence or `null` when it did not. Correction and diagnosis evidence identities remain distinct, so replaying either consumed identity cannot authorize another correction; only genuinely different later evidence can. These commands update only the bounded host-local ledger: they do not create a Dispatch, clear admission, retry work, or override project governance.

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
- Fresh agent sessions for every new Task, while answers and recovery remain on the existing Dispatch session; settled sessions are released with fail-closed `release_unknown` containment.
- Durable intervention records and one bounded diagnosis before an unchanged correction can repeat without new evidence.
- Per-Run capacity grants, conservative occupied-slot accounting, and additive efficiency observations with stable event identities.
- A state-free `orchestrate-wsl` argument/exit forwarding boundary to the canonical Windows installation.
- Non-consuming `status`, model-free `explain`, and exact `packet` output.
- A focused ordinary-terminal bootstrap with durable result, exit, and exact uncertain-close reconciliation receipts.
- Passive compatibility diagnostics and a separately contained no-edit active probe.
- Synthetic practical regressions under `tests/incidents`, including ignored dependency authority, Task-history tampering, and capacity waves.
- A CI workflow with the complete suite on Windows, the host-neutral subset on Linux, and explicit incident-discovery, build, and isolated-install smoke checks on both.
- A sanitized evidence matrix in [`docs/validation.md`](docs/validation.md).

Detailed role, dependency, review-invalidation, intervention, and uncertain-release shapes live in [Execution contracts and recovery](docs/contracts-and-recovery.md).

There is no parallel task database, worktree manager, provider API, policy engine, automatic retry campaign, dashboard, PyPI release, or governance replacement.

## Philosophy

- **Orca owns lifecycle.** Runs, Tasks, Dispatches, workers, environments, and UI remain native.
- **Projects own authority.** A candidate policy edit cannot grant itself more power.
- **One writer first.** Parallelism waits until work is independently useful and shared interfaces are settled.
- **Fresh Task, fresh session.** A same-agent match never carries context across a new Task; long-lived sessions are reported at two hours and require a fresh-session handoff at eight.
- **Admission is provenance, not a sandbox.** Managed workers prove exact preflight identity; hostile shell bypass and unrelated external writers remain outside this guarantee.
- **Effects are replayed, not guessed.** Unknown external effects stop repetition.
- **Every message counts.** FIFO Deliveries are processed in full and acknowledged as a whole.
- **Accepted input is not invented progress.** An unproven turn start becomes a bounded diagnostic, never duplicate input.
- **Evidence keeps its label.** Worker success, local verification, hosted proof, independent review, acceptance, merge, and release are different things.
- **WIP is a candidate, not clutter.** Dirty status is bound opaquely; only operationally selected or reader-consulted paths are read and hashed, and uncovered paths hold dispatch.

## Contributing

Read [AGENTS.md](AGENTS.md) before changing code. The approved implementation plan was archived outside this repository at final project completion.

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

Pull a reviewed revision in the same checkout. The source-bound receipt will refuse to bless changed checkout bytes automatically. After review, move aside the prior receipt, pinned archive, installed command, and host-local anchor, then let the checkout entry install a new reviewed-source archive:

```powershell
git pull --ff-only
Move-Item "$env:LOCALAPPDATA\orchestrate\install.json" "$env:LOCALAPPDATA\orchestrate\install.json.reviewed-old"
Move-Item "$env:LOCALAPPDATA\orchestrate\installed-source.zip" "$env:LOCALAPPDATA\orchestrate\installed-source.zip.reviewed-old"
Move-Item "$env:LOCALAPPDATA\orchestrate\venv\Scripts\orchestrate.exe" "$env:LOCALAPPDATA\orchestrate\venv\Scripts\orchestrate.exe.reviewed-old"
Move-Item "$env:LOCALAPPDATA\orchestrate-state\machine-install.json" "$env:LOCALAPPDATA\orchestrate-state\machine-install.json.reviewed-old"
py -3.13 .\bootstrap.py setup --project C:\path\to\your-project --json
orchestrate doctor
```

Re-run `doctor` after an Orca update. Public contracts and effective launch behavior are live compatibility inputs, not assumptions frozen into this repository.

## License

orchestrate is available under the MIT License. See [LICENSE](LICENSE).
