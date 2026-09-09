# Approved implementation plan

## Product and delivery

Build all three increments of an Orca-native execution layer in Python 3.13+. Publish source to public GitHub repository j3w1/orchestrate under MIT. One canonical Windows coordinator serves Windows and Arch Linux WSL workers; a thin WSL launcher forwards arguments and project identity without its own state. SSH, macOS and connected-server live certification are deferred. No PyPI publication.

Ship one CLI package, one short agent skill, one small project profile and host-local SQLite bookkeeping. Do not create a parallel task database, generic runtime adapter, governance engine, worktree manager, provider mutation/recovery framework or dashboard.

Orca owns native Runs, Tasks, Dispatches, dependencies, gates, workers, environments and UI. Project/user authority owns scope, required checks, review, release and formal completion. orchestrate prepares packets, selects execution order and models, supervises economically, and identifies unresolved obligations.

## Interfaces

- `setup`: shallow discovery of project identity, applicable instruction entrypoints, task registry, command manifests and Orca environment. Write only .orchestrate.json and host-local metadata; never execute setup hooks or checks during discovery.
- `implement <objective>`: authority resolution, bounded plan selection, native task preparation, foreground coordination. Omitted objective may resume an unambiguous existing run, never invent work.
- `status [--run ID]`: non-consuming live native state plus evidence and next obligation.
- `resume [--run ID]`: lock, reconcile state and mutation intentions, then continue eligible work.
- `explain [--run ID]`: source-backed explanation without routine model calls.
- `packet --run ID --task ID`: versioned compact worker packet.
- `answer --run ID --question ID --text TEXT`: explicit user response handled by the controller.
- `doctor`: bounded compatibility diagnostics; active launch probes require an explicit option.
- Human output and --json; require explicit Run selection if ambiguous.

Use argparse, typed data classes, sqlite3, argument-array subprocesses and src layout. Profile schema `orchestrate-profile/v1` stores references/preferences only: reader repo or ce-gd, instruction/task entrypoints, single-owner-first strategy, max_workers default 3. Model roster and host binding live in user config. Local data belongs outside tracked and synchronized directories. Provide explicit skill installation, not hidden edits to agent instructions.

## Compatibility and host boundary

Resolve the Orca executable once by its installed guide; separate JSON stdout from stderr keepalives. Match tested command/response contracts, capabilities and effective launch receipts. Unfamiliar required contracts fail with bounded diagnostics; no command guessing, silent runtime/model substitution or fallback to generic governance.

FIRST vertical milestone: prove a plain Python controller can create/bind a Run, dispatch a worker, process/ack completion and resume after controller interruption through public Orca commands. Do not impersonate another terminal or replace this with a permanently reasoning agent. If unsupported, preserve the precise blocker and stop dependent implementation.

Current planning observation: Orca 1.4.198 advertises orchestration.contract.v1 and orchestration.worker-launch-preferences.v1. `skills get ... --references` is unsupported despite the stub mentioning it; --full is available. Revalidate live version. Guides were loaded from `orca skills get orca-cli` and `orca skills get orchestration`.

Use worker-start, native dependencies, check/reply, request-show, worker-show/read, release/reuse and gates. `--retry-request` is exact recovery after an uncertain mutation, not permission for arbitrary retry. Run namespace is not a scheduler. Observe documented caller/terminal authority.

Bind exact Orca workspace/environment and Git identity. Preserve host path semantics. Execution-host observations and verification go through Orca-owned execution surfaces; no Windows tools implicitly operating on WSL projects. WSL launcher only forwards distro/path/argv to canonical Windows installation.

## Binding, provenance and authority

Index only consulted non-secret source identities and exact bytes plus dependent interpretations. Include relevant tracked/index/worktree changes, deletions and new instruction files. Startup with unchanged bindings requires no model call. Mechanical config changes reparse; prose changes receive scoped reasoning. Candidate policy edits cannot grant authority.

Repository reader follows applicable user/repo instructions. CE reader resolves exact task registry/context packet, manifest-first log routing and existing checks. Preserve project-defined external review and acceptance. Missing authority never downgrades to generic mode. Do not duplicate CE budgets or stage decisions as authoritative profile fields.

Packets include objective, scope, source references, actual candidate identity, shared contracts, required checks, outputs, unresolved decisions. Results distinguish worker claims, observed commands and independent proof. Version packet/result formats. Secret-bearing files are excluded; inaccessible required sources or unresolved symlink/reparse boundaries remain limitations.

Verification is bound to actual candidate and host; coarse conservative invalidation, no universal test selector. Final required checks remain. Dirty candidate reviews require stable authorized source coverage; HEAD alone is insufficient.

## Coordination and recovery

One implementation owner by default. Additional workers require independently useful bounded tasks, explicit outputs and settled shared interfaces. One integration owner. Preserve existing WIP; no clean HEAD child falsely representing dirty work.

Separate native verification tasks/gates block dependents even after implementation worker_done completes its task. Model reasoning runs as bounded Orca tasks, not direct model-provider APIs. Cache selected proposals for deterministic replay. Empty waits and heartbeats cause zero model calls.

Process every message in full FIFO delivery, not merely wake-filter matches. Journal per-message progress/effects before acknowledgment; choose immediate exact-terminal reuse or native release for every accepted settled worker. Follow native uncertain-release recovery. Never release for inactivity alone.

Use host-local OS lock per Run and managed workspace write coordination. These do not enforce unrelated human/agent exclusivity. Persist intentions before Orca mutations, preserve request IDs, reconcile after restart before new effects. Missing receipt/history does not prove no effect or reset a budget. Ctrl-C stops controller dispatch without killing active workers. Unknown external effects stop repetition.

Track obligation, failing example, hypothesis, last meaningful evidence and next discriminating check. Same failure without new evidence triggers one bounded diagnosis before another correction; unproductive diagnosis leaves an unresolved decision. No arbitrary review campaigns or capability upgrades.

Status uses native tasks and concise worktree comments. Keep implementation/local checks/independent review/hosted proof/acceptance distinct.

## Delivery increments and tests

1. Complete single-worker: package/docs/CI, compatibility spike, setup/binding/readers/packets, native execution, verification/status and recovery. Real disposable Windows task with exact native provenance and interrupted-controller recovery.
2. Multi-worker + WSL: native dependencies, specialists/integration/review, reuse/release, intervention, WSL forwarding. Real Windows-coordinated WSL-native project. Independent parallel fixture and shared-contract serialization fixture; stale review invalidation.
3. Regression + cost + audit: read-only live binding trials against casaelida.com and j3w1.github.io; mutations/recovery in disposable representative repos only. Keep private source material and runtime records local, commit synthetic fixtures and sanitized summaries.

Required scenarios: unchanged binding has no model call; staged/unstaged/untracked/deleted instructions invalidate; replay/uncertain launch no blind duplicate; false worker success cannot unblock verification; missing history preserves uncertainty; repeated defect one diagnosis; CE failure no authority expansion; WIP preserved; unsupported model/contracts bounded failure; controller contention/runtime restart/stale handles/uncertain release preserve ownership; Windows/WSL spaces/Unicode/argv/exit codes and one state owner.

Windows/Linux CI runs unit/incident/install smoke tests; live Orca tests are separate. Compare direct Orca coordination and orchestrate on matched disposable tasks with same starting inputs, roster and checks. Record reported total tokens, wall time, repeated checks/retries/correctness; unknown usage remains unknown, not provider billing.

Fresh Sol xhigh final audit on exact candidate, then required checks and public GitHub visibility readback. No unsupported capability claims or fabricated live evidence.

## README acceptance

Match obra/superpowers README editorial style with original wording. Sections: title and direct introduction, contents, how it works, installation (Windows/skill/WSL), basic workflow with copyable CLI examples, what's inside, philosophy, contributing, updating, MIT license. Conversational and concrete; link detailed contracts/recovery elsewhere. No claims before behavior is implemented and verified. Source reference https://github.com/obra/superpowers/blob/main/README.md.
