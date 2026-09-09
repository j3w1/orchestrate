# First-increment contracts and recovery

This document describes the implemented single-worker increment. Orca remains authoritative for Runs, Tasks, Dispatches, workers, messages, and terminal ownership. The local database is an effects journal and evidence index, not a task scheduler.

## Project and source binding

`orchestrate setup` performs bounded name discovery and writes `.orchestrate.json`. It does not run setup hooks, tests, build commands, or model calls. The v1 profile contains only repository-relative references, optional explicitly approved `candidateSources`, the reader kind, check commands, and the `single-owner-first` preference.

Before task creation, the reader records exact Git HEAD/tree/branch and a digest of the complete porcelain status without opening every changed path. It records SHA-256 identities only for configured or reader-routed non-secret sources and explicit `candidateSources`; for a staged consulted source, worktree, index, and HEAD identities remain distinct. Dirty paths outside that bounded set remain path/status-only `uncoveredChanges` and hold dispatch until the profile or routing explicitly covers them. Secret-like names and every symlink, junction, or reparse ancestor fail closed before resolve/read. A changed or new candidate instruction can restrict work but is not treated as a new authority grant.

The CE reader does not infer a `.ceip.json` interface. It requires exactly one CE task named in the objective, resolves that task through `docs/tasks/README.md`, follows the task's required Context packet links, and resolves project-log sources through `docs/project-log/manifest.json`; a missing or ambiguous route is a hold.

The compact `orchestrate-worker-packet/v1` is stored outside the repository and binds the objective, native Run and Task, starting candidate, source identities, reader routing, required checks, outputs, and unresolved limitations. `packet` requires the exact Run and Task.

## Host-local records

State lives beneath the operating system's local state directory, or `ORCHESTRATE_HOME` in disposable tests. It is keyed by a normalized project-root digest and contains:

- local-to-native Run bindings;
- mutation intentions and public request receipts;
- immutable whole Delivery responses plus per-message effects;
- exact worker packets;
- questions and answers; and
- worker claims and other evidence with their stated status.

A non-blocking OS file lock fences two local controllers for the same Run. It does not claim to lock unrelated humans, agents, worktrees, or remote hosts.

## One-worker state machine

The normal sequence is `run-create`, `task-create`, `worker-start`, foreground `check`, full Delivery journaling, message effects, native settlement readback, worker release, and whole-Delivery acknowledgment. Every mutation receives a durable intention before the public Orca call. The default owner roster is Codex `gpt-5.6-sol` at `high`; a user may replace the named `owner` slot in the host-local `orchestrate-user-config/v1` config without adding model names to project files.

Only one new active local objective is admitted. `worker_done` must bind the exact Task and Dispatch and match native Task/Dispatch settlement. A succeeded worker is recorded as a worker claim with independent verification still `pending`; it never becomes acceptance or release proof. Failed workers are released and remain failed.

Every original FIFO element must be an object. Heartbeats, questions, escalations, and completion reports must bind the exact current Task and Dispatch. Questions keep the entire Delivery unacknowledged until `answer` durably replies; unsupported, malformed, stale, and unresolved escalation mail is held rather than discarded.

Release is accepted only when Orca confirms `released`/`already_released`, or confirms a retained `external_terminal`/`no_owned_resource` disposition. A Delivery is acknowledged only after every journaled message has a resolved effect and release disposition.

## Interruption and uncertainty

Ctrl-C stops foreground controller waiting and does not kill a worker. `resume` first takes the same host-local lock, reapplies locally committed receipts idempotently, and inspects unresolved native mutation request identities. The retry identity is Orca's `result.mutation.requestId` (or its documented error-data mutation receipt), never the top-level transport correlation ID. A `completed` or `pending` public request is replayed only with that exact `--retry-request` identity. An absent or missing receipt remains `unknown_external_effect`; absence never authorizes a fresh attempt.

Source changes or incomplete approved candidate coverage before worker launch stop the state machine. Source changes while the already-bound worker is active are expected worker output and do not cause a duplicate launch. Omitted Run IDs are accepted only when selection is unambiguous.

## Ordinary-terminal bootstrap

When invoked outside Orca, mutating commands create one focused ordinary Orca terminal in the exact project worktree. The inner Python process owns Run authority. The outer launcher waits in the foreground, forwards Ctrl-C as an interrupt, verifies the native `result.wait` exit receipt, reads an atomically written host-local result, reproduces stdout and the exact child exit code, closes only the dedicated terminal tab, and deletes the temporary result. Missing or contradictory exit/result evidence fails closed.

The bootstrap refuses to run from a reasoning-agent terminal, so a dispatched worker cannot use it to route around Orca's dispatch-depth rules. WSL forwarding and a WSL-native worker lifecycle remain outside this first increment.

## Verification boundaries

The unit and incident suite uses disposable Git repositories and synthetic public-command responses. Windows and Linux CI build a wheel, install it into an isolated environment, and run command smoke checks. Live Orca, model/provider, WSL worker, hosted CI, independent review, external acceptance, merge, release, and publication remain separate evidence unless explicitly exercised.
