# First-increment contracts and recovery

This document describes the implemented single-worker increment. Orca remains authoritative for Runs, Tasks, Dispatches, workers, messages, and terminal ownership. The local database is an effects journal and evidence index, not a task scheduler.

## Project and source binding

`orchestrate setup` performs bounded name discovery and writes `.orchestrate.json`. It does not run setup hooks, tests, build commands, or model calls. The v1 profile contains only repository-relative references, optional explicitly selected `candidateSources`, the reader kind, check commands, and the `single-owner-first` preference. The explicitly authorized initial setup action also records those exact bytes in a host-local `orchestrate-operational-profile-selection/v1` history outside the project. This is an operational configuration selection, not CE-GD or project-governance approval.

Neither Git `HEAD` nor a commit is an approval source. If candidate profile bytes differ from the selected digest—even after commit—mutating commands hold. After reviewing a deliberate change, `orchestrate setup --acknowledge-profile` appends the exact source bytes, digest, time, and acknowledgment source to that host-local history and selects the digest. Missing, malformed, or internally inconsistent selection history holds; it is never recreated implicitly by `implement` or `resume`. Repository instructions and governance remain authoritative for scope, checks, review, and acceptance.

Before task creation, the reader records exact Git HEAD/tree/branch and a digest of the complete porcelain status without opening every changed path. A bounded inventory includes at most 256 tracked, untracked, and ignored files having the conventional `AGENTS.md` or `CLAUDE.md` names; an oversized or non-UTF-8 inventory holds. It records SHA-256 identities only for configured, inventoried, or reader-routed non-secret sources and operationally selected `candidateSources`; for a staged consulted source, worktree, index, and HEAD identities remain distinct. Dirty paths outside that bounded set remain path/status-only `uncoveredChanges` and hold dispatch until the selected profile or exact routing covers them.

Conventional credential stores and names, environment files, and private-key/certificate suffixes are excluded even if named in `candidateSources`. POSIX reads traverse directory handles with no-follow flags. Windows opens the target with reparse-point semantics, verifies that the opened final identity is exactly the intended in-root path, denies concurrent writers for the bounded read, and consumes bytes from that same handle. A changed or new candidate instruction can restrict work but is not treated as a new authority grant.

The CE reader does not infer a `.ceip.json` interface. It requires exactly one task matching `CE-` plus exactly four digits in the objective, resolves that task through `docs/tasks/README.md`, and follows the task's required Context packet links. Project logs use only the observed `ce-systems-project-log-manifest` schema v1: shard metadata selects every declared shard whose `task_ids` contains the exact task, so multiple declared shards are legitimate and unrelated siblings are excluded.

Shard contents are not opened generically. When the selected operational profile includes `package.json`, the manifest advertises the exact query interface, and the clean tracked package script maps exactly to `node scripts/quality/project-log.mjs query`, the reader invokes `pnpm --silent run project-log:query -- <exact-task> --json --max-entries 12 --max-bytes 32768`. Strict JSON must echo the task, list exactly the manifest-selected shard paths, and return only typed matches from those shards. Any unknown schema, dirty query source, cross-task result, malformed response, timeout, or nonzero `omitted_matches` holds. The exact query result is hashed into routing, and the query implementation and manifest are re-read after execution. This is the one bounded machine interface, not authority to run arbitrary candidate scripts.

The compact `orchestrate-worker-packet/v2` is stored outside the repository and binds the objective, native Run, starting candidate, operational-profile selection provenance, source identities, reader routing, required checks, outputs, and unresolved limitations. Task creation necessarily precedes a native Task ID, so the one byte-identical packet sent and stored says that Task/Dispatch identity comes from Orca's injected dispatch preamble; it does not fabricate a pre-create Task ID. `packet` still requires the exact locally bound Run and Task to retrieve that immutable document.

## Host-local records

State lives beneath the operating system's local state directory, or `ORCHESTRATE_HOME` in disposable tests. Before creating a selection, database, packet, or lock, orchestrate rejects a target lexically or physically inside the project, a Windows UNC target, and positively identified OneDrive, Dropbox, Google Drive, iCloud Drive, Box, or Syncthing roots. This is a bounded detector, not proof that every proprietary, redirected, remotely mounted, or synchronized filesystem can be identified; operators remain responsible for choosing a private host-local root. State is keyed by a normalized project-root digest and contains:

- append-only operational-profile selection records with exact source bytes and digests;
- local-to-native Run bindings;
- mutation intentions and public request receipts;
- immutable whole Delivery responses plus per-message effects;
- exact worker packets;
- questions and answers; and
- worker claims and other evidence with their stated status.

A non-blocking OS file lock fences two local controllers for the same Run. It does not claim to lock unrelated humans, agents, worktrees, or remote hosts.

## One-worker state machine

The normal sequence is `run-create`, exact Run readback, `task-create`, exact immutable-spec Task readback, `worker-start`, exact Dispatch/worker readback, foreground `check`, full Delivery journaling, message effects, native settlement readback, worker release, and whole-Delivery acknowledgment. Every orchestration mutation receives a durable intention before the public Orca call. Worker start advances only after the returned Run, Task, ready/input state, no-setup effects, exact worktree, terminal, input receipt, residual resources, and requested/effective agent-model-effort agree with `worker-show`. The default owner roster is Codex `gpt-5.6-sol` at `high`; a user may replace the named `owner` slot in the host-local `orchestrate-user-config/v1` config without adding model names to project files.

Only one new active local objective is admitted. `worker_done` must bind the exact Task and Dispatch and match native Task/Dispatch settlement. A succeeded worker is recorded as a worker claim with independent verification still `pending`; it never becomes acceptance or release proof. Failed workers are released and remain failed.

Every original FIFO element must be an object. Heartbeats, questions, escalations, and completion reports must bind the exact current Task and Dispatch. Questions keep the entire Delivery unacknowledged until `answer` durably replies; unsupported, malformed, stale, and unresolved escalation mail is held rather than discarded.

Release is accepted only from an exact Dispatch-bound mutation response plus structured `worker-show` readback in one of two observed public states. A released owned terminal requires `ownershipState=releaseState=released`, null reason/error, exact origin/owner IDs, timestamps, and a captured transcript archive. Prior release with retained user ownership requires `ownershipState=user_owned`, `releaseState=retained`, `retainedReason=user_takeover`, null release timestamps/error, and null archive fields. Opaque prose, unknown fields, external retention, and unobserved dispositions such as `no_owned_resource` remain unresolved. A Delivery is acknowledged only after every journaled message has a resolved effect and release disposition. Worker evidence, the terminal Run phase, and the message effect are one local SQLite transaction, and an unacknowledged terminal-phase Delivery remains resumable and is replayed before return.

## Interruption and uncertainty

Ctrl-C stops foreground controller waiting and does not kill a worker. `resume` first takes the same host-local lock, reapplies locally committed receipts idempotently, and inspects unresolved native mutation request identities. The retry identity is Orca's `result.mutation.requestId` (or its documented error-data mutation receipt), never the top-level transport correlation ID. `request-show` must return that exact ID, the expected method, timestamps, receipt, and interpretation. A `completed` or `pending` public request is replayed only with that exact `--retry-request` identity. An absent or missing receipt remains `unknown_external_effect`; absence never authorizes a fresh attempt. A recovered worker-start receipt still undergoes the same effective-launch and `worker-show` checks before the local phase can become `waiting`.

Source changes or incomplete selected candidate coverage before worker launch stop the state machine. Source changes while the already-bound worker is active are expected worker output and do not cause a duplicate launch. Omitted Run IDs are accepted only when selection is unambiguous.

## Ordinary-terminal bootstrap

When invoked outside Orca, mutating commands create one focused ordinary Orca terminal in the exact project worktree. An existing caller is accepted only after exact `terminal show`, `worktree current`, `worker-list`, and `run-current` readback proves an ordinary terminal in the requested worktree with no active or context-only Dispatch; hook credentials alone do not indicate an agent. The inner Python process owns Run authority.

The outer launcher durably journals its exact arguments and result path before terminal creation, then separately journals create, interrupt, exit, close, and report phases. Orca 1.4.198 terminal create/send/close responses do not expose a mutation request identity, so a response lost after any of those effects cannot be retried or inferred: create and close uncertainty block continuation, while interrupt uncertainty permits only read-only observation of the already-known terminal reaching an exact exit. The launcher requires `result.wait` with `condition=exit`, `satisfied=true`, `status=exited`, and matching `exitCode`/`exitCause`; it also requires the atomically written result to agree, emits that output exactly once, returns the exact child exit, and closes only after both proofs. Missing or contradictory evidence fails closed.

The bootstrap refuses to run from a reasoning-agent terminal, so a dispatched worker cannot use it to route around Orca's dispatch-depth rules. WSL forwarding and a WSL-native worker lifecycle remain outside this first increment.

## Verification boundaries

The unit and incident suite uses disposable Git repositories and synthetic public-command responses. Windows and Linux CI build a wheel, install it into an isolated environment, and run command smoke checks. Live Orca, model/provider, WSL worker, hosted CI, independent review, external acceptance, merge, release, and publication remain separate evidence unless explicitly exercised.
