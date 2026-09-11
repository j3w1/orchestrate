# Validation evidence

Validation is recorded per exact candidate. A passing local suite does not prove live Orca lifecycle, WSL placement, provider behavior, hosted CI, independent review, project acceptance, merge, publication, or release.

This file defines the evidence matrix and record shape; it is not itself a result record. Every row starts at `NOT_RUN` for a new candidate and changes only when a matching sanitized record exists.

## Evidence record fields

Record these fields together:

- candidate commit and Git tree;
- host kind, such as Windows, Linux CI, or Windows-coordinated WSL;
- Orca version when Orca participates, otherwise `not_applicable`;
- UTC date;
- outcome: `PASS`, `FAILED`, `NOT_RUN`, or `UNAVAILABLE`; and
- a sanitized report reference, such as an artifact name plus SHA-256.

Do not record credentials, private source text, raw environments, personal filesystem paths, terminal or runtime identifiers, or provider billing guesses. A result without exact candidate and tree identity does not fill a candidate row.

## Gates

| Gate | Scope / boundary | Evidence record | Status |
| --- | --- | --- | --- |
| Fresh full suite | `python -m unittest discover -s tests -v` plus explicit `python -m unittest discover -s tests/incidents -t tests -v`; disposable Git and synthetic command fixtures only | Candidate implementation report | `NOT_RUN` |
| Compile and diff checks | `python -m compileall -q src tests` and `git diff --check` | Candidate implementation report | `NOT_RUN` |
| Frozen build | Build from a clean `git archive HEAD` export, not the mutable working tree | Candidate implementation report | `NOT_RUN` |
| Fresh wheel install | Install the frozen wheel into a new virtual environment | Candidate implementation report | `NOT_RUN` |
| CLI help smoke | Installed `orchestrate --help`, `orchestrate doctor --help`, and `orchestrate-wsl --help` | Candidate implementation report | `NOT_RUN` |
| Read-only discovery trial, CE-governed repository | Discovery functions only; never run `setup`, project hooks, checks, or mutations | Coordinator trial record | `NOT_RUN` |
| Read-only discovery trial, `j3w1.github.io` | Discovery functions only; never run `setup`, project hooks, checks, or mutations | Coordinator trial record | `NOT_RUN` |
| Disposable Windows multi-worker frontier | More ready work than `maxWorkers`, native dependencies/gates, deterministic waves, and restart reconciliation in a disposable repository | Coordinator live-lifecycle record | `NOT_RUN` |
| Windows-coordinated WSL lifecycle | This candidate admits managed workers only on Windows; prove launcher transport and direct Orca WSL lifecycle separately without claiming WSL worker admission | Coordinator WSL record | `NOT_RUN` |
| Matched direct-versus-orchestrate cost trial | Same disposable starting inputs, roster, objective, and required checks; no provider billing inference | Coordinator cost record | `NOT_RUN` |
| Hosted Windows/Linux CI | Exact candidate jobs, including both test-discovery commands, frozen build, install, and help smokes | Hosted run reference | `NOT_RUN` |
| Final repository verification | Public repository visibility and exact commit/tree readback; no merge or release inference | Coordinator repository record | `NOT_RUN` |
| Independent final audit | Fresh reviewer bound to the frozen candidate and tree; reviewer reports findings and does not repair | Independent audit record | `NOT_RUN` |

One earlier full-suite run failed once in `AdmissionTests.test_two_concurrent_public_preflights_cannot_both_receive_fresh_grants` because the test treated a five-second wall-clock window as semantic completion. That historical failure remains `FAILED` until a fresh full-suite record on the corrected candidate supersedes it; focused tests alone do not supersede it.

## Known limitations

### Missing selected source before the admission fence

When a packet-bound selected source no longer exists on disk, `worker-preflight` fails with `source_unavailable` during operational-profile loading. That load occurs before the state store is opened, the admission/effect fence is acquired, or an immutable observation is recorded, so the attempt leaves no durable rejected observation; restoring the exact bytes allows a later preflight of the same Dispatch to receive a fresh editing grant. This behavior predates the third increment and is present in the accepted second-increment base `5f250f38bb2defcb01833842c29dea5394c41877`. To reproduce it, build and bind a packet while a selected instruction exists, remove that file, call `worker-preflight`, then restore it byte-for-byte and call `worker-preflight` again for the same Dispatch. The documented immutability guarantee is therefore limited to attempts that reach the fence. Byte drift in a selected source that still exists is unaffected: it reaches the fence and produces a durable rejection, so restoring the bytes cannot turn a replay into admission.

### Incomplete packet source-record prepass

The packet source-record prepass does not validate every mandatory field of the source-index record shape. This behavior predates the third increment and is present in the accepted second-increment base `5f250f38bb2defcb01833842c29dea5394c41877`. To reproduce it, start with a canonical `orchestrate-worker-packet/v3`, keep a source record's `path`, `sha256`, and `bytes` valid, remove another mandatory field such as `authority`, recompute the canonical `packetId`, and retain matching stored-packet and native Task-spec identities. The malformed record passes the prepass and is rejected later as source drift with `source_binding_changed`, after native Orca readbacks, rather than immediately as a packet identity conflict.

## Cost trial record

For the matched trial, record reported worker tokens, reported coordinator tokens, wall time, repeated work such as retries or duplicate checks, and correctness against the same required checks. Keep unknown token usage as `unknown`; reported usage is not provider billing.

## Trial boundaries

The two existing-repository trials are read-only. They may inspect discovered names, source routing, and candidate identities but must not write `.orchestrate.json`, local selection state, Tasks, Runs, Dispatches, gates, comments, or project files. CasaElida and the Pages repository remain external authority domains, not disposable fixtures.

Lifecycle, recovery, and WSL mutation trials use newly created disposable projects only. Hosted CI, independent review, owner acceptance, merge, GitHub publication, release, and PyPI publication remain distinct gates; no earlier row promotes another automatically.
