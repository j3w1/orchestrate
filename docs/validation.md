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
| Fresh full suite | `python -m unittest discover -s tests -v` plus explicit `python -m unittest discover -s tests/incidents -t tests -v`; disposable Git and synthetic command fixtures only | Candidate implementation report; coordinator frozen-export run, 225 tests, exit 0, and explicit incident discovery, 6 tests | `PASS` |
| Compile and diff checks | `python -m compileall -q src tests` and `git diff --check` | Candidate implementation report | `PASS` |
| Frozen build | Build from a clean `git archive HEAD` export, not the mutable working tree | Candidate implementation report | `PASS` |
| Fresh wheel install | Install the frozen wheel into a new virtual environment | Candidate implementation report | `PASS` |
| CLI help smoke | Installed `orchestrate --help`, `orchestrate doctor --help`, and `orchestrate-wsl --help` | Candidate implementation report | `PASS` |
| Read-only discovery trial, CE-governed repository | Discovery functions only; never run `setup`, project hooks, checks, or mutations | Coordinator trial record; bounded inventory of two conventional instruction files, no ignored dependency path, repository byte-unchanged | `PASS` |
| Read-only discovery trial, `j3w1.github.io` | Discovery functions only; never run `setup`, project hooks, checks, or mutations | Coordinator trial record; bounded inventory of one conventional instruction file, ignored dependency instruction excluded, repository byte-unchanged | `PASS` |
| Disposable Windows multi-worker frontier | More ready work than `maxWorkers`, native dependencies/gates, deterministic waves, and restart reconciliation in a disposable repository | Coordinator live-lifecycle record; native Run, plan binding, Task creation and dispatch observed, and unproven-submission containment plus restart reconciliation without duplicate dispatch observed; the dispatched worker's model turn never started, so frontier waves and gate ordering were not exercised | `UNAVAILABLE` |
| Windows-coordinated WSL lifecycle | This candidate admits managed workers only on Windows; prove launcher transport and direct Orca WSL lifecycle separately without claiming WSL worker admission | Coordinator WSL record; launcher transport into the canonical Windows installation verified, including a forwarded doctor, project-path translation of a path containing a space and a non-ASCII character, refusal of an internal command, and no Linux state owner created; the direct Orca WSL worker lifecycle was not exercised for this candidate | `NOT_RUN` |
| Matched direct-versus-orchestrate cost trial | Same disposable starting inputs, roster, objective, and required checks; no provider billing inference | Coordinator cost record | `NOT_RUN` |
| Hosted Windows/Linux CI | Exact-candidate matrix jobs run their unit and incident suites, explicit incident discovery, an in-checkout wheel build, an isolated install, and the CLI help smokes | Hosted run reference; both matrix jobs succeeded for this exact candidate | `PASS` |
| Final repository verification | Public repository visibility and exact commit/tree readback; no merge or release inference | Coordinator repository record | `NOT_RUN` |
| Independent final audit | Fresh reviewer bound to the frozen candidate and tree; reviewer reports findings and does not repair | Independent audit record | `NOT_RUN` |

One earlier full-suite run failed once in `AdmissionTests.test_two_concurrent_public_preflights_cannot_both_receive_fresh_grants` because the test treated a five-second wall-clock window as semantic completion. That historical failure remains `FAILED` until a fresh full-suite record on the corrected candidate supersedes it; focused tests alone do not supersede it.

## Known limitations

### Native-Windows managed admission limits Linux test coverage

Managed worker preflight is win32-only by design in this milestone. Tests whose intended assertion requires a successful native-Windows admission path therefore use an explicit non-Windows skip, while Linux continues to run every host-neutral test and every pre-admission rejection test. The Windows and Linux CI jobs do not exercise equivalent unit/incident coverage: Windows runs the complete suite, and Linux runs that host-neutral subset; both jobs still run explicit incident discovery, wheel build, isolated install, and CLI help smokes. A passing Linux subset is not evidence of managed Linux or WSL worker admission, and the Windows-coordinated WSL lifecycle remains a separate `NOT_RUN` gate above.

### Managed admission source observation boundary

The earlier missing-source and incomplete-source-record limitations are corrected by public-entry regressions in `tests/test_admission.py`. A canonical packet and every mandatory source-record field are decoded inside the attempt fence before profile/source reads, Git, the CE query, or native Orca readback. A missing packet-bound source records a durable rejection, and restoring the exact bytes cannot grant the same Dispatch admission; removing a mandatory record field such as `authority` records `packet_identity_conflict` without an earlier source or native effect. Selected and discovered ignore transitions and explicit acknowledgment provenance remain covered by `tests/incidents/test_selected_before_ignored_authority_acknowledgment.py`.

The bounded source reads still do not make a multi-file snapshot atomic and do not constrain hostile or unrelated external writers. The CE query therefore rechecks the exact completed query-source identities immediately before and after its subprocess, while the immutable observation and admission/effect fence continue to order only managed preflight and controller paths.

### Hosted CI history and unstarted worker attempt

Hosted CI had never executed for this project before this candidate. Its first execution failed on both runners, and the failures were attributed before correction: all Linux failures and two of three Windows failures were reproduced against the accepted second-increment base and were therefore pre-existing, while one Windows failure was introduced by this increment's added Git-argument assertion.

A dispatched worker's model turn failing to start was observed live. The controller contained the attempt with a single read-only readback and no resend or external effect. Recovery of such an attempt is an explicit owner decision.

## Cost trial record

For the matched trial, record reported worker tokens, reported coordinator tokens, wall time, repeated work such as retries or duplicate checks, and correctness against the same required checks. Keep unknown token usage as `unknown`; reported usage is not provider billing.

## Trial boundaries

The two existing-repository trials are read-only. They may inspect discovered names, source routing, and candidate identities but must not write `.orchestrate.json`, local selection state, Tasks, Runs, Dispatches, gates, comments, or project files. CasaElida and the Pages repository remain external authority domains, not disposable fixtures.

Lifecycle, recovery, and WSL mutation trials use newly created disposable projects only. Hosted CI, independent review, owner acceptance, merge, GitHub publication, release, and PyPI publication remain distinct gates; no earlier row promotes another automatically.
