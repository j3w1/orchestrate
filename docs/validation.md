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
| Fresh full suite | Unit and explicit incident discovery run once with the POSIX provider and again with `ORCHESTRATE_BOOTSTRAP_PROVIDER=simulated-win32`; disposable Git and synthetic command fixtures only | No matching correction-candidate record yet | `NOT_RUN` |
| Compile and diff checks | `python -m compileall -q src tests` and `git diff --check` | No matching correction-candidate record yet | `NOT_RUN` |
| Frozen build | Build from a clean `git archive HEAD` export, not the mutable working tree | No matching correction-candidate record yet | `NOT_RUN` |
| Fresh wheel install | Install the frozen wheel into a new virtual environment | No matching correction-candidate record yet | `NOT_RUN` |
| CLI help smoke | Installed `orchestrate --help`, `orchestrate doctor --help`, and `orchestrate-wsl --help` | No matching correction-candidate record yet | `NOT_RUN` |
| Synthetic machine-bootstrap regressions | The same complete suite under POSIX and simulated-Win32 providers: `.cmd` forms, case-insensitive PATH spelling, file-index-style identity, typed registry state, byte-exact commits, staged-versus-committed digest equality, descriptor/share-binding analogues, causally bound source-archive consumption, physically bound external-anchor ancestry, descendant-bound venv creation, concurrency, and fault seams; the native API wrapper remains Windows-only | No matching correction-candidate record yet | `NOT_RUN` |
| Live Windows first-machine bootstrap | Reviewed checkout entry through project setup; dedicated Python 3.13 venv, pinned reviewed-source archive install, HKCU user-PATH registration, command resolution, rerun fast path, and recovery exercised on a disposable Windows user profile | No matching correction-candidate record yet | `NOT_RUN` |
| Live WSL bootstrap launcher | A new WSL shell resolves the generated user-PATH shim and forwards cwd/Unicode argv/exit to the canonical Windows installation without Linux state | No matching correction-candidate record yet | `NOT_RUN` |
| Read-only discovery trial, CE-governed repository | Discovery functions only; never run `setup`, project hooks, checks, or mutations | No matching correction-candidate record yet | `NOT_RUN` |
| Read-only discovery trial, `j3w1.github.io` | Discovery functions only; never run `setup`, project hooks, checks, or mutations | No matching correction-candidate record yet | `NOT_RUN` |
| Disposable Windows multi-worker frontier | More ready work than `maxWorkers`, native dependencies/gates, deterministic waves, and restart reconciliation in a disposable repository | No matching correction-candidate record yet | `NOT_RUN` |
| Windows-coordinated WSL lifecycle | This candidate admits managed workers only on Windows; prove launcher transport and direct Orca WSL lifecycle separately without claiming WSL worker admission | No matching correction-candidate record yet | `NOT_RUN` |
| Matched direct-versus-orchestrate cost trial | Same disposable starting inputs, roster, objective, and required checks; no provider billing inference | Coordinator cost record | `NOT_RUN` |
| Hosted Windows/Linux CI | Exact-candidate matrix jobs run their unit and incident suites, explicit incident discovery, an in-checkout wheel build, an isolated install, and the CLI help smokes | No matching correction-candidate record yet | `NOT_RUN` |
| Final repository verification | Public repository visibility and exact commit/tree readback; no merge or release inference | Coordinator repository record | `NOT_RUN` |
| Independent final audit | Fresh reviewer bound to the frozen candidate and tree; reviewer reports findings and does not repair | Independent audit record | `NOT_RUN` |

Candidate `9ec68be` locally ran 230 tests with 49 expected win32-only skips and 6 explicit incident tests with 1 expected skip. Those results are history for that candidate, not evidence for a correction candidate. One still-earlier full-suite run failed once in `AdmissionTests.test_two_concurrent_public_preflights_cannot_both_receive_fresh_grants` because the test treated a five-second wall-clock window as semantic completion; focused tests did not supersede that historical result.

## Known limitations

### Native-Windows managed admission limits Linux test coverage

Managed worker preflight is win32-only by design in this milestone. Tests whose intended assertion requires a successful native-Windows admission path therefore use an explicit non-Windows skip, while Linux continues to run every host-neutral test and every pre-admission rejection test. The Windows and Linux CI jobs do not exercise equivalent unit/incident coverage: Windows runs the complete suite, and Linux runs that host-neutral subset; both jobs still run explicit incident discovery, wheel build, isolated install, and CLI help smokes. A passing Linux subset is not evidence of managed Linux or WSL worker admission, and the Windows-coordinated WSL lifecycle remains a separate `NOT_RUN` gate above.

### First-machine bootstrap evidence boundary

Unit coverage injects a memory-only typed user-PATH store, transactional registry adapter, synthetic command resolver and subprocess runner, and disposable installation/source/state roots. A shared high-level provider contract selects bounded reads, physical pins, binary staging, and exact commit verification; native Windows and the simulated provider use different OS primitives at that boundary. The complete unit and incident corpus runs under both the POSIX provider and simulated-Win32 provider; the latter covers `.cmd` forms, case-insensitive spelling, stable identity, registry semantics, share-binding analogues, and Windows staging branches without touching a real profile. Regressions prove that every staged commit preserves bare LF bytes and equal staged/committed digests, injected newline drift fails, pip receives a pinned archive causally derived from the reviewed source digest, transient archive input and staged-name substitutions cannot become authority, redirected anchor ancestry is refused, expected venv descendants exist and are pinned before the builder runs, the documented local-venv and bytecode exclusions are exact, and identity failures contain expected/observed data. The native wrapper remains separately authoritative for CreateFile sharing, explicit application-name execution, registry transactions, real junctions, and hosted Windows behavior. The suite cannot prove live registry permissions, environment propagation, `py.exe` discovery, pip/network behavior, WSL Windows-PATH import, or mounted-file execution; those remain live `NOT_RUN` gates.

### Managed admission source observation boundary

The earlier missing-source and incomplete-source-record limitations are corrected by public-entry regressions in `tests/test_admission.py`. A bounded canonical packet and every mandatory source-record field are decoded inside the attempt fence before profile/source reads, Git, the CE query, or native Orca readback. A missing packet-bound source records a durable definitive rejection, and restoring the exact bytes cannot grant the same Dispatch admission; nested JSON and removing a mandatory record field such as `authority` likewise record `packet_identity_conflict` without an earlier source or native effect. Removing a mandatory tracked CE query source from the index is likewise a definitive semantic change even when its worktree bytes remain present. In contrast, an unavailable Git/Orca/query subprocess, a nonzero Git object read for a source that a successful listing still proves present, or a still-present source that is temporarily unopenable records an append-only retryable failed-attempt observation without occupying the Dispatch's conclusive admission row. Recovery may retry the same Dispatch, but no retryable result grants editing. Selected and discovered ignore transitions and explicit acknowledgment provenance remain covered by `tests/incidents/test_selected_before_ignored_authority_acknowledgment.py`.

The bounded source reads still do not make a multi-file snapshot atomic and do not constrain hostile or unrelated external writers. The CE query therefore rechecks the exact completed query-source identities immediately before and after its subprocess, while the immutable observation and admission/effect fence continue to order only managed preflight and controller paths.

The tracked milestone plan has explicit controller-bound source membership. A host-neutral public `implement --plan` prelaunch regression reaches `worker-start` only after that non-reader source validates, while Windows-only tests retain responsibility for the subsequent managed-admission lifecycle. Repo-reader routing cannot make an otherwise ineligible packet path readable, and a reference-only source that appears between checks is rejected without a byte read.

### Hosted CI history and unstarted worker attempt

The hosted matrix for exact candidate `9ec68be` is historical `FAILED`: [run 34979019335](https://github.com/j3w1/orchestrate/actions/runs/34979019335) passed Ubuntu but failed Windows after 230 tests with 1 failure and 11 errors. All 12 failures were win32-only milestone/controller cases caused by the candidate rejecting its tracked milestone-plan source. No hosted run has yet filled the correction-candidate row above.

The hosted matrix for exact candidate `f61e226` is also historical `FAILED`: [run 34984041928](https://github.com/j3w1/orchestrate/actions/runs/34984041928) passed Ubuntu but failed Windows after 236 tests with 1 failure and 11 errors. All 12 failures were win32-only milestone/controller paths caused by the strict decoder rejecting the controller's canonical prefixed shared-contract digest in specialist and reviewer packets. This result is history for `f61e226`, not evidence for a later correction candidate.

The hosted matrix for exact candidate `2d0ae0f` is historical `FAILED`: [run 34987878003](https://github.com/j3w1/orchestrate/actions/runs/34987878003) passed Ubuntu but failed Windows after 243 tests with one failure and no errors. The only failure was the new Git-object-read regression's exact path-string matcher failing to intercept the production `git show` call under Windows path spelling; all twelve previously failing Windows milestone/controller tests passed. This result is history for `2d0ae0f`, not evidence for a later correction candidate.

The hosted matrix for exact issue-3 candidate `0a97896` is historical `FAILED`: run `35052303656` passed Ubuntu but failed Windows after 282 tests with 5 failures and 10 errors. The failures grouped into a shared Windows lock regression, path-alias rejection in machine-bootstrap safe reads, and downstream bootstrap expectations blocked by those two defects. Four affected lock/admission tests predated issue #3. This result is history for `0a97896`, not evidence for a later correction candidate.

The hosted matrix for exact issue-3 candidate `2bce451` is historical `FAILED`: run `35054423802` passed Ubuntu but failed Windows after 290 tests with 4 failures and 3 errors. The seven failures were confined to new machine-bootstrap coverage: three fixture/production receipt-identity disagreements, two fault seams that did not intercept the Win32 branch, one launcher verdict masked by receipt checking, and one native interpreter-replacement denial that the metadata-only handle did not enforce. This result is history for `2bce451`, not evidence for a later correction candidate.

The hosted matrix for exact issue-3 candidate `d1acaa1` is historical `FAILED`: run `35056421606` passed Ubuntu but failed Windows after 295 tests with 3 failures and 6 errors. The nine results mapped to four receipt/command-proof errors, two incomplete final verifications, one peer `.cmd` launcher refusal, one venv-error-precedence failure, and one bypassed shim-commit seam. The round-4 provider runs those scenarios locally through the simulated-Win32 seam; that does not promote simulated evidence to native Windows proof.

The diagnostic hosted matrix for exact candidate `9bfd9d4` is historical `FAILED`: run `35061993700` passed Ubuntu and intentionally retained five Windows failures plus seven errors while surfacing bounded proof records. Those records established that the native staging descriptor translated LF to CRLF: the 283-byte staged logical archive became a 285-byte committed target, and canonical receipt bytes were likewise changed. That evidence is history for `9bfd9d4`, not proof for a correction candidate.

An earlier hosted execution failed on both runners. Its failures were attributed before this correction: all Linux failures and two of three Windows failures reproduced against the accepted second-increment base, while one Windows failure came from that increment's added Git-argument assertion. Those results likewise do not fill a later candidate row.

A dispatched worker's model turn failing to start was observed live. The controller contained the attempt with a single read-only readback and no resend or external effect. Recovery of such an attempt is an explicit owner decision.

## Cost trial record

For the matched trial, record reported worker tokens, reported coordinator tokens, wall time, repeated work such as retries or duplicate checks, and correctness against the same required checks. Keep unknown token usage as `unknown`; reported usage is not provider billing.

## Trial boundaries

The two existing-repository trials are read-only. They may inspect discovered names, source routing, and candidate identities but must not write `.orchestrate.json`, local selection state, Tasks, Runs, Dispatches, gates, comments, or project files. CasaElida and the Pages repository remain external authority domains, not disposable fixtures.

Lifecycle, recovery, and WSL mutation trials use newly created disposable projects only. Hosted CI, independent review, owner acceptance, merge, GitHub publication, release, and PyPI publication remain distinct gates; no earlier row promotes another automatically.
