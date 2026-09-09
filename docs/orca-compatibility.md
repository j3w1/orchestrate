# Orca public CLI compatibility

This is a bounded compatibility record, not general Orca, provider, WSL, hosted, or release certification.

## Observed contract

The planning and first-increment work used Windows, Python 3.13.15, and Orca 1.4.198. The runtime advertised `orchestration.contract.v1` and `orchestration.worker-launch-preferences.v1`. Version-matched `orca-cli` and `orchestration` guides and command help were read from the installed binary.

The strict client resolves one executable for the process, invokes argument arrays, decodes contract stdout as strict UTF-8, keeps bounded stderr diagnostics separate, and requires the public envelope to say `ok: true`. Passive `orchestrate doctor` performs no orchestration mutation.

## Caller identity observations

A caller with no Orca terminal association failed closed with `no_active_sender_terminal`. Scrubbing environment variables from a subprocess launched by an active terminal is not proof of an independent controller: installed source shows an implicit active-terminal fallback that may select another live identity.

A plain Python process in a fresh ordinary Orca shell successfully owned a Run. In the ordinary PowerShell terminal observed for this correction, `ORCA_TERMINAL_HANDLE`, `ORCA_AGENT_HOOK_TOKEN`, and `ORCA_AGENT_HOOK_ENDPOINT` were present while `ORCA_AGENT_LAUNCH_TOKEN` was absent; `terminal show` omitted `agentIdentity`, `worker-list` succeeded, and no record matched that terminal. A separate reviewer agent's terminal reported `agentIdentity=codex`. These facts mean hook credentials are not agent identity. The implementation therefore binds the exact terminal, worktree, Dispatch inventory, and Run state without clearing environment values, substituting `--from`, or treating terminal resource state as Dispatch status.

A fresh Python process in that same ordinary shell rebound the Run and created a Task. A source-backed architecture assessment confirmed that a dedicated ordinary-terminal bootstrap fits the approved plan and requires no established public API change. It must not pass another terminal through `--from`, transplant identity, or let a dispatched worker route around dispatch depth.

The observed `worker-start` result is flat: exact `runId`, `taskId`, `dispatchId`, `state`, `stage`, `setup`, `launch.requested`, `launch.effective`, `effects`, `residualResources`, and `mutation`. `worker-show` supplies the corresponding Dispatch identity and worker `startOptions`/resource readback. The observed `request-show` result carries `requestId`, `state`, `method`, timestamps, `receipt`, and `interpretation`; only the nested mutation request ID is a retry identity. Terminal create/send/close return their operation-specific resource receipt but no `result.mutation`, so bootstrap records intentions before calls and never retries an uncertain effect.

## Historical failed launch

The first composed `worker-start` attempt created a native Dispatch and then failed `agent_prompt_stalled` at `dispatch_input`, with no working-sequence advance. That attempt was inspected, preserved, and released without a blind retry. It remains historical failure evidence.

## Later readiness-first evidence

In a separate parent-owned disposable exercise, a fresh Codex terminal reached `tui-idle`; `worker-start --terminal --retry-of <settled-prior-failure>` accepted the Task. An exact succeeded `worker_done` settled the native Task and Dispatch, the whole Delivery was acknowledged, Orca correctly retained the external terminal, and the parent closed its dedicated controller terminal. This proves the public readiness-first composition that informed the implementation. It is not a test of this uncommitted Python candidate and does not rewrite the earlier failure.

The live terminal-exit probe also established the exact receipt shape:

```text
result.wait = {
  handle,
  condition: "exit",
  satisfied: true,
  status: "exited",
  exitCode,
  exitCause: { kind: "exited", exitCode }
}
```

Fast-exiting terminal output was not durably readable from the terminal stream. The bootstrap therefore requires both that native exit receipt and its own atomically written host-local result; missing or contradictory evidence cannot become exit code zero.

Observed structured release readbacks establish owned-and-released and retained `user_takeover` states using exact resource ID, ownership/release/reason, Dispatch ownership, timestamp/error, and archive fields. Live external retention and `no_owned_resource` have not been exercised for this candidate and are not accepted by the implementation.

## Gate state

| Gate | Result |
| --- | --- |
| Executable resolution and strict JSON client | Verified locally and by passive live doctor |
| Required runtime capabilities | Verified on Orca 1.4.198 |
| Ordinary-terminal Run ownership and fresh-process rebind | Verified in parent-owned live probes |
| Historical composed worker launch | Failed at prompt input; preserved |
| Readiness-first accepted Task/completion/release/ack composition | Verified in separate parent-owned disposable probe |
| First-increment Python state machine | Synthetic subprocess/native fixtures pass; candidate live run NOT_RUN by implementation worker |
| Windows terminal exit receipt | Verified by parent-owned disposable probe |
| WSL CLI bridge status | Read-only verified after official bridge registration |
| WSL worker lifecycle and forwarding shim | NOT_RUN; later increment |
| Hosted CI, provider behavior, independent candidate acceptance, release | NOT_RUN |

## Contained no-edit probe

The active doctor probe is retained for narrow compatibility diagnosis. It requires an ordinary terminal in an exact clean disposable Git/Orca worktree with a committed `.orchestrate-disposable` marker. `--active-run-probe` mints a host-local one-use token; the worker probe accepts that token, not an arbitrary Run ID.

Before mutation it read-only verifies the nonce-bearing Run, empty Task inventory, caller binding, worktree identity, and Git baseline. It explicitly places the worker in that disposable worktree. It rejects malformed FIFO entries without filtering, verifies an empty `filesModified` declaration plus independent Git readback, settles release, and acknowledges only a fully valid Delivery.

See [Live first-increment exercise](live-first-increment.md) for exact commands.
