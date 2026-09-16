---
name: orchestrate
description: Use the j3w1-orchestrate CLI to prepare, supervise, resume, inspect, or explain one Orca-native implementation Run in a configured project.
---

# orchestrate

Use the installed `orchestrate` command; do not reproduce its SQLite state or Orca lifecycle calls by hand.

Read the project `AGENTS.md` and `.orchestrate.json` first. Run `orchestrate setup` only when both the user-scope machine bootstrap and the initial small operational profile selection are authorized. First-machine bootstrap may create the dedicated `%LOCALAPPDATA%\orchestrate` venv and launchers and update only the Windows user PATH; it serializes repair, preserves the existing supported PATH kind, and refuses redirected or unproved install entries. After its healthy fast-path check it is silent and write-free. Initial setup records exact host-local selection history; after any later edit, review it and use `setup --acknowledge-profile` explicitly, because committing candidate bytes does not update the selection or grant authority. Prefer `status`, `explain`, and `packet` for inspection; use `answer` only for the exact pending question and `resume` only for an explicitly selected or unambiguous Run.

Start implementation from an ordinary shell. The launcher uses exact native terminal/worktree/Dispatch/Run readback—not hook credentials—to prove an ordinary caller, creates its own ordinary Orca controller terminal when needed, and refuses agent-terminal depth bypass. Treat worker completion, independent verification, hosted proof, acceptance, merge, and release as distinct outcomes.

As the dispatched implementation worker, read the packet's `admission.commandTemplate`, replace only its injected Run/Task/Dispatch and packet-ID placeholders, and run that exact isolated-interpreter `worker-preflight` command before any project edit or project check. Continue only when it returns a fresh `admitted` result; an already-passed replay is historical evidence rather than a fresh editing grant, and any rejected/missing/conflicting observation is a hold. This protocol records managed provenance but cannot prevent arbitrary shell bypass or unrelated external writers.

If an orchestration mutation is uncertain, preserve its nested mutation request identity and use `resume`; never start a fresh replacement merely because history is missing. If bootstrap terminal close loses its response, resume permits only the documented same-runtime, exact-identity, complete-inventory read-only reconciliation; it never repeats close. Create uncertainty and incomplete cleanup evidence remain held. Do not edit host-local state, transplant another terminal identity, or put packets/runtime IDs in Git.
