---
name: orchestrate
description: Use the j3w1-orchestrate CLI to prepare, supervise, resume, inspect, or explain one Orca-native implementation Run in a configured project.
---

# orchestrate

Use the installed `orchestrate` command; do not reproduce its SQLite state or Orca lifecycle calls by hand.

Read the project `AGENTS.md` and `.orchestrate.json` first. Run `orchestrate setup` only when creating or refreshing the small reviewable profile is authorized. Prefer `status`, `explain`, and `packet` for inspection; use `answer` only for the exact pending question and `resume` only for an explicitly selected or unambiguous Run.

Start implementation from an ordinary shell. The launcher creates its own ordinary Orca controller terminal when needed and refuses agent-terminal depth bypass. Treat worker completion, independent verification, hosted proof, acceptance, merge, and release as distinct outcomes.

If a mutation is uncertain, preserve its request identity and use `resume`; never start a fresh replacement merely because history is missing. Do not edit host-local state, transplant another terminal identity, or put packets/runtime IDs in Git.
