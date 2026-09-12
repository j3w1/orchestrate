# orchestrate contributor instructions

Read `docs/contracts-and-recovery.md` and `docs/validation.md` before implementation. Inspect Git state and preserve unrelated work. The project is an Orca-native Python CLI; Orca owns native tasks, dispatches, environments and worker lifecycle. Existing project governance owns authority and acceptance.

Use Python 3.13+, a standard src layout, standard-library runtime facilities where practical, and a small JSON project profile. Never embed personal paths, runtime IDs, credentials, private source packets or live local bookkeeping in committed files.

The main implementation owner is GPT-5.6 Sol at high effort. Use Sol xhigh for bounded milestone review and difficult implementation knots; Astra xhigh only for demonstrated architecture contradictions or unresolved rescue. A fresh Sol xhigh reviewer performs the final independent audit. Use native Orca orchestration for supervised workers. No worker delegation is needed by default.

One writer owns overlapping source changes. Reviews bind to a frozen candidate; reviewers report findings and do not repair. Consolidate corrections and preserve prior findings. Worker success, local verification, hosted CI, independent review and external project acceptance are distinct.

Run focused tests during development and the documented final checks at milestone boundaries. Missing live Orca, WSL, model or provider evidence must remain unavailable or NOT_RUN. Do not weaken checks or fabricate compatibility.

Keep README.md approachable and follow the editorial structure/style of https://github.com/obra/superpowers/blob/main/README.md with original orchestrate-specific wording. Document only implemented behavior. Keep detailed contracts and recovery semantics in linked docs.

Use apply_patch for file edits. Do not inspect secret values. Fixture execution must use disposable projects; existing CE and ordinary project trials are read-only. No production/provider mutations or PyPI publication.
