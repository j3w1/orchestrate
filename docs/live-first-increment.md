# Live first-increment exercise

The implementation worker did not launch a nested worker. Run the following only from a user-owned root context after reviewing the candidate. The fixture is disposable; do not substitute a CE or ordinary project checkout.

## Create a disposable project

```powershell
$fixture = Join-Path $env:TEMP ("orchestrate-live-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $fixture | Out-Null
git -C $fixture init
git -C $fixture config user.name "Orchestrate Fixture"
git -C $fixture config user.email "fixture@example.invalid"
Set-Content -LiteralPath (Join-Path $fixture "AGENTS.md") -Value "Work only in this disposable fixture. Preserve WIP. Run the documented test."
Set-Content -LiteralPath (Join-Path $fixture "fixture.py") -Value "def value(): return 1"
Set-Content -LiteralPath (Join-Path $fixture "test_fixture.py") -Value "import unittest`nfrom fixture import value`n`nclass FixtureTest(unittest.TestCase):`n    def test_value(self):`n        self.assertEqual(value(), 2)"
git -C $fixture add .
git -C $fixture commit -m "disposable failing fixture"
orca repo add --path $fixture --json
```

## Configure and run

Use the exact Python installation containing this reviewed wheel or editable checkout:

```powershell
orchestrate setup --project $fixture --json
orchestrate doctor --json
orchestrate implement "Make test_fixture.py pass by changing only fixture.py, then run py -3.13 -m unittest -v" --project $fixture --json
```

The explicit initial setup selects the generated profile in host-local state. Review the file before implementation. If you edit it, run `orchestrate setup --project $fixture --acknowledge-profile --json` only after reviewing that exact change; a Git commit alone does not update the operational selection.

The command creates a dedicated focused ordinary terminal when called outside Orca. Record the returned Run and Task. If waiting is interrupted, do not start a second objective:

```powershell
orchestrate status --project $fixture --run <run-id> --json
orchestrate explain --project $fixture --run <run-id> --json
orchestrate packet --project $fixture --run <run-id> --task <task-id> --json
orchestrate resume --project $fixture --run <run-id> --json
```

If a question is pending:

```powershell
orchestrate answer --project $fixture --run <run-id> --question <message-id> --text "<explicit answer>" --json
orchestrate resume --project $fixture --run <run-id> --json
```

Expected evidence is an exact native Run/Task/Dispatch, accepted task input, whole FIFO Delivery, matching native settlement, explicit terminal disposition, acknowledged Delivery, and an exited/closed dedicated controller terminal. A `worker_succeeded` result still leaves independent verification and owner acceptance pending.

`ready`/`input_accepted` is not itself proof that the agent turn began. If the command returns `awaiting_preflight` with a `worker_input_submission_unproven` compatibility record, preserve that active Dispatch and its terminal for inspection. Do not rerun `implement`, resend the packet, send an arbitrary Enter key, or claim a worker failure; the record was produced by one read-only `worker-show` and leaves recovery as an explicit owner decision.

## Compatibility no-edit probe

The active doctor probe requires a second, clean disposable project with a committed marker and a one-use token. It is not the product workflow and must not reuse the worker-edited fixture above:

```powershell
$probeFixture = Join-Path $env:TEMP ("orchestrate-probe-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $probeFixture | Out-Null
git -C $probeFixture init
git -C $probeFixture config user.name "Orchestrate Probe"
git -C $probeFixture config user.email "probe@example.invalid"
Set-Content -LiteralPath (Join-Path $probeFixture ".orchestrate-disposable") -NoNewline -Value "orchestrate-disposable/v1`n"
Set-Content -LiteralPath (Join-Path $probeFixture "README.md") -Value "no-edit compatibility probe"
git -C $probeFixture add .
git -C $probeFixture commit -m "clean compatibility fixture"
orca repo add --path $probeFixture --json
orchestrate doctor --active-run-probe --disposable-project $probeFixture --json
orchestrate doctor --active-worker-probe <probe-token> --disposable-project $probeFixture --json
```

The probe rejects arbitrary Run IDs, dirty or mismatched worktrees, agent callers, reused tokens, pre-existing Tasks, malformed FIFO entries, declared file changes, and baseline drift.
