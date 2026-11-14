# Watch commands and execution protocol

## State and runtime

Keep one campaign ledger outside tracked files. The orchestrator is its writer; workers receive the absolute path and their operation/run IDs. Preserve the agent/issue/PR registry, permissions, exact heads/readiness/dependencies, feedback revisions and reply IDs, event cursors, retry budgets, and per-PR operation ownership. Keep tokens and other secrets out of it.

Keep these records separate: `agents` includes each name/ID, role, task, worktree, PRs, live/dormant state, and archive flag; `prs` includes the current head, readiness, dependencies, and owner; `permissions`, `feedback`, `reply_ids`, `cursors`, `budgets`, and `operations` retain their corresponding evidence. Register watcher/receiver identities and verification evidence under `watch.runtime`, tagged with the current run ID.

The control helper changes only the ledger's `watch`/`watch_history` fields and preserves other fields. It uses POSIX file locking and atomic replacement; use it from a POSIX checkout such as the repository's supported Linux environment. Run it through Bazel from the repository root:

```bash
./tool/bazel run //.agents/skills/orchestrate-sdlc/scripts:watch-control -- --state /absolute/campaign.json STARTWATCH 48h
```

The user-facing commands are also orchestration instructions: the helper alone does not query GitHub, start processes, stop native agent tasks, wake the main agent, or prove a backend exists. Connect the available harness tools as below. Do not substitute an unverified background log file for an autonomous dispatcher.

Place `--state` and, for internal commands, `--run-id` before the command name, as in `--state /absolute/campaign.json --run-id CURRENT_RUN ARM`.

## STARTWATCH [maximum]

1. Verify watch authorization and select this campaign's PRs, not all repository PRs. Inventory existing campaign watchers as well as the ledger: an empty/new ledger is not proof that none is running. Register and reuse a verified existing run's original deadline, cursors, and runtime identities before starting anything. Enroll later session PRs through the same registry and remaining window. Use the backend's enrollment operation, or a clean handoff if its PR set is fixed; do not add a sidecar poller.
2. Execute `STARTWATCH` with no argument for 48 hours or a positive whole-unit duration (`s`, `m`, `h`, `d`). Save its run ID, start, and absolute UTC deadline. An active repeat reuses the run; it cannot extend the deadline. An explicit shorter maximum can tighten it.
3. For an existing run, verify and reuse its registered live backend; never launch a second instance. Reconcile partial/unknown ownership first. Otherwise discover actual background/agent capabilities and use an available established watcher with one multiplexed event receiver where possible; do not invent private tool dependencies or add parallel ad-hoc polling loops. Register its owned process/task IDs and stop mechanism in the ledger.
4. Verify a successful current poll for every enrolled PR and delivery of an actual local readiness/heartbeat event to the orchestrator. Verify it can dispatch to the mapped owner. Only then execute internal `ARM` with `--run-id` and report armed, coverage, and the fixed deadline. If the runtime or wakeup path is unavailable, report the limitation, do not arm, and stop any partial setup.
5. Stay in the harness's event-receive/wait loop if an inactive main agent cannot be woken. On a timeout affirmatively check watcher/receiver liveness and fresh polls. Remain quiet while healthy; report actionable failures, terminal changes, and impending lease/deadline limits.

The backend must enforce the recorded deadline, including any tightened maximum. If it cannot update a limit in place, hand it off cleanly with the same shortened deadline. At expiry, fence new actions and run the stop/drain/confirmation procedure; report the deadline stop rather than starting another window.

Codex and Claude Code have different task/background interfaces. Bind to the tools actually present rather than assuming a particular tool name or configured concurrency is available. The registry and event protocol remain the same.

## Events, budgets, and handoffs

The watcher detects; the orchestrator dispatches; owners correct and publish. Handle current-head CI failures, new/edited feedback, necessary rebases, readiness/approval changes, and terminal PRs. Route one fixer per PR; independent PR work runs concurrently. Suppress only exact recorded agent replies. A candidate event is advisory, not permission to merge.

Use configurable bounded defaults: 20 fixer attempts per PR, at most 3 identical consecutive failures, and at most 3 explicit CI reruns. Count base-refresh/fix turns too. Normalize repeated failure signatures by cause, not changing fixture IDs. A real successful run may clear a consecutive-failure streak; handoffs/restarts never reset budgets or hide failure history. Stop the failing loop and report when a cap is reached.

For context/runtime lease renewal, retain the same user watch run, original deadline, cursors, dedupe/feedback epochs, budgets, and registry. Stop/drain the old watcher, verify ownership and that all owned children are gone and its lock released, then resume the same backend. This is a backend handoff, not public `STOPWATCH`; retain the control run and phase. Do not call `STARTWATCH` to manufacture a new window or start a duplicate poller. A runtime lease is not a new authorization period.

Release obsolete per-PR operation holds after verifying the owning operation finished or the PR became terminal. Never clear a live owner's lock by guess, or block unrelated PRs behind one publication. Remove terminal PRs from active polling while retaining their registry/history.

## STOPWATCH

1. Execute `STOPWATCH` against this campaign. It immediately records `stopping`; delayed watch workers fail `CHECK`. An already stopped/missing watch is a no-op.
2. Stop and drain only the registered owned watcher/receiver and watch-triggered workers using the harness's real stop mechanisms. Verify PID/task/run ownership before signaling; do not kill by a broad process name. Preserve local work, events, replies, cursors, and receipts. Unrelated user-authorized work is not cancelled.
3. Check outstanding external calls: a call already in flight cannot necessarily be recalled. Observe its result and report it rather than falsely claiming cancellation. Permit no new watch-triggered external mutation.
4. After verifying all owned watching/dispatch work is quiescent, execute internal `CONFIRMSTOP --run-id …` and report stopped. Do not claim stopped while owned children are still running. Repeating stop is idempotent.

Stop/expiry does not close PRs, mark Ready, merge, archive owners, delete branches/worktrees, or discard history. After a clean stop, an explicit new user `STARTWATCH` may create a new run/window; old run IDs remain fenced.

## Mutation fence

Immediately before every watch-triggered push, reply, readiness action, or merge, run:

```bash
./tool/bazel run //.agents/skills/orchestrate-sdlc/scripts:watch-control -- --state /absolute/campaign.json --run-id CURRENT_RUN CHECK
```

Also verify the action's authorization, actual PR state/head, operation ownership, and action-specific gates. The helper's success is a time/run/stop fence, not a permission grant or an atomic transaction with GitHub. Recheck at the final mutation boundary; drain already-started calls before confirming stop.

Internal `ARM`, `CHECK`, and `CONFIRMSTOP` require the exact current run ID. `ARM` records an independently verified runtime, not a promise to create one. Expired or stopped runs cannot arm or mutate. An expired run must be stopped/quiesced before a new one begins.

## Final merge gate

When merging is authorized, fetch fresh evidence immediately before the operation:

- PR is open and Ready; exact expected head and current reviewed scope.
- Actual required approvals and applicable CODEOWNERS are satisfied; no outstanding change request, unaddressed feedback, unresolved required thread, pending batch, or competing fixer.
- Required CI from the required app is explicitly successful on that head. Skipped/absent/unknown is not passing. Distinguish latest replacement jobs from superseded cancelled attempts. Apply only exact explicitly authorized exceptions, preserving their raw status.
- Read the actual target/default-branch ref and current repository rules. PR `base.sha`/`baseRefOid` is a historical snapshot, not proof of current target HEAD. Meet strict freshness and genuine stack dependencies.
- The watch run, action authorization, operation ownership, and absolute deadline remain valid.

Default preference is rebase. If a queue/policy would instead squash or enqueue, stop and explain the conflict unless the user has explicitly authorized the precise alternative or bypass. Never silently change method, sign an agreement, forge CI/approvals, alter protection, or enable platform automerge.

Serialize only the short final same-target-ref check/mutation when required. Prepare, publish, and run CI independently in parallel; use safe backend-supported parallel merges when available. Protect the exact head, make one verified merge attempt, then confirm actual merged state, landed commit/tree, and target ref. Do not blindly retry an unchanged rejected head; reconcile stale bases/dependencies through their owners.
