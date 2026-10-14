# Pausing and Unpausing a Queue

Operators need to stop a queue during an incident, a repository freeze, or maintenance, and resume it later, at runtime and without redeploying the gateway or orchestrator. The pause state lives in its own extension, `queuepause`, and OSS code enforces it, so every deployment runs the same rejection and freezing logic and a deployer only supplies where the state comes from.

## Behavior

### Pause
Pause is two independent switches per queue, plus an optional free-text reason.

| Reject new lands | Freeze processing | Effect |
|---|---|---|
| off | off | Running |
| on | off | Drain: new requests are refused, in-flight work finishes |
| off | on | Hold: requests are accepted and durable, nothing advances |
| on | on | Full pause |

All four combinations are meaningful, so two booleans are enough and no mode enum is needed.

- **Reject** happens in `Land()` before a request ID is minted, so a refused request consumes no sequence number and writes nothing. It returns a distinct user error that carries the queue and the operator's reason, and the gateway maps it to `FailedPrecondition`. A caller can tell a paused queue from a malformed request (`InvalidArgument`) or an unknown queue. The pause check runs after the queue is recognized, so an unknown queue is reported as unknown.
- **Freeze** is a live-read `consumergate.Gate` that closes for every orchestrator stage of the paused queue, including the dead-letter consumers, so no processing happens while frozen. The primary and DLQ consumers share one gate, so a gate that matches on queue alone covers both. The gate finds a message's queue from its tenant, which every orchestrator and gateway publish site sets to the queue name. It is a barrier, not preemption: a message already inside a controller runs to completion ([consumer-gate.md](../consumer-gate.md)). Blocked deliveries are postponed, so they spend no retry budget ([consumer-hold.md](../consumer-hold.md)).
- **Both fail closed.** When pause state cannot be read, `Land()` returns the error and the freeze gate stays closed. The consumer treats a gate error as open, so the freeze gate returns a blocked entry on a read error instead of the error. The one exception is a cancelled context, which is passed through so a shutting-down consumer is not logged as a pause backend failure.
- **Defaults**: when nothing is configured, no queue is ever paused. OSS ships a working way to pause, and a deployer can back the state with its own config service.
- Only `Land()` is gated at the gateway. Status, list, and history reads are unaffected. The gateway's own request-log consumer is not frozen, so request status and history keep materializing while a queue is frozen.
- All orchestrator stages share the consumer group `orchestrator`, so freeze covers a queue's whole pipeline. Freezing a single stage is out of scope.

### Unpause

- **Reject** stops applying on the first `Land()` after the store reflects the change. There is nothing to drain or replay.
- **Freeze** releases within one re-check delay (a fixed one second) plus the queue poll interval, plus any caching in the store. A held message is a barrier for its partition, so it runs first and the messages behind it follow in order. A released message is a fresh attempt, because postponing resets failure accounting, so a freeze never pushes healthy messages toward the dead-letter queue ([consumer-hold.md](../consumer-hold.md)).
- **Queues** have their own partitions at every stage, so they release independently and with no ordering across queues.
- **Long freezes** stall message-log cleanup for the frozen partitions, because a postponed message stays unacknowledged until release ([consumer-hold.md](../consumer-hold.md)). This is a storage cost, not a correctness problem.
- **Requests that waited through a freeze** go through ordinary staleness validation, so a PR that changed in the meantime fails validation, as it would after any long wait.

## Cancellation

Reject does not affect cancel, since cancel is not a `Land()`. Freeze does: the gateway records a `Cancelling` status when it accepts a cancel, but the orchestrator does not carry it out until the queue is unfrozen. Cancel cannot be exempted from freeze with the current setup, for two reasons:

- **The gate cannot tell cancel from any other stage.** It is keyed by consumer group and partition, and every orchestrator stage shares the consumer group `orchestrator`. The key carries no topic, so a gate that closes for a queue closes for all of its stages.
- **Cancel is not a stage of its own.** The cancel stage only marks the batch cancelling and hands off to speculate, which stops the paths and writes the terminal state. Build signal then asks the build runner to stop the running build. Speculate and build signal also do ordinary work, and a speculate message is only a batch ID, so nothing on the message says it belongs to a cancel. Message-level rules were rejected for the gate ([consumer-gate.md](../consumer-gate.md)).

The consequence is that freeze, cancel, then unfreeze does not guarantee the cancel wins. Held cancels release together with held ordinary work, and cancel is already best-effort against a land in flight, so a request can land before its cancel takes effect. Builds already running in CI also keep running while the queue is frozen.

Supporting cancel during a freeze needs the topic on the gate key and exemptions for cancel, speculate, and build signal. Freeze would then mean no new effects rather than nothing runs.

## Design

`queuepause.Store` is a per-queue source of pause state. It returns a `QueuePause` entity for a queue and decides nothing. `QueuePause` holds the two switches and the reason. `Get` is keyed by queue name, like `queueconfig.Store`, and returns the zero state for a queue with no entry, so absence means not paused. Both the gateway and the orchestrator resolve it, so the contract and its implementations live at `submitqueue/extension/queuepause/`.

OSS ships three implementations:

- `noop` never pauses and is the default when nothing is configured.
- `file` re-reads a small YAML file on each `Get`, so an operator pauses or unpauses a queue by editing the file, with no restart. This mirrors the file consumer gate, which re-reads on every delivery. An operator should replace the file by rename so a reader never sees a partial write.
- `fake` is an in-memory store for tests, with injectable state and read errors. It is never used in production.

A deployer supplies its own store backed by its config service. Both services select the store through one shared helper in `service/submitqueue/core`. Setting `QUEUE_PAUSE_CONFIG_PATH` selects `file`, and the file must exist at startup. Leaving it unset selects `noop`.

The gateway reads the state in `Land()` after the queue lookup and before minting an ID, and rejects when `RejectLands` is set. The orchestrator wraps its existing consumer gate in a decorator that closes for a queue while `FreezeProcessing` is set and otherwise defers to the wrapped gate. It cannot live under `platform/` because it depends on `queuepause.Store`.

### Requirements for implementations

An implementation of `queuepause.Store` must:

- **Return the zero state, and no error, for a queue it has no entry for.** Absence means not paused.
- **Return an error when the backend cannot be read, never the zero state.** Callers that enforce a switch fail closed on it, so an outage must not look like an unpaused queue.
- **Reflect a change without a restart.** Any caching bounds how long a pause takes to apply and release, and should stay short, because `Get` runs on every `Land()` and every gated delivery.
- **Be safe for concurrent use.**
- **Fail at construction on misconfiguration**, such as a missing path or invalid contents, so a bad deployment fails at startup and not on the first land.
- **Parse strictly.** Unknown fields and entries with an empty or duplicate queue name are errors, so a misspelled switch cannot silently leave a queue unpaused.
- **Wrap context errors with `%w`**, so a cancelled read is recognized as cancellation and not as a backend failure.

## Alternative: pause fields on queue config

`entity.QueueConfig` could carry the two booleans, with the gateway reading them through the existing `queueconfig.Store` and the orchestrator wrapping that store in a gate. It is the smaller change, and a failed `Get` already fails `Land()`. It was rejected for these reasons:

- The orchestrator would gain its own `queueconfig` dependency and a second queue list next to `Profiles`, and the two can disagree.
- The OSS YAML store is read once at startup, so each service would apply a pause change only after a restart.
- It puts operational state on an entity documented as an immutable registry of names, and makes queue config the default home for the next runtime knob.

## Open questions

- **Cancel during freeze.** Should cancel take effect while processing is frozen, as an incident workflow of freeze, cancel, unfreeze would need? If so, freeze becomes no new effects rather than nothing runs, and needs the topic on the gate key ([Cancellation](#cancellation)).
- **OSS backing.** `file` needs no schema and works with a mounted config file. A MySQL-backed store would serve multi-host OSS deployments, and needs a way for an operator to write it.
- **Read cost.** The `file` store reads on every `Land()` and on every gated delivery. A short cache may be needed.
- **Queue-name validation.** If `queueconfig` leaves OSS, nothing rejects an unknown queue, because `Profiles.For` falls back to the default profile for any name.
