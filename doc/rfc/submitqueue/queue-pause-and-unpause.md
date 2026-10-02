# Pausing and Unpausing a Queue

Operators need to stop a queue during an incident, a repository freeze, or maintenance, and resume it later. We need to pause and unpause a queue at runtime, without redeploying the gateway or orchestrator. This RFC lays out 2 options for where the pause state should live.

## Behavior

### Pause
Pause is two independent switches per queue.

| Reject new lands | Freeze processing | Effect |
|---|---|---|
| off | off | Running |
| on | off | Drain: new requests are refused, in-flight work finishes |
| off | on | Hold: requests are accepted and durable, nothing advances |
| on | on | Full pause |

All four combinations are meaningful, so two booleans are enough and no mode enum is needed. Both options share the following decisions:

- **Reject** happens in `Land()` before a request ID is minted, so a refused request consumes no sequence number and writes nothing. It returns a distinct user error, so callers can tell a paused queue from a malformed request or an unknown queue.
- **Freeze** is a live-read `consumergate.Gate` implementation that closes the gate for every orchestrator stage of the paused queue, including the dead-letter consumers, so no processing happens while frozen. The primary and DLQ consumers share one gate, so a gate that matches on queue alone covers both. The gate finds a message's queue from its tenant, which every orchestrator and gateway publish site sets to the queue name. It is a barrier, not preemption: a message already inside a controller runs to completion ([consumer-gate.md](../consumer-gate.md)). Blocked deliveries are postponed, so they spend no retry budget ([consumer-hold.md](../consumer-hold.md)). Unpausing releases within one re-check delay plus the queue poll interval.
- **Defaults**: when nothing is configured, no queue is ever paused. OSS ships a working way to pause; a deployer can back the state with its own config service instead.
- Only `Land()` is gated at the gateway. Status, list, and history reads are unaffected.
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

## Problem
- `queueconfig.Store` is the registry of valid queue names. The YAML implementation is read once at startup and the entity is documented as immutable.
- `consumergate` can stop a controller at runtime, but its only backend is a shared directory of files meant for e2e tests and single-host development. It has no notion of a queue: it is keyed by consumer group and partition. It fails open when its state cannot be read.
- The gateway has no pause check in `Land()`.

## Option A: pause fields on queue config

`entity.QueueConfig` gains two booleans, zero value false. The `queueconfig.Store` interface is unchanged. The gateway reads the fields through the existing `Get` in `land.go`. The orchestrator builds a `queueconfig.Store` and a `consumergate.Gate` that wraps it and closes for queues whose freeze field is set.

The entity gains the two fields:

```go
type QueueConfig struct {
	Name string `json:"name" yaml:"name"`

	// RejectLands is true while the queue refuses new land requests.
	RejectLands bool `json:"reject_lands" yaml:"reject_lands"`

	// FreezeProcessing is true while the queue's accepted requests are not advanced.
	FreezeProcessing bool `json:"freeze_processing" yaml:"freeze_processing"`
}
```

The gateway keeps the cfg it currently discards in `land.go` and rejects before minting an ID:

```go
cfg, err := c.queueConfigs.Get(ctx, queue)
if err != nil {
	// existing not-found and lookup-failure handling
}
if cfg.RejectLands {
	return entity.LandResult{}, errs.NewUserError(&QueuePausedError{Queue: queue})
}
```

The orchestrator wires a gate over the same store:

```go
func (g queueConfigGate) Enter(ctx context.Context, key consumergate.Key) (consumergate.Entry, error) {
	cfg, err := g.queueConfigs.Get(ctx, key.Partition.Tenant) // tenant is the queue name
	if err != nil {
		return nil, err
	}
	return g.entry(cfg.FreezeProcessing), nil
}
```

Pros:
- Smallest change, and no new extension.
- Both switches live on one entity, so a deployer wires a single `Store` backend.
- A `Get` failure already fails `Land()`, so reject fails closed with no new code.

Cons:
- Adds surface to queue config, setting a precedent for more runtime knobs there.
- The orchestrator gains a `queueconfig` dependency it does not have today: its own `QUEUE_CONFIG_PATH` and store construction. It learns its queues from `Profiles`, so it would then hold two queue lists (`Profiles` and queue config) that can disagree.
- With the OSS YAML store each service holds its own read-once copy, so a pause change takes effect in each service only after that service restarts.
- Puts operational state on an entity documented as an immutable registry of names, with behavior living in extensions, so the entity comment must be rewritten. This is milder than it sounds: each value `Get` returns is still an immutable snapshot, and only what `Get` returns changes over time, as with any live-backed store. The lasting cost is precedent, since the next runtime knob then has an obvious home on `QueueConfig`.

## Option B: separate pause-state extension

A new extension, `queuepause`, is a per-queue source of pause state. It returns the two switches for a queue and decides nothing. OSS code enforces them, so OSS and every deployment run the same rejection and freezing logic, and a deployer only supplies where the state comes from.

The state and the contract:

```go
package entity

type QueuePauseState struct {
	// RejectLands is true while the queue refuses new land requests.
	RejectLands bool

	// FreezeProcessing is true while the queue's accepted requests are not advanced.
	FreezeProcessing bool
}
```

```go
package queuepause

type Store interface {
	Get(ctx context.Context, queue string) (entity.QueuePauseState, error)
}
```

`Get` is keyed by queue name, like `queueconfig.Store`, and returns the zero state for a queue with no entry, so absence means not paused. Both the gateway and the orchestrator resolve it, so the contract and its implementations live at `submitqueue/extension/queuepause/`.

OSS ships two implementations. `noop` never pauses and is the default when nothing is configured. `file` re-reads a small YAML file on each `Get`, so an operator pauses or unpauses a queue by editing the file, with no restart. This mirrors the file consumer gate, which re-reads on every delivery. A deployer supplies its own store backed by its config service.

`land.go` reads the state and rejects before minting an ID. A failed read fails the request, so reject fails closed:

```go
state, err := c.queuePauses.Get(ctx, queue)
if err != nil {
	return entity.LandResult{}, fmt.Errorf("failed to read pause state for queue=%s: %w", queue, err)
}
if state.RejectLands {
	return entity.LandResult{}, errs.NewUserError(&QueuePausedError{Queue: queue})
}
```

A `consumergate.Gate` adapter in the SubmitQueue domain closes the gate for a queue while `FreezeProcessing` is set. It depends on `queuepause.Store`, so it cannot live under `platform/`:

```go
func (g queuePauseGate) Enter(ctx context.Context, key consumergate.Key) (consumergate.Entry, error) {
	state, err := g.pauses.Get(ctx, key.Partition.Tenant) // tenant is the queue name
	if err != nil {
		return nil, err
	}
	return entry{blocked: state.FreezeProcessing}, nil
}
```

Pros:
- Keeps behavior off queue config.
- OSS users get a working pause, and a deployer needs one implementation per backend.
- Leaves the name `admission` free for a richer, content-based gate later.

Cons:
- New extension surface: interface, `noop` and `file` implementations, mock, README, and wiring in both services.
- The orchestrator gains a dependency it does not have today: a `queuepause.Store`.
- Queue-name validation still needs a home. If `queueconfig` leaves OSS, nothing rejects an unknown queue: `Profiles.For` falls back to the default profile for any name.

## Open questions

- **Fail open or closed on freeze.** Reject fails closed when pause state cannot be read, because `Land()` returns the error. However, the consumer treats a gate read error as open, so an adapter that returns the store's error would silently unfreeze a queue during a store outage. The adapter can instead return a blocked entry on error to fail closed. Which posture should freeze take?
- **Cancel during freeze.** Should cancel take effect while processing is frozen, as an incident workflow of freeze, cancel, unfreeze would need? If so, freeze becomes no new effects rather than nothing runs, and needs the topic on the gate key ([Cancellation](#cancellation)).
- **OSS backing.** `file` is proposed first because it needs no schema and works with a mounted config file. A MySQL-backed store would serve multi-host OSS deployments, and needs a way for an operator to write it.
- **Read cost.** The `file` store reads on every `Land()` and on every gated delivery. A short cache may be needed.
