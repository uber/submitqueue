// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package consumergate defines the consumer-gate extension: runtime stop/start
// of individual queue controllers without stopping the service that hosts them.
//
// A gate is keyed by consumer group (the controller's stable runtime name),
// optionally narrowed to a single partition. Gate.Enter checks a delivery's
// gate key synchronously and returns an Entry: an open gate admits the
// delivery immediately, while a closed gate holds it. A blocked Entry does not
// block the caller — Entry.Watch records the parked delivery and returns a
// channel that yields exactly one result: nil when the gate opens, or an error
// if gate state cannot be read or the record written. This lets the caller
// multiplex the wait against its own events (context cancellation, visibility
// extension) in a single select. Callers that only need the simple blocking
// behaviour use the package-level Wait helper. The gate owns the monitoring
// goroutine and the parked-delivery observation records: it records the parked
// delivery before monitoring (stamping ParkedAtMs) and removes the record when
// monitoring ends, so parked records describe only deliveries currently held
// behind a gate.
//
// The package holds the contract only: Gate and Entry (the admission
// interfaces), the Wait helper, Admin (the write surface used by tests and
// tooling), Config, and the Factory interface. Implementations live in
// subdirectories (see file/, noop/). See doc/rfc/consumer-gate.md for the
// design.
package consumergate

//go:generate mockgen -source=consumergate.go -destination=mock/consumergate_mock.go -package=mock

import "context"

// Key identifies a gate: a consumer group, optionally narrowed to one partition.
type Key struct {
	// ConsumerGroup is the gated controller's consumer group — its stable runtime name.
	ConsumerGroup string

	// PartitionKey optionally narrows the gate to a single partition.
	// Empty gates every partition of the consumer group.
	PartitionKey string
}

// Metadata records why a gate was closed, for the operator who finds it later.
type Metadata struct {
	// Reason is a human-readable explanation for the closure.
	Reason string

	// CreatedBy identifies who or what closed the gate.
	CreatedBy string

	// CreatedAtMs is when the gate was closed (Unix milliseconds).
	CreatedAtMs int64
}

// DeliveryDescriptor is the caller-owned description of a delivery that may
// be parked. It contains only values known by the consumer; the gate
// implementation owns the gate identity and parked timestamp added to the
// observable Parked record.
type DeliveryDescriptor struct {
	// Topic is the topic key (the stable logical name) the delivery was
	// consumed from.
	Topic string

	// MessageID is the queue message ID of the delivery.
	MessageID string

	// Payload is the message payload, recorded so an observer can assert on it.
	Payload []byte

	// Attempt is the delivery attempt the message is on.
	Attempt int
}

// Parked is the gate-owned observation record for one blocked delivery.
type Parked struct {
	// ConsumerGroup is the consumer group whose gate is consulted.
	ConsumerGroup string

	// Topic is the topic key (the stable logical name) the delivery was
	// consumed from.
	Topic string

	// MessageID is the queue message ID of the delivery.
	MessageID string

	// PartitionKey is the partition the delivery belongs to.
	PartitionKey string

	// Payload is the message payload, recorded so an observer can assert on it.
	Payload []byte

	// Attempt is the delivery attempt the message is on.
	Attempt int

	// ParkedAtMs is when the delivery was parked (Unix milliseconds). Stamped
	// by the gate implementation when it actually blocks.
	ParkedAtMs int64
}

// Gate admits deliveries past their gates. Implementations must be safe for
// concurrent use.
type Gate interface {
	// Enter checks the gate identified by key — the delivery's consumer group
	// and partition — and returns synchronously. When the gate is open, the
	// returned Entry is unblocked and the delivery may proceed at once; no
	// other input is needed on that path. When the gate is closed, the
	// returned Entry is blocked and its Watch monitors the gate until it
	// opens.
	//
	// An error reports that gate state could not be read, without further
	// interpretation — what to do with a failed check is the caller's policy.
	Enter(ctx context.Context, key Key) (Entry, error)
}

// Entry is the outcome of Gate.Enter for one delivery.
type Entry interface {
	// Blocked reports whether the gate was closed when the delivery entered.
	// An unblocked entry needs no Watch — the delivery may proceed at once.
	Blocked() bool

	// Watch records delivery as parked, adding the gate identity captured by
	// Enter and the implementation-owned ParkedAtMs, and begins monitoring the
	// gate. It returns a channel that yields exactly one value: nil when the gate
	// opens, or a non-nil error if gate state could not be read or the record
	// written — without further interpretation, as what to do with a failed wait
	// is the caller's policy. If ctx is cancelled while monitoring, the channel
	// yields ctx.Err(). The implementation removes the parked record before
	// yielding on every terminal path, so ListParked contains only deliveries
	// currently blocked behind a gate.
	//
	// The returned channel is buffered so the monitoring goroutine never blocks
	// on its single send: a caller may cancel ctx and walk away without draining
	// it, and the goroutine still exits. Watch must be called at most once per
	// blocked Entry.
	Watch(ctx context.Context, descriptor DeliveryDescriptor) <-chan error
}

// Wait blocks until the gate behind entry opens or fails, or ctx is cancelled.
// It is the simple blocking adapter over Entry.Watch for callers (and tests)
// that do not need to multiplex the wait against other events: it returns nil
// for an unblocked entry or when the gate opens, the gate's error if the wait
// failed, or ctx.Err() after the watcher observes cancellation and completes
// its cleanup.
func Wait(ctx context.Context, entry Entry, descriptor DeliveryDescriptor) error {
	if !entry.Blocked() {
		return nil
	}
	return <-entry.Watch(ctx, descriptor)
}

// Admin is the write surface used by tests and tooling to operate gates and
// inspect what a stopped controller is holding.
type Admin interface {
	// Close closes the gate for the key. Closing an already-closed gate
	// overwrites its metadata.
	Close(ctx context.Context, key Key, meta Metadata) error

	// Open opens the gate for the key. Opening an already-open gate is a no-op.
	Open(ctx context.Context, key Key) error

	// ListParked returns every delivery currently parked for the consumer group.
	// Callers may filter by topic or message ID.
	ListParked(ctx context.Context, consumerGroup string) ([]Parked, error)
}

// Config holds the knobs for polling-based gate implementations.
type Config struct {
	// PollIntervalMs is the cadence at which polling implementations re-read
	// gate state (milliseconds). Notification-capable implementations may
	// ignore it.
	PollIntervalMs int64
}

// DefaultConfig returns the default gate configuration: 1s poll interval.
func DefaultConfig() Config {
	return Config{
		PollIntervalMs: 1000,
	}
}

// Factory creates Gate instances for dependency injection. Factory
// implementations live in the wiring layer, not in this package.
type Factory interface {
	// For returns a Gate for the given configuration.
	For(cfg Config) (Gate, error)
}
