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
// delivery immediately, while a closed gate stops it. The gate never waits —
// when an Entry is blocked, the caller records the observable Parked record
// with Entry.Park and postpones the delivery, so the same message redelivers
// after a re-check delay and passes through Enter again; when the gate has
// opened, the caller removes the record with Entry.Unpark and proceeds. The
// gate owns the parked-delivery observation records (stamping ParkedAtMs and
// the gate identity captured by Enter); the caller owns the delivery outcome.
//
// The package holds the contract only: Gate and Entry (the admission
// interfaces) and Admin (the write surface used by tests and tooling).
// Implementations live in subdirectories (see file/, noop/). See
// doc/rfc/consumer-gate.md for the design.
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
	// by the gate implementation on Park; re-parking the same delivery
	// refreshes it.
	ParkedAtMs int64
}

// Gate admits deliveries past their gates. Implementations must be safe for
// concurrent use.
type Gate interface {
	// Enter checks the gate identified by key — the delivery's consumer group
	// and partition — and returns synchronously. When the gate is open, the
	// returned Entry is unblocked and the delivery may proceed at once. When
	// the gate is closed, the returned Entry is blocked; the caller parks the
	// delivery's record and defers the delivery itself.
	//
	// An error reports that gate state could not be read, without further
	// interpretation — what to do with a failed check is the caller's policy.
	Enter(ctx context.Context, key Key) (Entry, error)
}

// Entry is the outcome of Gate.Enter for one delivery.
type Entry interface {
	// Blocked reports whether the gate was closed when the delivery entered.
	// An unblocked entry needs no Park — the delivery may proceed at once.
	Blocked() bool

	// Park records the delivery as parked, adding the gate identity captured
	// by Enter and the implementation-owned ParkedAtMs. Parking the same
	// delivery again (on a re-check of a still-closed gate) overwrites the
	// previous record, so records stay bounded by currently blocked
	// deliveries. Park observes only the descriptor — it does not own or
	// mutate the source queue delivery.
	Park(ctx context.Context, descriptor DeliveryDescriptor) error

	// Unpark removes the delivery's parked record, if one exists. Unparking a
	// delivery that was never parked is a no-op, so callers may invoke it
	// unconditionally on the admit path.
	Unpark(ctx context.Context, descriptor DeliveryDescriptor) error
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
