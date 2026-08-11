// Copyright (c) 2025 Uber Technologies, Inc.
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

// Package observability defines the contract through which Stovepipe reports
// the current health of a queue. A Reporter answers "how is this queue doing
// right now?" by sampling state the pipeline already persists and emitting it
// as metrics, so operators can see queue health without querying the store.
//
// A Reporter is bound to a single queue at construction by its Factory, so its
// methods take no queue argument. Reporting is deliberately best-effort: Report
// returns nothing, because an observation that cannot be made must never change
// what the pipeline records or decides. Implementations emit their own error
// metrics instead, which keeps every call site a one-line defer.
package observability

//go:generate mockgen -source=observability.go -destination=mock/observability_mock.go -package=mock

import "context"

// Reporter emits one sample of the current state of the queue it is bound to.
type Reporter interface {
	// Report observes the queue and emits the result. Failures are emitted as
	// metrics rather than returned.
	Report(ctx context.Context)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs beyond that (the
// stores and providers it observes through) is injected at construction by the
// integrator.
type Config struct {
	// QueueName identifies the queue the resolved Reporter observes.
	QueueName string
}

// Factory builds the Reporter for a queue. Implementations are provided by
// integrators (and tests) and resolve whatever queue-scoped dependencies they
// observe through. The per-queue routing adapter lives in the wiring layer,
// not here.
type Factory interface {
	// For returns the Reporter for the given queue.
	For(cfg Config) (Reporter, error)
}
