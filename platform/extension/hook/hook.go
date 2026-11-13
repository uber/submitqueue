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

// Package hook defines the contract for a hook: a pluggable side effect run in
// response to a pipeline lifecycle event. Warehouse exports, code-host comments,
// notifications, and audit trails are all hooks.
//
// Which hooks run is a property of the event rather than of the deployment: two
// queues in one host can point at different providers and want different
// integrations. A host therefore supplies a Hooks resolver, and the controller
// in platform/hook asks it once per event.
//
// Hooks run behind a durable queue, never inline in the pipeline, so a slow or
// failing integration cannot stall or fail the work that triggered it.
package hook

//go:generate mockgen -source=hook.go -destination=mock/hook_mock.go -package=mock

import (
	"context"

	basehook "github.com/uber/submitqueue/api/base/hook"
)

// Result describes whether a hook has finished handling an event.
type Result struct {
	// RetryNotBeforeMs is the earliest retry time in Unix milliseconds; zero means done.
	RetryNotBeforeMs int64
}

// Hook performs a side effect in response to a lifecycle event.
type Hook interface {
	// Handle is idempotent on event id and returns a zero Result when done or uninterested.
	// A positive RetryNotBeforeMs defers redelivery to all hooks until the latest requested
	// deadline, without consuming retry budget. Any hook error takes precedence over deferral.
	// Return plain errors for failures; the consumer classifies them. Honor ctx and bound
	// backend calls. Hooks must never write pipeline state or report successful effects as failures.
	Handle(ctx context.Context, event *basehook.HookEvent) (Result, error)

	// Name identifies the hook in logs, metrics, and the failure attribution
	// the controller reports. Stable and unique among the hooks a host wires.
	Name() string
}

// Hooks resolves the hooks that run for an event.
type Hooks interface {
	// For returns the hooks to run for event. They run concurrently, so the
	// order of the returned slice does not sequence them: no hook may depend on
	// another having already run.
	//
	// Returning none is an ordinary outcome: it means nothing this host wired
	// is interested in the event.
	//
	// It takes the event rather than a queue name because the envelope carries
	// no queue. Which scope selects hooks differs per domain — queue, source,
	// event type — and only the host that publishes the payload can read a
	// queue out of it, so the choice belongs to the resolver.
	//
	// Called on every delivery, so resolution must be cheap and must not fail:
	// an integration that cannot be reached is a Handle error, not an absent
	// hook.
	For(event *basehook.HookEvent) []Hook
}
