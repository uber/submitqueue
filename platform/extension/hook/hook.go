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

// Hook performs a side effect in response to a lifecycle event.
type Hook interface {
	// Handle performs the side effect for event.
	//
	// Delivery is at-least-once, so the same event — identical id — may arrive
	// more than once, including after a successful Handle. Implementations must
	// be idempotent on the event id.
	//
	// Returning nil means "done with this event", which is also how a hook
	// ignores one: there is no filter or subscription API, because a hook that
	// does not care about a type simply returns nil, and routing can be added as
	// a wiring decorator if it ever pays for itself.
	//
	// Returning an error retries the event and, past the retry budget,
	// dead-letters it. Return plain errors; classification is the consumer's
	// job. An error must mean the side effect did not happen — reporting failure
	// for work that succeeded turns at-least-once into repeated duplicate
	// effects.
	//
	// A hook must never write pipeline state. Its outcome is invisible to the
	// pipeline by design: that is what makes the side effect unable to affect
	// the transition that triggered it.
	//
	// ctx is the delivery's, canceled when the consumer shuts down. Honor it
	// and bound the work Handle does: nothing interrupts a Handle that blocks,
	// and the delivery waits for it.
	Handle(ctx context.Context, event *basehook.HookEvent) error

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
