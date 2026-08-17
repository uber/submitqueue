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

// Package composite provides a hook.Hook that fans one event out to several
// children. It is how a host wires more than one integration, since the
// dispatcher takes a single hook.
//
// Every child runs on every event, even after one fails, so a broken
// integration cannot stop the others from seeing the event. Failures are
// collected and joined, each wrapped with the name of the child that raised it,
// so the error reaching the dispatcher says which integration failed rather than
// just that something did.
//
// # Children share one retry budget
//
// The composite is a single consumer, so a retry re-delivers the event to every
// child, including the ones that already succeeded. Two consequences: children
// must be idempotent on the event id (the hook contract requires this anyway),
// and one persistently failing child spends the budget for all of them, so the
// event eventually dead-letters even though the others were fine.
//
// The fix is a consumer group per hook on the shared hook topic, which the queue
// cannot express today: the registry admits one consumer group per topic key,
// and a rejection moves the shared message row to the DLQ for every group rather
// than only the one that rejected it. Until both change, prefer wiring children
// whose failure modes are independent and short-lived, and treat a chronically
// failing integration as something to remove from the composite rather than to
// absorb.
package composite

import (
	"context"
	"errors"
	"fmt"

	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/extension/hook"
)

// Verify interface compliance at compile time.
var _ hook.Hook = Hook{}

// Hook fans an event out to every child hook.
type Hook struct {
	// children are the hooks the event is handed to, in wiring order.
	children []hook.Hook
}

// New returns a Hook that hands each event to every child in the order given.
// With no children it accepts every event and does nothing.
func New(children ...hook.Hook) Hook {
	return Hook{children: children}
}

// Handle implements hook.Hook. It runs every child and returns the joined
// failures, each attributed to the child that raised it, or nil when all
// succeeded.
func (h Hook) Handle(ctx context.Context, event *basehook.HookEvent) error {
	var failures []error
	for _, child := range h.children {
		if err := child.Handle(ctx, event); err != nil {
			failures = append(failures, fmt.Errorf("hook %s: %w", child.Name(), err))
		}
	}
	return errors.Join(failures...)
}

// Name implements hook.Hook.
func (Hook) Name() string { return "composite" }
