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

// Package noop provides a hook.Hook that accepts every event and does nothing.
// It is a placeholder for a host that wants the stage registered before it has
// any integration — a resolver returning no hooks does the same thing. Either
// way events are still published, consumed, and acked, so turning a real hook on
// later changes only what happens to the event, not whether the seam works.
package noop

import (
	"context"

	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/extension/hook"
)

// Verify interface compliance at compile time.
var _ hook.Hook = Hook{}

// Hook is a hook that discards every event.
type Hook struct{}

// New returns a no-op Hook.
func New() Hook {
	return Hook{}
}

// Handle implements hook.Hook. The event is discarded.
func (Hook) Handle(context.Context, *basehook.HookEvent) error { return nil }

// Name implements hook.Hook.
func (Hook) Name() string { return "noop" }
