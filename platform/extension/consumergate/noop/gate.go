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

// Package noop provides a no-op consumergate.Gate that admits every delivery
// immediately. Wire it in services and tests that do not need runtime gating.
package noop

import (
	"context"

	"github.com/uber/submitqueue/platform/extension/consumergate"
)

// Verify interface compliance at compile time.
var (
	_ consumergate.Gate  = Gate{}
	_ consumergate.Entry = Gate{}
)

// Gate is a no-op consumer gate: Enter always returns an unblocked Entry.
// The same value serves as its own Entry.
type Gate struct{}

// New returns a no-op Gate.
func New() Gate {
	return Gate{}
}

// Enter implements consumergate.Gate. The delivery is never gated.
func (g Gate) Enter(_ context.Context, _ consumergate.Key) (consumergate.Entry, error) {
	return g, nil
}

// Blocked implements consumergate.Entry. A no-op gate never blocks.
func (Gate) Blocked() bool { return false }

// Watch implements consumergate.Entry. A no-op gate never blocks, so the
// returned channel yields nil at once. It is never reached in practice because
// Blocked reports false.
func (Gate) Watch(context.Context, consumergate.DeliveryDescriptor) <-chan error {
	ch := make(chan error, 1)
	ch <- nil
	return ch
}
