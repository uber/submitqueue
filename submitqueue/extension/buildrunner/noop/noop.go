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

// Package noop provides a buildrunner.BuildRunner that performs no real
// work: every triggered build immediately succeeds. It is intended as a
// stub for wiring tests and as a best-case baseline where every build
// passes.
package noop

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// runner is a buildrunner.BuildRunner that does no real work and reports
// every build as immediately succeeded. The atomic counter hands out
// unique build IDs and makes the type safe for concurrent use.
type runner struct {
	counter atomic.Uint64
}

// New returns a buildrunner.BuildRunner that performs no real work.
func New() buildrunner.BuildRunner {
	return &runner{}
}

// Trigger returns a unique build ID without contacting any runner.
// Inputs are ignored.
func (r *runner) Trigger(_ context.Context, _ []entity.Change, _ []entity.Change, _ entity.BuildMetadata) (entity.BuildID, error) {
	return entity.BuildID{ID: fmt.Sprintf("noop-%d", r.counter.Add(1))}, nil
}

// Status always reports BuildStatusSucceeded with no metadata.
func (r *runner) Status(_ context.Context, _ entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	return entity.BuildStatusSucceeded, nil, nil
}

// Cancel is a no-op.
func (r *runner) Cancel(_ context.Context, _ entity.BuildID) error {
	return nil
}
