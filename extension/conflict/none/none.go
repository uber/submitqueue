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

// Package none provides a conflict.Analyzer that never reports a conflict.
// It is intended as a stub for wiring tests and as a best-case baseline for
// speculation behavior (maximum parallelism).
package none

import (
	"context"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/conflict"
)

// analyzer is a conflict.Analyzer that always returns no conflicts.
type analyzer struct{}

// New returns a conflict.Analyzer that never reports a conflict.
func New() conflict.Analyzer {
	return analyzer{}
}

// Analyze always returns a nil conflict slice, regardless of inputs.
func (analyzer) Analyze(_ context.Context, _ entity.Batch, _ []entity.Batch) ([]conflict.Conflict, error) {
	return nil, nil
}
