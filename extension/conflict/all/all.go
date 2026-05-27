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

// Package all provides a conflict.Analyzer that pessimistically reports a
// conflict against every in-flight batch. It is intended as a stub for
// wiring tests and as a worst-case baseline for speculation behavior.
package all

import (
	"context"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/conflict"
)

// analyzer is a conflict.Analyzer that reports every in-flight batch as a
// conflict, classified as ConflictTypeConservative.
type analyzer struct{}

// New returns a conflict.Analyzer that reports a conflict against every
// in-flight batch.
func New() conflict.Analyzer {
	return analyzer{}
}

// Analyze returns one ConflictTypeConservative Conflict per in-flight batch,
// preserving the input order. Returns an empty slice when inFlight is empty.
func (analyzer) Analyze(_ context.Context, _ entity.Batch, inFlight []entity.Batch) ([]conflict.Conflict, error) {
	if len(inFlight) == 0 {
		return nil, nil
	}
	conflicts := make([]conflict.Conflict, len(inFlight))
	for i, b := range inFlight {
		conflicts[i] = conflict.Conflict{
			BatchID: b.ID,
			Type:    conflict.ConflictTypeConservative,
		}
	}
	return conflicts, nil
}
