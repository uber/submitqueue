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

// Package all provides a selector.Selector that promotes every Candidate
// path in a batch's speculation tree — maximum parallelism, maximum build
// cost. It is a baseline policy: build every candidate the enumerator
// produced rather than picking a subset.
package all

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/selector"
)

// allSelector is a selector.Selector that promotes every Candidate path.
type allSelector struct{}

// New returns a selector.Selector that promotes every Candidate path and
// leaves every other path as-is.
func New() selector.Selector {
	return allSelector{}
}

// Select returns a Promote decision, in tree order, for every path whose
// Status is Candidate, each naming its path by ID. Paths in any other status
// are omitted.
func (allSelector) Select(_ context.Context, tree entity.SpeculationTree) ([]entity.PathDecision, error) {
	var decisions []entity.PathDecision
	for _, p := range tree.Paths {
		if p.Status != entity.SpeculationPathStatusCandidate {
			continue
		}
		decisions = append(decisions, entity.PathDecision{
			PathID: p.ID,
			Action: entity.SpeculationPathActionPromote,
		})
	}
	return decisions, nil
}
