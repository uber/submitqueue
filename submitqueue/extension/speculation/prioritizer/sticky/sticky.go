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

// Package sticky provides a prioritizer.Prioritizer implementing sticky
// build slots: a running path keeps its slot until it resolves, and only the
// budget left over is handed out to pending candidates. It never preempts a
// running path to admit a higher-scored one.
package sticky

import (
	"context"
	"sort"
	"strings"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/prioritizationlimit"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/prioritizer"
)

// stickyPrioritizer is a prioritizer.Prioritizer that never emits Cancel: it
// only fills whatever budget running paths have not already claimed.
type stickyPrioritizer struct {
	// limit reports the queue's current concurrent-build budget. It is
	// consulted on every Prioritize call rather than cached, since the value
	// may change between calls.
	limit prioritizationlimit.PrioritizationLimit
}

// New returns a prioritizer.Prioritizer implementing sticky build slots: it
// never emits Cancel, and admits Selected candidates by descending Score into
// whatever budget limit leaves free once running paths are accounted for.
func New(limit prioritizationlimit.PrioritizationLimit) prioritizer.Prioritizer {
	return &stickyPrioritizer{limit: limit}
}

// Prioritize counts candidates already holding a slot (Prioritized or
// Building) against the queue's prioritization limit to compute the
// remaining free budget, then admits the free budget's worth of Selected
// candidates, highest Score first, with a Promote decision (naming the path by
// its ID) each. Ties break
// on Path.Head, then on the base joined with commas, so repeated rounds over
// the same input are stable. Candidates in any other status, and any Selected
// candidate beyond the free budget, are left as-is: sticky prioritization
// never cancels a running path.
func (p *stickyPrioritizer) Prioritize(ctx context.Context, candidates []entity.SpeculationPathInfo) ([]entity.PathDecision, error) {
	used := 0
	var pending []entity.SpeculationPathInfo
	for _, c := range candidates {
		switch c.Status {
		case entity.SpeculationPathStatusPrioritized, entity.SpeculationPathStatusBuilding:
			used++
		case entity.SpeculationPathStatusSelected:
			pending = append(pending, c)
		}
	}

	limit, err := p.limit.Limit(ctx)
	if err != nil {
		return nil, err
	}
	free := limit - used
	if free < 0 {
		free = 0
	}
	if free == 0 || len(pending) == 0 {
		return nil, nil
	}

	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].Score != pending[j].Score {
			return pending[i].Score > pending[j].Score
		}
		if pending[i].Path.Head != pending[j].Path.Head {
			return pending[i].Path.Head < pending[j].Path.Head
		}
		return strings.Join(pending[i].Path.Base, ",") < strings.Join(pending[j].Path.Base, ",")
	})

	if free > len(pending) {
		free = len(pending)
	}
	decisions := make([]entity.PathDecision, free)
	for i := 0; i < free; i++ {
		decisions[i] = entity.PathDecision{
			PathID: pending[i].ID,
			Action: entity.SpeculationPathActionPromote,
		}
	}
	return decisions, nil
}
