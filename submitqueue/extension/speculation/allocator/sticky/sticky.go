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

// Package sticky provides an allocator.Allocator that fills only free budget
// slots and leaves every in-flight build running — it never preempts. Its policy
// counterpart is a preempting allocator, which would cancel a low-value in-flight
// path to fund a better candidate; sticky trades that responsiveness for never
// discarding a build it has already started.
package sticky

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/allocator"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
)

// alloc is a sticky allocator.Allocator.
type alloc struct {
	// budget is the queue's cap on concurrently held build slots, measured in
	// builds: one slot per path whose attempt is pending, building, or
	// cancelling, whatever that build's size. The unit is deliberately coarse —
	// an allocator that understands build size could weight paths instead.
	budget int
}

// New returns a sticky allocator.Allocator with the given budget, measured in
// concurrent builds.
func New(budget int) allocator.Allocator {
	return alloc{budget: budget}
}

// Allocate keeps every in-flight path funded and pulls candidates in order into
// the remaining free budget, proposing a build for each new path until the
// budget fills. It never proposes a cancel.
//
// A cancelled or expired ctx aborts with its error and no actions. The run's
// output is all-or-nothing: a partial action list would fund an arbitrary
// prefix of the ranking, so a caller that gave up mid-run gets nothing rather
// than a half-spent budget.
func (a alloc) Allocate(ctx context.Context, pathSets []entity.SpeculationPathSet, iter generator.Iterator) ([]entity.Speculation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Paths already holding a build slot. This set does double duty: it is both
	// the count of budget already spent and the guard against proposing a second
	// build for something already running, so a status belongs here if either
	// reason applies.
	funded := make(map[string]bool)
	// Paths whose build already finished. They hold no slot, but they must not
	// be built again either.
	terminal := make(map[string]bool)
	for _, set := range pathSets {
		for _, entry := range set.Paths {
			if entry.Status == entity.SpeculationPathStatusUnknown {
				// A stored path always carries a real status, so this record is
				// corrupt. Fail fast rather than guess: any reading of it — slot
				// held or free, buildable or not — could be wrong in a way that
				// either pins a slot forever or double-builds a path.
				return nil, fmt.Errorf("speculation path %q of head %q has no status", entry.ID, set.Head)
			}
			if entry.Status.IsTerminal() {
				terminal[entry.ID] = true
				continue
			}
			// Anything else that has not finished is still holding a slot:
			// pending and building obviously, and cancelling too, since the build
			// keeps its slot until it actually stops.
			//
			// Only the path's status matters. No batch state enters this
			// decision — "landing" and the rest are states of a batch, never of
			// a path — so the rule is simply that CI is still busy with it.
			funded[entry.ID] = true
		}
	}

	free := a.budget - len(funded)

	var out []entity.Speculation
	proposed := make(map[string]bool)
	for free > 0 {
		// Next reports cancellation too, but check here as well: a generator
		// that ignores ctx must not be able to spin this loop forever.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c, ok, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			// ok=false means the generator has nothing left to offer: every path
			// the queue could speculate on has been seen. Stopping here with
			// budget still free is normal, not a failure — there is simply
			// nothing else worth building right now.
			break
		}
		id := c.Path.ID()
		if terminal[id] {
			// This path's build already ran. The generator enumerates the whole
			// space with no knowledge of the path sets, so finished paths come
			// back round as candidates; they need no slot and must not be rebuilt.
			continue
		}
		if funded[id] || proposed[id] {
			// funded: this path is already building. It needs no new action and
			// is already charging the budget, so skip it without spending
			// anything.
			//
			// proposed: a generator is not supposed to repeat a candidate, so
			// this should never fire. It is a cheap guard against one that does,
			// since a duplicate would otherwise be paid for twice.
			continue
		}
		out = append(out, entity.Speculation{Path: c.Path, Action: entity.PathActionBuild})
		proposed[id] = true
		free--
	}
	return out, nil
}
