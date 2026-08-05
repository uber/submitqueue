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

// Package bestfirst implements the default speculation path Generator. Every
// candidate path is a bet: building it pays off only if every one of its
// dependency assumptions matches how the dependency actually resolves. The
// generator prices each path by the probability of exactly that — resolved
// dependencies are pinned to their outcome, undecided ones get a success
// probability from the injected scorer — and yields candidates in strictly
// non-increasing price order, lazily, so the exponential path space is never
// materialized.
package bestfirst

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
)

// New returns the best-first Generator. The scorer supplies the success
// probability of undecided dependency batches; it is called at Open, at most
// once per batch per run, so anything expensive belongs behind the scorer's own
// cache. Wiring is trusted: a nil scorer panics on first use.
func New(sc scorer.Scorer) generator.Generator {
	return &bestFirst{scorer: sc}
}

// bestFirst builds one iterator per Open call; the generator itself carries no
// cross-run state, matching the speculate controller's recompute-from-scratch
// model.
type bestFirst struct {
	// scorer supplies success probabilities for undecided dependency batches.
	scorer scorer.Scorer
}

// Open compiles every Speculating head in the snapshot into its lazy
// enumeration state and seeds the shared best-first heap with each head's most
// likely path. All scorer calls happen here; pulling from the iterator does no
// further I/O.
func (g *bestFirst) Open(ctx context.Context, batches []entity.Batch) (generator.PathIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	byID := make(map[string]entity.Batch, len(batches))
	for _, b := range batches {
		byID[b.ID] = b
	}

	// Heads are seeded in queue order so that equally priced candidates — most
	// notably the probability-1 paths of heads whose dependencies are all
	// decided — stream oldest first.
	heads := make([]entity.Batch, 0, len(batches))
	for _, b := range batches {
		if b.State == entity.BatchStateSpeculating {
			heads = append(heads, b)
		}
	}
	slices.SortFunc(heads, func(a, b entity.Batch) int { return compareQueueOrder(a.ID, b.ID) })

	it := &iterator{}
	scores := make(map[string]float64, len(batches))
	for _, head := range heads {
		hs, ok, err := g.prepareHead(ctx, head, byID, scores)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		it.addHead(hs)
	}
	return it, nil
}

// prepareHead compiles one head into its stream state: the modal path template,
// the swing dependencies in flip order, and the modal path's probability.
//
// ok is false when the snapshot cannot support coherent candidates for this
// head — a dependency is missing from the snapshot, is the head itself, or is
// in a state the model does not cover. The head then yields nothing this run;
// a later run sees a complete snapshot and plans it again.
func (g *bestFirst) prepareHead(ctx context.Context, head entity.Batch, byID map[string]entity.Batch, scores map[string]float64) (headStream, bool, error) {
	// Dependency order must be canonical: entity.SpeculationPath hashes the
	// ordered dependencies into the path ID, and a run that ordered them
	// differently would re-propose paths the queue already built. The batch's
	// own Dependencies order is unspecified, so it is normalized here.
	depIDs := slices.Clone(head.Dependencies)
	slices.SortFunc(depIDs, compareQueueOrder)
	depIDs = slices.Compact(depIDs)

	hs := headStream{
		template: entity.SpeculationPath{
			Head:         head.ID,
			Dependencies: make([]entity.PathDependency, 0, len(depIDs)),
		},
		baseProbability: 1,
	}
	for _, depID := range depIDs {
		if depID == head.ID {
			return headStream{}, false, nil
		}
		dep, present := byID[depID]
		if !present {
			return headStream{}, false, nil
		}
		p, known, err := g.successProbability(ctx, dep, scores)
		if err != nil {
			return headStream{}, false, err
		}
		if !known {
			return headStream{}, false, nil
		}

		modal, flipped := entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails
		if p < 0.5 {
			modal, flipped = flipped, modal
		}
		hs.template.Dependencies = append(hs.template.Dependencies, entity.PathDependency{Batch: depID, Assumption: modal})

		// Only genuinely undecided dependencies swing: a probability of exactly
		// 0 or 1 pins the assumption, contributes no probability factor, and
		// suppresses the opposite branch outright — its paths would be priced 0.
		if p > 0 && p < 1 {
			q := math.Max(p, 1-p)
			hs.flips = append(hs.flips, flip{
				depIndex:   len(hs.template.Dependencies) - 1,
				assumption: flipped,
				ratio:      (1 - q) / q,
			})
			hs.baseProbability *= q
		}
	}
	// Cheapest flips first; the stable sort keeps queue order between equal
	// ratios, so the whole stream stays deterministic.
	slices.SortStableFunc(hs.flips, func(a, b flip) int { return cmp.Compare(b.ratio, a.ratio) })
	return hs, true, nil
}

// successProbability is the probability the model assigns to dep resolving as
// succeeded.
//
// Resolved states are facts: 1 or 0, per the contract that a candidate never
// contradicts a resolved outcome. Merging and Cancelling are not facts, but
// their modal outcomes are lopsided enough to price as certainties — a Merging
// batch has already passed a build and been handed off to land, and a
// Cancelling batch is halted, resurrected only by a lost cancel race. Pricing
// them at 1 and 0 suppresses the near-worthless opposite branch; if the long
// shot lands anyway, the controller refutes the affected paths and the next
// run re-plans against the new facts, like any other lost bet.
//
// Undecided pipeline states ask the scorer, memoized per run. known is false
// for states the model does not cover (Creating, unknown), which are not
// eligible dependencies in the first place.
func (g *bestFirst) successProbability(ctx context.Context, dep entity.Batch, scores map[string]float64) (p float64, known bool, err error) {
	switch dep.State {
	case entity.BatchStateSucceeded, entity.BatchStateMerging:
		return 1, true, nil
	case entity.BatchStateFailed, entity.BatchStateCancelled, entity.BatchStateCancelling:
		return 0, true, nil
	case entity.BatchStateCreated, entity.BatchStateSpeculating:
		if p, ok := scores[dep.ID]; ok {
			return p, true, nil
		}
		p, err := g.scorer.Score(ctx, dep)
		if err != nil {
			return 0, false, fmt.Errorf("score dependency %s: %w", dep.ID, err)
		}
		if math.IsNaN(p) || p < 0 || p > 1 {
			return 0, false, fmt.Errorf("scorer returned probability %v for batch %s, want a value in [0, 1]", p, dep.ID)
		}
		scores[dep.ID] = p
		return p, true, nil
	default:
		return 0, false, nil
	}
}

// compareQueueOrder orders batch IDs by queue order: ascending counter for the
// documented "<queue>/batch/<counter>" ID format. Dependencies always share
// their head's queue, so the counter alone decides. IDs whose counter does not
// parse sort after all that do, then by plain string comparison, keeping the
// order total and deterministic for any input.
func compareQueueOrder(a, b string) int {
	na, aok := batchCounter(a)
	nb, bok := batchCounter(b)
	switch {
	case aok && bok:
		if c := cmp.Compare(na, nb); c != 0 {
			return c
		}
		return strings.Compare(a, b)
	case aok:
		return -1
	case bok:
		return 1
	default:
		return strings.Compare(a, b)
	}
}

// batchCounter extracts the numeric counter from a "<queue>/batch/<counter>"
// batch ID.
func batchCounter(id string) (int64, bool) {
	i := strings.LastIndexByte(id, '/')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(id[i+1:], 10, 64)
	return n, err == nil
}
