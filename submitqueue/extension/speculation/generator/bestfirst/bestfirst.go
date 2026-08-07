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

// Package bestfirst provides a probability-ordered speculation path generator.
//
// Throughout, a probability is the [0, 1] value a scorer gives; a score is its
// logarithm. Scores are summed and compared, never exponentiated, so wide
// heads cannot underflow into ties. The algorithm — per-head streams
// enumerating flip subsets lazily, merged through one global heap — is
// documented in doc/rfc/submitqueue/speculation-generator-best-first.md.
package bestfirst

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
)

// bestFirst generates candidate paths using independent dependency
// probabilities supplied by scorer.
type bestFirst struct {
	scorer scorer.Scorer
}

var _ generator.Generator = (*bestFirst)(nil)

// New returns a Generator that ranks paths by the probability that every
// unresolved dependency assumption holds. The scorer is called at most once
// per unresolved dependency batch in each Generate call.
func New(s scorer.Scorer) generator.Generator {
	if s == nil {
		panic("bestfirst.New: scorer must not be nil")
	}
	return &bestFirst{scorer: s}
}

// Generate validates the queue snapshot, scores the unresolved dependencies of
// Speculating heads, and opens a lazy global best-first iterator.
func (g *bestFirst) Generate(ctx context.Context, batches []entity.Batch) (generator.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	batchByID := make(map[string]entity.Batch, len(batches))
	for _, batch := range batches {
		if batch.ID == "" {
			return nil, errors.New("batch has an empty ID")
		}
		if _, exists := batchByID[batch.ID]; exists {
			return nil, fmt.Errorf("duplicate batch ID %q", batch.ID)
		}
		batchByID[batch.ID] = batch
	}

	heads := make([]entity.Batch, 0)
	unresolvedDependencyIDs := make(map[string]struct{})
	for _, batch := range batches {
		if batch.State != entity.BatchStateSpeculating {
			continue
		}
		heads = append(heads, batch)

		seen := make(map[string]struct{}, len(batch.Dependencies))
		for _, dependencyID := range batch.Dependencies {
			if dependencyID == "" {
				return nil, fmt.Errorf("head %q has an empty dependency ID", batch.ID)
			}
			if dependencyID == batch.ID {
				return nil, fmt.Errorf("head %q depends on itself", batch.ID)
			}
			if _, duplicate := seen[dependencyID]; duplicate {
				return nil, fmt.Errorf("head %q repeats dependency %q", batch.ID, dependencyID)
			}
			seen[dependencyID] = struct{}{}

			dependency, exists := batchByID[dependencyID]
			if !exists {
				return nil, fmt.Errorf("head %q references dependency %q missing from the snapshot", batch.ID, dependencyID)
			}
			if dependency.State == entity.BatchStateUnknown {
				return nil, fmt.Errorf("dependency %q has an unknown state", dependencyID)
			}
			if _, resolved := resolvedAssumption(dependency.State); !resolved {
				unresolvedDependencyIDs[dependencyID] = struct{}{}
			}
		}
	}

	// Score each unique unresolved dependency once, in a stable order. A score
	// outside [0, 1] is rejected here because everything downstream treats it
	// as a probability, and a bad value would corrupt the ordering silently.
	ids := make([]string, 0, len(unresolvedDependencyIDs))
	for id := range unresolvedDependencyIDs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	probabilityByID := make(map[string]float64, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		probability, err := g.scorer.Score(ctx, batchByID[id])
		if err != nil {
			return nil, fmt.Errorf("score dependency %q: %w", id, err)
		}
		if math.IsNaN(probability) || probability < 0 || probability > 1 {
			return nil, fmt.Errorf("scorer returned %v for batch %q: want a probability in [0, 1]", probability, id)
		}
		probabilityByID[id] = probability
	}

	it := &candidateIterator{}
	heap.Init(&it.candidates)
	for _, head := range heads {
		stream := newPathStream(head, batchByID, probabilityByID)
		heap.Push(&it.candidates, candidateItem{
			stream: stream,
			score:  stream.bestScore,
		})
	}
	return it, nil
}

// resolvedAssumption converts a terminal dependency outcome into the only
// coherent path assumption. Cancelling remains unresolved because cancellation
// is best-effort and the batch may still succeed.
func resolvedAssumption(state entity.BatchState) (entity.DependencyAssumption, bool) {
	switch state {
	case entity.BatchStateSucceeded:
		return entity.DependencyAssumptionSucceeds, true
	case entity.BatchStateFailed, entity.BatchStateCancelled:
		return entity.DependencyAssumptionFails, true
	default:
		return entity.DependencyAssumptionUnknown, false
	}
}

// dependencyVariable is one unresolved dependency and the penalty for flipping
// away from its most likely outcome.
type dependencyVariable struct {
	// dependencyIndex is the dependency's position in the head's dependency
	// list — the slot in a built path a flip rewrites.
	dependencyIndex int
	// flipCost is what flipping adds to a path's score:
	// log(opposite) - log(preferred). At most 0, and -Inf when the opposite
	// side cannot happen.
	flipCost float64
}

// pathStream lazily enumerates one head's paths in descending probability. Its
// local heap holds the flip subsets reached but not yet handed out; the head's
// current best candidate sits in the iterator's global heap instead.
type pathStream struct {
	// head is the head batch's ID, the Head of every path the stream yields.
	head string
	// base is the most likely path's assumptions in queue order: the preferred
	// side where unresolved, the forced side where resolved. build copies it
	// and flips the taken slots.
	base []entity.PathDependency
	// variables are the head's unresolved dependencies, sorted by ascending
	// flip penalty once prepared.
	variables []dependencyVariable
	// bestScore is the no-flip path's score: the preferred log probabilities
	// summed. Every other path's score is this plus its taken flip costs.
	bestScore float64
	// prepared reports that variables are sorted and the local heap is seeded,
	// which is deferred until the head is first pulled.
	prepared bool
	// subsets is the local heap of flip subsets.
	subsets flipHeap
}

func newPathStream(head entity.Batch, batchByID map[string]entity.Batch, probabilityByID map[string]float64) *pathStream {
	stream := &pathStream{
		head: head.ID,
		base: make([]entity.PathDependency, len(head.Dependencies)),
	}
	for i, dependencyID := range head.Dependencies {
		if assumption, resolved := resolvedAssumption(batchByID[dependencyID].State); resolved {
			// A fact: it contributes probability 1 (log 0) and offers no flip.
			stream.base[i] = entity.PathDependency{Batch: dependencyID, Assumption: assumption}
			continue
		}

		// The preferred side is the more probable one; on an exact tie,
		// succeeds. Its probability is at least 0.5, so its log is finite.
		probability := probabilityByID[dependencyID]
		preferredProbability := math.Max(probability, 1-probability)
		oppositeProbability := math.Min(probability, 1-probability)
		preferred := entity.DependencyAssumptionFails
		if probability >= 0.5 {
			preferred = entity.DependencyAssumptionSucceeds
		}
		stream.base[i] = entity.PathDependency{Batch: dependencyID, Assumption: preferred}
		stream.bestScore += math.Log(preferredProbability)
		stream.variables = append(stream.variables, dependencyVariable{
			dependencyIndex: i,
			flipCost:        math.Log(oppositeProbability) - math.Log(preferredProbability),
		})
	}
	return stream
}

// prepare sorts the variables cheapest-flip-first and seeds the local heap
// with the single cheapest flip — the no-flip path's lone successor. Deferred
// until the stream first advances, so a head nobody pulls never pays the sort.
func (s *pathStream) prepare() {
	if s.prepared {
		return
	}
	slices.SortFunc(s.variables, func(left, right dependencyVariable) int {
		if left.flipCost != right.flipCost {
			// Descending: costs are at most 0 and closest to 0 is cheapest.
			return cmp.Compare(right.flipCost, left.flipCost)
		}
		return cmp.Compare(left.dependencyIndex, right.dependencyIndex)
	})
	s.prepared = true
	if len(s.variables) > 0 {
		s.push([]int{0})
	}
}

// next returns the head's next-best flip subset and pushes that subset's
// extend and swap successors, so the local heap always holds the subsets that
// could follow. ok is false once the head is exhausted.
//
// The extend/swap tree reaches every subset exactly once, and because the
// variables are sorted and every flipCost is at most 0, no successor outscores
// its parent — in floating point as computed, since scoreFor sums parent and
// child from bestScore over the identical prefix.
func (s *pathStream) next() (flipped []int, score float64, ok bool) {
	s.prepare()
	if s.subsets.Len() == 0 {
		return nil, 0, false
	}
	entry := heap.Pop(&s.subsets).(flipEntry)

	if j := entry.flipped[len(entry.flipped)-1]; j+1 < len(s.variables) {
		s.push(appendCopy(entry.flipped, j+1))      // extend: also flip j+1
		s.push(replaceLastCopy(entry.flipped, j+1)) // swap: trade flip j for j+1
	}
	return entry.flipped, entry.score, true
}

func (s *pathStream) push(flipped []int) {
	heap.Push(&s.subsets, flipEntry{flipped: flipped, score: s.scoreFor(flipped)})
}

// scoreFor sums the path's score from bestScore in ascending flip order. It is
// never adjusted incrementally from a parent's score: floating-point addition
// does not associate, and the heap ordering depends on parent and child being
// summed the same way. The fresh sum is also why a -Inf flip needs no special
// case — nothing is ever subtracted.
func (s *pathStream) scoreFor(flipped []int) float64 {
	score := s.bestScore
	for _, i := range flipped {
		score += s.variables[i].flipCost
	}
	return score
}

// build constructs the path taking the given flips. The returned path owns its
// dependencies.
func (s *pathStream) build(flipped []int) entity.SpeculationPath {
	dependencies := make([]entity.PathDependency, len(s.base))
	copy(dependencies, s.base)
	for _, i := range flipped {
		at := s.variables[i].dependencyIndex
		dependencies[at].Assumption = opposite(dependencies[at].Assumption)
	}
	return entity.SpeculationPath{Head: s.head, Dependencies: dependencies}
}

// opposite is the other side of an assumption. Flips only apply to unresolved
// dependencies, whose base assumption is exactly one of the two sides.
func opposite(assumption entity.DependencyAssumption) entity.DependencyAssumption {
	if assumption == entity.DependencyAssumptionSucceeds {
		return entity.DependencyAssumptionFails
	}
	return entity.DependencyAssumptionSucceeds
}

func appendCopy(values []int, value int) []int {
	result := make([]int, len(values)+1)
	copy(result, values)
	result[len(values)] = value
	return result
}

func replaceLastCopy(values []int, value int) []int {
	result := make([]int, len(values))
	copy(result, values)
	result[len(result)-1] = value
	return result
}

// flipEntry is one subset in a stream's local heap: the flips a path takes, as
// ascending indexes into the sorted variables, plus the path's score.
type flipEntry struct {
	flipped []int
	score   float64
}

// flipHeap orders a stream's subsets best-first: highest score first, ties
// preferring fewer flips and then the lexicographically smaller subset. That
// is a strict total order, so iteration is deterministic without insertion
// counters.
type flipHeap []flipEntry

var _ heap.Interface = (*flipHeap)(nil)

func (h flipHeap) Len() int { return len(h) }

func (h flipHeap) Less(i, j int) bool {
	left, right := h[i], h[j]
	if left.score != right.score {
		return left.score > right.score
	}
	if len(left.flipped) != len(right.flipped) {
		return len(left.flipped) < len(right.flipped)
	}
	return slices.Compare(left.flipped, right.flipped) < 0
}

func (h flipHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *flipHeap) Push(value any) {
	*h = append(*h, value.(flipEntry))
}

func (h *flipHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// candidateIterator performs a k-way merge of the per-head ordered streams.
type candidateIterator struct {
	candidates candidateHeap
}

var _ generator.Iterator = (*candidateIterator)(nil)

// Next returns the best remaining path across all heads: it pops the global
// best, advances only that head's stream, and reinserts the head's next
// candidate. The path is built only here, when it is handed out, and nothing
// after the pop can fail — a cancelled ctx does not consume a candidate.
func (i *candidateIterator) Next(ctx context.Context) (entity.CandidatePath, bool, error) {
	if err := ctx.Err(); err != nil {
		return entity.CandidatePath{}, false, err
	}
	if i.candidates.Len() == 0 {
		return entity.CandidatePath{}, false, nil
	}

	item := heap.Pop(&i.candidates).(candidateItem)
	if flipped, score, ok := item.stream.next(); ok {
		heap.Push(&i.candidates, candidateItem{stream: item.stream, flipped: flipped, score: score})
	}
	return entity.CandidatePath{
		Path:         item.stream.build(item.flipped),
		RankingScore: item.score,
	}, true, nil
}

// candidateItem is one head's current best: the stream it advances, the flips
// the path takes, and its score. flipped is nil for the head's no-flip path.
type candidateItem struct {
	stream  *pathStream
	flipped []int
	score   float64
}

// candidateHeap is a max-heap by score, holding one item per live head.
// RankingScore carries the same log value, so ordering never underflows.
//
// Ties prefer fewer flips before comparing heads: a dependency at exactly 0.5
// flips for free, and breaking on head first would drain one head's whole
// coin-flip subtree before another head's best path was ever offered. Head ID
// then decides — the heap holds at most one item per head, so it is a strict
// total order that makes a run repeatable.
type candidateHeap []candidateItem

var _ heap.Interface = (*candidateHeap)(nil)

func (h candidateHeap) Len() int { return len(h) }

func (h candidateHeap) Less(i, j int) bool {
	left, right := h[i], h[j]
	if left.score != right.score {
		return left.score > right.score
	}
	if len(left.flipped) != len(right.flipped) {
		return len(left.flipped) < len(right.flipped)
	}
	return left.stream.head < right.stream.head
}

func (h candidateHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *candidateHeap) Push(value any) {
	*h = append(*h, value.(candidateItem))
}

func (h *candidateHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}
