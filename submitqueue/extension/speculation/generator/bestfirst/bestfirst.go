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
// Throughout, a probability is the [0, 1] value a predictor gives; a score is
// its logarithm. Scores are summed and compared, never exponentiated, so wide
// heads cannot underflow into ties. The algorithm — per-head streams
// enumerating flip subsets lazily, merged through one global heap — is
// documented in doc/rfc/submitqueue/speculation-generator-best-first.md.
package bestfirst

import (
	"cmp"
	"container/heap"
	"context"
	"maps"
	"math"
	"slices"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/predictor"
)

// bestFirst generates candidate paths using independent dependency
// probabilities supplied by predictor.
type bestFirst struct {
	predictor predictor.Predictor
}

var _ generator.Generator = (*bestFirst)(nil)

// New returns a Generator that ranks paths by the probability that every
// unresolved dependency assumption holds. The predictor is called at most once
// per unresolved dependency batch in each Generate call.
func New(p predictor.Predictor) generator.Generator {
	return &bestFirst{predictor: p}
}

// Generate prices the unresolved dependencies of the snapshot's Speculating
// heads and opens a lazy global best-first iterator. The snapshot is taken as
// given: it is the caller's to keep well formed, and nothing here re-checks it.
func (g *bestFirst) Generate(ctx context.Context, batches []entity.Batch, pathSets []entity.SpeculationPathSet) (generator.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	batchByID := make(map[string]entity.Batch, len(batches))
	for _, batch := range batches {
		batchByID[batch.ID] = batch
	}
	pathsByHead := make(map[string]entity.SpeculationPathSet, len(pathSets))
	for _, set := range pathSets {
		pathsByHead[set.Head] = set
	}
	heads, unresolvedIDs := speculatingHeads(batches, batchByID)

	probabilityByID, err := g.predict(ctx, unresolvedIDs, batchByID, pathsByHead)
	if err != nil {
		return nil, err
	}

	// Seeding the heap by appending and then heapifying once is linear, where
	// pushing head by head would cost a sift per head.
	it := &candidateIterator{candidates: make(candidateHeap, 0, len(heads))}
	for _, head := range heads {
		stream := newPathStream(head, batchByID, probabilityByID)
		it.candidates = append(it.candidates, candidateItem{
			stream: stream,
			score:  stream.bestScore,
		})
	}
	heap.Init(&it.candidates)
	return it, nil
}

// speculatingHeads picks out the batches worth proposing work on and the
// distinct dependencies of theirs still awaiting an outcome. The dependency IDs
// come back sorted, so scoring order does not vary with map iteration.
func speculatingHeads(batches []entity.Batch, batchByID map[string]entity.Batch) (heads []entity.Batch, unresolvedIDs []string) {
	unresolved := make(map[string]struct{})
	for _, batch := range batches {
		if batch.State != entity.BatchStateSpeculating {
			continue
		}
		heads = append(heads, batch)
		for _, dependencyID := range batch.Dependencies {
			if _, resolved := resolvedAssumption(batchByID[dependencyID].State); !resolved {
				unresolved[dependencyID] = struct{}{}
			}
		}
	}
	return heads, slices.Sorted(maps.Keys(unresolved))
}

// predict asks the predictor for each unresolved dependency exactly once,
// however many heads wait on it. Each dependency is priced against its own path
// set, zero-valued for one that has never speculated.
//
// A dependency that cannot be priced takes defaultProbability rather than
// ending the run — one unusable number must not cost the queue every candidate
// it had. Only cancellation is an error.
func (g *bestFirst) predict(ctx context.Context, ids []string, batchByID map[string]entity.Batch, pathsByHead map[string]entity.SpeculationPathSet) (map[string]float64, error) {
	probabilityByID := make(map[string]float64, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, known := batchByID[id]
		if !known {
			// A batch the snapshot never carried is zero in every field, not
			// just missing — pricing it would price some other batch entirely,
			// or fail on its empty queue. It is unpriceable, not cheap.
			probabilityByID[id] = defaultProbability
			continue
		}
		probability, err := g.predictor.Predict(ctx, batch, pathsByHead[id])
		if err != nil {
			// A predictor that failed because the caller went away has not
			// found an unpriceable dependency — it has found a dead ctx, which
			// ends the run. The loop's own check would not catch it on the last
			// dependency, and a cancelled Generate must never hand back an
			// iterator.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			probability = defaultProbability
		}
		probabilityByID[id] = asProbability(float64(probability))
	}
	return probabilityByID, nil
}

// defaultProbability stands in for a score that is not a probability, one the
// predictor could not produce at all, and one for a dependency the snapshot
// never carried. It is optimistic on purpose: a dependency nobody could
// estimate is treated as very likely to succeed, which keeps its head's preferred
// path near the front rather than burying it or dropping the queue's whole
// snapshot on one bad number.
const defaultProbability = 0.95

// asProbability keeps a usable score and substitutes the default for anything
// else. The comparisons are both false for NaN, so NaN takes the default too.
func asProbability(score float64) float64 {
	if score >= 0 && score <= 1 {
		return score
	}
	return defaultProbability
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
	dependencies := slices.Clone(s.base)
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
	return append(slices.Clone(values), value)
}

func replaceLastCopy(values []int, value int) []int {
	result := slices.Clone(values)
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
