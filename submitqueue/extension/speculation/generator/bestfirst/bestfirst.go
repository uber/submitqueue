// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bestfirst provides a probability-ordered speculation path generator.
package bestfirst

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"

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

// Generate validates the queue snapshot, resolves the dependency probabilities
// needed by Speculating heads, and opens a lazy global best-first iterator.
func (g *bestFirst) Generate(ctx context.Context, batches []entity.Batch) (generator.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	batchByID := make(map[string]entity.Batch, len(batches))
	for _, batch := range batches {
		if batch.ID == "" {
			return nil, fmt.Errorf("bestfirst: batch has empty ID")
		}
		if _, exists := batchByID[batch.ID]; exists {
			return nil, fmt.Errorf("bestfirst: duplicate batch ID %q", batch.ID)
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
				return nil, fmt.Errorf("bestfirst: head %q has an empty dependency ID", batch.ID)
			}
			if dependencyID == batch.ID {
				return nil, fmt.Errorf("bestfirst: head %q depends on itself", batch.ID)
			}
			if _, duplicate := seen[dependencyID]; duplicate {
				return nil, fmt.Errorf("bestfirst: head %q repeats dependency %q", batch.ID, dependencyID)
			}
			seen[dependencyID] = struct{}{}

			dependency, exists := batchByID[dependencyID]
			if !exists {
				return nil, fmt.Errorf("bestfirst: head %q references missing dependency %q", batch.ID, dependencyID)
			}
			if dependency.State == entity.BatchStateUnknown {
				return nil, fmt.Errorf("bestfirst: dependency %q has unknown state", dependencyID)
			}
			if _, resolved := resolvedAssumption(dependency.State); !resolved {
				unresolvedDependencyIDs[dependencyID] = struct{}{}
			}
		}
	}
	sort.Slice(heads, func(i, j int) bool {
		return heads[i].ID < heads[j].ID
	})

	probabilityByID := make(map[string]float64, len(unresolvedDependencyIDs))
	ids := make([]string, 0, len(unresolvedDependencyIDs))
	for id := range unresolvedDependencyIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		probability, err := g.scorer.Score(ctx, batchByID[id])
		if err != nil {
			return nil, fmt.Errorf("bestfirst: score dependency %q: %w", id, err)
		}
		if math.IsNaN(probability) || math.IsInf(probability, 0) || probability < 0 || probability > 1 {
			return nil, fmt.Errorf("bestfirst: dependency %q has invalid probability %v", id, probability)
		}
		probabilityByID[id] = probability
	}

	it := &candidateIterator{}
	heap.Init(&it.candidates)
	for _, head := range heads {
		stream := newPathStream(head, batchByID, probabilityByID)
		candidate, ok := stream.next()
		if !ok {
			continue
		}
		heap.Push(&it.candidates, candidateItem{
			rankedCandidate: candidate,
			pathID:          candidate.candidate.Path.ID(),
			stream:          stream,
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

// dependencyVariable is one unresolved dependency ordered by the penalty for
// flipping away from its most likely outcome.
type dependencyVariable struct {
	dependencyIndex int
	flipCost        float64
	zeroProbability bool
}

// pathStream lazily enumerates one head's paths in descending probability.
type pathStream struct {
	head                  string
	base                  []entity.PathDependency
	variables             []dependencyVariable
	optimalLogProbability float64
	emitOptimal           bool
	subsets               flipHeap
	nextSequence          uint64
}

func newPathStream(
	head entity.Batch,
	batchByID map[string]entity.Batch,
	probabilityByID map[string]float64,
) *pathStream {
	stream := &pathStream{
		head:        head.ID,
		base:        make([]entity.PathDependency, len(head.Dependencies)),
		emitOptimal: true,
	}

	for i, dependencyID := range head.Dependencies {
		dependency := batchByID[dependencyID]
		if assumption, resolved := resolvedAssumption(dependency.State); resolved {
			stream.base[i] = entity.PathDependency{
				Batch:      dependencyID,
				Assumption: assumption,
			}
			continue
		}

		probability := probabilityByID[dependencyID]
		preferredProbability := math.Max(probability, 1-probability)
		nonPreferredProbability := math.Min(probability, 1-probability)
		preferredAssumption := entity.DependencyAssumptionFails
		if probability >= 0.5 {
			preferredAssumption = entity.DependencyAssumptionSucceeds
		}
		stream.base[i] = entity.PathDependency{
			Batch:      dependencyID,
			Assumption: preferredAssumption,
		}
		stream.optimalLogProbability += math.Log(preferredProbability)

		variable := dependencyVariable{dependencyIndex: i}
		if nonPreferredProbability == 0 {
			variable.zeroProbability = true
		} else {
			variable.flipCost = math.Log(preferredProbability / nonPreferredProbability)
		}
		stream.variables = append(stream.variables, variable)
	}

	sort.SliceStable(stream.variables, func(i, j int) bool {
		left, right := stream.variables[i], stream.variables[j]
		if left.zeroProbability != right.zeroProbability {
			return !left.zeroProbability
		}
		if left.flipCost != right.flipCost {
			return left.flipCost < right.flipCost
		}
		return left.dependencyIndex < right.dependencyIndex
	})

	heap.Init(&stream.subsets)
	if len(stream.variables) > 0 {
		entry := flipEntry{
			lastIndex: 0,
			flipped:   []int{0},
			sequence:  stream.takeSequence(),
		}
		entry.add(stream.variables[0])
		heap.Push(&stream.subsets, entry)
	}
	return stream
}

func (s *pathStream) takeSequence() uint64 {
	sequence := s.nextSequence
	s.nextSequence++
	return sequence
}

type rankedCandidate struct {
	candidate      entity.CandidatePath
	logProbability float64
}

func (s *pathStream) next() (rankedCandidate, bool) {
	if s.emitOptimal {
		s.emitOptimal = false
		return s.build(nil, flipCost{}), true
	}
	if s.subsets.Len() == 0 {
		return rankedCandidate{}, false
	}

	entry := heap.Pop(&s.subsets).(flipEntry)
	j := entry.lastIndex
	if j+1 < len(s.variables) {
		extend := flipEntry{
			cost:      entry.cost,
			lastIndex: j + 1,
			flipped:   appendCopy(entry.flipped, j+1),
			sequence:  s.takeSequence(),
		}
		extend.add(s.variables[j+1])
		heap.Push(&s.subsets, extend)

		swap := flipEntry{
			cost:      entry.cost,
			lastIndex: j + 1,
			flipped:   replaceLastCopy(entry.flipped, j+1),
			sequence:  s.takeSequence(),
		}
		swap.remove(s.variables[j])
		swap.add(s.variables[j+1])
		heap.Push(&s.subsets, swap)
	}
	return s.build(entry.flipped, entry.cost), true
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

func (s *pathStream) build(flipped []int, cost flipCost) rankedCandidate {
	dependencies := make([]entity.PathDependency, len(s.base))
	copy(dependencies, s.base)
	for _, variableIndex := range flipped {
		dependencyIndex := s.variables[variableIndex].dependencyIndex
		dependencies[dependencyIndex].Assumption = opposite(dependencies[dependencyIndex].Assumption)
	}

	logProbability := s.optimalLogProbability - cost.finite
	rankingScore := math.Exp(logProbability)
	if cost.zeroProbabilityFlips > 0 {
		logProbability = math.Inf(-1)
		rankingScore = 0
	}
	return rankedCandidate{
		candidate: entity.CandidatePath{
			Path: entity.SpeculationPath{
				Head:         s.head,
				Dependencies: dependencies,
			},
			RankingScore: rankingScore,
		},
		logProbability: logProbability,
	}
}

func opposite(assumption entity.DependencyAssumption) entity.DependencyAssumption {
	if assumption == entity.DependencyAssumptionSucceeds {
		return entity.DependencyAssumptionFails
	}
	return entity.DependencyAssumptionSucceeds
}

// flipCost separates finite log penalties from flips whose probability is
// exactly zero. Keeping a count avoids undefined Inf-Inf arithmetic in the
// extend/swap enumeration.
type flipCost struct {
	finite               float64
	zeroProbabilityFlips int
}

type flipEntry struct {
	cost      flipCost
	lastIndex int
	flipped   []int
	sequence  uint64
}

func (e *flipEntry) add(variable dependencyVariable) {
	if variable.zeroProbability {
		e.cost.zeroProbabilityFlips++
		return
	}
	e.cost.finite += variable.flipCost
}

func (e *flipEntry) remove(variable dependencyVariable) {
	if variable.zeroProbability {
		e.cost.zeroProbabilityFlips--
		return
	}
	e.cost.finite -= variable.flipCost
}

type flipHeap []flipEntry

var _ heap.Interface = (*flipHeap)(nil)

func (h flipHeap) Len() int { return len(h) }

func (h flipHeap) Less(i, j int) bool {
	left, right := h[i], h[j]
	if left.cost.zeroProbabilityFlips != right.cost.zeroProbabilityFlips {
		return left.cost.zeroProbabilityFlips < right.cost.zeroProbabilityFlips
	}
	if left.cost.finite != right.cost.finite {
		return left.cost.finite < right.cost.finite
	}
	return left.sequence < right.sequence
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

func (i *candidateIterator) Next(ctx context.Context) (entity.CandidatePath, bool, error) {
	if err := ctx.Err(); err != nil {
		return entity.CandidatePath{}, false, err
	}
	if i.candidates.Len() == 0 {
		return entity.CandidatePath{}, false, nil
	}

	item := heap.Pop(&i.candidates).(candidateItem)
	if next, ok := item.stream.next(); ok {
		heap.Push(&i.candidates, candidateItem{
			rankedCandidate: next,
			pathID:          next.candidate.Path.ID(),
			stream:          item.stream,
		})
	}
	return item.candidate, true, nil
}

type candidateItem struct {
	rankedCandidate
	pathID string
	stream *pathStream
}

// candidateHeap is a max-heap by internal log probability. RankingScore can
// underflow to zero for deep paths, so it is not used to preserve ordering.
type candidateHeap []candidateItem

var _ heap.Interface = (*candidateHeap)(nil)

func (h candidateHeap) Len() int { return len(h) }

func (h candidateHeap) Less(i, j int) bool {
	left, right := h[i], h[j]
	if left.logProbability != right.logProbability {
		return left.logProbability > right.logProbability
	}
	if left.candidate.Path.Head != right.candidate.Path.Head {
		return left.candidate.Path.Head < right.candidate.Path.Head
	}
	return left.pathID < right.pathID
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
