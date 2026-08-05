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

package bestfirst

import (
	"container/heap"
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// headStream is one head's compiled enumeration state. Every path of the head
// is the modal template with some subset of the flips applied, priced at
// baseProbability times the product of the applied flip ratios — which is what
// lets the iterator walk the 2^k subsets best-first without materializing them.
type headStream struct {
	// template is the head's modal path: every dependency in queue order at its
	// most likely assumption.
	template entity.SpeculationPath
	// flips are the head's swing dependencies — those whose outcome is genuinely
	// undecided — in enumeration order: descending ratio, queue order on ties.
	flips []flip
	// baseProbability prices the modal path: the product of every swing
	// dependency's modal-outcome probability. Pinned dependencies are certain
	// and contribute no factor.
	baseProbability float64
}

// flip is one swing dependency's less likely branch.
type flip struct {
	// depIndex locates the dependency inside template.Dependencies.
	depIndex int
	// assumption is the flipped, less likely assumption.
	assumption entity.DependencyAssumption
	// ratio multiplies a path's price when this flip is applied: (1−q)/q for
	// modal-outcome probability q. It is in (0, 1] because swing dependencies
	// have probability strictly between 0 and 1.
	ratio float64
}

// flipNode is one link of an entry's immutable flip chain. Entries share their
// chain tails with the parent they were derived from, so an entry costs O(1)
// memory regardless of how many flips it carries.
type flipNode struct {
	// idx indexes the head's flips.
	idx int
	// prev is the rest of the chain, nil at the start.
	prev *flipNode
}

// entry is one node of a head's enumeration tree: a flip subset represented by
// its chain, extended add/shift-style through lastFlip.
type entry struct {
	// headIdx indexes iterator.heads.
	headIdx int
	// flips is the applied flip subset, nil for the modal path.
	flips *flipNode
	// lastFlip is the largest applied flip index, -1 for the modal path.
	// Children either add lastFlip+1 to the subset or shift lastFlip up to it,
	// which generates every subset exactly once.
	lastFlip int
	// nFlips is the subset size, the first tie-break: between equally priced
	// candidates, the one closer to its modal path streams first.
	nFlips int
	// probability prices the materialized path — the chance that every
	// assumption on it holds — and doubles as the candidate's ranking score.
	probability float64
	// seq is the entry's insertion sequence, the final tie-break; it makes the
	// stream fully deterministic and, all else equal, favors entries seeded or
	// derived earlier.
	seq int
}

// iterator merges every head's lazy enumeration through one max-heap: pop the
// best entry, materialize it, push its at most two children. A child's
// probability never exceeds its parent's, so popped prices are non-increasing
// and the stream is exactly best-first.
type iterator struct {
	// heads holds the per-head enumeration state entries index into.
	heads []headStream
	// entries is the frontier: for every head, the best not-yet-yielded subsets.
	entries entryHeap
	// seq numbers heap insertions to keep ties deterministic.
	seq int
}

// addHead seeds a head's stream with its modal path.
func (it *iterator) addHead(hs headStream) {
	it.heads = append(it.heads, hs)
	it.push(entry{
		headIdx:     len(it.heads) - 1,
		lastFlip:    -1,
		probability: hs.baseProbability,
	})
}

func (it *iterator) push(e entry) {
	e.seq = it.seq
	it.seq++
	heap.Push(&it.entries, e)
}

// Next yields the next candidate best-first. It never repeats a path: each
// heap entry is a distinct flip subset of its head, generated exactly once.
func (it *iterator) Next(ctx context.Context) (entity.CandidatePath, bool, error) {
	if err := ctx.Err(); err != nil {
		return entity.CandidatePath{}, false, err
	}
	if it.entries.Len() == 0 {
		return entity.CandidatePath{}, false, nil
	}
	e := heap.Pop(&it.entries).(entry)
	it.pushChildren(e)
	return entity.CandidatePath{Path: it.materialize(e), RankingScore: e.probability}, true, nil
}

// pushChildren pushes the popped entry's enumeration children: add extends the
// subset with the next flip, shift replaces the last flip with the next one.
// Flips are ordered by descending ratio, so both multiplications keep a child
// priced at or below its parent.
func (it *iterator) pushChildren(e entry) {
	flips := it.heads[e.headIdx].flips
	next := e.lastFlip + 1
	if next >= len(flips) {
		return
	}
	it.push(entry{
		headIdx:     e.headIdx,
		flips:       &flipNode{idx: next, prev: e.flips},
		lastFlip:    next,
		nFlips:      e.nFlips + 1,
		probability: e.probability * flips[next].ratio,
	})
	if e.lastFlip < 0 {
		return
	}
	it.push(entry{
		headIdx:     e.headIdx,
		flips:       &flipNode{idx: next, prev: e.flips.prev},
		lastFlip:    next,
		nFlips:      e.nFlips,
		probability: e.probability / flips[e.lastFlip].ratio * flips[next].ratio,
	})
}

// materialize renders an entry into a self-contained path: a copy of the modal
// template with the entry's flips applied.
func (it *iterator) materialize(e entry) entity.SpeculationPath {
	hs := it.heads[e.headIdx]
	deps := make([]entity.PathDependency, len(hs.template.Dependencies))
	copy(deps, hs.template.Dependencies)
	for n := e.flips; n != nil; n = n.prev {
		f := hs.flips[n.idx]
		deps[f.depIndex].Assumption = f.assumption
	}
	return entity.SpeculationPath{Head: hs.template.Head, Dependencies: deps}
}

// entryHeap orders entries highest probability first, then fewest flips, then
// lowest insertion sequence.
type entryHeap []entry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.probability != b.probability {
		return a.probability > b.probability
	}
	if a.nFlips != b.nFlips {
		return a.nFlips < b.nFlips
	}
	return a.seq < b.seq
}

func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x any) { *h = append(*h, x.(entry)) }

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
