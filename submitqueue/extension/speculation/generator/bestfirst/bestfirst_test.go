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
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
)

// stubScorer scores each batch by ID, defaulting to 0.5 for unknown batches. It
// is a minimal scorer.Scorer for exercising the generator without a resolver.
type stubScorer struct {
	scores map[string]float64
}

func (s stubScorer) Score(_ context.Context, b entity.Batch) (float64, error) {
	if v, ok := s.scores[b.ID]; ok {
		return v, nil
	}
	return 0.5, nil
}

func scored(scores map[string]float64) scorer.Scorer {
	return stubScorer{scores: scores}
}

// drainAll pulls every candidate from an iterator.
func drainAll(t *testing.T, iter generator.Iterator) []entity.CandidatePath {
	t.Helper()
	var out []entity.CandidatePath
	for {
		c, ok, err := iter.Next(context.Background())
		require.NoError(t, err)
		if !ok {
			break
		}
		out = append(out, c)
	}
	return out
}

// forHead returns only the candidates whose head is headID.
func forHead(cands []entity.CandidatePath, headID string) []entity.CandidatePath {
	var out []entity.CandidatePath
	for _, c := range cands {
		if c.Path.Head == headID {
			out = append(out, c)
		}
	}
	return out
}

// assumptionFor returns what a path assumes about a given dependency batch.
func assumptionFor(p entity.SpeculationPath, dep string) entity.DependencyAssumption {
	for _, d := range p.Dependencies {
		if d.Batch == dep {
			return d.Assumption
		}
	}
	return entity.DependencyAssumptionUnknown
}

// assumptionKey renders a path's assumptions in queue order, to compare whole
// combinations.
func assumptionKey(p entity.SpeculationPath) string {
	var b strings.Builder
	for _, dep := range p.Dependencies {
		b.WriteString(dep.Batch)
		b.WriteByte('=')
		b.WriteString(string(dep.Assumption))
		b.WriteByte(';')
	}
	return b.String()
}

// pathIDs renders each candidate's path ID, in order.
func pathIDs(cands []entity.CandidatePath) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.Path.ID())
	}
	return out
}

// heads renders each candidate's head, in order.
func heads(cands []entity.CandidatePath) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.Path.Head)
	}
	return out
}

// iteratorOf reaches into the concrete iterator, so laziness invariants can be
// asserted on how many paths have actually been built.
func iteratorOf(t *testing.T, iter generator.Iterator) *candidateIterator {
	t.Helper()
	it, ok := iter.(*candidateIterator)
	require.True(t, ok, "iterator is not the bestfirst iterator")
	return it
}

// countingScorer records how many times each batch is scored.
type countingScorer struct {
	scores map[string]float64
	calls  map[string]int
	total  int
}

func newCountingScorer(scores map[string]float64) *countingScorer {
	return &countingScorer{scores: scores, calls: map[string]int{}}
}

func (c *countingScorer) Score(_ context.Context, b entity.Batch) (float64, error) {
	c.calls[b.ID]++
	c.total++
	if v, ok := c.scores[b.ID]; ok {
		return v, nil
	}
	return 0.5, nil
}

// errScorer always fails, to exercise error propagation from scoring.
type errScorer struct{}

func (errScorer) Score(context.Context, entity.Batch) (float64, error) {
	return 0, assert.AnError
}

// constScorer scores every batch identically, regardless of ID.
type constScorer struct{ v float64 }

func (c constScorer) Score(context.Context, entity.Batch) (float64, error) { return c.v, nil }

// wideHead builds one Speculating head over n unresolved dependencies, each at a
// distinct score so no two combinations tie.
func wideHead(n int) ([]entity.Batch, scorer.Scorer) {
	head := entity.Batch{ID: "q/head", State: entity.BatchStateSpeculating}
	batches := []entity.Batch{}
	scores := map[string]float64{}
	for i := 0; i < n; i++ {
		dep := fmt.Sprintf("q/dep%02d", i)
		head.Dependencies = append(head.Dependencies, dep)
		// Created deps are unresolved (so they're scored) but not eligible heads.
		batches = append(batches, entity.Batch{ID: dep, State: entity.BatchStateCreated})
		scores[dep] = 0.55 + 0.02*float64(i)
	}
	return append(batches, head), scored(scores)
}

func TestBestFirst_OrderingAndEnumeration(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateSpeculating},
		{ID: "q/B", State: entity.BatchStateSpeculating},
		{ID: "q/C", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A", "q/B"}},
	}
	sc := scored(map[string]float64{"q/A": 0.9, "q/B": 0.8})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := forHead(drainAll(t, iter), "q/C")

	// Two unresolved dependencies -> 2^2 candidates.
	require.Len(t, cands, 4)

	// Best-first: the all-succeeds path leads, scoring the product of the
	// dependency scores (0.9 * 0.8), and scores descend from there.
	assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(cands[0].Path, "q/A"))
	assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(cands[0].Path, "q/B"))
	assert.InDelta(t, math.Log(0.72), cands[0].RankingScore, 1e-9)
	for i := 1; i < len(cands); i++ {
		assert.LessOrEqual(t, cands[i].RankingScore, cands[i-1].RankingScore)
	}
	// The least optimistic candidate excludes both dependencies: (1-0.9)(1-0.8).
	last := cands[len(cands)-1]
	assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(last.Path, "q/A"))
	assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(last.Path, "q/B"))
	assert.InDelta(t, math.Log(0.02), last.RankingScore, 1e-9)
}

func TestBestFirst_PinsResolvedDependencies(t *testing.T) {
	// A resolved dependency is a fact: its assumption is forced by its
	// state and it never varies across the head's paths.
	tests := []struct {
		name  string
		state entity.BatchState
		want  entity.DependencyAssumption
	}{
		{name: "succeeded dependency forced to succeeds", state: entity.BatchStateSucceeded, want: entity.DependencyAssumptionSucceeds},
		{name: "failed dependency forced to fails", state: entity.BatchStateFailed, want: entity.DependencyAssumptionFails},
		{name: "cancelled dependency forced to fails", state: entity.BatchStateCancelled, want: entity.DependencyAssumptionFails},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batches := []entity.Batch{
				{ID: "q/A", State: tt.state},
				{ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A"}},
			}
			iter, err := New(scored(nil)).Generate(context.Background(), batches)
			require.NoError(t, err)
			cands := forHead(drainAll(t, iter), "q/H")

			// The pinned dependency has no flip, so there is exactly one
			// path, and it carries the forced assumption.
			require.Len(t, cands, 1)
			assert.Equal(t, tt.want, assumptionFor(cands[0].Path, "q/A"))
		})
	}
}

func TestBestFirst_ResolvedDependenciesDropOutOfSearch(t *testing.T) {
	// Three dependencies, but two are resolved facts: only q/open is still
	// unresolved, so the head yields 2 paths rather than 8.
	batches := []entity.Batch{
		{ID: "q/succeeded", State: entity.BatchStateSucceeded},
		{ID: "q/failed", State: entity.BatchStateFailed},
		{ID: "q/open", State: entity.BatchStateCreated},
		{ID: "q/H", State: entity.BatchStateSpeculating,
			Dependencies: []string{"q/succeeded", "q/failed", "q/open"}},
	}
	iter, err := New(scored(map[string]float64{"q/open": 0.7})).
		Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := forHead(drainAll(t, iter), "q/H")

	require.Len(t, cands, 2)
	// Pinned assumptions are identical on both paths and stay in queue order; the
	// score reflects only the open dependency, since a fact is a certainty.
	assert.Equal(t, "q/succeeded=succeeds;q/failed=fails;q/open=succeeds;", assumptionKey(cands[0].Path))
	assert.InDelta(t, math.Log(0.7), cands[0].RankingScore, 1e-9)
	assert.Equal(t, "q/succeeded=succeeds;q/failed=fails;q/open=fails;", assumptionKey(cands[1].Path))
	assert.InDelta(t, math.Log(0.3), cands[1].RankingScore, 1e-9)
}

func TestBestFirst_EmitsExactSequenceAcrossHeads(t *testing.T) {
	// q/D has nothing to wait on; q/C assumes outcomes for two in-flight batches. The
	// two heads interleave by score rather than draining one at a time.
	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateCreated},
		{ID: "q/B", State: entity.BatchStateCreated},
		{ID: "q/D", State: entity.BatchStateSpeculating},
		{ID: "q/C", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A", "q/B"}},
	}
	sc := scored(map[string]float64{"q/A": 0.9, "q/B": 0.8})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	want := []struct {
		head        string
		assumptions string
		probability float64
	}{
		{head: "q/D", assumptions: "", probability: 1.0},
		{head: "q/C", assumptions: "q/A=succeeds;q/B=succeeds;", probability: 0.72},
		{head: "q/C", assumptions: "q/A=succeeds;q/B=fails;", probability: 0.18},
		{head: "q/C", assumptions: "q/A=fails;q/B=succeeds;", probability: 0.08},
		{head: "q/C", assumptions: "q/A=fails;q/B=fails;", probability: 0.02},
	}
	require.Len(t, cands, len(want))
	for i, w := range want {
		assert.Equal(t, w.head, cands[i].Path.Head, "head at %d", i)
		assert.Equal(t, w.assumptions, assumptionKey(cands[i].Path), "assumptions at %d", i)
		assert.InDelta(t, math.Log(w.probability), cands[i].RankingScore, 1e-9, "score at %d", i)
	}
}

func TestBestFirst_PreferredAssumptionFollowsScore(t *testing.T) {
	// q/low is more likely to fail than to succeed, so the head's most likely path assumes
	// it fails. The leading path is the preferred-assumption path, not the
	// all-succeeds one.
	batches := []entity.Batch{
		{ID: "q/high", State: entity.BatchStateCreated},
		{ID: "q/low", State: entity.BatchStateCreated},
		{ID: "q/H", State: entity.BatchStateSpeculating,
			Dependencies: []string{"q/high", "q/low"}},
	}
	sc := scored(map[string]float64{"q/high": 0.8, "q/low": 0.3})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	want := []struct {
		assumptions string
		probability float64
	}{
		{assumptions: "q/high=succeeds;q/low=fails;", probability: 0.56},
		{assumptions: "q/high=succeeds;q/low=succeeds;", probability: 0.24},
		{assumptions: "q/high=fails;q/low=fails;", probability: 0.14},
		{assumptions: "q/high=fails;q/low=succeeds;", probability: 0.06},
	}
	require.Len(t, cands, len(want))
	for i, w := range want {
		assert.Equal(t, w.assumptions, assumptionKey(cands[i].Path), "assumptions at %d", i)
		assert.InDelta(t, math.Log(w.probability), cands[i].RankingScore, 1e-9, "score at %d", i)
	}

	// Spelled out: the all-succeeds path exists but only ranks second.
	assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(cands[0].Path, "q/low"))
}

func TestBestFirst_OnlySpeculatingHeadsProduceCandidates(t *testing.T) {
	// Batches in any state supply facts about their dependents, but only a
	// Speculating batch is a head worth proposing work on.
	tests := []struct {
		state entity.BatchState
		want  bool
	}{
		{state: entity.BatchStateUnknown},
		{state: entity.BatchStateCreated},
		{state: entity.BatchStateSpeculating, want: true},
		{state: entity.BatchStateMerging},
		{state: entity.BatchStateSucceeded},
		{state: entity.BatchStateFailed},
		{state: entity.BatchStateCancelling},
		{state: entity.BatchStateCancelled},
	}

	for _, tt := range tests {
		name := string(tt.state)
		if name == "" {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			batches := []entity.Batch{{ID: "q/H", State: tt.state}}

			iter, err := New(scored(nil)).Generate(context.Background(), batches)
			require.NoError(t, err)
			cands := drainAll(t, iter)

			if !tt.want {
				assert.Empty(t, cands)
				return
			}
			require.Len(t, cands, 1)
			assert.Equal(t, "q/H", cands[0].Path.Head)
		})
	}
}

func TestBestFirst_HeadWithNoDependencies(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/H", State: entity.BatchStateSpeculating},
	}

	iter, err := New(scored(nil)).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	require.Len(t, cands, 1)
	assert.Equal(t, "q/H", cands[0].Path.Head)
	assert.Empty(t, cands[0].Path.Dependencies)
	assert.InDelta(t, math.Log(1.0), cands[0].RankingScore, 1e-9)
}

func TestBestFirst_PropagatesScorerError(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateSpeculating},
		{ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A"}},
	}

	iter, err := New(errScorer{}).Generate(context.Background(), batches)
	assert.Error(t, err)
	assert.Nil(t, iter)
}

func TestBestFirst_GeneratesOnlyWhatIsPulled(t *testing.T) {
	// 12 unresolved dependencies is an outcome space of 4096 paths.
	const deps, space = 12, 1 << 12
	batches, sc := wideHead(deps)

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	it := iteratorOf(t, iter)

	// Everything ever worked out is either handed out already, waiting in the
	// global heap, or waiting in the head's local heap. Generate pushes the
	// head's most likely path and nothing more.
	require.Equal(t, 1, it.candidates.Len())
	stream := it.candidates[0].stream
	require.Empty(t, stream.subsets, "Generate must not touch the local heap")

	pulled := 0
	for i := 0; i < 3; i++ {
		_, ok, err := iter.Next(context.Background())
		require.NoError(t, err)
		require.True(t, ok)
		pulled++
	}

	// Advancing the stream pushes at most two successor subsets per pull, on
	// top of the one-time seed of the cheapest single flip, so three pulls can
	// have worked out at most 2+2*3 paths — a tiny fraction of the space.
	built := pulled + it.candidates.Len() + len(stream.subsets)
	assert.LessOrEqual(t, built, 8)
	assert.Less(t, built, space)
}

func TestBestFirst_DrainYieldsEveryCombinationOnce(t *testing.T) {
	deps := []string{"q/A", "q/B", "q/C"}
	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateCreated},
		{ID: "q/B", State: entity.BatchStateCreated},
		{ID: "q/C", State: entity.BatchStateCreated},
		{ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: deps},
	}
	sc := scored(map[string]float64{"q/A": 0.9, "q/B": 0.7, "q/C": 0.6})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	require.Len(t, cands, 8)

	// Every include/exclude combination appears exactly once, and each path
	// carries its assumptions in queue order.
	seen := map[string]int{}
	for _, c := range cands {
		got := make([]string, 0, len(c.Path.Dependencies))
		for _, dep := range c.Path.Dependencies {
			got = append(got, dep.Batch)
		}
		assert.Equal(t, deps, got, "assumptions must stay in dependency order")
		seen[assumptionKey(c.Path)]++
	}
	for mask := 0; mask < 8; mask++ {
		var want strings.Builder
		for i, dep := range deps {
			assumption := entity.DependencyAssumptionFails
			if mask&(1<<i) != 0 {
				assumption = entity.DependencyAssumptionSucceeds
			}
			want.WriteString(dep)
			want.WriteByte('=')
			want.WriteString(string(assumption))
			want.WriteByte(';')
		}
		assert.Equal(t, 1, seen[want.String()], "combination %q", want.String())
	}

	// Scores are non-increasing across the whole drain.
	for i := 1; i < len(cands); i++ {
		assert.LessOrEqual(t, cands[i].RankingScore, cands[i-1].RankingScore)
	}
}

func TestBestFirst_ScoresGloballyNonIncreasing(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateCreated},
		{ID: "q/B", State: entity.BatchStateCreated},
		{ID: "q/C", State: entity.BatchStateCreated},
		{ID: "q/free", State: entity.BatchStateSpeculating},
		{ID: "q/AB", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A", "q/B"}},
		{ID: "q/BC", State: entity.BatchStateSpeculating, Dependencies: []string{"q/B", "q/C"}},
		{ID: "q/ABC", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A", "q/B", "q/C"}},
	}
	sc := scored(map[string]float64{"q/A": 0.85, "q/B": 0.3, "q/C": 0.65})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	// 1 (free) + 4 (AB) + 4 (BC) + 8 (ABC).
	require.Len(t, cands, 17)
	for i := 1; i < len(cands); i++ {
		assert.LessOrEqual(t, cands[i].RankingScore, cands[i-1].RankingScore,
			"score rose at index %d", i)
	}
}

func TestBestFirst_EqualScoresOrderDeterministically(t *testing.T) {
	t.Run("ties break on head ID among roots", func(t *testing.T) {
		// Dependency-free heads all score 1.0, so every root sits in the
		// cross-head heap at once with nothing but the head to tell them apart.
		batches := []entity.Batch{
			{ID: "q/e", State: entity.BatchStateSpeculating},
			{ID: "q/b", State: entity.BatchStateSpeculating},
			{ID: "q/d", State: entity.BatchStateSpeculating},
			{ID: "q/a", State: entity.BatchStateSpeculating},
			{ID: "q/c", State: entity.BatchStateSpeculating},
		}

		iter, err := New(scored(nil)).Generate(context.Background(), batches)
		require.NoError(t, err)
		cands := drainAll(t, iter)

		require.Len(t, cands, 5)
		assert.Equal(t, []string{"q/a", "q/b", "q/c", "q/d", "q/e"}, heads(cands))
	})

	t.Run("every root comes before any deeper path", func(t *testing.T) {
		// A dependency at exactly 0.5 deviates for free, so both of a head's
		// paths score the same — and with two such heads all four paths tie.
		// Depth decides first, so the two roots come out before either
		// deviation; only then do the heads interleave at equal depth. Ordering
		// on head first would drain q/a's whole subtree before q/b was offered
		// anything, which would let one head monopolize a caller's budget.
		batches := []entity.Batch{
			{ID: "q/coinA", State: entity.BatchStateCreated},
			{ID: "q/coinB", State: entity.BatchStateCreated},
			{ID: "q/a", State: entity.BatchStateSpeculating, Dependencies: []string{"q/coinA"}},
			{ID: "q/b", State: entity.BatchStateSpeculating, Dependencies: []string{"q/coinB"}},
		}
		sc := scored(map[string]float64{"q/coinA": 0.5, "q/coinB": 0.5})

		iter, err := New(sc).Generate(context.Background(), batches)
		require.NoError(t, err)
		cands := drainAll(t, iter)

		require.Len(t, cands, 4)
		for i, c := range cands {
			assert.Equal(t, cands[0].RankingScore, c.RankingScore, "path %d must tie", i)
		}
		assert.Equal(t, []string{"q/a", "q/b", "q/a", "q/b"}, heads(cands))
		// Preferred everywhere is the root; the deviation assumes fails.
		assert.Equal(t, []entity.DependencyAssumption{
			entity.DependencyAssumptionSucceeds,
			entity.DependencyAssumptionSucceeds,
			entity.DependencyAssumptionFails,
			entity.DependencyAssumptionFails,
		}, []entity.DependencyAssumption{
			cands[0].Path.Dependencies[0].Assumption,
			cands[1].Path.Dependencies[0].Assumption,
			cands[2].Path.Dependencies[0].Assumption,
			cands[3].Path.Dependencies[0].Assumption,
		})
	})

	t.Run("repeated runs agree", func(t *testing.T) {
		// Every dependency at an even score: all 8 combinations of q/H score
		// the same, so ordering rests entirely on the tie-break.
		batches := []entity.Batch{
			{ID: "q/A", State: entity.BatchStateCreated},
			{ID: "q/B", State: entity.BatchStateCreated},
			{ID: "q/C", State: entity.BatchStateCreated},
			{ID: "q/H", State: entity.BatchStateSpeculating,
				Dependencies: []string{"q/A", "q/B", "q/C"}},
		}
		sc := scored(map[string]float64{"q/A": 0.5, "q/B": 0.5, "q/C": 0.5})

		first, err := New(sc).Generate(context.Background(), batches)
		require.NoError(t, err)
		second, err := New(sc).Generate(context.Background(), batches)
		require.NoError(t, err)

		a, b := drainAll(t, first), drainAll(t, second)
		require.Len(t, a, 8)
		assert.Equal(t, pathIDs(a), pathIDs(b))
	})
}

func TestBestFirst_NextMakesNoScorerCalls(t *testing.T) {
	batches, _ := wideHead(6)
	sc := newCountingScorer(map[string]float64{})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)

	afterGenerate := sc.total
	require.Positive(t, afterGenerate, "Generate must do the scoring")

	cands := drainAll(t, iter)
	require.Len(t, cands, 1<<6)
	assert.Equal(t, afterGenerate, sc.total, "Next must never call the scorer")
}

func TestBestFirst_MemoizesDependencyScoresAcrossHeads(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/shared", State: entity.BatchStateCreated},
		{ID: "q/H1", State: entity.BatchStateSpeculating, Dependencies: []string{"q/shared"}},
		{ID: "q/H2", State: entity.BatchStateSpeculating, Dependencies: []string{"q/shared"}},
		{ID: "q/H3", State: entity.BatchStateSpeculating, Dependencies: []string{"q/shared"}},
	}
	sc := newCountingScorer(map[string]float64{"q/shared": 0.7})

	_, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)

	assert.Equal(t, 1, sc.calls["q/shared"], "a shared dependency is scored once")
}

func TestBestFirst_MatchesBruteForceEnumeration(t *testing.T) {
	// Cross-check the lazy walk against the whole space enumerated eagerly: for
	// randomized score vectors, the candidates emitted must be exactly the 2^n
	// combinations, and their scores must be the brute-force scores in
	// descending order.
	rng := rand.New(rand.NewSource(20260727))

	for n := 1; n <= 6; n++ {
		for trial := 0; trial < 25; trial++ {
			t.Run(fmt.Sprintf("deps=%d/trial=%d", n, trial), func(t *testing.T) {
				var deps []string
				var depScores []float64
				batches := []entity.Batch{}
				scores := map[string]float64{}
				for i := 0; i < n; i++ {
					dep := fmt.Sprintf("q/dep%d", i)
					c := rng.Float64()
					deps = append(deps, dep)
					depScores = append(depScores, c)
					scores[dep] = c
					batches = append(batches, entity.Batch{ID: dep, State: entity.BatchStateCreated})
				}
				batches = append(batches, entity.Batch{
					ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: deps,
				})

				iter, err := New(scored(scores)).Generate(context.Background(), batches)
				require.NoError(t, err)
				got := drainAll(t, iter)

				// Brute force: every succeeds/fails combination, scored the
				// straightforward way as the product of the probability each
				// assumption takes, then converted to a log at the end. The
				// generator sums logs instead; at this width both are exact
				// enough to compare, which is the point of the cross-check.
				wantKeys := map[string]bool{}
				var wantScores []float64
				for mask := 0; mask < 1<<n; mask++ {
					score := 1.0
					var key strings.Builder
					for i, dep := range deps {
						assumption, c := entity.DependencyAssumptionFails, 1-depScores[i]
						if mask&(1<<i) != 0 {
							assumption, c = entity.DependencyAssumptionSucceeds, depScores[i]
						}
						score *= c
						key.WriteString(dep)
						key.WriteByte('=')
						key.WriteString(string(assumption))
						key.WriteByte(';')
					}
					wantKeys[key.String()] = true
					wantScores = append(wantScores, math.Log(score))
				}
				sort.Sort(sort.Reverse(sort.Float64Slice(wantScores)))

				require.Len(t, got, 1<<n)
				gotKeys := map[string]bool{}
				for i, c := range got {
					gotKeys[assumptionKey(c.Path)] = true
					assert.InDelta(t, wantScores[i], c.RankingScore, 1e-9,
						"score at index %d", i)
				}
				assert.Equal(t, wantKeys, gotKeys, "every combination exactly once")
			})
		}
	}
}

func TestBestFirst_WideHeadsRankWithoutUnderflow(t *testing.T) {
	// The regression that made scores logarithmic. Two heads wide enough that
	// the product of their dependency probabilities is not a representable
	// float64: at 0.9 apiece, 7100 dependencies multiply out to about e^-748,
	// and the smallest positive float64 is about e^-744. Ranked on the product
	// both heads score exactly 0.0, they tie, and the heap falls back to
	// comparing path IDs — which can hand out the wider, strictly less probable
	// head first. Ranked on summed logs they stay ordinary numbers a hundredth
	// apart and order correctly.
	const (
		narrowWidth = 7100
		wideWidth   = narrowWidth + 1
		depScore    = 0.9
	)

	var batches []entity.Batch
	wide := func(head string, n int) {
		b := entity.Batch{ID: head, State: entity.BatchStateSpeculating}
		for i := 0; i < n; i++ {
			dep := fmt.Sprintf("%s/dep%d", head, i)
			b.Dependencies = append(b.Dependencies, dep)
			batches = append(batches, entity.Batch{ID: dep, State: entity.BatchStateCreated})
		}
		batches = append(batches, b)
	}
	wide("q/narrow", narrowWidth)
	wide("q/wide", wideWidth)

	iter, err := New(constScorer{depScore}).Generate(context.Background(), batches)
	require.NoError(t, err)

	first, ok, err := iter.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	second, ok, err := iter.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)

	// The narrower head is more probable, so it leads. Taking one flip costs
	// log(0.1/0.9) ≈ -2.2, far more than the single extra dependency's -0.105,
	// so the wider head's best path is what comes second.
	assert.Equal(t, "q/narrow", first.Path.Head)
	assert.Equal(t, "q/wide", second.Path.Head)

	// Scores are finite, correctly ordered, and exactly the summed logs.
	assert.Greater(t, first.RankingScore, second.RankingScore)
	assert.False(t, math.IsInf(first.RankingScore, -1), "score underflowed to -Inf")
	assert.InDelta(t, narrowWidth*math.Log(depScore), first.RankingScore, 1e-6)
	assert.InDelta(t, wideWidth*math.Log(depScore), second.RankingScore, 1e-6)

	// And the proof that the multiplicative form could not have ordered these:
	// both probabilities round to zero, so both paths would have tied at 0.0.
	assert.Zero(t, math.Exp(first.RankingScore))
	assert.Zero(t, math.Exp(second.RankingScore))
}

func TestBestFirst_HonorsCancelledContext(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateCreated},
		{ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A"}},
	}

	t.Run("Generate returns the context error and no iterator", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		iter, err := New(scored(nil)).Generate(ctx, batches)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, iter)
	})

	t.Run("Generate reports a deadline that has passed", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
		defer cancel()

		_, err := New(scored(nil)).Generate(ctx, batches)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("Next returns the context error and no candidate", func(t *testing.T) {
		// Generate on a live context so the stream has candidates waiting; the
		// cancel lands between pulls, which is where a caller that has given up
		// actually stops.
		iter, err := New(scored(nil)).Generate(context.Background(), batches)
		require.NoError(t, err)

		_, ok, err := iter.Next(context.Background())
		require.NoError(t, err)
		require.True(t, ok, "the stream should have candidates before cancellation")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		c, ok, err := iter.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)
		assert.False(t, ok)
		assert.Equal(t, entity.CandidatePath{}, c)
	})
}

func TestBestFirst_DefaultsScoreOutsideUnitInterval(t *testing.T) {
	// Everything downstream treats a score as a probability: its log is the
	// ranking key and its complement is the other side's probability. Carrying
	// an out-of-range value would not fail loudly, it would quietly produce a
	// positive log, an inverted ordering, or a NaN that makes every comparison
	// false. The scorer is an injected extension and nothing earlier can vet
	// what it returns, so a value that is not a probability is replaced with an
	// optimistic default here rather than sinking the whole run.
	tests := []struct {
		name  string
		score float64
	}{
		{name: "above one", score: 1.5},
		{name: "below zero", score: -0.1},
		{name: "not a number", score: math.NaN()},
		{name: "positive infinity", score: math.Inf(1)},
		{name: "negative infinity", score: math.Inf(-1)},
	}

	batches := []entity.Batch{
		{ID: "q/A", State: entity.BatchStateCreated},
		{ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: []string{"q/A"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := New(constScorer{tt.score}).Generate(context.Background(), batches)
			require.NoError(t, err)
			cands := drainAll(t, iter)

			// The dependency is scored at the default, so the head still yields
			// both of its paths, ranked as that default and its complement.
			require.Len(t, cands, 2)
			assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(cands[0].Path, "q/A"))
			assert.InDelta(t, math.Log(defaultProbability), cands[0].RankingScore, 1e-9)
			assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(cands[1].Path, "q/A"))
			assert.InDelta(t, math.Log(1-defaultProbability), cands[1].RankingScore, 1e-9)
		})
	}
}

func TestBestFirst_ImpossibleFlipScoresNegativeInfinity(t *testing.T) {
	// A dependency certain to succeed leaves a flip that cannot happen.
	// Its log is negative infinity, which is the correct ranking: the path
	// taking it is impossible, and it must sort below every path that is merely
	// improbable — including paths from other heads.
	batches := []entity.Batch{
		{ID: "q/certain", State: entity.BatchStateCreated},
		{ID: "q/toss", State: entity.BatchStateCreated},
		{ID: "q/H1", State: entity.BatchStateSpeculating, Dependencies: []string{"q/certain"}},
		{ID: "q/H2", State: entity.BatchStateSpeculating, Dependencies: []string{"q/toss"}},
	}
	sc := scored(map[string]float64{"q/certain": 1.0, "q/toss": 0.6})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	require.Len(t, cands, 4)
	last := cands[len(cands)-1]
	assert.Equal(t, "q/H1", last.Path.Head)
	assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(last.Path, "q/certain"))
	assert.True(t, math.IsInf(last.RankingScore, -1), "impossible path should score -Inf")

	// Everything above it is finite and descending, so -Inf did not poison the
	// rest of the ordering.
	for i := 0; i < len(cands)-1; i++ {
		assert.False(t, math.IsInf(cands[i].RankingScore, -1), "candidate %d", i)
		if i > 0 {
			assert.LessOrEqual(t, cands[i].RankingScore, cands[i-1].RankingScore)
		}
	}
}

func TestBestFirst_ResolvedDependenciesAreNeverScored(t *testing.T) {
	// A finished build has no probability left to estimate, so the scorer is
	// never asked about one. Only the dependency still in flight is scored, and
	// only it opens a second path.
	batches := []entity.Batch{
		{ID: "q/passed", State: entity.BatchStateSucceeded},
		{ID: "q/broke", State: entity.BatchStateFailed},
		{ID: "q/stopped", State: entity.BatchStateCancelled},
		{ID: "q/running", State: entity.BatchStateCreated},
		{ID: "q/H", State: entity.BatchStateSpeculating,
			Dependencies: []string{"q/passed", "q/broke", "q/stopped", "q/running"}},
	}
	sc := newCountingScorer(map[string]float64{"q/running": 0.8})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)
	cands := drainAll(t, iter)

	assert.Equal(t, 1, sc.total, "only the unresolved dependency is scored")
	assert.Equal(t, 1, sc.calls["q/running"])
	require.Len(t, cands, 2, "one unresolved dependency, so two paths")

	// The resolved three keep their forced assumption on both paths.
	for _, c := range cands {
		assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(c.Path, "q/passed"))
		assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(c.Path, "q/broke"))
		assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(c.Path, "q/stopped"))
	}
}

func TestBestFirst_ReturnedPathsAreIndependent(t *testing.T) {
	// Paths are built one at a time from state the head shares, so a caller
	// scribbling on what it was handed must not reach the paths still to come.
	batches, _ := wideHead(3)
	iter, err := New(scored(map[string]float64{"q/dep00": 0.9, "q/dep01": 0.8, "q/dep02": 0.7})).
		Generate(context.Background(), batches)
	require.NoError(t, err)

	first, ok, err := iter.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	before := assumptionKey(first.Path)

	for i := range first.Path.Dependencies {
		first.Path.Dependencies[i].Batch = "clobbered"
		first.Path.Dependencies[i].Assumption = entity.DependencyAssumptionIgnored
	}

	rest := drainAll(t, iter)
	require.NotEmpty(t, rest)
	for _, c := range rest {
		assert.NotContains(t, assumptionKey(c.Path), "clobbered")
		assert.NotContains(t, assumptionKey(c.Path), string(entity.DependencyAssumptionIgnored))
	}
	assert.NotEqual(t, before, assumptionKey(first.Path), "the test mutated what it was handed")
}

func TestBestFirst_ScoresAreSummedFromTheHeadsBestScore(t *testing.T) {
	// Every score is summed from the head's best score in ascending
	// flip order. Deriving one from the path it grew out of instead —
	// subtracting the flip being moved and adding its replacement —
	// gives a different float, because floating-point addition does not
	// associate, and the heap's ordering guarantee rests on the two being
	// summed the same way. The expected scores below come from scoreFor, so a
	// walk that stops going through it is what this catches.
	deps := []string{"q/A", "q/B", "q/C", "q/D"}
	scores := map[string]float64{"q/A": 0.9, "q/B": 0.7, "q/C": 0.61, "q/D": 0.5003}

	batches := []entity.Batch{{
		ID: "q/H", State: entity.BatchStateSpeculating, Dependencies: deps,
	}}
	byID := map[string]entity.Batch{"q/H": batches[0]}
	for _, d := range deps {
		b := entity.Batch{ID: d, State: entity.BatchStateCreated}
		batches = append(batches, b)
		byID[d] = b
	}

	// Build the same head separately and total every subset the canonical way.
	s := newPathStream(byID["q/H"], byID, scores)
	s.prepare()

	want := map[float64]int{}
	for mask := 0; mask < 1<<len(deps); mask++ {
		var taken []int
		for i := range s.variables {
			if mask&(1<<i) != 0 {
				taken = append(taken, i)
			}
		}
		want[s.scoreFor(taken)]++
	}

	iter, err := New(scored(scores)).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := map[float64]int{}
	for _, c := range drainAll(t, iter) {
		got[c.RankingScore]++
	}

	assert.Equal(t, want, got, "every score must be exactly its canonical sum")
}

func TestBestFirst_UntouchedHeadsNeverWorkOutFlips(t *testing.T) {
	// Generate ranks heads against each other, so it has to total every head's
	// best score. What it must not do is work out and sort the
	// flips of heads nobody reaches — the cost that grows with the whole
	// queue rather than with what a caller pulls.
	const heads, pulls = 40, 3
	var batches []entity.Batch
	scores := map[string]float64{}
	for i := 0; i < heads; i++ {
		dep := fmt.Sprintf("q/dep%02d", i)
		batches = append(batches,
			entity.Batch{ID: dep, State: entity.BatchStateCreated},
			entity.Batch{ID: fmt.Sprintf("q/head%02d", i), State: entity.BatchStateSpeculating,
				Dependencies: []string{dep}},
		)
		scores[dep] = 0.6 + 0.005*float64(i)
	}

	iter, err := New(scored(scores)).Generate(context.Background(), batches)
	require.NoError(t, err)
	it := iteratorOf(t, iter)

	// Count streams whose variables have been sorted. Every head here has two
	// paths, so after a few pulls each pulled head's stream still has a
	// candidate in the global heap and none escapes the count.
	prepared := func() int {
		n := 0
		for _, c := range it.candidates {
			if c.stream.prepared {
				n++
			}
		}
		return n
	}

	require.Equal(t, heads, it.candidates.Len(), "one root per head, and nothing else")
	assert.Zero(t, prepared(), "Generate must not work out any head's flips")

	for i := 0; i < pulls; i++ {
		_, ok, err := iter.Next(context.Background())
		require.NoError(t, err)
		require.True(t, ok)
	}
	assert.LessOrEqual(t, prepared(), pulls, "at most one head per pull")
}

func TestBestFirst_AFailedPullConsumesNothing(t *testing.T) {
	// Next takes a candidate off the heap and must then be unable to fail, or
	// the path it stood for — and every path below it in that head's tree —
	// would be lost with no way to ask for them again. A pull that ends in an
	// error must leave the stream exactly where it was.
	batches := []entity.Batch{
		{ID: "q/dep", State: entity.BatchStateCreated},
		{ID: "q/a", State: entity.BatchStateSpeculating, Dependencies: []string{"q/dep"}},
		{ID: "q/b", State: entity.BatchStateSpeculating},
	}
	sc := scored(map[string]float64{"q/dep": 0.8})

	iter, err := New(sc).Generate(context.Background(), batches)
	require.NoError(t, err)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok, err := iter.Next(cancelled)
	require.Error(t, err)
	require.False(t, ok)

	// Everything the queue had to offer is still on the stream.
	got := drainAll(t, iter)
	assert.Len(t, got, 3, "two paths for q/a, one for q/b")
	assert.ElementsMatch(t, []string{"q/a", "q/a", "q/b"}, heads(got))
}
