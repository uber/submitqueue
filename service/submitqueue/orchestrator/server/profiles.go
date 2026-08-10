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

package main

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildfake "github.com/uber/submitqueue/submitqueue/extension/buildrunner/fake"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	conflictfake "github.com/uber/submitqueue/submitqueue/extension/conflict/fake"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/fileoverlap"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/none"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/composite"
	scorerfake "github.com/uber/submitqueue/submitqueue/extension/scorer/fake"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/heuristic"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/allocator/sticky"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator/bestfirst"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	specstandard "github.com/uber/submitqueue/submitqueue/extension/speculation/speculator/standard"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Profile holds the per-queue extension factories. Grouping them per queue
// (rather than per extension) lets the wiring read as "for this queue, here are
// its analyzer, change provider, …", and lets a queue profile start from a
// baseline and override only what differs.
//
// Every field is a Factory rather than a built implementation. Selecting the
// profile consumes only the queue name; the Config itself is forwarded into the
// profile's factory, so the implementation is constructed knowing which queue it
// serves. That matters most for defaultProfile, which backs every queue without
// an explicit entry: one shared instance could not carry a correct queue name,
// but one factory can build a correct instance per queue.
type Profile struct {
	// ChangeProvider resolves change metadata for requests in this queue.
	ChangeProvider changeprovider.Factory

	// BuildRunner triggers and polls builds for batches in this queue.
	BuildRunner buildrunner.Factory

	// Analyzer detects conflicts between concurrent batches in this queue.
	Analyzer conflict.Factory

	// Storage resolves the queue-scoped store aggregate for this queue. Every
	// profile points at the shared backend by default; a deployment that
	// splits queues across storage backends overrides this per queue.
	Storage storage.Factory

	// Scorer holds this queue's scoring profile. There is no scoring stage: the
	// scorer feeds the queue's speculator, which ranks candidate paths by how
	// likely their assumptions are to hold.
	Scorer scorer.Factory

	// Speculator decides which of this queue's speculation paths to build and
	// which running ones to preempt, within the build budget.
	Speculator speculator.Factory
}

// Profiles maps a queue name to its extension Profile, falling back to a
// default profile for queues without an explicit entry. This is the single
// place that knows the queue topology; the extension packages remain
// queue-agnostic.
type Profiles struct {
	byQueue        map[string]Profile
	defaultProfile Profile
}

// For returns the profile for the named queue, or the default.
func (p Profiles) For(queue string) Profile {
	if prof, ok := p.byQueue[queue]; ok {
		return prof
	}
	return p.defaultProfile
}

// Each XFactory method resolves the queue's profile by name, then forwards the
// whole Config to that profile's factory. The second For(c) is what carries the
// queue identity past the profile lookup and into the implementation.

// ChangeProviderFactory returns a changeprovider.Factory that resolves the
// ChangeProvider for each queue from the profile registry.
func (p Profiles) ChangeProviderFactory() changeprovider.Factory {
	return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
		return p.For(c.QueueName).ChangeProvider.For(c)
	})
}

// BuildRunnerFactory returns a buildrunner.Factory that resolves the
// BuildRunner for each queue from the profile registry.
func (p Profiles) BuildRunnerFactory() buildrunner.Factory {
	return buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
		return p.For(c.QueueName).BuildRunner.For(c)
	})
}

// AnalyzerFactory returns a conflict.Factory that resolves the Analyzer for
// each queue from the profile registry.
func (p Profiles) AnalyzerFactory() conflict.Factory {
	return analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
		return p.For(c.QueueName).Analyzer.For(c)
	})
}

// StorageFactory returns a storage.Factory that routes each queue to its
// profile's storage backend before binding the queue-scoped store aggregate.
func (p Profiles) StorageFactory() storage.Factory {
	return storageFunc(func(c storage.Config) (storage.Storage, error) {
		return p.For(c.QueueName).Storage.For(c)
	})
}

// ScorerFactory returns a scorer.Factory that resolves the Scorer for each
// queue from the profile registry.
func (p Profiles) ScorerFactory() scorer.Factory {
	return scorerFunc(func(c scorer.Config) (scorer.Scorer, error) {
		return p.For(c.QueueName).Scorer.For(c)
	})
}

// SpeculatorFactory returns a speculator.Factory that resolves the Speculator
// for each queue from the profile registry.
func (p Profiles) SpeculatorFactory() speculator.Factory {
	return speculatorFunc(func(c speculator.Config) (speculator.Speculator, error) {
		return p.For(c.QueueName).Speculator.For(c)
	})
}

// Thin func-type adapters — the http.HandlerFunc trick applied to each
// extension Factory interface. Each func type satisfies the Factory contract,
// letting Profiles cross the host/library boundary without dedicated structs,
// and letting a Profile field hold a closure instead of a built instance.

type changeProviderFunc func(changeprovider.Config) (changeprovider.ChangeProvider, error)

func (f changeProviderFunc) For(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
	return f(c)
}

type buildRunnerFunc func(buildrunner.Config) (buildrunner.BuildRunner, error)

func (f buildRunnerFunc) For(c buildrunner.Config) (buildrunner.BuildRunner, error) { return f(c) }

type analyzerFunc func(conflict.Config) (conflict.Analyzer, error)

func (f analyzerFunc) For(c conflict.Config) (conflict.Analyzer, error) { return f(c) }

type storageFunc func(storage.Config) (storage.Storage, error)

func (f storageFunc) For(c storage.Config) (storage.Storage, error) { return f(c) }

type scorerFunc func(scorer.Config) (scorer.Scorer, error)

func (f scorerFunc) For(c scorer.Config) (scorer.Scorer, error) { return f(c) }

type speculatorFunc func(speculator.Config) (speculator.Speculator, error)

func (f speculatorFunc) For(c speculator.Config) (speculator.Speculator, error) { return f(c) }

// newProfiles builds the per-queue extension profiles for the example.
// Edge integrations (change provider) and the build runner form a shared
// baseline; each per-queue profile starts from that baseline and overrides
// only the extensions that differ — here the conflict analyzer and the scorer.
// Queues without an explicit profile fall back to the baseline.
//
// Anything expensive and queue-independent — the resolver, the metrics scope,
// the change provider's HTTP clients — is built once, here. The closures below
// only construct the cheap per-queue wrapper, which is the same shape
// counterFactory.For already uses for the MySQL counter.
func newProfiles(logger *zap.Logger, scope tally.Scope, resolver changeset.Resolver, stores storage.Factory) (Profiles, error) {
	changeProviders, err := newChangeProviderFactory(logger, scope)
	if err != nil {
		return Profiles{}, fmt.Errorf("failed to create change provider: %w", err)
	}

	// batchLines buckets a batch by total lines changed across all its changes —
	// larger batches are likelier to fail to land.
	batchLines := func(_ context.Context, changes entity.BatchChanges) (int, error) {
		return changes.TotalLinesChanged(), nil
	}

	// heuristicScorer builds a bucketed heuristic scorer, wrapped by scorerfake
	// so a change URI carrying "sq-fake=score-error" forces a scoring error
	// end-to-end; the wrapper is a pure passthrough otherwise.
	heuristicScorer := func(buckets []heuristic.Bucket, scopeName string) scorer.Factory {
		return scorerFunc(func(c scorer.Config) (scorer.Scorer, error) {
			return scorerfake.New(c, resolver, heuristic.New(
				c, resolver, buckets, batchLines, scope.SubScope(scopeName),
			)), nil
		})
	}

	// Baseline profile: shared edge integrations + a fake build runner (every
	// build succeeds unless a head URI carries a failure marker), plus permissive
	// defaults for scorer and conflict.
	//
	// The analyzer is wrapped by conflictfake with a nil predicate (passthrough)
	// — swap the predicate (e.g. conflictfake.FailAlways) on a queue to exercise
	// the analyzer error path, as e2e-conflict-error-queue below does.
	base := Profile{
		ChangeProvider: changeProviders,
		BuildRunner: buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
			return buildfake.New(c, resolver), nil
		}),
		// TODO: replace the delegate with a real analyzer (e.g. Tango target
		// analysis). "all" serializes the queue conservatively.
		Analyzer: analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
			return conflictfake.New(c, all.New(c), nil), nil
		}),
		Storage: stores,
		Scorer: heuristicScorer(
			[]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: 0.5}},
			"scorer.default",
		),
	}

	// test-queue: bucketed heuristic scorer; conservative (serialized) conflicts
	// inherited from the baseline.
	testQueue := base
	testQueue.Scorer = heuristicScorer(
		[]heuristic.Bucket{
			{Min: 0, Max: 1, Score: 0.95},
			{Min: 2, Max: 5, Score: 0.80},
			{Min: 6, Max: 20, Score: 0.60},
			{Min: 21, Max: 1<<31 - 1, Score: 0.40},
		},
		"scorer.test-queue",
	)

	// e2e-conflict-error-queue: every conflict analysis fails, exercising the
	// analyzer error path. Scorer/edge integrations inherit the baseline.
	conflictErrQueue := base
	conflictErrQueue.Analyzer = analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
		return conflictfake.New(c, all.New(c), conflictfake.FailAlways), nil
	})

	// file-overlap-queue: a real analyzer that serializes only batches sharing
	// a changed file, resolving each batch's files itself via the resolver.
	fileOverlapQueue := base
	fileOverlapQueue.Analyzer = analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
		return fileoverlap.New(c, resolver), nil
	})

	// e2e-test-queue: composite scorer; no conflicts (maximum parallelism).
	e2eQueue := base
	e2eQueue.Analyzer = analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
		return conflictfake.New(c, none.New(c), nil), nil
	})
	e2eQueue.Scorer = scorerFunc(func(c scorer.Config) (scorer.Scorer, error) {
		flat := func(score float64) scorer.Scorer {
			return heuristic.New(c, resolver,
				[]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: score}}, batchLines, scope)
		}
		return scorerfake.New(c, resolver, composite.New(
			c,
			map[string]scorer.Scorer{"size": flat(0.8), "flat": flat(0.6)},
			composite.Avg, scope.SubScope("scorer.e2e-test-queue"),
		)), nil
	})

	// The speculator is composed last, because it is built from whatever scorer
	// the profile ended up with.
	return Profiles{
		defaultProfile: withSpeculator(base),
		byQueue: map[string]Profile{
			"test-queue":               withSpeculator(testQueue),
			"e2e-test-queue":           withSpeculator(e2eQueue),
			"e2e-conflict-error-queue": withSpeculator(conflictErrQueue),
			"file-overlap-queue":       withSpeculator(fileOverlapQueue),
		},
	}, nil
}

// defaultBuildBudget caps how many builds a queue may have occupying CI at
// once. It is the only rationing lever the allocator has.
//
// TODO: move this onto entity.QueueConfig so operators can tune it per queue
// without a code change. QueueConfig carries only the queue name today.
const defaultBuildBudget = 4

// withSpeculator returns the profile with its speculator composed from its own
// scorer: bestfirst ranks a queue's candidate paths by how likely all their
// assumptions are to hold, and sticky spends the build budget down that ranking
// without preempting builds already running. Swapping either part changes the
// policy without touching the speculate controller, which depends only on the
// Speculator contract.
//
// The scorer is resolved at the same queue identity the speculator was asked
// for, so a queue falling through to defaultProfile gets a speculator whose
// underlying scorer was also built for that queue.
func withSpeculator(p Profile) Profile {
	p.Speculator = speculatorFunc(func(c speculator.Config) (speculator.Speculator, error) {
		sc, err := p.Scorer.For(scorer.Config{QueueName: c.QueueName})
		if err != nil {
			return nil, fmt.Errorf("failed to resolve scorer for queue %q: %w", c.QueueName, err)
		}
		return specstandard.New(c, bestfirst.New(sc), sticky.New(defaultBuildBudget)), nil
	})
	return p
}
