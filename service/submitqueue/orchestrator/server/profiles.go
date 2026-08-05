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
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildfake "github.com/uber/submitqueue/submitqueue/extension/buildrunner/fake"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	conflictfake "github.com/uber/submitqueue/submitqueue/extension/conflict/fake"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/fileoverlap"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/none"
	"go.uber.org/zap"
)

// Profile holds the per-queue extension implementations. Grouping them per
// queue (rather than per extension) lets the wiring read as "for this queue,
// here are its analyzer, change provider, …", and lets a queue profile start
// from a baseline and override only what differs.
type Profile struct {
	// ChangeProvider resolves change metadata for requests in this queue.
	ChangeProvider changeprovider.ChangeProvider

	// BuildRunner triggers and polls builds for batches in this queue.
	BuildRunner buildrunner.BuildRunner

	// Analyzer detects conflicts between concurrent batches in this queue.
	Analyzer conflict.Analyzer
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

// ChangeProviderFactory returns a changeprovider.Factory that resolves the
// ChangeProvider for each queue from the profile registry.
func (p Profiles) ChangeProviderFactory() changeprovider.Factory {
	return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
		return p.For(c.QueueName).ChangeProvider, nil
	})
}

// BuildRunnerFactory returns a buildrunner.Factory that resolves the
// BuildRunner for each queue from the profile registry.
func (p Profiles) BuildRunnerFactory() buildrunner.Factory {
	return buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
		return p.For(c.QueueName).BuildRunner, nil
	})
}

// AnalyzerFactory returns a conflict.Factory that resolves the Analyzer for
// each queue from the profile registry.
func (p Profiles) AnalyzerFactory() conflict.Factory {
	return analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
		return p.For(c.QueueName).Analyzer, nil
	})
}

// Thin func-type adapters — the http.HandlerFunc trick applied to each
// extension Factory interface. Each func type satisfies the Factory contract,
// letting Profiles cross the host/library boundary without dedicated structs.

type changeProviderFunc func(changeprovider.Config) (changeprovider.ChangeProvider, error)

func (f changeProviderFunc) For(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
	return f(c)
}

type buildRunnerFunc func(buildrunner.Config) (buildrunner.BuildRunner, error)

func (f buildRunnerFunc) For(c buildrunner.Config) (buildrunner.BuildRunner, error) { return f(c) }

type analyzerFunc func(conflict.Config) (conflict.Analyzer, error)

func (f analyzerFunc) For(c conflict.Config) (conflict.Analyzer, error) { return f(c) }

// newProfiles builds the per-queue extension profiles for the example.
// Edge integrations (change provider) and the build runner form a shared
// baseline; each per-queue profile starts from that baseline and overrides
// only the extensions that differ — here the conflict analyzer.
// Queues without an explicit profile fall back to the baseline.
func newProfiles(logger *zap.Logger, scope tally.Scope, resolver changeset.Resolver) (Profiles, error) {
	cp, err := newChangeProvider(logger, scope)
	if err != nil {
		return Profiles{}, fmt.Errorf("failed to create change provider: %w", err)
	}

	// Baseline profile: shared edge integrations + a fake build runner (every
	// build succeeds unless a head URI carries a failure marker). The build
	// runner instance is shared by the build and buildsignal controllers (same
	// profile, same instance) so a build's recorded outcome survives across
	// their separate factory lookups.
	//
	// The analyzer is wrapped by conflictfake with a nil predicate
	// (passthrough) — swap the predicate (e.g. conflictfake.FailAlways) on a
	// queue to exercise the analyzer error path, as e2e-conflict-error-queue
	// below does.
	base := Profile{
		ChangeProvider: cp,
		BuildRunner:    buildfake.New(resolver),
		// TODO: replace the delegate with a real analyzer (e.g. Tango target
		// analysis). "all" serializes the queue conservatively.
		Analyzer: conflictfake.New(all.New(), nil),
	}

	// e2e-conflict-error-queue: every conflict analysis fails, exercising the
	// analyzer error path. Edge integrations inherit the baseline.
	conflictErrQueue := base
	conflictErrQueue.Analyzer = conflictfake.New(all.New(), conflictfake.FailAlways)

	// file-overlap-queue: a real analyzer that serializes only batches sharing
	// a changed file, resolving each batch's files itself via the resolver.
	fileOverlapQueue := base
	fileOverlapQueue.Analyzer = fileoverlap.New(resolver)

	// e2e-test-queue: no conflicts (maximum parallelism).
	e2eQueue := base
	e2eQueue.Analyzer = conflictfake.New(none.New(), nil)

	return Profiles{
		defaultProfile: base,
		byQueue: map[string]Profile{
			"e2e-test-queue":           e2eQueue,
			"e2e-conflict-error-queue": conflictErrQueue,
			"file-overlap-queue":       fileOverlapQueue,
		},
	}, nil
}
