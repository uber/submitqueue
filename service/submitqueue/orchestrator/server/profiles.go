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
	nethttp "net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	yamlv3 "gopkg.in/yaml.v3"

	"context"

	platformbuildkite "github.com/uber/submitqueue/platform/buildkite"
	platformgithubactions "github.com/uber/submitqueue/platform/githubactions"
	"github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildkiterunner "github.com/uber/submitqueue/submitqueue/extension/buildrunner/buildkite"
	buildfake "github.com/uber/submitqueue/submitqueue/extension/buildrunner/fake"
	githubactionsrunner "github.com/uber/submitqueue/submitqueue/extension/buildrunner/githubactions"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	cpfake "github.com/uber/submitqueue/submitqueue/extension/changeprovider/fake"
	githubprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/github"
	phabprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/phabricator"
	routingprovider "github.com/uber/submitqueue/submitqueue/extension/changeprovider/routing"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	conflictfake "github.com/uber/submitqueue/submitqueue/extension/conflict/fake"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/none"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/pathoverlap"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/composite"
	scorerfake "github.com/uber/submitqueue/submitqueue/extension/scorer/fake"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/heuristic"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/allocator/sticky"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator/bestfirst"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	specstandard "github.com/uber/submitqueue/submitqueue/extension/speculation/speculator/standard"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Profile holds the per-queue extension implementations. Grouping them per
// queue (rather than per extension) lets the wiring read as "for this queue,
// here are its analyzer, change provider, …", and lets a queue profile start
// from a baseline and override only what differs.
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

// SpeculatorFactory returns a speculator.Factory that resolves the Speculator
// for each queue from the profile registry.
func (p Profiles) SpeculatorFactory() speculator.Factory {
	return speculatorFunc(func(c speculator.Config) (speculator.Speculator, error) {
		return p.For(c.QueueName).Speculator.For(c)
	})
}

// ScorerFactory returns a scorer.Factory that resolves the Scorer for each
// queue from the profile registry.
func (p Profiles) ScorerFactory() scorer.Factory {
	return scorerFunc(func(c scorer.Config) (scorer.Scorer, error) {
		return p.For(c.QueueName).Scorer.For(c)
	})
}

// StorageFactory returns a storage.Factory that routes each queue to its
// profile's storage backend before binding the queue-scoped store aggregate.
func (p Profiles) StorageFactory() storage.Factory {
	return storageFunc(func(c storage.Config) (storage.Storage, error) {
		return p.For(c.QueueName).Storage.For(c)
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

type storageFunc func(storage.Config) (storage.Storage, error)

func (f storageFunc) For(c storage.Config) (storage.Storage, error) { return f(c) }

type scorerFunc func(scorer.Config) (scorer.Scorer, error)

func (f scorerFunc) For(c scorer.Config) (scorer.Scorer, error) { return f(c) }

type speculatorFunc func(speculator.Config) (speculator.Speculator, error)

func (f speculatorFunc) For(c speculator.Config) (speculator.Speculator, error) { return f(c) }

// newProfiles builds the per-queue extension profiles from configuration.
//
// Every queue resolves to its own set of implementations, so one deployment can
// run a queue against a real provider next to one running entirely on fakes —
// which is what lets the same wiring serve both the hermetic test stack and a
// live repository.
func newProfiles(
	logger *zap.Logger,
	scope tally.Scope,
	resolver changeset.Resolver,
	stores storage.Factory,
	cfg profilesConfig,
) (Profiles, error) {
	b := &profileBuilder{
		logger:   logger,
		scope:    scope,
		resolver: resolver,
		stores:   stores,
		built:    make(map[string]any),
	}

	defaultProfile, err := b.build(cfg.Defaults, "defaults")
	if err != nil {
		return Profiles{}, err
	}

	byQueue := make(map[string]Profile, len(cfg.Queues))
	for _, q := range cfg.Queues {
		profile, err := b.build(cfg.resolve(q), fmt.Sprintf("queue %q", q.Name))
		if err != nil {
			return Profiles{}, err
		}
		byQueue[q.Name] = profile
	}

	logger.Info("extension profiles built",
		zap.String("default_change_provider", cfg.Defaults.ChangeProvider.Type),
		zap.String("default_build_runner", cfg.Defaults.BuildRunner.Type),
		zap.String("default_analyzer", cfg.Defaults.Analyzer.Type),
		zap.String("default_scorer", cfg.Defaults.Scorer.Type),
		zap.Int("queue_overrides", len(byQueue)),
	)
	return Profiles{defaultProfile: defaultProfile, byQueue: byQueue}, nil
}

// profileBuilder constructs extension factories, reusing one factory per
// distinct configuration.
//
// What is reused is the factory, not the implementation: an implementation is
// built per resolution so it carries the queue's own Config, while everything
// expensive and queue-independent — the HTTP clients, the resolver, the metrics
// scopes — is built once here and captured by the factory. Several queues
// pointing at one repository therefore still share a single HTTP client.
type profileBuilder struct {
	logger   *zap.Logger
	scope    tally.Scope
	resolver changeset.Resolver
	stores   storage.Factory
	built    map[string]any
}

func (b *profileBuilder) build(cfg queueProfileConfig, where string) (Profile, error) {
	provider, err := reuse(b, cfg.ChangeProvider, where, b.newChangeProviderFactory)
	if err != nil {
		return Profile{}, err
	}
	runner, err := reuse(b, cfg.BuildRunner, where, b.newBuildRunnerFactory)
	if err != nil {
		return Profile{}, err
	}
	analyzer, err := reuse(b, cfg.Analyzer, where, b.newAnalyzerFactory)
	if err != nil {
		return Profile{}, err
	}
	sc, err := reuse(b, cfg.Scorer, where, b.newScorerFactory)
	if err != nil {
		return Profile{}, err
	}
	// The speculator is composed last, because it is built from whatever scorer
	// the profile ended up with.
	return withSpeculator(Profile{
		ChangeProvider: provider,
		BuildRunner:    runner,
		Analyzer:       analyzer,
		Storage:        b.stores,
		Scorer:         sc,
	}), nil
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
// The scorer is resolved lazily, at the queue the speculator itself was asked
// for, so the queue's identity reaches one level down into the scorer too.
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

// batchLines buckets a batch by total lines changed across all its changes —
// larger batches are likelier to fail to land.
func batchLines(_ context.Context, changes entity.BatchChanges) (int, error) {
	return changes.TotalLinesChanged(), nil
}

// newScorerFactory builds the configured scorer's factory.
//
// Every scorer is wrapped by scorerfake so a change URI carrying
// "sq-fake=score-error" forces a scoring error end to end; it is a pure
// passthrough otherwise.
//
// The configuration is walked once up front so an unusable scorer config fails
// at wiring time rather than on the first queue that resolves it.
func (b *profileBuilder) newScorerFactory(cfg scorerConfig, where string) (scorer.Factory, error) {
	if _, err := b.buildScorer(scorer.Config{}, cfg, where, "scorer"); err != nil {
		return nil, err
	}
	return scorerFunc(func(c scorer.Config) (scorer.Scorer, error) {
		inner, err := b.buildScorer(c, cfg, where, "scorer")
		if err != nil {
			return nil, err
		}
		return scorerfake.New(c, b.resolver, inner), nil
	}), nil
}

// buildScorer constructs one scorer for the given queue, recursing for a
// composite's components. scopeName keeps each component's metrics
// distinguishable.
func (b *profileBuilder) buildScorer(c scorer.Config, cfg scorerConfig, where, scopeName string) (scorer.Scorer, error) {
	switch cfg.Type {
	case scorerTypeHeuristic:
		buckets := make([]heuristic.Bucket, 0, len(cfg.Buckets))
		for _, bc := range cfg.Buckets {
			buckets = append(buckets, heuristic.Bucket{Min: bc.Min, Max: bc.Max, Score: bc.Score})
		}
		return heuristic.New(c, b.resolver, buckets, batchLines, b.scope.SubScope(scopeName)), nil

	case scorerTypeComposite:
		components := make(map[string]scorer.Scorer, len(cfg.Components))
		for name, component := range cfg.Components {
			built, err := b.buildScorer(c, component, where, scopeName+"."+name)
			if err != nil {
				return nil, err
			}
			components[name] = built
		}
		// avg is the only combine the schema admits.
		return composite.New(c, components, composite.Avg, b.scope.SubScope(scopeName)), nil

	default:
		return nil, fmt.Errorf("%s: unknown scorer type %q", where, cfg.Type)
	}
}

// reuse returns the instance already built for an identical configuration, or
// builds and remembers one. Identity is the configuration's own YAML encoding,
// which compares nested optional blocks by value rather than by pointer.
func reuse[C any, T any](b *profileBuilder, cfg C, where string, make func(C, string) (T, error)) (T, error) {
	var zero T
	encoded, err := yamlv3.Marshal(cfg)
	if err != nil {
		return zero, fmt.Errorf("%s: failed to key extension config: %w", where, err)
	}
	key := fmt.Sprintf("%T:%s", zero, encoded)
	if existing, ok := b.built[key]; ok {
		return existing.(T), nil
	}
	built, err := make(cfg, where)
	if err != nil {
		return zero, err
	}
	b.built[key] = built
	return built, nil
}

// newChangeProviderFactory builds the configured change provider's factory.
func (b *profileBuilder) newChangeProviderFactory(cfg changeProviderConfig, where string) (changeprovider.Factory, error) {
	switch cfg.Type {
	case changeProviderTypeFake:
		return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
			return cpfake.New(c), nil
		}), nil

	case changeProviderTypeGitHub:
		makeProvider, err := b.newGitHubChangeProvider(*cfg.GitHub, where)
		if err != nil {
			return nil, err
		}
		if makeProvider == nil {
			return nil, fmt.Errorf("%s: github change provider needs %s to be set", where, cfg.GitHub.TokenEnv)
		}
		return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
			return makeProvider(c), nil
		}), nil

	case changeProviderTypePhabricator:
		makeProvider, err := b.newPhabChangeProvider(*cfg.Phabricator, where)
		if err != nil {
			return nil, err
		}
		if makeProvider == nil {
			return nil, fmt.Errorf("%s: phabricator change provider needs %s to be set", where, cfg.Phabricator.TokenEnv)
		}
		return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
			return makeProvider(c), nil
		}), nil

	case changeProviderTypeRouting:
		return b.newRoutingChangeProviderFactory(cfg, where)

	default:
		return nil, fmt.Errorf("%s: unknown change provider type %q", where, cfg.Type)
	}
}

// newRoutingChangeProviderFactory builds a provider that dispatches on the change
// URI's scheme. A configured provider whose token is unset is left out rather than
// failing: routing exists for a deployment that reaches several providers, and
// requiring credentials for all of them to use any of them would make it
// unusable partway through a migration.
func (b *profileBuilder) newRoutingChangeProviderFactory(cfg changeProviderConfig, where string) (changeprovider.Factory, error) {
	var (
		makeGitHub, makePhab func(changeprovider.Config) changeprovider.ChangeProvider
		err                  error
	)
	if cfg.GitHub != nil {
		if makeGitHub, err = b.newGitHubChangeProvider(*cfg.GitHub, where); err != nil {
			return nil, err
		}
	}
	if cfg.Phabricator != nil {
		if makePhab, err = b.newPhabChangeProvider(*cfg.Phabricator, where); err != nil {
			return nil, err
		}
	}

	if makeGitHub == nil && makePhab == nil {
		b.logger.Warn("no change provider tokens set; using fake change provider (empty change info unless URI-marked)",
			zap.String("profile", where))
		return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
			return cpfake.New(c), nil
		}), nil
	}

	return changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
		var gh, phab changeprovider.ChangeProvider
		if makeGitHub != nil {
			gh = makeGitHub(c)
		}
		if makePhab != nil {
			phab = makePhab(c)
		}
		provider, err := routingprovider.NewProvider(routingprovider.Params{Config: c, GitHub: gh, Phabricator: phab})
		if err != nil {
			return nil, fmt.Errorf("%s: failed to create routing change provider: %w", where, err)
		}
		return provider, nil
	}), nil
}

// newGitHubChangeProvider returns a function building the GitHub change provider
// for a given queue, or nil when its token is unset. The HTTP client and metrics
// scope are built once here; only the per-queue wrapper is built per call.
func (b *profileBuilder) newGitHubChangeProvider(cfg githubProviderConfig, where string) (func(changeprovider.Config) changeprovider.ChangeProvider, error) {
	token, ok := tokenFrom(cfg.TokenEnv)
	if !ok {
		return nil, nil
	}
	client, err := b.githubClient(cfg.BaseURL, cfg.Timeout, token, where)
	if err != nil {
		return nil, err
	}
	scope := b.scope.SubScope("changeprovider.github")
	return func(c changeprovider.Config) changeprovider.ChangeProvider {
		return githubprovider.NewProvider(githubprovider.Params{
			Config:       c,
			HTTPClient:   client,
			Logger:       b.logger.Sugar(),
			MetricsScope: scope,
		})
	}, nil
}

// newPhabChangeProvider returns a function building the Phabricator change
// provider for a given queue, or nil when its token is unset. As with GitHub,
// the HTTP client is built once here.
func (b *profileBuilder) newPhabChangeProvider(cfg phabProviderConfig, where string) (func(changeprovider.Config) changeprovider.ChangeProvider, error) {
	token, ok := tokenFrom(cfg.TokenEnv)
	if !ok {
		return nil, nil
	}
	client, err := http.NewClient(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to build Phabricator HTTP client: %w", where, err)
	}
	client.Timeout = timeoutOr(cfg.Timeout, defaultHTTPTimeout)

	baseTransport := client.Transport.(*http.BaseURLTransport)
	baseTransport.Next = &apiTokenTransport{token: token, next: baseTransport.Next}

	scope := b.scope.SubScope("changeprovider.phabricator")
	return func(c changeprovider.Config) changeprovider.ChangeProvider {
		return phabprovider.NewProvider(phabprovider.Params{
			Config:       c,
			HTTPClient:   client,
			Logger:       b.logger.Sugar(),
			MetricsScope: scope,
		})
	}, nil
}

// newBuildRunnerFactory builds the configured build runner's factory. The HTTP
// client is built once here; only the per-queue runner is built per call.
func (b *profileBuilder) newBuildRunnerFactory(cfg buildRunnerConfig, where string) (buildrunner.Factory, error) {
	switch cfg.Type {
	case buildRunnerTypeFake:
		return buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
			return buildfake.New(c, b.resolver), nil
		}), nil

	case buildRunnerTypeGitHubActions:
		token, ok := tokenFrom(cfg.TokenEnv)
		if !ok {
			return nil, fmt.Errorf("%s: githubactions build runner needs %s to be set", where, cfg.TokenEnv)
		}
		client, err := b.githubClient(cfg.BaseURL, cfg.Timeout, token, where)
		if err != nil {
			return nil, err
		}
		actions := platformgithubactions.NewClient(client, cfg.Owner, cfg.Repo, cfg.Workflow)
		return buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
			return githubactionsrunner.NewBuildRunner(githubactionsrunner.Params{
				Config:      c,
				Client:      actions,
				Resolver:    b.resolver,
				Logger:      b.logger.Sugar(),
				Ref:         cfg.Ref,
				ExtraInputs: cfg.ExtraInputs,
			}), nil
		}), nil

	case buildRunnerTypeBuildkite:
		token, ok := tokenFrom(cfg.TokenEnv)
		if !ok {
			return nil, fmt.Errorf("%s: buildkite build runner needs %s to be set", where, cfg.TokenEnv)
		}
		client, err := http.NewClient(cfg.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to build Buildkite HTTP client: %w", where, err)
		}
		client.Timeout = timeoutOr(cfg.Timeout, defaultHTTPTimeout)
		client.Transport = &oauth2.Transport{
			Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}),
			Base:   client.Transport,
		}
		bk := platformbuildkite.NewClient(client)
		return buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
			return buildkiterunner.NewBuildRunner(buildkiterunner.Params{
				Config:   c,
				Client:   bk,
				Resolver: b.resolver,
				Logger:   b.logger.Sugar(),
			}), nil
		}), nil

	default:
		return nil, fmt.Errorf("%s: unknown build runner type %q", where, cfg.Type)
	}
}

// newAnalyzerFactory builds the configured conflict analyzer's factory. The
// type is validated here so an unusable config fails at wiring time rather than
// on the first queue that resolves it.
func (b *profileBuilder) newAnalyzerFactory(cfg analyzerConfig, where string) (conflict.Factory, error) {
	switch cfg.Type {
	case analyzerTypeAll, analyzerTypeNone, analyzerTypePathOverlap:
	default:
		return nil, fmt.Errorf("%s: unknown analyzer type %q", where, cfg.Type)
	}

	return analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
		var delegate conflict.Analyzer
		switch cfg.Type {
		case analyzerTypeAll:
			// Serializes the queue conservatively.
			// TODO: replace with a real analyzer (e.g. Tango target analysis).
			delegate = all.New(c)
		case analyzerTypeNone:
			delegate = none.New(c)
		case analyzerTypePathOverlap:
			// Serializes only batches whose changed paths collide at the
			// configured granularity, resolving each batch's paths itself via
			// the resolver.
			return pathoverlap.New(c, b.resolver, pathOverlapKey(cfg.By)), nil
		}

		if cfg.FailAlways {
			return conflictfake.New(c, delegate, conflictfake.FailAlways), nil
		}
		return conflictfake.New(c, delegate, nil), nil
	}), nil
}

// pathOverlapKey maps the configured granularity onto the projection the
// analyzer keys on. The value is validated when the config is loaded, so an
// unrecognized one here would be a programming error; it falls back to the
// narrowest granularity rather than widening a queue's conflicts silently.
func pathOverlapKey(by string) pathoverlap.PathKey {
	if by == pathOverlapByDirectory {
		return pathoverlap.ByDirectory
	}
	return pathoverlap.ByFile
}

// githubClient builds an HTTP client rooted at a GitHub API and authenticated
// with the given token. Shared by the change provider and the Actions runner,
// which differ only in which endpoints they call.
func (b *profileBuilder) githubClient(baseURL, timeout, token, where string) (*nethttp.Client, error) {
	client, err := http.NewClient(baseURL)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to build GitHub HTTP client: %w", where, err)
	}
	client.Timeout = timeoutOr(timeout, defaultHTTPTimeout)
	client.Transport = &oauth2.Transport{
		Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}),
		Base:   client.Transport,
	}
	return client, nil
}

// apiTokenTransport injects a Phabricator API token as a query parameter in
// each request.
type apiTokenTransport struct {
	token string
	next  nethttp.RoundTripper
}

func (t *apiTokenTransport) RoundTrip(req *nethttp.Request) (*nethttp.Response, error) {
	r := req.Clone(req.Context())
	q := r.URL.Query()
	q.Set("api.token", t.token)
	r.URL.RawQuery = q.Encode()
	return t.next.RoundTrip(r)
}
