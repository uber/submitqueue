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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
)

func writeProfiles(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profiles.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestDefaultProfilesConfig_MatchesTheBuiltInTopology(t *testing.T) {
	// The e2e suite depends on these queues behaving exactly as they did before
	// profiles became configurable, so this pins the built-in fallback.
	t.Setenv("PHAB_API_ENDPOINT", "")

	cfg := defaultProfilesConfig()

	assert.Equal(t, changeProviderTypeRouting, cfg.Defaults.ChangeProvider.Type)
	assert.Equal(t, buildRunnerTypeFake, cfg.Defaults.BuildRunner.Type)
	assert.Equal(t, analyzerTypeAll, cfg.Defaults.Analyzer.Type)
	assert.False(t, cfg.Defaults.Analyzer.FailAlways)
	assert.Nil(t, cfg.Defaults.ChangeProvider.Phabricator,
		"phabricator joins routing only once an endpoint says where to go")

	byName := make(map[string]namedQueueProfileConfig, len(cfg.Queues))
	for _, q := range cfg.Queues {
		byName[q.Name] = q
	}

	tests := []struct {
		queue      string
		wantType   string
		wantBy     string
		failAlways bool
	}{
		{queue: "e2e-test-queue", wantType: analyzerTypeNone},
		{queue: "e2e-conflict-error-queue", wantType: analyzerTypeAll, failAlways: true},
		{queue: "file-overlap-queue", wantType: analyzerTypePathOverlap, wantBy: pathOverlapByFile},
	}
	for _, tt := range tests {
		t.Run(tt.queue, func(t *testing.T) {
			q, ok := byName[tt.queue]
			require.True(t, ok)
			require.NotNil(t, q.Analyzer)
			assert.Equal(t, tt.wantType, q.Analyzer.Type)
			assert.Equal(t, tt.wantBy, q.Analyzer.By)
			assert.Equal(t, tt.failAlways, q.Analyzer.FailAlways)

			// Only the analyzer differs; everything else stays the baseline.
			assert.Nil(t, q.ChangeProvider)
			assert.Nil(t, q.BuildRunner)
		})
	}
}

func TestDefaultProfilesConfig_AddsPhabricatorWhenPointedAtOne(t *testing.T) {
	t.Setenv("PHAB_API_ENDPOINT", "https://phab.example.com/api")

	cfg := defaultProfilesConfig()
	require.NotNil(t, cfg.Defaults.ChangeProvider.Phabricator)
	assert.Equal(t, "https://phab.example.com/api", cfg.Defaults.ChangeProvider.Phabricator.Endpoint)
}

func TestLoadProfilesConfig_InheritsPerExtension(t *testing.T) {
	// A queue that differs only in its analyzer says only that, and keeps the
	// default change provider and build runner.
	path := writeProfiles(t, `
defaults:
  changeProvider: {type: fake}
  buildRunner: {type: fake}
  analyzer: {type: all}
queues:
  - name: parallel
    analyzer: {type: none}
`)

	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)

	resolved := cfg.resolve(cfg.Queues[0])
	assert.Equal(t, analyzerTypeNone, resolved.Analyzer.Type)
	assert.Equal(t, changeProviderTypeFake, resolved.ChangeProvider.Type)
	assert.Equal(t, buildRunnerTypeFake, resolved.BuildRunner.Type)
}

func TestLoadProfilesConfig_AppliesForgeDefaults(t *testing.T) {
	path := writeProfiles(t, `
defaults:
  changeProvider: {type: github}
  buildRunner: {type: githubactions, owner: uber, repo: sq-sandbox, workflow: validate.yml}
`)

	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)

	require.NotNil(t, cfg.Defaults.ChangeProvider.GitHub)
	assert.Equal(t, defaultGitHubTokenEnv, cfg.Defaults.ChangeProvider.GitHub.TokenEnv)
	assert.Equal(t, defaultGitHubBaseURL, cfg.Defaults.ChangeProvider.GitHub.BaseURL)
	assert.Equal(t, defaultGitHubTokenEnv, cfg.Defaults.BuildRunner.TokenEnv)
	assert.Equal(t, defaultGitHubBaseURL, cfg.Defaults.BuildRunner.BaseURL)
}

func TestLoadProfilesConfig_EmptyFileIsAllFakes(t *testing.T) {
	cfg, err := loadProfilesConfig(writeProfiles(t, ""))
	require.NoError(t, err)
	assert.Equal(t, changeProviderTypeFake, cfg.Defaults.ChangeProvider.Type)
	assert.Equal(t, buildRunnerTypeFake, cfg.Defaults.BuildRunner.Type)
	assert.Equal(t, analyzerTypeAll, cfg.Defaults.Analyzer.Type)
}

func TestLoadProfilesConfig_PathOverlapDefaultsToFile(t *testing.T) {
	cfg, err := loadProfilesConfig(writeProfiles(t, "defaults:\n  analyzer: {type: pathoverlap}\n"))
	require.NoError(t, err)
	assert.Equal(t, pathOverlapByFile, cfg.Defaults.Analyzer.By,
		"the narrower granularity is the safer default: directory would widen a queue's conflicts")

	cfg, err = loadProfilesConfig(writeProfiles(t, "defaults:\n  analyzer: {type: pathoverlap, by: directory}\n"))
	require.NoError(t, err)
	assert.Equal(t, pathOverlapByDirectory, cfg.Defaults.Analyzer.By)
}

func TestLoadProfilesConfig_Rejects(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{
			name:     "unknown change provider",
			contents: "defaults:\n  changeProvider: {type: gitlab}\n",
		},
		{
			name:     "unknown build runner",
			contents: "defaults:\n  buildRunner: {type: jenkins}\n",
		},
		{
			name:     "unknown analyzer",
			contents: "defaults:\n  analyzer: {type: clairvoyant}\n",
		},
		{
			name:     "unknown pathoverlap granularity",
			contents: "defaults:\n  analyzer: {type: pathoverlap, by: repository}\n",
		},
		{
			name:     "phabricator without an endpoint",
			contents: "defaults:\n  changeProvider: {type: phabricator}\n",
		},
		{
			name:     "routing with nothing to route to",
			contents: "defaults:\n  changeProvider: {type: routing}\n",
		},
		{
			name:     "github actions without a workflow",
			contents: "defaults:\n  buildRunner: {type: githubactions, owner: uber, repo: r}\n",
		},
		{
			name:     "buildkite without a pipeline url",
			contents: "defaults:\n  buildRunner: {type: buildkite, tokenEnv: BK}\n",
		},
		{
			name:     "queue without a name",
			contents: "queues:\n  - analyzer: {type: none}\n",
		},
		{
			name:     "duplicate queue",
			contents: "queues:\n  - name: a\n  - name: a\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadProfilesConfig(writeProfiles(t, tt.contents))
			require.Error(t, err)
		})
	}
}

func TestNewProfiles_ReusesOneBuildRunnerFactoryAcrossIdenticalQueues(t *testing.T) {
	// Queues configured alike resolve to one factory, which is what keeps a
	// single HTTP client behind several queues pointing at the same repository.
	//
	// It is the factory that is shared, not the runner: implementations are
	// built per resolution so each carries its own queue's Config. Sharing an
	// instance is not required, because the fake runner encodes a build's
	// outcome in the BuildID it returns rather than holding it in memory, so
	// the build and buildsignal controllers can look it up independently.
	path := writeProfiles(t, `
defaults:
  changeProvider: {type: fake}
  buildRunner: {type: fake}
  analyzer: {type: all}
queues:
  - name: a
    analyzer: {type: none}
  - name: b
    analyzer: {type: none}
`)
	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)

	b := &profileBuilder{
		logger: zaptest.NewLogger(t),
		scope:  tally.NoopScope,
		built:  make(map[string]any),
	}

	runnerConfigs := []buildRunnerConfig{cfg.Defaults.BuildRunner}
	for _, q := range cfg.Queues {
		runnerConfigs = append(runnerConfigs, cfg.resolve(q).BuildRunner)
	}
	for _, rc := range runnerConfigs {
		_, err := reuse(b, rc, "profile", b.newBuildRunnerFactory)
		require.NoError(t, err)
	}

	assert.Len(t, b.built, 1,
		"the default profile and both queues are configured alike, so they share one factory")
}

// analyzeOutcome runs an analyzer against one in-flight batch and reduces the
// result to what distinguishes the three kinds from each other.
func analyzeOutcome(t *testing.T, analyzer conflict.Analyzer) (conflicts int, failed bool) {
	t.Helper()
	batch := entity.Batch{ID: "q/batch/2", Queue: "q"}
	inFlight := []entity.Batch{{ID: "q/batch/1", Queue: "q"}}

	found, err := analyzer.Analyze(context.Background(), batch, inFlight)
	if err != nil {
		return 0, true
	}
	return len(found), false
}

func TestNewProfiles_ResolvesPerQueueAnalyzers(t *testing.T) {
	// Asserted on behavior rather than instance identity: the analyzers are
	// stateless value types, so what matters is that each queue got the type it
	// asked for.
	profiles, err := newProfiles(zaptest.NewLogger(t), tally.NoopScope, nil, nil, defaultProfilesConfig())
	require.NoError(t, err)

	tests := []struct {
		queue         string
		wantConflicts int
		wantFailed    bool
	}{
		// Maximum parallelism: never conflicts with anything in flight.
		{queue: "e2e-test-queue", wantConflicts: 0},
		// Serializes conservatively: conflicts with everything in flight.
		{queue: "unlisted-falls-back-to-default", wantConflicts: 1},
		// Exercises the analyzer error path.
		{queue: "e2e-conflict-error-queue", wantFailed: true},
	}
	for _, tt := range tests {
		t.Run(tt.queue, func(t *testing.T) {
			analyzer, err := profiles.AnalyzerFactory().For(conflict.Config{QueueName: tt.queue})
			require.NoError(t, err)

			conflicts, failed := analyzeOutcome(t, analyzer)
			assert.Equal(t, tt.wantFailed, failed)
			if !tt.wantFailed {
				assert.Equal(t, tt.wantConflicts, conflicts)
			}
		})
	}
}

func TestNewProfiles_FailsWhenAForgeTokenIsMissing(t *testing.T) {
	// Failing at startup beats accepting land requests and discovering the
	// credential gap one API call into the first merge.
	t.Setenv("TEST_GH_TOKEN_UNSET", "")
	path := writeProfiles(t, `
defaults:
  changeProvider:
    type: github
    github: {tokenEnv: TEST_GH_TOKEN_UNSET}
`)
	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)

	_, err = newProfiles(zaptest.NewLogger(t), tally.NoopScope, nil, nil, cfg)
	require.Error(t, err)
}

func TestNewProfiles_RoutingFallsBackToFakeWithoutTokens(t *testing.T) {
	// Routing serves a deployment reaching several providers, so a provider whose
	// token is unset drops out rather than failing the whole provider.
	t.Setenv("TEST_GH_TOKEN_UNSET", "")
	path := writeProfiles(t, `
defaults:
  changeProvider:
    type: routing
    github: {tokenEnv: TEST_GH_TOKEN_UNSET}
`)
	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)

	profiles, err := newProfiles(zaptest.NewLogger(t), tally.NoopScope, nil, nil, cfg)
	require.NoError(t, err)
	assert.NotNil(t, profiles.For("anything").ChangeProvider)
}

func TestDefaultProfilesConfig_KeepsPerQueueScorers(t *testing.T) {
	// Speculation ranks candidate paths by score, so a queue silently losing its
	// scoring profile changes which paths get built without failing anything.
	cfg := defaultProfilesConfig()

	byName := make(map[string]namedQueueProfileConfig, len(cfg.Queues))
	for _, q := range cfg.Queues {
		byName[q.Name] = q
	}

	assert.Equal(t, scorerTypeHeuristic, cfg.Defaults.Scorer.Type)
	assert.Len(t, cfg.Defaults.Scorer.Buckets, 1, "the baseline scores every batch alike")

	bucketed, ok := byName["test-queue"]
	require.True(t, ok)
	require.NotNil(t, bucketed.Scorer)
	assert.Equal(t, scorerTypeHeuristic, bucketed.Scorer.Type)
	assert.Len(t, bucketed.Scorer.Buckets, 4, "smaller batches must rank ahead of larger ones")

	comp, ok := byName["e2e-test-queue"]
	require.True(t, ok)
	require.NotNil(t, comp.Scorer)
	assert.Equal(t, scorerTypeComposite, comp.Scorer.Type)
	assert.ElementsMatch(t, []string{"size", "flat"}, keysOf(comp.Scorer.Components))
}

func keysOf(m map[string]scorerConfig) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestNewProfiles_ComposesASpeculatorPerQueue(t *testing.T) {
	// Speculation is only "on" if every queue resolves to a real speculator —
	// a nil one leaves the stage inert and nothing ever gets built.
	profiles, err := newProfiles(zaptest.NewLogger(t), tally.NoopScope, nil, nil, defaultProfilesConfig())
	require.NoError(t, err)

	for _, queue := range []string{"test-queue", "e2e-test-queue", "file-overlap-queue", "unlisted"} {
		t.Run(queue, func(t *testing.T) {
			p := profiles.For(queue)
			assert.NotNil(t, p.Scorer, "scorer feeds the speculator")
			assert.NotNil(t, p.Speculator)
		})
	}
}

// TestNewProfiles_SpendsTheConfiguredBuildBudget is the assertion that matters
// for the setting: parsing a number proves nothing if it never reaches the
// allocator, so this drives a real speculator and counts what it proposes.
//
// Each batch speculates with no dependencies, so every one is a candidate and
// the only thing capping the proposals is the budget.
func TestNewProfiles_SpendsTheConfiguredBuildBudget(t *testing.T) {
	path := writeProfiles(t, `
defaults:
  speculator: {buildBudget: 2}
queues:
  - name: wide-queue
    speculator: {buildBudget: 5}
  - name: inherits-queue
    analyzer: {type: none}
`)
	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)
	profiles, err := newProfiles(zaptest.NewLogger(t), tally.NoopScope, nil, nil, cfg)
	require.NoError(t, err)

	batches := make([]entity.Batch, 0, 8)
	for i := range 8 {
		batches = append(batches, entity.Batch{
			ID:    fmt.Sprintf("b%d", i),
			State: entity.BatchStateSpeculating,
		})
	}

	for _, tt := range []struct {
		queue string
		want  int
	}{
		{queue: "wide-queue", want: 5},
		{queue: "inherits-queue", want: 2},
		{queue: "unlisted-queue", want: 2},
	} {
		t.Run(tt.queue, func(t *testing.T) {
			spec, err := profiles.SpeculatorFactory().For(speculator.Config{QueueName: tt.queue})
			require.NoError(t, err)

			proposals, err := spec.Speculate(context.Background(), batches, nil)
			require.NoError(t, err)
			assert.Len(t, proposals, tt.want)
		})
	}
}

func TestLoadProfilesConfig_RejectsBudgets(t *testing.T) {
	// A negative budget leaves sticky with no free slots forever, so a queue
	// would batch and then never build — indistinguishable from a stuck queue.
	path := writeProfiles(t, `
defaults:
  speculator: {buildBudget: -1}
`)
	_, err := loadProfilesConfig(path)
	require.Error(t, err)
}

func TestLoadProfilesConfig_DefaultsAnUnstatedBudget(t *testing.T) {
	path := writeProfiles(t, `
defaults: {}
queues:
  - name: q
    speculator: {buildBudget: 9}
`)
	cfg, err := loadProfilesConfig(path)
	require.NoError(t, err)

	assert.Equal(t, defaultBuildBudget, cfg.Defaults.Speculator.BuildBudget)
	require.NotNil(t, cfg.Queues[0].Speculator)
	assert.Equal(t, 9, cfg.Queues[0].Speculator.BuildBudget)
}

func TestLoadProfilesConfig_RejectsBadScorers(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{name: "unknown scorer type", contents: "defaults:\n  scorer: {type: vibes}\n"},
		{name: "composite with no components", contents: "defaults:\n  scorer: {type: composite}\n"},
		{name: "unknown combine", contents: "defaults:\n  scorer:\n    type: composite\n    combine: median\n    components: {a: {type: heuristic}}\n"},
		{name: "score out of range", contents: "defaults:\n  scorer:\n    type: heuristic\n    buckets: [{min: 0, max: 10, score: 2.0}]\n"},
		{name: "inverted bucket", contents: "defaults:\n  scorer:\n    type: heuristic\n    buckets: [{min: 10, max: 1, score: 0.5}]\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadProfilesConfig(writeProfiles(t, tt.contents))
			require.Error(t, err)
		})
	}
}
