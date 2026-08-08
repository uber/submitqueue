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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// recorder captures the queue name each seam's factory was handed, so a test
// can assert the Config survived the profile lookup instead of stopping at it.
type recorder struct {
	changeProvider string
	buildRunner    string
	analyzer       string
	storage        string
	scorer         string
}

// profileRecording returns a Profile whose every factory records the queue name
// it receives into rec and returns a nil implementation. Nil is fine: these
// tests are about what reaches the factory, not what it builds.
func profileRecording(rec *recorder) Profile {
	return Profile{
		ChangeProvider: changeProviderFunc(func(c changeprovider.Config) (changeprovider.ChangeProvider, error) {
			rec.changeProvider = c.QueueName
			return nil, nil
		}),
		BuildRunner: buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
			rec.buildRunner = c.QueueName
			return nil, nil
		}),
		Analyzer: analyzerFunc(func(c conflict.Config) (conflict.Analyzer, error) {
			rec.analyzer = c.QueueName
			return nil, nil
		}),
		Storage: storageFunc(func(c storage.Config) (storage.Storage, error) {
			rec.storage = c.QueueName
			return nil, nil
		}),
		Scorer: scorerFunc(func(c scorer.Config) (scorer.Scorer, error) {
			rec.scorer = c.QueueName
			return nil, nil
		}),
	}
}

// TestProfilesForwardQueueNameToFactories is the regression guard for the seam
// this file exists to fix. Selecting a profile consumes the queue name, but the
// Config must keep travelling into the profile's own factory — otherwise an
// implementation is built without knowing which queue it serves.
//
// The unlisted-queue case is the one that used to be unfixable: defaultProfile
// backs every queue with no explicit entry, so a single shared instance could
// only ever carry one queue name (or none).
func TestProfilesForwardQueueNameToFactories(t *testing.T) {
	tests := []struct {
		name  string
		queue string
	}{
		{name: "queue with an explicit profile", queue: "listed-queue"},
		{name: "queue falling through to the default profile", queue: "unlisted-queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rec recorder
			profile := profileRecording(&rec)
			profiles := Profiles{
				defaultProfile: profile,
				byQueue:        map[string]Profile{"listed-queue": profile},
			}

			_, err := profiles.ChangeProviderFactory().For(changeprovider.Config{QueueName: tt.queue})
			require.NoError(t, err)
			_, err = profiles.BuildRunnerFactory().For(buildrunner.Config{QueueName: tt.queue})
			require.NoError(t, err)
			_, err = profiles.AnalyzerFactory().For(conflict.Config{QueueName: tt.queue})
			require.NoError(t, err)
			_, err = profiles.StorageFactory().For(storage.Config{QueueName: tt.queue})
			require.NoError(t, err)
			_, err = profiles.ScorerFactory().For(scorer.Config{QueueName: tt.queue})
			require.NoError(t, err)

			assert.Equal(t, tt.queue, rec.changeProvider)
			assert.Equal(t, tt.queue, rec.buildRunner)
			assert.Equal(t, tt.queue, rec.analyzer)
			assert.Equal(t, tt.queue, rec.storage)
			assert.Equal(t, tt.queue, rec.scorer)
		})
	}
}

// TestWithSpeculatorResolvesScorerAtSameQueue covers the one seam that resolves
// another seam: the speculator is composed from the profile's scorer, and must
// ask for it at the queue it was itself asked for.
func TestWithSpeculatorResolvesScorerAtSameQueue(t *testing.T) {
	var rec recorder
	profile := withSpeculator(profileRecording(&rec))
	profiles := Profiles{defaultProfile: profile}

	spec, err := profiles.SpeculatorFactory().For(speculator.Config{QueueName: "unlisted-queue"})
	require.NoError(t, err)
	assert.NotNil(t, spec)
	assert.Equal(t, "unlisted-queue", rec.scorer)
}

// TestWithSpeculatorPropagatesScorerError covers the error path the factory
// conversion introduced: resolving the scorer can now fail where reading a
// struct field could not, and the failure must surface rather than yielding a
// speculator built over a nil scorer.
func TestWithSpeculatorPropagatesScorerError(t *testing.T) {
	sentinel := errors.New("scorer unavailable")
	profile := withSpeculator(Profile{
		Scorer: scorerFunc(func(scorer.Config) (scorer.Scorer, error) { return nil, sentinel }),
	})

	spec, err := profile.Speculator.For(speculator.Config{QueueName: "any-queue"})
	require.ErrorIs(t, err, sentinel)
	assert.Nil(t, spec)
}
