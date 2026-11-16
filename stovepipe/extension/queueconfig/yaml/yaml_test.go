// Copyright (c) 2026 Uber Technologies, Inc.
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

package yaml

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/core/queueconfig"
	queueconfigprovider "github.com/uber/submitqueue/stovepipe/extension/queueconfig"
)

const testStages = `stages:
  process:
    maxConcurrent: 3
    gateWaitDelay: 123ms
  buildSignal:
    acceptedPollDelay: 234ms
    runningPollDelay: 345ms
  buildAnalysis:
    pollDelay: 456ms
  issueTrack:
    recheckDelay: 567ms
    recoveryWaitDelay: 678ms
`

func writeTestConfig(t *testing.T, input string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "queues.yaml")
	require.NoError(t, os.WriteFile(path, []byte(input), 0600))
	return path
}

func TestStageAndQueueOverrides(t *testing.T) {
	store, err := NewStore(writeTestConfig(t, testStages+`queues:
  - name: special
    stages:
      process:
        maxConcurrent: 4
      buildAnalysis:
        pollDelay: 2s
`), []string{"ordinary", "special"})
	require.NoError(t, err)

	ordinary, err := store.Get(context.Background(), "ordinary")
	require.NoError(t, err)

	special, err := store.Get(context.Background(), "special")
	require.NoError(t, err)

	assert.Equal(t, int32(3), ordinary.Stages.Process.MaxConcurrent)
	assert.Equal(t, int64(123), ordinary.Stages.Process.GateWaitDelayMs)
	assert.Equal(t, int64(234), ordinary.Stages.BuildSignal.AcceptedPollDelayMs)
	assert.Equal(t, int64(345), ordinary.Stages.BuildSignal.RunningPollDelayMs)
	assert.Equal(t, int64(456), ordinary.Stages.BuildAnalysis.PollDelayMs)
	assert.Equal(t, int64(567), ordinary.Stages.IssueTrack.RecheckDelayMs)
	assert.Equal(t, int64(678), ordinary.Stages.IssueTrack.RecoveryWaitDelayMs)

	assert.Equal(t, int32(4), special.Stages.Process.MaxConcurrent)
	assert.Equal(t, ordinary.Stages.Process.GateWaitDelayMs, special.Stages.Process.GateWaitDelayMs)
	assert.Equal(t, int64(2000), special.Stages.BuildAnalysis.PollDelayMs)
	assert.Equal(t, ordinary.Stages.BuildSignal, special.Stages.BuildSignal)
	assert.Equal(t, ordinary.Stages.IssueTrack, special.Stages.IssueTrack)
}

func TestOmittedStageSettingsUseStandardValues(t *testing.T) {
	baseline := queueconfig.DefaultStages()

	for _, tt := range []struct {
		name            string
		input           string
		analysisDelayMs int64
	}{
		{"empty file", "", baseline.BuildAnalysis.PollDelayMs},
		{"empty document", "{}", baseline.BuildAnalysis.PollDelayMs},
		{"empty queue overrides", "queues: []", baseline.BuildAnalysis.PollDelayMs},
		{"empty stages", "stages: {}", baseline.BuildAnalysis.PollDelayMs},
		{"partial shared stages", "stages: {buildAnalysis: {pollDelay: 250ms}}", 250},
		{"partial queue stages", "queues: [{name: known, stages: {buildAnalysis: {pollDelay: 250ms}}}]", 250},
		{"fractional milliseconds truncate", "queues: [{name: known, stages: {buildAnalysis: {pollDelay: 1500us}}}]", 1},
		{"queue overrides shared stages", "stages: {buildAnalysis: {pollDelay: 1s}}\nqueues: [{name: known, stages: {buildAnalysis: {pollDelay: 250ms}}}]", 250},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewStore(writeTestConfig(t, tt.input), []string{"known"})
			require.NoError(t, err)

			cfg, err := store.Get(context.Background(), "known")
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			want := baseline
			want.BuildAnalysis.PollDelayMs = tt.analysisDelayMs
			assert.Equal(t, want, cfg.Stages)
		})
	}
}

func TestValidationUsesResolvedQueueSettings(t *testing.T) {
	input := strings.Replace(testStages, "pollDelay: 456ms", "pollDelay: 0s", 1) + `queues:
  - name: known
    stages:
      buildAnalysis:
        pollDelay: 2s
`
	store, err := NewStore(writeTestConfig(t, input), []string{"known"})
	require.NoError(t, err)

	cfg, err := store.Get(context.Background(), "known")
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	assert.Equal(t, int64(2000), cfg.Stages.BuildAnalysis.PollDelayMs)
}

func TestRejectsInvalidConfiguration(t *testing.T) {
	valid := testStages + "queues:\n  - name: known\n"

	for _, tt := range []struct {
		name  string
		input string
	}{
		{"unknown section", valid + "default: {}"},
		{"unknown stages field", strings.Replace(valid, "stages:", "stagez:", 1)},
		{"unknown stage", strings.Replace(valid, "  process:", "  unknown:", 1)},
		{"unknown process field", strings.Replace(valid, "maxConcurrent:", "maxConcurent:", 1)},
		{"unknown signal field", strings.Replace(valid, "acceptedPollDelay:", "pollDelay:", 1)},
		{"unknown analysis field", strings.Replace(valid, "pollDelay:", "polDelay:", 1)},
		{"unknown tracking field", strings.Replace(valid, "recheckDelay:", "pollDelay:", 1)},
		{"unknown queue field", valid + "    stagez: {}"},
		{"unknown queue stage", valid + "    stages: {unknown: {}}"},
		{"duplicate key", strings.Replace(valid, "maxConcurrent: 3", "maxConcurrent: 3\n    maxConcurrent: 4", 1)},
		{"duplicate queue override", valid + "  - name: known\n"},
		{"unregistered queue override", valid + "  - name: unknown\n"},
		{"empty name", strings.Replace(valid, "name: known", "name: ''", 1)},
		{"missing name", testStages + "queues: [{}]"},
		{"zero concurrency", strings.Replace(valid, "maxConcurrent: 3", "maxConcurrent: 0", 1)},
		{"zero wait", strings.Replace(valid, "pollDelay: 456ms", "pollDelay: 0s", 1)},
		{"negative wait", strings.Replace(valid, "recheckDelay: 567ms", "recheckDelay: -1s", 1)},
		{"invalid duration", strings.Replace(valid, "gateWaitDelay: 123ms", "gateWaitDelay: soon", 1)},
		{"submillisecond", strings.Replace(valid, "acceptedPollDelay: 234ms", "acceptedPollDelay: 1us", 1)},
		{"invalid queue override", valid + "    stages: {process: {gateWaitDelay: 0s}}"},
		{"invalid sibling queue", valid + "  - name: invalid\n    stages: {buildAnalysis: {pollDelay: 0s}}"},
		{"invalid partial shared settings", "stages: {buildAnalysis: {pollDelay: 0s}}\nqueues: [{name: known}]"},
		{"invalid partial queue settings", "queues: [{name: known, stages: {process: {maxConcurrent: 0}}}]"},
		{"obsolete defaults wrapper", "defaults: {}\nqueues: [{name: known}]"},
		{"second document", valid + "---\n{}"},
		{"malformed second document", valid + "---\n["},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewStore(writeTestConfig(t, tt.input), []string{"known", "invalid"})
			require.Error(t, err)
			require.Nil(t, store)
		})
	}
}

func TestStoreSnapshotAndIsolation(t *testing.T) {
	ctx := context.Background()
	path := writeTestConfig(t, testStages)
	names := []string{"second", "first"}
	store, err := NewStore(path, names)
	require.NoError(t, err)

	names[0] = "changed"
	want, err := store.Get(ctx, "second")
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("invalid replacement"), 0600))

	got := want
	got.Stages.Process.MaxConcurrent = 99

	listed, err := store.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 2)
	assert.Equal(t, []string{"second", "first"}, []string{listed[0].Name, listed[1].Name})
	listed[0] = got

	again, err := store.Get(ctx, "second")
	require.NoError(t, err)
	assert.Equal(t, want, again)

	listedAgain, err := store.List(ctx)
	require.NoError(t, err)
	assert.Equal(t, want, listedAgain[0])

	for _, name := range []string{"", "unknown"} {
		_, err := store.Get(ctx, name)
		require.ErrorIs(t, err, queueconfigprovider.ErrNotFound)
	}
}

func TestMissingFileFails(t *testing.T) {
	_, err := NewStore(filepath.Join(t.TempDir(), "missing"), []string{"known"})
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestNoFileUsesStandardSettingsForRegisteredQueues(t *testing.T) {
	store, err := NewStore("", []string{"known"})
	require.NoError(t, err)

	cfg, err := store.Get(context.Background(), "known")
	require.NoError(t, err)
	assert.Equal(t, queueconfig.QueueConfig{Name: "known", Stages: queueconfig.DefaultStages()}, cfg)
}

func TestRejectsInvalidRegisteredQueues(t *testing.T) {
	for _, tt := range []struct {
		name   string
		queues []string
	}{
		{"empty name", []string{""}},
		{"duplicate name", []string{"known", "known"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStore("", tt.queues)
			require.Error(t, err)
		})
	}
}
