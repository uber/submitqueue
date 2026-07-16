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

package metrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
)

func TestBegin_EmitsCalled(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	_ = Begin(scope, "process")

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	c, ok := counters["process.called+"]
	assert.True(t, ok, "expected process.called counter")
	assert.Equal(t, int64(1), c.Value())
}

func TestComplete(t *testing.T) {
	tests := []struct {
		name             string
		err              error
		expectSucceeded  bool
		expectResultTag  string
		expectOrigin     string
		expectRetryable  string
		expectDependency bool
	}{
		{
			name:            "nil error records success",
			err:             nil,
			expectSucceeded: true,
			expectResultTag: "success",
		},
		{
			name:            "generic error records failure with infra origin",
			err:             fmt.Errorf("something broke"),
			expectSucceeded: false,
			expectResultTag: "error",
			expectOrigin:    "infra",
			expectRetryable: "false",
		},
		{
			name:            "retryable error records retryable=true",
			err:             errs.NewRetryableError(fmt.Errorf("timeout")),
			expectSucceeded: false,
			expectResultTag: "error",
			expectOrigin:    "infra",
			expectRetryable: "true",
		},
		{
			name:            "user error records error_origin=user",
			err:             errs.NewUserError(fmt.Errorf("bad input")),
			expectSucceeded: false,
			expectResultTag: "error",
			expectOrigin:    "user",
			expectRetryable: "false",
		},
		{
			name:             "dependency error records dependency=true",
			err:              errs.NewDependencyError(fmt.Errorf("db down")),
			expectSucceeded:  false,
			expectResultTag:  "error",
			expectOrigin:     "infra",
			expectRetryable:  "false",
			expectDependency: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			op := Begin(scope, "process")
			op.Complete(tt.err, FastLatencyBuckets)

			snapshot := scope.Snapshot()
			counters := snapshot.Counters()

			// Begin always emits called
			c, ok := counters["process.called+"]
			assert.True(t, ok, "expected process.called counter")
			assert.Equal(t, int64(1), c.Value())

			if tt.expectSucceeded {
				c, ok := counters["process.succeeded+"]
				assert.True(t, ok, "expected process.succeeded counter")
				assert.Equal(t, int64(1), c.Value())

				histograms := snapshot.Histograms()
				_, ok = histograms["process.latency+result=success"]
				assert.True(t, ok, "expected process.latency histogram with result=success")
			} else {
				c, ok := counters["process.failed+"]
				assert.True(t, ok, "expected process.failed counter")
				assert.Equal(t, int64(1), c.Value())

				// Build expected tag suffix (tally sorts tags alphabetically)
				tagSuffix := ""
				if tt.expectDependency {
					tagSuffix += "dependency=true,"
				}
				tagSuffix += "error_origin=" + tt.expectOrigin
				tagSuffix += ",result=" + tt.expectResultTag
				tagSuffix += ",retryable=" + tt.expectRetryable

				histogramKey := "process.latency+" + tagSuffix
				histograms := snapshot.Histograms()
				_, ok = histograms[histogramKey]
				assert.True(t, ok, "expected histogram key %s, got keys: %v", histogramKey, histogramKeys(histograms))
			}
		})
	}
}

func TestBegin_WithTags(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	op := Begin(scope, "process", NewTag("env", "prod"))
	op.Complete(nil, FastLatencyBuckets)

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()

	c, ok := counters["process.called+env=prod"]
	assert.True(t, ok, "expected tagged called counter, got keys: %v", counterKeys(counters))
	assert.Equal(t, int64(1), c.Value())

	c, ok = counters["process.succeeded+env=prod"]
	assert.True(t, ok, "expected tagged succeeded counter, got keys: %v", counterKeys(counters))
	assert.Equal(t, int64(1), c.Value())
}

func TestNamedCounter(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	NamedCounter(scope, "publish", "attempts", 5)

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	c, ok := counters["publish.attempts+"]
	assert.True(t, ok, "expected publish.attempts counter")
	assert.Equal(t, int64(5), c.Value())
}

func TestNamedHistogram(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	h := NamedHistogram(scope, "process", "duration", StorageLatencyBuckets)
	assert.NotNil(t, h)

	h.RecordDuration(50 * time.Millisecond)

	snapshot := scope.Snapshot()
	histograms := snapshot.Histograms()
	_, ok := histograms["process.duration+"]
	assert.True(t, ok, "expected process.duration histogram")
}

func TestNamedGauge(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	NamedGauge(scope, "consumer", "pending_messages", 42)

	snapshot := scope.Snapshot()
	gauges := snapshot.Gauges()
	g, ok := gauges["consumer.pending_messages+"]
	assert.True(t, ok, "expected consumer.pending_messages gauge")
	assert.Equal(t, float64(42), g.Value())
}

func TestLatencyBuckets_Sorted(t *testing.T) {
	sets := map[string]tally.DurationBuckets{
		"FastLatencyBuckets":    FastLatencyBuckets,
		"StorageLatencyBuckets": StorageLatencyBuckets,
		"LongLatencyBuckets":    LongLatencyBuckets,
	}
	for name, buckets := range sets {
		t.Run(name, func(t *testing.T) {
			for i := 1; i < len(buckets); i++ {
				assert.Greater(t, buckets[i], buckets[i-1],
					"%s[%d] (%v) must be greater than %s[%d] (%v)",
					name, i, buckets[i], name, i-1, buckets[i-1])
			}
		})
	}
}

func TestErrorTags(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected []Tag
	}{
		{
			name:     "nil error returns nil",
			err:      nil,
			expected: nil,
		},
		{
			name: "generic error returns infra non-retryable",
			err:  fmt.Errorf("fail"),
			expected: []Tag{
				{Key: "error_origin", Value: "infra"},
				{Key: "retryable", Value: "false"},
			},
		},
		{
			name: "user error returns user origin",
			err:  errs.NewUserError(fmt.Errorf("bad")),
			expected: []Tag{
				{Key: "error_origin", Value: "user"},
				{Key: "retryable", Value: "false"},
			},
		},
		{
			name: "retryable error returns retryable=true",
			err:  errs.NewRetryableError(fmt.Errorf("timeout")),
			expected: []Tag{
				{Key: "error_origin", Value: "infra"},
				{Key: "retryable", Value: "true"},
			},
		},
		{
			name: "dependency error includes dependency tag",
			err:  errs.NewDependencyError(fmt.Errorf("db down")),
			expected: []Tag{
				{Key: "error_origin", Value: "infra"},
				{Key: "retryable", Value: "false"},
				{Key: "dependency", Value: "true"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags := ErrorTags(tt.err)
			assert.Equal(t, tt.expected, tags)
		})
	}
}

// counterKeys extracts map keys for error messages.
func counterKeys(m map[string]tally.CounterSnapshot) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// histogramKeys extracts map keys for error messages.
func histogramKeys(m map[string]tally.HistogramSnapshot) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
