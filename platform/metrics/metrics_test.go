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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestBegin_EmitsStart(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	_ = Begin(scope, "process", FastLatencyBuckets)

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	c, ok := counters["process.start+"]
	assert.True(t, ok, "expected process.start counter")
	assert.Equal(t, int64(1), c.Value())
}

func TestComplete(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		result string
	}{
		{
			name:   "nil error records success",
			result: "success",
		},
		{
			name:   "generic error records error",
			err:    fmt.Errorf("something broke"),
			result: "error",
		},
		{
			name:   "canceled context records cancel",
			err:    context.Canceled,
			result: "cancel",
		},
		{
			name:   "wrapped canceled context records cancel",
			err:    fmt.Errorf("process: %w", context.Canceled),
			result: "cancel",
		},
		{
			name:   "deadline exceeded records error",
			err:    context.DeadlineExceeded,
			result: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			op := Begin(scope, "process", FastLatencyBuckets)
			op.Complete(tt.err)

			snapshot := scope.Snapshot()
			counters := snapshot.Counters()
			c, ok := counters["process.start+"]
			assert.True(t, ok, "expected process.start counter")
			assert.Equal(t, int64(1), c.Value())
			assert.Len(t, counters, 1, "Complete should not emit a counter")

			histograms := snapshot.Histograms()
			histogramKey := "process.finish+result=" + tt.result
			_, ok = histograms[histogramKey]
			assert.True(t, ok, "expected histogram key %s, got keys: %v", histogramKey, histogramKeys(histograms))
			assert.Len(t, histograms, 1, "finish histogram should only include the result tag")
		})
	}
}

func TestBegin_WithTags(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	op := Begin(scope, "process", FastLatencyBuckets, NewTag("env", "prod"))
	op.Complete(nil)

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()

	c, ok := counters["process.start+env=prod"]
	assert.True(t, ok, "expected tagged start counter, got keys: %v", counterKeys(counters))
	assert.Equal(t, int64(1), c.Value())

	histograms := snapshot.Histograms()
	_, ok = histograms["process.finish+env=prod,result=success"]
	assert.True(t, ok, "expected tagged finish histogram, got keys: %v", histogramKeys(histograms))
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
