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
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
)

// Tag is a key-value pair attached to a metric for dimensional filtering.
type Tag struct {
	// Key is the tag name (e.g., "controller", "topic").
	Key string
	// Value is the tag value (e.g., "land", "request").
	Value string
}

// NewTag creates a Tag with the given key and value.
func NewTag(key, value string) Tag {
	return Tag{Key: key, Value: value}
}

// DefaultLatencyBuckets provides pre-defined duration buckets for common latency histograms.
// Covers sub-millisecond to multi-hour ranges suitable for RPC calls, queue processing,
// and long-running operations like builds and merges.
var DefaultLatencyBuckets = tally.DurationBuckets{
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
	2 * time.Hour,
	4 * time.Hour,
}

// Op tracks the lifecycle of a named operation. It captures the start time on
// creation, emits a {name}.called counter, and records the outcome (succeeded/failed
// counters + latency timer with error classification tags) when Complete is called.
//
// Usage:
//
//	func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
//	    op := metrics.Begin(c.scope, "process")
//	    defer func() { op.Complete(retErr) }()
//	    // ... business logic ...
//	}
type Op struct {
	// scope is the tally scope with tags and sub-scope already applied.
	scope tally.Scope
	// start is the time the operation began.
	start time.Time
}

// Begin starts a new operation. It emits a {name}.called counter and captures
// the start time. Call Complete on the returned Op to record the outcome.
func Begin(scope tally.Scope, name string, tags ...Tag) Op {
	sub := tagged(scope, tags).SubScope(name)
	sub.Counter("called").Inc(1)
	return Op{scope: sub, start: time.Now()}
}

// Complete records the outcome of the operation. It emits a {name}.succeeded or
// {name}.failed counter based on err, and records elapsed time on both
// {name}.latency (timer) and {name}.latency_histogram (histogram with
// DefaultLatencyBuckets for percentile distributions), tagged with result=success|error.
// On failure, error classification tags (error_origin, retryable, dependency)
// are added to both the timer and histogram.
func (o Op) Complete(err error) {
	elapsed := time.Since(o.start)

	if err == nil {
		o.scope.Counter("succeeded").Inc(1)
		s := o.scope.Tagged(map[string]string{"result": "success"})
		s.Timer("latency").Record(elapsed)
		s.Histogram("latency_histogram", DefaultLatencyBuckets).RecordDuration(elapsed)
		return
	}

	o.scope.Counter("failed").Inc(1)

	latencyTags := map[string]string{"result": "error"}
	for _, t := range ErrorTags(err) {
		latencyTags[t.Key] = t.Value
	}
	s := o.scope.Tagged(latencyTags)
	s.Timer("latency").Record(elapsed)
	s.Histogram("latency_histogram", DefaultLatencyBuckets).RecordDuration(elapsed)
}

// NamedCounter increments the {name}.{counter} counter by value.
func NamedCounter(scope tally.Scope, name string, counter string, value int64, tags ...Tag) {
	tagged(scope, tags).SubScope(name).Counter(counter).Inc(value)
}

// NamedTimer records a duration on the {name}.{timer} timer.
func NamedTimer(scope tally.Scope, name string, timer string, d time.Duration, tags ...Tag) {
	tagged(scope, tags).SubScope(name).Timer(timer).Record(d)
}

// NamedHistogram returns a tally.Histogram at {name}.{histogram} with the given
// bucket configuration. Store the returned histogram and call RecordDuration or
// RecordValue on each invocation.
func NamedHistogram(scope tally.Scope, name string, histogram string, buckets tally.Buckets, tags ...Tag) tally.Histogram {
	return tagged(scope, tags).SubScope(name).Histogram(histogram, buckets)
}

// NamedGauge updates the {name}.{gauge} gauge to value. Gauges represent a
// current point-in-time measurement that can go up or down, such as queue depth,
// active connections, or in-flight requests.
func NamedGauge(scope tally.Scope, name string, gauge string, value float64, tags ...Tag) {
	tagged(scope, tags).SubScope(name).Gauge(gauge).Update(value)
}

// ErrorTags returns classification tags for an error using platform/errs.
// Returns error_origin (user|infra), retryable (true|false), and
// dependency (true) tags. Returns nil for a nil error.
func ErrorTags(err error) []Tag {
	if err == nil {
		return nil
	}

	origin := "infra"
	if errs.IsUserError(err) {
		origin = "user"
	}

	retryable := "false"
	if errs.IsRetryable(err) {
		retryable = "true"
	}

	tags := []Tag{
		{Key: "error_origin", Value: origin},
		{Key: "retryable", Value: retryable},
	}

	if errs.IsDependencyError(err) {
		tags = append(tags, Tag{Key: "dependency", Value: "true"})
	}

	return tags
}

// tagsToMap converts a slice of Tag to a map for tally.
func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

// tagged applies tags to a scope if any are provided, otherwise returns the
// scope unchanged.
func tagged(scope tally.Scope, tags []Tag) tally.Scope {
	if len(tags) == 0 {
		return scope
	}
	return scope.Tagged(tagsToMap(tags))
}
