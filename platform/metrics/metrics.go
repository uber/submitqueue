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
	"errors"
	"time"

	"github.com/uber-go/tally"
)

// Tag is a key-value pair attached to a metric for dimensional filtering.
type Tag struct {
	// Key is the tag name (e.g., "controller", "topic").
	Key string
	// Value is the tag value (e.g., "land", "request").
	Value string
}

type contextTagsKey struct{}

// NewTag creates a Tag with the given key and value.
func NewTag(key, value string) Tag {
	return Tag{Key: key, Value: value}
}

// WithContextTags returns a child context carrying the supplied metric tags.
// Tags already carried by ctx are preserved, and both inputs are copied.
func WithContextTags(ctx context.Context, tags ...Tag) context.Context {
	existing, _ := ctx.Value(contextTagsKey{}).([]Tag)
	contextTags := make([]Tag, 0, len(existing)+len(tags))
	contextTags = append(contextTags, existing...)
	contextTags = append(contextTags, tags...)
	return context.WithValue(ctx, contextTagsKey{}, contextTags)
}

// TagsFromContext appends metric tags explicitly carried by ctx. Additional
// tags are preserved, and context-derived tags win when the same key is present.
func TagsFromContext(ctx context.Context, tags ...Tag) []Tag {
	stored, _ := ctx.Value(contextTagsKey{}).([]Tag)
	if len(stored) == 0 {
		return tags
	}

	contextTags := make([]Tag, 0, len(tags)+len(stored))
	contextTags = append(contextTags, tags...)
	return append(contextTags, stored...)
}

// Common duration bucket sets for latency histograms. Operations differ widely
// in expected latency, so there is no single default — pick the set whose range
// matches the operation and pass it to Begin or NamedHistogram. Buckets
// far outside an operation's real latency waste series cardinality and lose
// resolution where the data actually lands.
var (
	// FastLatencyBuckets suits fast in-process operations (~microseconds to
	// seconds): scoring, cache lookups, and other CPU-bound work.
	FastLatencyBuckets = tally.DurationBuckets{
		100 * time.Microsecond,
		250 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		2500 * time.Microsecond,
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
	}

	// StorageLatencyBuckets suits storage and message-queue round-trips
	// (~1ms to a minute): database reads/writes, publish/consume, and RPC
	// handlers whose latency is dominated by such calls.
	StorageLatencyBuckets = tally.DurationBuckets{
		1 * time.Millisecond,
		2500 * time.Microsecond,
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
	}

	// LongLatencyBuckets suits long-running pipeline work and external calls
	// (~5ms to hours): builds, merges, git pushes, and external provider calls.
	LongLatencyBuckets = tally.DurationBuckets{
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

	// ChangeAgeBuckets suits durations measured from a source-control change's
	// commit timestamp. They span minutes to a month because that is the honest
	// range of such a signal: a break caught in minutes and one that survived a
	// fortnight are both ordinary observations, and collapsing the tail would hide
	// exactly the cases worth seeing.
	ChangeAgeBuckets = tally.DurationBuckets{
		1 * time.Minute,
		5 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
		1 * time.Hour,
		2 * time.Hour,
		4 * time.Hour,
		8 * time.Hour,
		12 * time.Hour,
		24 * time.Hour,
		48 * time.Hour,
		7 * 24 * time.Hour,
		14 * 24 * time.Hour,
		30 * 24 * time.Hour,
	}
)

// Op tracks the lifecycle of a named operation. It captures the start time on
// creation, emits a {name}.start counter, and records the duration and result
// on a {name}.finish histogram when Complete is called.
//
// Usage:
//
//	func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
//	    op := metrics.Begin(c.scope, "process", metrics.StorageLatencyBuckets)
//	    defer func() { op.Complete(retErr) }()
//	    // ... business logic ...
//	}
type Op struct {
	// scope is the tally scope with tags and sub-scope already applied.
	scope tally.Scope
	// start is the time the operation began.
	start time.Time
	// buckets defines the finish histogram's duration buckets.
	buckets tally.Buckets
}

// Begin starts a new operation. It emits a {name}.start counter, captures the
// start time, and retains the buckets used by Complete.
func Begin(scope tally.Scope, name string, buckets tally.Buckets, tags ...Tag) Op {
	sub := tagged(scope, tags).SubScope(name)
	sub.Counter("start").Inc(1)
	return Op{
		scope:   sub,
		start:   time.Now(),
		buckets: buckets,
	}
}

// Complete records elapsed time on the {name}.finish histogram, tagged with
// result=success|error|cancel and any additional tags accumulated while the
// operation ran. The histogram records both duration and count. Cancellation
// is detected through the error chain.
func (o Op) Complete(err error, tags ...Tag) {
	result := "success"
	if err != nil {
		result = "error"
		if errors.Is(err, context.Canceled) {
			result = "cancel"
		}
	}

	tagged(o.scope, tags).
		Tagged(map[string]string{"result": result}).
		Histogram("finish", o.buckets).
		RecordDuration(time.Since(o.start))
}

// NamedCounter increments the {name}.{counter} counter by value.
func NamedCounter(scope tally.Scope, name string, counter string, value int64, tags ...Tag) {
	tagged(scope, tags).SubScope(name).Counter(counter).Inc(value)
}

// NamedHistogram returns a tally.Histogram at {name}.{histogram} with the given
// bucket configuration. Store the returned histogram and call RecordDuration or
// RecordValue on each invocation.
func NamedHistogram(scope tally.Scope, name string, histogram string, buckets tally.Buckets, tags ...Tag) tally.Histogram {
	return tagged(scope, tags).SubScope(name).Histogram(histogram, buckets)
}

// NamedGauge sets the {name}.{gauge} gauge to value.
func NamedGauge(scope tally.Scope, name string, gauge string, value float64, tags ...Tag) {
	tagged(scope, tags).SubScope(name).Gauge(gauge).Update(value)
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
