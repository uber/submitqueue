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

// Package lastgreen is an observability.Reporter that reports how old a queue's
// last-known-green commit is. Callers gate deployments on that commit, so its
// age is the staleness of the newest thing they are allowed to ship: a queue
// whose green bookmark stopped advancing looks healthy from the pipeline's
// perspective — nothing is failing — while the answer it serves silently ages.
package lastgreen

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/extension/observability"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "last_green"

// factory resolves the queue-scoped dependencies a reporter observes through.
type factory struct {
	scope          tally.Scope
	stores         storage.Factory
	sourceControls sourcecontrol.Factory
}

// Verify factory implements observability.Factory interface at compile time.
var _ observability.Factory = (*factory)(nil)

// NewFactory creates a Factory of last-known-green age reporters.
func NewFactory(
	scope tally.Scope,
	stores storage.Factory,
	sourceControls sourcecontrol.Factory,
) observability.Factory {
	return &factory{
		scope:          scope,
		stores:         stores,
		sourceControls: sourceControls,
	}
}

// For returns the Reporter bound to the queue named in cfg, resolving the
// storage aggregate holding its green bookmark and the source control that
// dates the commit the bookmark points at.
func (f *factory) For(cfg observability.Config) (observability.Reporter, error) {
	store, err := f.stores.For(storage.Config{QueueName: cfg.QueueName})
	if err != nil {
		return nil, fmt.Errorf("failed to resolve storage for queue %q: %w", cfg.QueueName, err)
	}
	sourceControl, err := f.sourceControls.For(sourcecontrol.Config{QueueName: cfg.QueueName})
	if err != nil {
		return nil, fmt.Errorf("failed to resolve source control for queue %q: %w", cfg.QueueName, err)
	}
	return New(f.scope, cfg.QueueName, store, sourceControl), nil
}

// reporter reports the last-known-green age of the single queue it is bound to.
type reporter struct {
	scope         tally.Scope
	queue         string
	store         storage.Storage
	sourceControl sourcecontrol.SourceControl
}

// Verify reporter implements observability.Reporter interface at compile time.
var _ observability.Reporter = (*reporter)(nil)

// New creates a Reporter over one queue's already-resolved store and source
// control.
func New(
	scope tally.Scope,
	queue string,
	store storage.Storage,
	sourceControl sourcecontrol.SourceControl,
) observability.Reporter {
	return &reporter{
		scope:         scope,
		queue:         queue,
		store:         store,
		sourceControl: sourceControl,
	}
}

// Report updates the gauge holding the current age of the queue's last-known-green
// commit. A gauge rather than a histogram because the answer is the latest
// observation, not a distribution: how stale the bookmark is *now*.
func (r *reporter) Report(ctx context.Context) {
	queueTag := metrics.NewTag("queue", r.queue)

	queueRow, err := r.store.GetQueueStore().Get(ctx, r.queue)
	if err != nil {
		r.ageError(queueTag, "get_queue")
		return
	}

	// A queue that has never gone green has no age to report. Emitting zero
	// would read as "green as of right now", the opposite of the truth.
	if queueRow.LastGreenURI == "" {
		metrics.NamedCounter(r.scope, _opName, "age_missing", 1, queueTag)
		return
	}

	info, err := r.sourceControl.ChangeInfo(ctx, queueRow.LastGreenURI)
	if err != nil || info.CreatedAt.IsZero() {
		r.ageError(queueTag, "get_change_info")
		return
	}

	// A commit dated in the future means the provider's clock disagrees with
	// ours; a negative age would corrupt the series rather than describe it.
	age := time.Since(info.CreatedAt)
	if age < 0 {
		r.ageError(queueTag, "future_change")
		return
	}

	metrics.NamedGauge(r.scope, _opName, "age_seconds", age.Seconds(), queueTag)
}

// ageError counts an observation that could not be made, tagged with the step
// that failed so a silent gauge can be told apart from a broken dependency.
func (r *reporter) ageError(queueTag metrics.Tag, stage string) {
	metrics.NamedCounter(r.scope, _opName, "age_errors", 1, queueTag, metrics.NewTag("stage", stage))
}
