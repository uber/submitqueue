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

// Package periodicmetrics samples last-green and ref-head health even when the
// pipeline is idle. Gauges use Unix seconds or elapsed seconds and are tagged by
// queue. A ref-head lookup failure leaves the last-green gauges available.
package periodicmetrics

import (
	"context"
	"fmt"
	"time"

	tally "github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const _metricsName = "periodicmetrics"

// Controller samples queue health independently of pipeline traffic.
type Controller interface {
	Observe(ctx context.Context, queue string) error
}

type controller struct {
	scope          tally.Scope
	stores         storage.Factory
	sourceControls sourcecontrol.Factory
	now            func() time.Time
}

func New(
	scope tally.Scope,
	stores storage.Factory,
	sourceControls sourcecontrol.Factory,
) Controller {
	return &controller{
		scope:          scope,
		stores:         stores,
		sourceControls: sourceControls,
		now:            time.Now,
	}
}

func (c *controller) Observe(ctx context.Context, queue string) error {
	if queue == "" {
		return fmt.Errorf("cannot observe an unnamed queue")
	}

	if err := c.observeQueue(ctx, queue); err != nil {
		return err
	}

	metrics.NamedCounter(c.scope, _metricsName, "observed", 1, metrics.NewTag("queue", queue))
	return nil
}

func (c *controller) observeQueue(ctx context.Context, queue string) error {
	queueTag := metrics.NewTag("queue", queue)

	store, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_timestamp_storage_errors", 1, queueTag)
		return fmt.Errorf("resolve storage: %w", err)
	}

	queueRow, err := store.GetQueueStore().Get(ctx, queue)
	if err != nil {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_timestamp_storage_errors", 1, queueTag)
		return fmt.Errorf("load queue: %w", err)
	}

	// Never-green queues have no timestamp to report.
	if queueRow.LastGreenURI == "" {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_missing", 1, queueTag)
		return nil
	}

	sourceControl, err := c.sourceControls.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_timestamp_resolve_errors", 1, queueTag)
		return fmt.Errorf("resolve source control: %w", err)
	}

	lastGreen, err := c.observeLastGreen(ctx, sourceControl, queue, queueRow.LastGreenURI)
	if err != nil {
		return err
	}

	return c.observeRefHead(ctx, sourceControl, queue, lastGreen)
}

func (c *controller) observeLastGreen(
	ctx context.Context,
	sourceControl sourcecontrol.SourceControl,
	queue string,
	lastGreenURI string,
) (time.Time, error) {
	queueTag := metrics.NewTag("queue", queue)

	info, err := sourceControl.ChangeInfo(ctx, lastGreenURI)
	if err != nil {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_timestamp_errors", 1, queueTag)
		return time.Time{}, fmt.Errorf("look up change %s: %w", lastGreenURI, err)
	}

	// Undated changes must not appear as infinitely stale queues.
	if info.CreatedAt <= 0 {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_timestamp_invalid", 1, queueTag)
		return time.Time{}, fmt.Errorf("change %s has no creation timestamp", lastGreenURI)
	}

	createdAt := time.UnixMilli(info.CreatedAt)
	metrics.NamedGauge(c.scope, _metricsName, "last_green_timestamp_seconds", float64(createdAt.Unix()), queueTag)

	// Clock skew must not produce a negative age.
	duration := c.now().Sub(createdAt)
	if duration < 0 {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_timestamp_future", 1, queueTag)
		duration = 0
	}
	metrics.NamedGauge(c.scope, _metricsName, "last_green_duration_seconds", duration.Seconds(), queueTag)

	return createdAt, nil
}

func (c *controller) observeRefHead(
	ctx context.Context,
	sourceControl sourcecontrol.SourceControl,
	queue string,
	lastGreen time.Time,
) error {
	queueTag := metrics.NewTag("queue", queue)

	head, err := sourceControl.Latest(ctx)
	if err != nil {
		metrics.NamedCounter(c.scope, _metricsName, "ref_head_timestamp_errors", 1, queueTag)
		return fmt.Errorf("resolve the newest change on the ref: %w", err)
	}

	info, err := sourceControl.ChangeInfo(ctx, head)
	if err != nil {
		metrics.NamedCounter(c.scope, _metricsName, "ref_head_timestamp_errors", 1, queueTag)
		return fmt.Errorf("look up change %s: %w", head, err)
	}

	if info.CreatedAt <= 0 {
		metrics.NamedCounter(c.scope, _metricsName, "ref_head_timestamp_invalid", 1, queueTag)
		return fmt.Errorf("change %s has no creation timestamp", head)
	}

	createdAt := time.UnixMilli(info.CreatedAt)
	metrics.NamedGauge(c.scope, _metricsName, "ref_head_timestamp_seconds", float64(createdAt.Unix()), queueTag)

	// Rewritten history or clock skew can date the head before last green.
	behind := createdAt.Sub(lastGreen)
	if behind < 0 {
		metrics.NamedCounter(c.scope, _metricsName, "last_green_ahead_of_head", 1, queueTag)
		behind = 0
	}
	metrics.NamedGauge(c.scope, _metricsName, "last_green_behind_head_seconds", behind.Seconds(), queueTag)

	return nil
}
