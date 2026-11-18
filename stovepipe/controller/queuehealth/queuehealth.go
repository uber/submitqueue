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

// Package queuehealth samples durable pipeline state without changing scheduling decisions.
package queuehealth

//go:generate mockgen -source=queuehealth.go -destination=mock/queuehealth_mock.go -package=mock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueconfig"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	pageSize            = 100
	maxObservedRequests = 1000
)

// Observation is a successful, bounded sample; ages include runner queue time, in milliseconds.
type Observation struct {
	// PendingWithCapacity means an accepted request is waiting while the admission gate is open.
	PendingWithCapacity bool
	// HeadNotIngested means the source-control head has no latest request yet.
	HeadNotIngested bool
	// Dispatches describes processing requests with no retained build trigger.
	Dispatches []PendingDispatch
	// BuildAgeMs is the oldest trigger age among nonterminal builds, including runner queue time.
	BuildAgeMs int64
	// FinalizationAgeMs is the oldest time awaiting request finalization after all builds ended.
	FinalizationAgeMs int64
	// ActiveBuildCount counts nonterminal builds attached to processing requests.
	ActiveBuildCount int
	// UnaccountedSlots is the positive excess of occupied slots over observed processing requests.
	UnaccountedSlots int32
}

// PendingDispatch identifies a request awaiting its build handoff.
type PendingDispatch struct {
	// RequestID is the queue-local identity and the dispatch partition key.
	RequestID string
	// AgeMs is the elapsed time since retained admission, in milliseconds.
	AgeMs int64
}

// Controller reads queue health. Failed or incomplete reads never return a healthy observation.
type Controller interface {
	Observe(ctx context.Context, queue string) (Observation, error)
}

type controller struct {
	stores  storage.Factory
	configs queueconfig.Store
	sources sourcecontrol.Factory
	now     func() time.Time
}

// New constructs a state sampler. Hosts own cadence, pause controls, metrics, and alert thresholds.
func New(stores storage.Factory, configs queueconfig.Store, sources sourcecontrol.Factory) Controller {
	return &controller{stores: stores, configs: configs, sources: sources, now: time.Now}
}

func (c *controller) Observe(ctx context.Context, queue string) (Observation, error) {
	observation, err := c.observeQueue(ctx, queue)
	if err != nil {
		return Observation{}, err
	}
	return observation, nil
}

func (c *controller) observeQueue(ctx context.Context, queue string) (Observation, error) {
	if queue == "" {
		return Observation{}, fmt.Errorf("queue is empty")
	}
	now := c.now().UnixMilli()
	store, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return Observation{}, fmt.Errorf("resolve health storage: %w", err)
	}
	cfg, err := c.configs.Get(ctx, queue)
	if err != nil {
		return Observation{}, fmt.Errorf("load health configuration: %w", err)
	}
	if cfg.MaxConcurrent <= 0 {
		return Observation{}, fmt.Errorf("queue concurrency must be positive")
	}
	source, err := c.sources.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		return Observation{}, fmt.Errorf("resolve health source control: %w", err)
	}
	head, err := source.Latest(ctx)
	if err != nil {
		return Observation{}, fmt.Errorf("resolve health ref head: %w", err)
	}
	if head == "" {
		return Observation{}, fmt.Errorf("health ref head is empty")
	}
	row, err := store.GetQueueStore().Get(ctx, queue)
	if errors.Is(err, storage.ErrNotFound) {
		return Observation{HeadNotIngested: true}, nil
	}
	if err != nil {
		return Observation{}, fmt.Errorf("load health queue: %w", err)
	}
	if row.Name != queue || row.InFlightCount < 0 {
		return Observation{}, fmt.Errorf("invalid health queue")
	}

	result := Observation{HeadNotIngested: true}
	var processing int32
	var pending bool
	seenLatest := false
	after := ""
	observed := 0
	for {
		ids, err := store.GetActiveRequestStore().List(ctx, after, pageSize)
		if err != nil {
			return Observation{}, fmt.Errorf("list active requests: %w", err)
		}
		if len(ids) > pageSize {
			return Observation{}, fmt.Errorf("active request page exceeds limit")
		}
		for _, id := range ids {
			if id <= after {
				return Observation{}, fmt.Errorf("active request page is not ordered")
			}
			after = id
			observed++
			if observed > maxObservedRequests {
				return Observation{}, fmt.Errorf("active request observation exceeds limit")
			}
			request, err := store.GetRequestStore().Get(ctx, id)
			if err != nil {
				return Observation{}, fmt.Errorf("load active request %s: %w", id, err)
			}
			if request.ID != id || request.Queue != queue {
				return Observation{}, fmt.Errorf("active request identity mismatch")
			}
			if request.ID == row.LatestRequestID {
				seenLatest = true
				result.HeadNotIngested = request.URI != head
			}
			if request.State.IsTerminal() {
				if err := store.GetActiveRequestStore().Delete(ctx, id); err != nil {
					return Observation{}, fmt.Errorf("remove terminal request: %w", err)
				}
				continue
			}
			switch request.State {
			case entity.RequestStateAccepted:
				pending = true
			case entity.RequestStateProcessing:
				processing++
				if err := observeProcessing(ctx, store, request, now, &result); err != nil {
					return Observation{}, err
				}
			default:
				return Observation{}, fmt.Errorf("unknown active request state")
			}
		}
		if len(ids) < pageSize {
			break
		}
	}
	if row.LatestRequestID != "" && !seenLatest {
		latest, err := store.GetRequestStore().Get(ctx, row.LatestRequestID)
		if err != nil {
			return Observation{}, fmt.Errorf("load latest health request: %w", err)
		}
		if latest.ID != row.LatestRequestID || latest.Queue != queue {
			return Observation{}, fmt.Errorf("latest request identity mismatch")
		}
		if !latest.State.IsTerminal() {
			return Observation{}, fmt.Errorf("latest active request is not indexed")
		}
		result.HeadNotIngested = latest.URI != head
	}
	current, err := store.GetQueueStore().Get(ctx, queue)
	if err != nil {
		return Observation{}, fmt.Errorf("recheck health queue: %w", err)
	}
	if current.Version != row.Version {
		return Observation{}, fmt.Errorf("queue changed during health observation")
	}
	result.PendingWithCapacity = pending && row.InFlightCount < cfg.MaxConcurrent
	result.UnaccountedSlots = max(0, row.InFlightCount-processing)
	return result, nil
}

func observeProcessing(ctx context.Context, store storage.Storage, request entity.Request, now int64, result *Observation) error {
	logs, err := store.GetRequestLogStore().List(ctx, request.ID)
	if err != nil {
		return fmt.Errorf("load processing history: %w", err)
	}
	var processingAt int64
	hasBuild := false
	seenBuilds := make(map[string]struct{})
	activeBuilds := 0
	var finishedAt int64
	for _, log := range logs {
		if log.RequestID != request.ID || log.Queue != request.Queue {
			return fmt.Errorf("processing history identity mismatch")
		}
		if log.TimestampMs <= 0 || log.TimestampMs > now {
			return fmt.Errorf("invalid processing history timestamp")
		}
		if log.State == entity.RequestStateProcessing && (processingAt == 0 || log.TimestampMs < processingAt) {
			processingAt = log.TimestampMs
		}
		if log.Event == entity.RequestEventBuildFinished {
			finishedAt = max(finishedAt, log.TimestampMs)
		}
		if log.Event != entity.RequestEventBuildTriggered {
			continue
		}
		id := log.Metadata[requestlog.MetadataKeyBuildID]
		if id == "" {
			return fmt.Errorf("trigger history has no build identity")
		}
		hasBuild = true
		if _, ok := seenBuilds[id]; ok {
			continue
		}
		seenBuilds[id] = struct{}{}
		build, err := store.GetBuildStore().Get(ctx, id)
		if err != nil {
			return fmt.Errorf("load observed build: %w", err)
		}
		if build.ID != id || build.RequestID != request.ID {
			return fmt.Errorf("observed build identity mismatch")
		}
		switch build.Status {
		case entity.BuildStatusAccepted, entity.BuildStatusRunning:
			result.ActiveBuildCount++
			activeBuilds++
			result.BuildAgeMs = max(result.BuildAgeMs, now-log.TimestampMs)
		case entity.BuildStatusSucceeded, entity.BuildStatusFailed, entity.BuildStatusCancelled:
		default:
			return fmt.Errorf("unknown observed build status")
		}
	}
	if processingAt == 0 {
		return fmt.Errorf("processing request has no admission timestamp")
	}
	if !hasBuild {
		result.Dispatches = append(result.Dispatches, PendingDispatch{RequestID: request.ID, AgeMs: now - processingAt})
	}
	if hasBuild && activeBuilds == 0 {
		if finishedAt == 0 {
			finishedAt = processingAt
		}
		result.FinalizationAgeMs = max(result.FinalizationAgeMs, now-finishedAt)
	}
	return nil
}
