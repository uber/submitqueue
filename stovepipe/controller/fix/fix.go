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

// Package fix submits the first failed unit per build once.
package fix

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type Controller struct{ params Params }

type Params struct {
	Stores        storage.Factory
	Executors     fixer.Factory
	Materializer  requestlog.Materializer
	TopicKey      consumer.TopicKey
	ConsumerGroup string
}

func NewController(params Params) *Controller     { return &Controller{params: params} }
func (c *Controller) Name() string                { return "fix" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.params.TopicKey }
func (c *Controller) ConsumerGroup() string       { return c.params.ConsumerGroup }

func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	message := delivery.Message()
	request := &mq.FixRequest{}
	if err := mq.Unmarshal(message.Payload, request); err != nil {
		return err
	}
	if err := entityqueue.ValidatePayloadQueue(message, request.GetQueueName()); err != nil {
		return err
	}
	if request.GetBuildId() == "" || request.GetTaskId() == "" || request.GetFirstSeenBuildId() == "" {
		return fmt.Errorf("repair request requires build, task, and opening build")
	}
	if request.GetUnitId() == "" {
		return nil
	}
	store, err := c.params.Stores.For(storage.Config{QueueName: request.GetQueueName()})
	if err != nil {
		return err
	}
	build, err := loader.ByID(ctx, request.GetBuildId(), store.GetBuildStore().Get, "build")
	if err != nil {
		return err
	}
	source, err := loader.ByID(ctx, build.RequestID, store.GetRequestStore().Get, "request")
	if err != nil {
		return err
	}
	if source.Queue != request.GetQueueName() {
		return fmt.Errorf("repair build belongs to a different queue")
	}
	fix, selected, err := c.selectInitialFix(ctx, store, build.ID, request)
	if err != nil || !selected {
		return err
	}
	if fix.Queue != source.Queue || fix.BuildID != build.ID {
		return fmt.Errorf("selected repair belongs to a different queue or build")
	}
	if len(fix.Issues) != 1 {
		return fmt.Errorf("initial repair requires one issue")
	}
	if fix.State != entity.FixQueued {
		return c.persistSubmissionHistory(ctx, store, source, fix)
	}

	current, err := currentOpening(ctx, store, fix.Issues[0])
	if err != nil {
		return err
	}
	if !current {
		fix.State = entity.FixSkipped
		_, err = saveFix(ctx, store.GetFixStore(), fix)
		return err
	}
	executor, err := c.params.Executors.For(fixer.Config{QueueName: fix.Queue})
	if err != nil {
		return err
	}
	// The durable claim makes dispatch at-most-once, including after a crash before Submit.
	fix.State = entity.FixSubmitting
	fix, err = saveFix(ctx, store.GetFixStore(), fix)
	if err != nil {
		return err
	}
	claimed := fix
	// History failure must not strand the claimed dispatch; replay repairs the log.
	historyErr := c.persistSubmissionHistory(ctx, store, source, claimed)
	executionRef, submitErr := executor.Submit(ctx, fixer.SubmitRequest{BuildID: fix.BuildID, Issue: fix.Issues[0]})
	if submitErr == nil && executionRef == "" {
		submitErr = fmt.Errorf("executor returned an empty execution reference")
	}
	fix.Metadata = maps.Clone(fix.Metadata)
	if submitErr != nil {
		fix.State = entity.FixSubmissionUnknown
		if fix.Metadata == nil {
			fix.Metadata = make(map[string]string)
		}
		fix.Metadata["submission.error"] = submitErr.Error()
	} else {
		fix.State = entity.FixSubmitted
		fix.ExecutionRef = executionRef
	}
	fix, err = saveFix(ctx, store.GetFixStore(), fix)
	if err != nil {
		return errors.Join(err, historyErr)
	}
	return c.persistSubmissionHistory(ctx, store, source, fix)
}

func (c *Controller) selectInitialFix(ctx context.Context, store storage.Storage, buildID string, request *mq.FixRequest) (entity.Fix, bool, error) {
	// Build IDs fit the FixStore key unchanged; the initial attempt needs no allocated ID.
	existing, err := store.GetFixStore().Get(ctx, buildID)
	if err == nil {
		return existing, true, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return entity.Fix{}, false, err
	}
	if _, err := store.GetBuildIssueStore().Get(ctx, buildID, request.GetTaskId(), request.GetUnitId()); err != nil {
		return entity.Fix{}, false, fmt.Errorf("load confirmed repair observation: %w", err)
	}
	issue := entity.OpenIssue{TaskID: request.GetTaskId(), UnitID: request.GetUnitId(), FirstSeenBuildID: request.GetFirstSeenBuildId()}
	current, err := currentOpening(ctx, store, issue)
	if err != nil || !current {
		return entity.Fix{}, false, err
	}
	now := time.Now().UnixMilli()
	candidate := entity.Fix{ID: buildID, Queue: request.GetQueueName(), BuildID: buildID, State: entity.FixQueued, Issues: []entity.OpenIssue{issue}, Version: 1, CreatedAt: now, UpdatedAt: now}
	err = store.GetFixStore().Create(ctx, candidate)
	if errors.Is(err, storage.ErrAlreadyExists) {
		existing, err = store.GetFixStore().Get(ctx, buildID)
		return existing, err == nil, err
	}
	return candidate, err == nil, err
}

func currentOpening(ctx context.Context, store storage.Storage, ref entity.OpenIssue) (bool, error) {
	issue, err := store.GetOpenIssueStore().Get(ctx, ref.TaskID, ref.UnitID)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return issue == ref, nil
}

func saveFix(ctx context.Context, store storage.FixStore, fix entity.Fix) (entity.Fix, error) {
	fix.UpdatedAt = time.Now().UnixMilli()
	if err := store.Update(ctx, fix, fix.Version, fix.Version+1); err != nil {
		return entity.Fix{}, err
	}
	fix.Version++
	return fix, nil
}

var _ consumer.Controller = (*Controller)(nil)
