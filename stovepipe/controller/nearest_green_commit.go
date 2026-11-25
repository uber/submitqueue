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

package controller

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/greenproject"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// NearestGreenCommitController finds a recorded green project result on trunk.
type NearestGreenCommitController interface {
	NearestGreenCommitThatIncludes(ctx context.Context, req entity.NearestGreenCommitRequest) (entity.NearestGreenCommitResult, error)
}

type nearestGreenCommitController struct {
	scope   tally.Scope
	stores  storage.Factory
	sources sourcecontrol.Factory
}

// NewNearestGreenCommitController constructs the project green lookup.
func NewNearestGreenCommitController(scope tally.Scope, stores storage.Factory, sources sourcecontrol.Factory) NearestGreenCommitController {
	return &nearestGreenCommitController{scope: scope, stores: stores, sources: sources}
}

func (c *nearestGreenCommitController) NearestGreenCommitThatIncludes(ctx context.Context, req entity.NearestGreenCommitRequest) (result entity.NearestGreenCommitResult, retErr error) {
	op := metrics.Begin(c.scope, "nearest_green_commit", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return result, err
	}
	if err := validateHistoryIdentifier("desired commit URI", req.DesiredCommitURI); err != nil {
		return result, err
	}
	if err := validateHistoryIdentifier("project", req.Project); err != nil {
		return result, err
	}
	source, err := c.sources.For(sourcecontrol.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve source control for queue %q: %w", req.Queue, err)
	}
	position, err := greenproject.LocateTrunkCommit(ctx, source, req.DesiredCommitURI)
	if err != nil {
		return result, err
	}
	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve green project storage for queue %q: %w", req.Queue, err)
	}
	uri, err := store.GetGreenProjectStore().FindAtOrAfter(ctx, position.Lineage, req.Project, position.Position)
	if err != nil || uri == "" {
		return result, err
	}
	fact, err := store.GetValidationFactStore().Get(ctx, uri, req.Project)
	if err != nil {
		return result, fmt.Errorf("load projected green fact for commit %q and project %q: %w", uri, req.Project, err)
	}
	if fact.URI != uri || fact.Project != req.Project || !fact.IsGreen() || id.Validate(fact.RequestID) != nil {
		return result, fmt.Errorf("projected green disagrees with the validation fact for commit %q and project %q", uri, req.Project)
	}
	return entity.NearestGreenCommitResult{CommitURI: uri, RequestID: fact.RequestID}, nil
}
