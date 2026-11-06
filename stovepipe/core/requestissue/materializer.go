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

// Package requestissue projects durable build observations into request-scoped read models.
package requestissue

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const observationPageSize = 100

// MaterializeObservation merges independently retained observation facts.
// Empty fields never clear known facts; conflicting nonempty facts are errors.
func MaterializeObservation(ctx context.Context, summaries storage.RequestIssueSummaryStore, incoming entity.RequestIssueSummary) error {
	if err := incoming.Validate(); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := summaries.Get(ctx, incoming.RequestID, incoming.BuildID, incoming.TaskID, incoming.UnitID)
		if errors.Is(err, storage.ErrNotFound) {
			initial := incoming
			initial.Version = 1
			if err := summaries.Create(ctx, initial); errors.Is(err, storage.ErrAlreadyExists) {
				continue
			} else if err != nil {
				return fmt.Errorf("create request issue projection: %w", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read request issue projection: %w", err)
		}
		if err := current.Validate(); err != nil {
			return fmt.Errorf("invalid request issue projection: %w", err)
		}
		if current.RequestID != incoming.RequestID || current.BuildID != incoming.BuildID || current.TaskID != incoming.TaskID || current.UnitID != incoming.UnitID {
			return fmt.Errorf("request issue projection identity does not match source")
		}
		if incoming.FixID != "" && current.FixID != "" && incoming.FixID != current.FixID {
			return fmt.Errorf("request issue repair association conflicts with retained source")
		}
		if incoming.Novelty != "" && current.Novelty != "" && incoming.Novelty != current.Novelty {
			return fmt.Errorf("request issue novelty conflicts with retained source")
		}
		replacement := current
		if incoming.FixID != "" {
			replacement.FixID = incoming.FixID
		}
		if incoming.Novelty != "" {
			replacement.Novelty = incoming.Novelty
		}
		if replacement == current {
			return nil
		}
		if current.Version == math.MaxInt32 {
			return fmt.Errorf("request issue projection version exhausted")
		}
		if err := summaries.Update(ctx, replacement, current.Version, current.Version+1); errors.Is(err, storage.ErrVersionMismatch) {
			continue
		} else if err != nil {
			return fmt.Errorf("update request issue projection: %w", err)
		}
		return nil
	}
}

// MaterializeIssueNovelty retains classification only for an accepted open observation.
// A later build's mutable issue state cannot classify an earlier build's observation.
func MaterializeIssueNovelty(ctx context.Context, stores storage.Storage, requestID, buildID string, issue entity.IssueState) error {
	if issue.LastProcessedBuildID != buildID || issue.Status != entity.IssueStatusOpen {
		return nil
	}
	if issue.FirstSeenBuildID == "" {
		return fmt.Errorf("open issue requires an opening build for classification")
	}
	novelty := entity.IssueNoveltyExisting
	if issue.FirstSeenBuildID == buildID {
		novelty = entity.IssueNoveltyNew
	}
	return MaterializeObservation(ctx, stores.GetRequestIssueSummaryStore(), entity.RequestIssueSummary{
		RequestID: requestID, BuildID: buildID, TaskID: issue.TaskID, UnitID: issue.UnitID, Novelty: novelty, Version: 1,
	})
}

// MaterializeBuild repairs observation membership from durable facts, including partial passes.
// Repair IDs are projected separately from retained repair events, not operational associations.
func MaterializeBuild(ctx context.Context, stores storage.Storage, requestID, buildID string) error {
	observations := stores.GetBuildIssueStore()
	bounds := storage.BuildIssueRange{Limit: observationPageSize}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := observations.List(ctx, buildID, bounds)
		if err != nil {
			return fmt.Errorf("list source build observations: %w", err)
		}
		for _, observation := range page {
			summary := entity.RequestIssueSummary{RequestID: requestID, BuildID: buildID, TaskID: observation.TaskID, UnitID: observation.UnitID, Version: 1}
			if err := MaterializeObservation(ctx, stores.GetRequestIssueSummaryStore(), summary); err != nil {
				return err
			}
		}
		if len(page) < bounds.Limit {
			return nil
		}
		last := page[len(page)-1]
		bounds.After = storage.BuildIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
	}
}
