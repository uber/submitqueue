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

// MaterializeObservation merges an observation or a retained repair association.
// Empty repair IDs never clear associations; different nonempty repair IDs conflict.
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
		if incoming.FixID == "" || incoming.FixID == current.FixID {
			return nil
		}
		if current.FixID != "" {
			return fmt.Errorf("request issue repair association conflicts with retained source")
		}
		if current.Version == math.MaxInt32 {
			return fmt.Errorf("request issue projection version exhausted")
		}
		replacement := current
		replacement.FixID = incoming.FixID
		if err := summaries.Update(ctx, replacement, current.Version, current.Version+1); errors.Is(err, storage.ErrVersionMismatch) {
			continue
		} else if err != nil {
			return fmt.Errorf("update request issue projection: %w", err)
		}
		return nil
	}
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
