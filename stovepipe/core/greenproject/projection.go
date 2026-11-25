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

// Package greenproject maintains the ordered projection of explicit project greens.
package greenproject

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const backfillPageSize = 200

// LocateTrunkCommit resolves and checks the ordering metadata returned by a provider.
func LocateTrunkCommit(ctx context.Context, source sourcecontrol.SourceControl, uri string) (sourcecontrol.TrunkPosition, error) {
	position, err := source.TrunkPosition(ctx, uri)
	if err != nil {
		return sourcecontrol.TrunkPosition{}, fmt.Errorf("locate trunk commit %q: %w", uri, err)
	}
	if len(position.Lineage) == 0 || len(position.Lineage) > 255 || position.Position <= 0 {
		return sourcecontrol.TrunkPosition{}, fmt.Errorf("source control returned an invalid trunk position for %q", uri)
	}
	return position, nil
}

// IndexCommit adds the supplied persisted green project IDs to the projection.
// An off-trunk verdict remains a valid fact, but is not a candidate for the current trunk.
func IndexCommit(ctx context.Context, sources sourcecontrol.Factory, store storage.Storage, queue, uri string, projects []string) error {
	if len(projects) == 0 {
		return nil
	}
	source, err := sources.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		return fmt.Errorf("resolve source control for queue %q: %w", queue, err)
	}
	position, err := LocateTrunkCommit(ctx, source, uri)
	if sourcecontrol.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return store.GetGreenProjectStore().CreateForCommit(ctx, position.Lineage, position.Position, uri, projects)
}

// Backfill indexes retained facts for the current trunk and marks its lineage ready.
// Run it after all live project writers are maintaining the projection. It is safe to
// rerun after a partial write or run on a ready lineage to repair missing entries.
func Backfill(ctx context.Context, stores storage.Factory, sources sourcecontrol.Factory, queue string) error {
	source, err := sources.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		return fmt.Errorf("resolve source control for queue %q: %w", queue, err)
	}
	store, err := stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return fmt.Errorf("resolve green project storage for queue %q: %w", queue, err)
	}
	tip, err := source.Latest(ctx)
	if err != nil {
		return fmt.Errorf("resolve trunk tip for green project backfill: %w", err)
	}
	start, err := LocateTrunkCommit(ctx, source, tip)
	if err != nil {
		return err
	}
	projection := store.GetGreenProjectStore()
	facts := store.GetValidationFactStore()
	for cursor := ""; ; {
		page, err := source.History(ctx, cursor, backfillPageSize)
		if err != nil {
			return fmt.Errorf("read trunk history for green project backfill: %w", err)
		}
		for _, uri := range page.Items {
			position, err := LocateTrunkCommit(ctx, source, uri)
			if err != nil {
				return err
			}
			if position.Lineage != start.Lineage {
				return errs.NewRetryableError(fmt.Errorf("trunk lineage changed during green project backfill"))
			}
			if err := backfillCommit(ctx, facts, projection, uri, position); err != nil {
				return err
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	tip, err = source.Latest(ctx)
	if err != nil {
		return fmt.Errorf("recheck trunk tip after green project backfill: %w", err)
	}
	end, err := LocateTrunkCommit(ctx, source, tip)
	if err != nil {
		return err
	}
	if end.Lineage != start.Lineage {
		return errs.NewRetryableError(fmt.Errorf("trunk lineage changed during green project backfill"))
	}
	return projection.MarkLineageReady(ctx, start.Lineage)
}

func backfillCommit(ctx context.Context, facts storage.ValidationFactStore, projection storage.GreenProjectStore, uri string, position sourcecontrol.TrunkPosition) error {
	for after := ""; ; {
		page, err := facts.ListProjects(ctx, uri, after, backfillPageSize)
		if err != nil {
			return fmt.Errorf("read retained project facts at %q: %w", uri, err)
		}
		var projects []string
		for _, fact := range page {
			if fact.IsGreen() {
				projects = append(projects, fact.Project)
			}
		}
		if len(projects) > 0 {
			if err := projection.CreateForCommit(ctx, position.Lineage, position.Position, uri, projects); err != nil {
				return err
			}
		}
		if len(page) < backfillPageSize {
			return nil
		}
		after = page[len(page)-1].Project
	}
}
