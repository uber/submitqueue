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

	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

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

// IndexGreenProjectsForCommit adds the supplied persisted green project IDs to the projection.
// An off-trunk verdict remains a valid fact, but is not a candidate for the current trunk.
func IndexGreenProjectsForCommit(ctx context.Context, sources sourcecontrol.Factory, store storage.Storage, queue, uri string, projects []string) error {
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
