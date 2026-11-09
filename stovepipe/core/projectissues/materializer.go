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

// Package projectissues retains request attribution and materializes its reverse lookup.
package projectissues

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type Materializer interface {
	// PersistAttribution retains one pair before projecting its reverse lookup.
	// Replaying a retained pair repairs a missing lookup.
	PersistAttribution(ctx context.Context, stores storage.Storage, requestID string, entry entity.ProjectIssueAttribution) error
}

type materializer struct{ scope tally.Scope }

func NewMaterializer(scope tally.Scope) Materializer {
	return &materializer{scope: scope.SubScope("project_issues_materializer")}
}

func (m *materializer) PersistAttribution(ctx context.Context, stores storage.Storage, requestID string, entry entity.ProjectIssueAttribution) (retErr error) {
	op := metrics.Begin(m.scope, "persist_attribution", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := id.Validate(requestID); err != nil {
		return fmt.Errorf("invalid attribution request ID: %w", err)
	}
	if err := entry.Validate(); err != nil {
		return fmt.Errorf("invalid project issue attribution: %w", err)
	}

	// Both rows contain only their immutable key, so a duplicate is the same pair.
	err := stores.GetRequestProjectsByIssueStore().Create(ctx, requestID, entry)
	if err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("retain request projects by issue: %w", err)
	}
	err = stores.GetRequestIssuesByProjectStore().Create(ctx, requestID, entry)
	if err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("materialize request issues by project: %w", err)
	}
	return nil
}

var _ Materializer = (*materializer)(nil)
