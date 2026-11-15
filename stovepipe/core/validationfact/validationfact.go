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

// Package validationfact retains immutable repository and project validation facts.
package validationfact

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Persist retains an immutable fact, adopting an existing fact only for the same request.
func Persist(
	ctx context.Context,
	logger *zap.SugaredLogger,
	scope tally.Scope,
	factStore storage.ValidationFactStore,
	fact entity.ValidationFact,
) (entity.ValidationFact, bool, error) {
	err := factStore.Create(ctx, fact)
	switch {
	case err == nil:
		metrics.NamedCounter(scope, "record", "fact_created", 1, metrics.TagsFromContext(ctx)...)
		logger.Infow("recorded validation fact",
			"request_id", fact.RequestID,
			"uri", fact.URI,
			"project", fact.Project,
			"degree", fact.Degree,
		)
		return fact, true, nil

	case errors.Is(err, storage.ErrAlreadyExists):
		stored, getErr := factStore.Get(ctx, fact.URI, fact.Project)
		if getErr != nil {
			metrics.NamedCounter(scope, "record", "storage_errors", 1, metrics.TagsFromContext(ctx)...)
			return entity.ValidationFact{}, false, fmt.Errorf("failed to load the existing fact for uri %s and project %q: %w", fact.URI, fact.Project, getErr)
		}
		if stored.RequestID != fact.RequestID {
			// Ingest deduplication forbids different requests owning the same immutable fact.
			metrics.NamedCounter(scope, "record", "invariant_errors", 1, metrics.TagsFromContext(ctx)...)
			return entity.ValidationFact{}, false, fmt.Errorf(
				"fact for uri %s and project %q is owned by request %s, not %s", fact.URI, fact.Project, stored.RequestID, fact.RequestID)
		}
		metrics.NamedCounter(scope, "record", "fact_exists", 1, metrics.TagsFromContext(ctx)...)
		return stored, false, nil

	default:
		metrics.NamedCounter(scope, "record", "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return entity.ValidationFact{}, false, fmt.Errorf("failed to create the fact for uri %s and project %q: %w", fact.URI, fact.Project, err)
	}
}
