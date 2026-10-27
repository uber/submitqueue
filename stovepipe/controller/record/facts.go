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

package record

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	platformhook "github.com/uber/submitqueue/platform/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

type factRecorder struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	materializer requestlog.Materializer
	registry     consumer.TopicRegistry
}

func (c *factRecorder) recordFact(ctx context.Context, store storage.Storage, request entity.Request, results []projectstatus.Result) (entity.ValidationFact, bool, error) {
	return c.recordValidationFact(ctx, store.GetValidationFactStore(), entity.ValidationFact{
		URI:       request.URI,
		Project:   wholeRepositoryProject,
		Degree:    degreeFor(request.State, results),
		RequestID: request.ID,
		CreatedAt: time.Now().UnixMilli(),
	})
}

func (c *factRecorder) recordValidationFact(ctx context.Context, factStore storage.ValidationFactStore, fact entity.ValidationFact) (entity.ValidationFact, bool, error) {
	err := factStore.Create(ctx, fact)
	switch {
	case err == nil:
		metrics.NamedCounter(c.metricsScope, _opName, "fact_created", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Infow("recorded validation fact",
			"request_id", fact.RequestID,
			"uri", fact.URI,
			"project", fact.Project,
			"degree", fact.Degree,
		)
		return fact, true, nil

	case errors.Is(err, storage.ErrAlreadyExists):
		stored, getErr := factStore.Get(ctx, fact.URI, fact.Project)
		if getErr != nil {
			metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
			return entity.ValidationFact{}, false, fmt.Errorf("failed to load the existing fact for uri %s and project %q: %w", fact.URI, fact.Project, getErr)
		}
		if stored.RequestID != fact.RequestID {
			// Ingest deduplication forbids different requests owning the same immutable fact.
			metrics.NamedCounter(c.metricsScope, _opName, "invariant_errors", 1, metrics.TagsFromContext(ctx)...)
			return entity.ValidationFact{}, false, fmt.Errorf(
				"fact for uri %s and project %q is owned by request %s, not %s", fact.URI, fact.Project, stored.RequestID, fact.RequestID)
		}
		metrics.NamedCounter(c.metricsScope, _opName, "fact_exists", 1, metrics.TagsFromContext(ctx)...)
		return stored, false, nil

	default:
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return entity.ValidationFact{}, false, fmt.Errorf("failed to create the fact for uri %s and project %q: %w", fact.URI, fact.Project, err)
	}
}

func (c *factRecorder) persistValidationFactRecordedLog(
	ctx context.Context,
	store storage.Storage,
	request entity.Request,
	fact entity.ValidationFact,
) error {
	// Hash the composite fact key so an arbitrarily long URI cannot overflow the bounded log ID.
	identity := sha256.Sum256([]byte(fact.URI + "\x00" + fact.Project))
	log := requestlog.NewRequestEventLog(
		request,
		entity.RequestEventValidationFactRecorded,
		hex.EncodeToString(identity[:]),
		map[string]string{requestlog.MetadataKeyFactDegree: strconv.FormatFloat(fact.Degree, 'g', -1, 64)},
	)
	log.TimestampMs = fact.CreatedAt
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record validation fact for request %s: %w", request.ID, err)
	}
	return nil
}

func (c *factRecorder) publishHookEvent(ctx context.Context, request entity.Request, event *basehook.HookEvent) error {
	if err := platformhook.Publish(ctx, c.registry, request.Queue, event, request.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "hook_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to announce %s for request %s: %w", event.GetType(), request.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, _opName, "hook_events_published", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("event_type", event.GetType()))...,
	)
	c.logger.Infow("announced validation outcome",
		"queue", request.Queue,
		"request_id", request.ID,
		"uri", request.URI,
		"event_type", event.GetType(),
		"event_id", event.GetId(),
	)
	return nil
}
