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
	"fmt"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	platformhook "github.com/uber/submitqueue/platform/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/core/validationfact"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

func recordFact(
	ctx context.Context,
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	request entity.Request,
	projectCount int32,
) (entity.ValidationFact, bool, error) {
	return validationfact.Persist(ctx, logger, scope, store.GetValidationFactStore(), entity.ValidationFact{
		URI:       request.URI,
		Project:   wholeRepositoryProject,
		Degree:    degreeFor(request.State, projectCount),
		RequestID: request.ID,
		CreatedAt: time.Now().UnixMilli(),
	})
}

func persistValidationFactRecordedLog(
	ctx context.Context,
	materializer requestlog.Materializer,
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
	if err := materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record validation fact for request %s: %w", request.ID, err)
	}
	return nil
}

func publishHookEvent(
	ctx context.Context,
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	request entity.Request,
	event *basehook.HookEvent,
) error {
	if err := platformhook.Publish(ctx, registry, request.Queue, event, request.ID); err != nil {
		metrics.NamedCounter(scope, _opName, "hook_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to announce %s for request %s: %w", event.GetType(), request.ID, err)
	}

	metrics.NamedCounter(scope, _opName, "hook_events_published", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("event_type", event.GetType()))...,
	)
	logger.Infow("announced validation outcome",
		"queue", request.Queue,
		"request_id", request.ID,
		"uri", request.URI,
		"event_type", event.GetType(),
		"event_id", event.GetId(),
	)
	return nil
}
