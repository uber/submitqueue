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

package fix

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func loadRequestForBuild(ctx context.Context, store storage.Storage, queue, buildID string) (entity.Request, error) {
	build, err := loader.ByID(ctx, buildID, store.GetBuildStore().Get, "build")
	if err != nil {
		return entity.Request{}, err
	}
	if build.RequestID == "" {
		return entity.Request{}, errs.NewUserError(fmt.Errorf("build %q has no request", buildID))
	}
	request, err := loader.ByID(ctx, build.RequestID, store.GetRequestStore().Get, "request")
	if err != nil {
		return entity.Request{}, err
	}
	if request.Queue != queue {
		return entity.Request{}, errs.NewUserError(fmt.Errorf("repair request belongs to another queue"))
	}
	return request, nil
}

func persistFixSubmittedLog(ctx context.Context, materializer requestlog.Materializer, store storage.Storage, request entity.Request, buildID string, observation entity.BuildIssue) error {
	// The queue-scoped executor's opaque fix ID identifies the accepted attempt.
	// Restoring this from the durable association must precede any replay skip.
	// Opaque fix IDs can fill the storage column; the event prefix must still fit its own key.
	identity := sha256.Sum256([]byte(observation.FixID))
	log := requestlog.NewRequestEventLog(request, entity.RequestEventFixSubmitted, fmt.Sprintf("%x", identity),
		map[string]string{
			requestlog.MetadataKeyBuildID: buildID,
			requestlog.MetadataKeyTaskID:  observation.TaskID,
			requestlog.MetadataKeyUnitID:  observation.UnitID,
			requestlog.MetadataKeyFixID:   observation.FixID,
		})
	if err := materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("retain submitted repair %q for build %q: %w", observation.FixID, buildID, err)
	}
	return nil
}

func persistFixSubmissionDecision(ctx context.Context, materializer requestlog.Materializer, store storage.Storage, request entity.Request, buildID string, event entity.RequestEvent, reason string) error {
	log := requestlog.NewRequestEventLog(request, event, buildID, map[string]string{
		requestlog.MetadataKeyBuildID: buildID,
		requestlog.MetadataKeyReason:  reason,
	})
	if err := materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("retain repair decision for build %q: %w", buildID, err)
	}
	return nil
}
