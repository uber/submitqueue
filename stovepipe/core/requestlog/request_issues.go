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

package requestlog

import (
	"context"
	"fmt"
	"slices"

	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/stovepipe/core/requestissue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func materializeRequestIssueRepair(ctx context.Context, stores storage.Storage, log entity.RequestLog) error {
	incoming := entity.RequestIssueSummary{RequestID: log.RequestID,
		BuildID: log.Metadata[MetadataKeyBuildID], TaskID: log.Metadata[MetadataKeyTaskID], UnitID: log.Metadata[MetadataKeyUnitID],
		FixID: log.Metadata[MetadataKeyFixID], Version: 1}
	if incoming.FixID == "" {
		return fmt.Errorf("retained repair event requires a repair ID")
	}
	return requestissue.MaterializeObservation(ctx, stores.GetRequestIssueSummaryStore(), incoming)
}

// RebuildRequestIssues repairs one request's read projection from retained facts and repair events.
// It adds missing observations without deleting rows or clearing repair associations.
// Builds without retained request correlation cannot be discovered by this operation.
func RebuildRequestIssues(ctx context.Context, stores storage.Storage, queue, requestID string) error {
	if queue == "" {
		return fmt.Errorf("request issue rebuild requires a queue")
	}
	if err := id.Validate(requestID); err != nil {
		return err
	}
	logs, err := stores.GetRequestLogStore().List(ctx, requestID)
	if err != nil {
		return fmt.Errorf("read request issue rebuild sources: %w", err)
	}
	builds := make(map[string]struct{})
	for _, log := range logs {
		if log.Queue != queue || log.RequestID != requestID {
			return fmt.Errorf("request issue rebuild log identity does not match selection")
		}
		if buildID := log.Metadata[MetadataKeyBuildID]; buildID != "" {
			builds[buildID] = struct{}{}
		}
	}
	buildIDs := make([]string, 0, len(builds))
	for buildID := range builds {
		buildIDs = append(buildIDs, buildID)
	}
	slices.Sort(buildIDs)
	for _, buildID := range buildIDs {
		if err := requestissue.MaterializeBuild(ctx, stores, requestID, buildID); err != nil {
			return err
		}
	}
	for _, log := range logs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if log.Event == entity.RequestEventFixSubmitted {
			if err := materializeRequestIssueRepair(ctx, stores, log); err != nil {
				return err
			}
		}
	}
	return nil
}
