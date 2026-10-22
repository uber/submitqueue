// Copyright (c) 2025 Uber Technologies, Inc.
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
	"maps"

	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func (c *Controller) persistSubmissionHistory(ctx context.Context, store storage.Storage, source entity.Request, fix entity.Fix) error {
	switch fix.State {
	case entity.FixSubmitting, entity.FixSubmitted, entity.FixSubmissionUnknown:
	default:
		return nil
	}
	issue := fix.Issues[0]
	metadata := map[string]string{
		requestlog.MetadataKeyBuildID: fix.BuildID,
		"fix_id":                      fix.ID,
		"task_id":                     issue.TaskID,
		"unit_id":                     issue.UnitID,
		"first_seen_build_id":         issue.FirstSeenBuildID,
	}
	started := requestlog.NewRequestEventLog(source, entity.RequestEventFixSubmissionStarted, fix.ID, metadata)
	started.TimestampMs = fix.UpdatedAt
	if err := c.params.Materializer.PersistLog(ctx, store, started); err != nil {
		return err
	}
	if fix.State == entity.FixSubmitting {
		return nil
	}
	result := maps.Clone(metadata)
	result["submission_state"] = string(fix.State)
	if fix.ExecutionRef != "" {
		result["execution_ref"] = fix.ExecutionRef
	}
	if detail := fix.Metadata["submission.error"]; detail != "" {
		result["submission.error"] = detail
	}
	log := requestlog.NewRequestEventLog(source, entity.RequestEventFixSubmissionRecorded, fix.ID, result)
	log.TimestampMs = fix.UpdatedAt
	return c.params.Materializer.PersistLog(ctx, store, log)
}
