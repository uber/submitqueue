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

package issuetrack

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func persistIssuesReconciledLog(ctx context.Context, materializer requestlog.Materializer, store storage.Storage, queue string, progress entity.IssueTrackingState) error {
	if progress.Phase != entity.IssueTrackingPhaseFinalized {
		return nil
	}
	if err := progress.Validate(); err != nil {
		return fmt.Errorf("invalid completed issue tracking progress: %w", err)
	}
	build, request, err := loadBuildAndRequest(ctx, store, queue, progress.BuildID)
	if err != nil {
		return err
	}
	if request.ID != progress.RequestID {
		return fmt.Errorf("completed issue tracking progress belongs to another request")
	}
	if err := requireTerminalBuild(build, request); err != nil {
		return err
	}
	mode := "reconciled"
	switch {
	case build.Status == entity.BuildStatusCancelled:
		mode = "suppressed_cancelled"
	case progress.HasTaskWideIssues:
		mode = "suppressed_task_wide_issue"
	}
	log := requestlog.NewRequestEventLog(request, entity.RequestEventIssuesReconciled, build.ID,
		map[string]string{
			requestlog.MetadataKeyBuildID:     build.ID,
			requestlog.MetadataKeyClosureMode: mode,
		})
	if err := materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("retain issue reconciliation for build %q: %w", build.ID, err)
	}
	return nil
}
