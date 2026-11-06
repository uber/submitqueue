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

// Package issuelog retains issue events and materializes lifecycle and request summaries.
package issuelog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// Materializer retains issue events and repairs lifecycle and request-level projections.
type Materializer interface {
	// PersistLog repairs projections even when the event already exists.
	// Reported events leave lifecycle projections unchanged and assign a zero timestamp on retention.
	// firstSeenBuildID identifies the opening retained by the source issue state.
	PersistLog(ctx context.Context, stores storage.Storage, log entity.IssueLog, firstSeenBuildID string) error
}

type materializer struct {
	scope tally.Scope
	now   func() time.Time
}

// NewMaterializer creates an issue-log materializer.
func NewMaterializer(scope tally.Scope) Materializer {
	return &materializer{scope: scope.SubScope("issue_log_materializer"), now: time.Now}
}

func (m *materializer) PersistLog(ctx context.Context, stores storage.Storage, log entity.IssueLog, firstSeenBuildID string) (retErr error) {
	op := metrics.Begin(m.scope, "persist_log", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := ctx.Err(); err != nil {
		return err
	}
	if log.FirstSeenBuildID != "" && log.FirstSeenBuildID != firstSeenBuildID {
		return fmt.Errorf("issue event opening disagrees with source state")
	}
	log.FirstSeenBuildID = firstSeenBuildID
	if log.Event == entity.IssueEventReported && log.TimestampMs == 0 {
		log.TimestampMs = m.now().UnixMilli()
	}
	if err := log.Validate(); err != nil {
		return fmt.Errorf("invalid issue transition: %w", err)
	}
	if firstSeenBuildID == "" || (log.Event == entity.IssueEventOpened && firstSeenBuildID != log.BuildID) {
		return fmt.Errorf("issue transition has an invalid opening build")
	}

	retained, err := retainIssueLog(ctx, stores.GetIssueLogStore(), log)
	if err != nil {
		return err
	}
	build, err := loader.ByID(ctx, retained.BuildID, stores.GetBuildStore().Get, "issue transition build")
	if err != nil {
		return err
	}
	if build.ID != retained.BuildID {
		return fmt.Errorf("issue transition build identity does not match retained history")
	}

	if retained.Event != entity.IssueEventReported {
		if retained.FirstSeenBuildID != "" {
			firstSeenBuildID = retained.FirstSeenBuildID
		}
		summary := entity.IssueSummary{TaskID: retained.TaskID, UnitID: retained.UnitID,
			Status: entity.IssueStatus(retained.Event), FirstSeenBuildID: firstSeenBuildID,
			RequestID: build.RequestID, BuildID: retained.BuildID, ChangedAtMs: retained.TimestampMs, Version: 1}
		if err := summary.Validate(); err != nil {
			return fmt.Errorf("invalid issue summary: %w", err)
		}
		if err := materializeIssueSummary(ctx, stores.GetIssueSummaryStore(), summary); err != nil {
			return err
		}
		if err := materializeOpenIssue(ctx, stores, summary.TaskID, summary.UnitID); err != nil {
			return err
		}
	}
	// Legacy events lack the retained opening needed to reconstruct request summaries.
	if retained.FirstSeenBuildID == "" {
		return nil
	}
	kind := entity.RequestIssueKindClosed
	switch retained.Event {
	case entity.IssueEventOpened:
		kind = entity.RequestIssueKindNew
	case entity.IssueEventReported:
		kind = entity.RequestIssueKindExisting
	}
	result := entity.RequestIssueSummary{RequestID: build.RequestID, TaskID: retained.TaskID, UnitID: retained.UnitID,
		Kind: kind, BuildID: retained.BuildID, FirstSeenBuildID: retained.FirstSeenBuildID}
	if err := result.Validate(); err != nil {
		return fmt.Errorf("invalid request issue summary: %w", err)
	}
	return materializeRequestIssueSummary(ctx, stores.GetRequestIssueSummaryStore(), result)
}

func retainIssueLog(ctx context.Context, logs storage.IssueLogStore, log entity.IssueLog) (entity.IssueLog, error) {
	if err := logs.Create(ctx, log); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return entity.IssueLog{}, fmt.Errorf("retain issue transition: %w", err)
		}
		retained, err := logs.Get(ctx, log.TaskID, log.UnitID, log.BuildID, log.Event)
		if err != nil {
			return entity.IssueLog{}, fmt.Errorf("read retained issue transition: %w", err)
		}
		if retained.TaskID != log.TaskID || retained.UnitID != log.UnitID ||
			retained.BuildID != log.BuildID || retained.Event != log.Event {
			return entity.IssueLog{}, fmt.Errorf("issue transition identity conflicts with retained history")
		}
		if retained.FirstSeenBuildID != "" && retained.FirstSeenBuildID != log.FirstSeenBuildID {
			return entity.IssueLog{}, fmt.Errorf("issue event opening conflicts with retained history")
		}
		if err := retained.Validate(); err != nil {
			return entity.IssueLog{}, fmt.Errorf("invalid retained issue transition: %w", err)
		}
		return retained, nil
	}
	return log, nil
}

func materializeIssueSummary(ctx context.Context, summaries storage.IssueSummaryStore, incoming entity.IssueSummary) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := summaries.Get(ctx, incoming.TaskID, incoming.UnitID)
		if errors.Is(err, storage.ErrNotFound) {
			if err := summaries.Create(ctx, incoming); errors.Is(err, storage.ErrAlreadyExists) {
				continue
			} else if err != nil {
				return fmt.Errorf("create issue summary: %w", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read issue summary: %w", err)
		}
		if err := current.Validate(); err != nil {
			return fmt.Errorf("invalid stored issue summary: %w", err)
		}
		if current.TaskID != incoming.TaskID || current.UnitID != incoming.UnitID {
			return fmt.Errorf("stored issue summary identity does not match transition")
		}

		order, err := entity.CompareRequestID(incoming.RequestID, current.RequestID)
		if err != nil {
			return err
		}
		// A request's closing transition wins over its opening, regardless of delivery order.
		if order < 0 || (order == 0 && current.Status == entity.IssueStatusClosed && incoming.Status == entity.IssueStatusOpen) {
			return nil
		}
		if order == 0 && current.Status == incoming.Status {
			if current.BuildID != incoming.BuildID || current.FirstSeenBuildID != incoming.FirstSeenBuildID || current.ChangedAtMs != incoming.ChangedAtMs {
				return fmt.Errorf("issue summary conflicts with retained transition")
			}
			return nil
		}
		if current.Version == math.MaxInt32 {
			return fmt.Errorf("issue summary version exhausted")
		}

		replacement := incoming
		replacement.Version = current.Version
		if err := summaries.Update(ctx, replacement, current.Version, current.Version+1); errors.Is(err, storage.ErrVersionMismatch) {
			continue
		} else if err != nil {
			return fmt.Errorf("replace issue summary: %w", err)
		}
		return nil
	}
}

func materializeOpenIssue(ctx context.Context, stores storage.Storage, taskID, unitID string) error {
	openIssues := stores.GetOpenIssueStore()
	summaries := stores.GetIssueSummaryStore()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Read the open-issue row before the summary so a stale closed
		// summary cannot authorize deleting a newer opening.
		current, err := openIssues.Get(ctx, taskID, unitID)
		missing := errors.Is(err, storage.ErrNotFound)
		if err != nil && !missing {
			return fmt.Errorf("read open issue: %w", err)
		}
		summary, err := summaries.Get(ctx, taskID, unitID)
		if err != nil {
			return fmt.Errorf("read issue summary for open issue: %w", err)
		}

		switch summary.Status {
		case entity.IssueStatusOpen:
			if !missing && current.FirstSeenBuildID == summary.FirstSeenBuildID {
				return nil
			}
			desired := entity.OpenIssue{TaskID: taskID, UnitID: unitID, FirstSeenBuildID: summary.FirstSeenBuildID}
			if missing {
				err = openIssues.Create(ctx, desired)
				if errors.Is(err, storage.ErrAlreadyExists) {
					continue
				}
			} else {
				_, err = openIssues.Replace(ctx, desired, current.FirstSeenBuildID)
			}
		case entity.IssueStatusClosed:
			if missing {
				return nil
			}
			_, err = openIssues.Delete(ctx, current)
		default:
			return fmt.Errorf("invalid issue summary status %q", summary.Status)
		}
		if err != nil {
			return fmt.Errorf("materialize open issue: %w", err)
		}
	}
}

var _ Materializer = (*materializer)(nil)

func materializeRequestIssueSummary(ctx context.Context, store storage.RequestIssueSummaryStore, incoming entity.RequestIssueSummary) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := store.Get(ctx, incoming.RequestID, incoming.TaskID, incoming.UnitID)
		if errors.Is(err, storage.ErrNotFound) {
			err := store.Create(ctx, incoming)
			if errors.Is(err, storage.ErrAlreadyExists) {
				continue
			}
			if err != nil {
				return fmt.Errorf("create request issue summary: %w", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read request issue summary: %w", err)
		}
		if err := current.Validate(); err != nil {
			return fmt.Errorf("invalid retained request issue summary: %w", err)
		}
		expected := incoming
		expected.Kind = current.Kind
		expected.Version = current.Version
		if current != expected {
			return fmt.Errorf("request issue summary conflicts with retained history")
		}
		if current.Kind == entity.RequestIssueKindClosed || current.Kind == incoming.Kind {
			return nil
		}
		if incoming.Kind != entity.RequestIssueKindClosed {
			return fmt.Errorf("request issue summary conflicts with retained history")
		}
		if current.Version == math.MaxInt32 {
			return fmt.Errorf("request issue summary version exhausted")
		}
		if err := store.Update(ctx, incoming, current.Version, current.Version+1); errors.Is(err, storage.ErrVersionMismatch) {
			continue
		} else if err != nil {
			return fmt.Errorf("update request issue summary: %w", err)
		}
		return nil
	}
}
