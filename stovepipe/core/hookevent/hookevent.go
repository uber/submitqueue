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

// Package hookevent holds stovepipe's hook events: the source it reports, the
// types it publishes, and a constructor per type. Centralizing them keeps the
// wire contract in one place, so a controller names an event rather than
// assembling one.
//
// Payloads carry identity only: the queue and request, issue transition, or repair.
// A hook can resolve additional context from storage without depending on a
// snapshot taken at publication time. See doc/rfc/stovepipe/steps/record.md.
package hookevent

import (
	"crypto/sha256"
	"fmt"
	"time"

	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/protobuf/types/known/structpb"
)

// Source is reported on every hook event stovepipe publishes. A consumer
// serving several domains matches on it to recognize stovepipe's events.
const Source = "stovepipe"

// Type is the event type a consumer filters on.
type Type string

const (
	// TypeIssueOpened announces a durable opening, including a reopening after closure.
	TypeIssueOpened Type = "issue.opened"
	// TypeIssueClosed announces a durable closure.
	TypeIssueClosed Type = "issue.closed"
	// TypeFixSubmitted announces backend acceptance of repair work for one opening and executor.
	TypeFixSubmitted Type = "fix.submitted"
	// TypeProjectAnalysisCompleted announces durable project facts and issue attribution.
	TypeProjectAnalysisCompleted Type = "project.analysis.completed"
	// TypeValidationRepositoryStarted announces that whole-repository validation
	// of the request named in the payload has begun. It carries no verdict; the
	// outcome arrives later as one of the types below.
	TypeValidationRepositoryStarted Type = "validation.repository.started"
	// TypeValidationRepositoryRecorded announces a durable whole-repository
	// validation fact for the request named in the payload.
	TypeValidationRepositoryRecorded Type = "validation.repository.recorded"
	// TypeValidationRepositoryCancelled announces a validation that ended
	// without establishing a fact, so a consumer can stop waiting on the commit.
	TypeValidationRepositoryCancelled Type = "validation.repository.cancelled"
)

// unversioned events are identified by their immutable request or transition.
const unversioned int32 = 0

// NewIssueOpened identifies one immutable opening. Replays retain its ID and timestamp.
func NewIssueOpened(queue string, log entity.IssueLog) *basehook.HookEvent {
	return newIssueEvent(TypeIssueOpened, "opening_build_id", queue, log)
}

// NewIssueClosed identifies one immutable closure. Replays retain its ID and timestamp.
func NewIssueClosed(queue string, log entity.IssueLog) *basehook.HookEvent {
	return newIssueEvent(TypeIssueClosed, "closing_build_id", queue, log)
}

// NewFixSubmitted retains the submitted log's identity and timestamp on replay.
// Acceptance does not establish repair completion or issue recovery.
func NewFixSubmitted(log entity.RequestLog) *basehook.HookEvent {
	buildID := log.Metadata[requestlog.MetadataKeyBuildID]
	taskID := log.Metadata[requestlog.MetadataKeyTaskID]
	unitID := log.Metadata[requestlog.MetadataKeyUnitID]
	executorID := log.Metadata[requestlog.MetadataKeyExecutorID]
	identity := fmt.Sprintf("%d:%s%d:%s%d:%s%d:%s%d:%s", len(log.Queue), log.Queue,
		len(buildID), buildID, len(taskID), taskID, len(unitID), unitID, len(executorID), executorID)
	subject := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
	return &basehook.HookEvent{
		Id:          basehook.NewEventID(Source, string(TypeFixSubmitted), subject, unversioned),
		Source:      Source,
		Type:        string(TypeFixSubmitted),
		TimestampMs: log.TimestampMs,
		Payload: &structpb.Struct{Fields: map[string]*structpb.Value{
			"queue":            structpb.NewStringValue(log.Queue),
			"opening_build_id": structpb.NewStringValue(buildID),
			"task_id":          structpb.NewStringValue(taskID),
			"unit_id":          structpb.NewStringValue(unitID),
			"executor_id":      structpb.NewStringValue(executorID),
		}},
	}
}

func newIssueEvent(eventType Type, buildField, queue string, log entity.IssueLog) *basehook.HookEvent {
	identity := fmt.Sprintf("%d:%s%d:%s%d:%s%d:%s", len(queue), queue, len(log.TaskID), log.TaskID,
		len(log.UnitID), log.UnitID, len(log.BuildID), log.BuildID)
	subject := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
	return &basehook.HookEvent{
		Id:          basehook.NewEventID(Source, string(eventType), subject, unversioned),
		Source:      Source,
		Type:        string(eventType),
		TimestampMs: log.TimestampMs,
		Payload: &structpb.Struct{Fields: map[string]*structpb.Value{
			"queue":    structpb.NewStringValue(queue),
			"task_id":  structpb.NewStringValue(log.TaskID),
			"unit_id":  structpb.NewStringValue(log.UnitID),
			buildField: structpb.NewStringValue(log.BuildID),
		}},
	}
}

// NewProjectAnalysisCompleted retains the completion log's identity and timestamp on replay.
func NewProjectAnalysisCompleted(log entity.RequestLog) *basehook.HookEvent {
	// Request IDs are queue-local; hash both keys to bound the event ID.
	identity := fmt.Sprintf("%d:%s%d:%s", len(log.Queue), log.Queue, len(log.RequestID), log.RequestID)
	subject := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
	event := newRequestEvent(TypeProjectAnalysisCompleted, log.Queue, log.RequestID, log.TimestampMs)
	event.Id = basehook.NewEventID(Source, string(TypeProjectAnalysisCompleted), subject, unversioned)
	return event
}

// NewValidationRepositoryStarted builds the event announcing that validation of
// request's commit has begun.
func NewValidationRepositoryStarted(request entity.Request) *basehook.HookEvent {
	return newEvent(TypeValidationRepositoryStarted, request)
}

// NewValidationRepositoryRecorded builds the event announcing that request's
// validation fact is durable.
func NewValidationRepositoryRecorded(request entity.Request) *basehook.HookEvent {
	return newEvent(TypeValidationRepositoryRecorded, request)
}

// NewValidationRepositoryCancelled builds the event announcing that request's
// validation ended without establishing a fact.
func NewValidationRepositoryCancelled(request entity.Request) *basehook.HookEvent {
	return newEvent(TypeValidationRepositoryCancelled, request)
}

func newEvent(eventType Type, request entity.Request) *basehook.HookEvent {
	return newRequestEvent(eventType, request.Queue, request.ID, time.Now().UnixMilli())
}

func newRequestEvent(eventType Type, queue, requestID string, timestampMs int64) *basehook.HookEvent {
	return &basehook.HookEvent{
		Id:          basehook.NewEventID(Source, string(eventType), requestID, unversioned),
		Source:      Source,
		Type:        string(eventType),
		TimestampMs: timestampMs,
		Version:     unversioned,
		Payload: &structpb.Struct{Fields: map[string]*structpb.Value{
			"queue":      structpb.NewStringValue(queue),
			"request_id": structpb.NewStringValue(requestID),
		}},
	}
}
