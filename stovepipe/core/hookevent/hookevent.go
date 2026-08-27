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
// Every payload carries identity only — the queue and the request id — and a
// hook resolves what it needs from storage. A richer payload would be a
// snapshot taken at publish time and read later, so it could describe state
// that has since moved on; identity cannot go stale. The cost is a store read
// per hook. See doc/rfc/stovepipe/steps/record.md.
package hookevent

import (
	"time"

	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/protobuf/types/known/structpb"
)

// Source is reported on every hook event stovepipe publishes. A consumer
// serving several domains matches on it to recognize stovepipe's events.
const Source = "stovepipe"

// Type is the event type a consumer filters on.
type Type string

const (
	// TypeValidationRepositoryRecorded announces a durable whole-repository
	// validation fact for the request named in the payload.
	TypeValidationRepositoryRecorded Type = "validation.repository.recorded"
	// TypeValidationRepositoryCancelled announces a validation that ended
	// without establishing a fact, so a consumer can stop waiting on the commit.
	TypeValidationRepositoryCancelled Type = "validation.repository.cancelled"
)

// unversioned is the version reported on these events. Neither describes a
// versioned write, so nothing separates one occurrence of a type from the next
// beyond the request it names.
const unversioned int32 = 0

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

// newEvent is the shared body of the constructors above. The payload keys are
// the wire field names a consumer in another repository mirrors, and this is
// the only place they are written.
func newEvent(eventType Type, request entity.Request) *basehook.HookEvent {
	return &basehook.HookEvent{
		Id:          basehook.NewEventID(Source, string(eventType), request.ID, unversioned),
		Source:      Source,
		Type:        string(eventType),
		TimestampMs: time.Now().UnixMilli(),
		Version:     unversioned,
		Payload: &structpb.Struct{Fields: map[string]*structpb.Value{
			"queue":      structpb.NewStringValue(request.Queue),
			"request_id": structpb.NewStringValue(request.ID),
		}},
	}
}
