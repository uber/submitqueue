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

// Package hookevent holds SubmitQueue's hook events: the source it reports, the
// types it publishes, and a constructor per type. Centralizing them keeps the
// wire contract in one place, so a publisher names an event rather than
// assembling one.
//
// The events follow a request's public status, the one its summary shows: one
// when the request is received, and one when it lands or fails. Cancellation
// has no event yet because no hook needs one. Deriving them from the summary
// rather than from pipeline transitions keeps every hook in step with what a
// user sees, including a request whose start failed before the pipeline saved
// it.
//
// Each status announces one event per change of the request, so a hook that
// retries one change's failed side effect never repeats the others'. Payloads
// carry the queue, the request id, and that change's URI. The gateway
// publishes these events and the orchestrator's hook stage consumes them, and a
// hook there cannot read the gateway's store, so the payload carries what a
// hook needs to reach the change. A request's URIs never change, so they cannot
// go stale on redelivery. See doc/rfc/hook-framework.md.
//
// Known gaps, accepted for now:
//   - request.received is published inside Land, an RPC. If Land records
//     accepted but the publish fails, no later status announces the receipt.
//   - A finished status is announced again whenever its log is redelivered or
//     resent, so a failed publish repairs itself; a copy that arrives after the
//     queue forgot the id reaches hooks twice. That follows a failure: an
//     orchestrator repair resend, a late log redelivery, or a retry after some
//     of a stack's events were published, which repeats those on every retry.
//   - Order holds within a request's partition, but a receipt waiting to retry
//     does not hold back the finish, so a hook can see the finish first.
//   - The queue drops a repeated event id only until the first copy is consumed
//     and collected, seconds later.
package hookevent

import (
	"crypto/sha256"
	"fmt"

	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/submitqueue/entity"
	"google.golang.org/protobuf/types/known/structpb"
)

// Source is reported on every hook event SubmitQueue publishes. A consumer
// serving several domains matches on it to recognize SubmitQueue's events.
const Source = "submitqueue"

// Type is the event type a consumer filters on.
type Type string

const (
	// TypeRequestReceived announces that the request named in the payload has
	// left the internal accepting status and is visible to users. It carries no
	// verdict: the request has not been validated yet.
	TypeRequestReceived Type = "request.received"
	// TypeRequestLanded announces that the request named in the payload landed.
	TypeRequestLanded Type = "request.landed"
	// TypeRequestFailed announces that the request named in the payload ended in
	// error. The request's summary carries the reason.
	TypeRequestFailed Type = "request.failed"
)

// finishedTypeByStatus maps each finished status that has an event to its type.
var finishedTypeByStatus = map[entity.RequestStatus]Type{
	entity.RequestStatusLanded: TypeRequestLanded,
	entity.RequestStatusError:  TypeRequestFailed,
}

// NewRequestReceived builds the events announcing that summary's request was
// received, one per change. A request is received once, so the ids carry version
// 0 and every republish for the same request mints the same ids.
func NewRequestReceived(summary entity.RequestSummary) []*basehook.HookEvent {
	return newRequestEvents(TypeRequestReceived, summary, 0, summary.ReceivedAtMs)
}

// NewRequestFinished builds the events announcing summary's finished status, one
// per change, and none for a status that has no event. The ids carry
// summary.RequestVersion, so a status the summary restates mints the same ids and
// a later finished status the summary moves to mints new ones.
func NewRequestFinished(summary entity.RequestSummary) []*basehook.HookEvent {
	eventType, ok := finishedTypeByStatus[summary.Status]
	if !ok {
		return nil
	}
	return newRequestEvents(eventType, summary, summary.RequestVersion, summary.StatusTimestampMs)
}

func newRequestEvents(eventType Type, summary entity.RequestSummary, version int32, timestampMs int64) []*basehook.HookEvent {
	events := make([]*basehook.HookEvent, 0, len(summary.ChangeURIs))
	for _, uri := range summary.ChangeURIs {
		events = append(events, &basehook.HookEvent{
			Id:          basehook.NewEventID(Source, string(eventType), changeSubject(summary, uri), version),
			Source:      Source,
			Type:        string(eventType),
			TimestampMs: timestampMs,
			Version:     version,
			Payload: &structpb.Struct{Fields: map[string]*structpb.Value{
				"queue":      structpb.NewStringValue(summary.Queue),
				"request_id": structpb.NewStringValue(summary.RequestID),
				"change_uri": structpb.NewStringValue(uri),
			}},
		})
	}
	return events
}

// changeSubject identifies one change of a request across queues. Request IDs
// are only unique within a queue, and queue names and URIs contain the id
// separator, so every part is length-prefixed and hashed into one separator-free
// component.
func changeSubject(summary entity.RequestSummary, uri string) string {
	identity := fmt.Sprintf("%d:%s%d:%s%d:%s", len(summary.Queue), summary.Queue, len(summary.RequestID), summary.RequestID, len(uri), uri)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
}
