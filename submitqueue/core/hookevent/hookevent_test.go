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

package hookevent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestNewRequestReceived(t *testing.T) {
	tests := []struct {
		name    string
		summary entity.RequestSummary
	}{
		{
			name:    "one change in a queue with a slash",
			summary: entity.RequestSummary{RequestID: "1149", Queue: "acme/monorepo", ChangeURIs: []string{"github://github.com/acme/monorepo/pull/42/abc123"}, Status: entity.RequestStatusAccepted, ReceivedAtMs: 100},
		},
		{
			name:    "a stack, received under a later status",
			summary: entity.RequestSummary{RequestID: "7", Queue: "demo-queue", ChangeURIs: []string{"uri/1", "uri/2"}, Status: entity.RequestStatusStarted, RequestVersion: 1, ReceivedAtMs: 200},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := NewRequestReceived(tt.summary)

			require.Len(t, events, len(tt.summary.ChangeURIs))
			for i, event := range events {
				uri := tt.summary.ChangeURIs[i]
				require.NoError(t, basehook.Validate(event))
				assert.Equal(t, Source, event.GetSource())
				assert.Equal(t, string(TypeRequestReceived), event.GetType())
				assert.Zero(t, event.GetVersion())
				assert.Equal(t, tt.summary.ReceivedAtMs, event.GetTimestampMs())
				assert.Equal(t, tt.summary.Queue, event.GetPayload().GetFields()["queue"].GetStringValue())
				assert.Equal(t, tt.summary.RequestID, event.GetPayload().GetFields()["request_id"].GetStringValue())
				assert.Equal(t, uri, event.GetPayload().GetFields()["change_uri"].GetStringValue())
				assert.Equal(t, basehook.NewEventID(Source, string(TypeRequestReceived), changeSubject(tt.summary, uri), 0), event.GetId())
			}

			later := tt.summary
			later.Status, later.RequestVersion = entity.RequestStatusLanded, 5
			assert.Equal(t, eventIDs(events), eventIDs(NewRequestReceived(later)), "a request is received once, whatever its status now")
		})
	}
}

func TestNewRequestFinished(t *testing.T) {
	tests := []struct {
		name      string
		status    entity.RequestStatus
		version   int32
		wantType  Type
		wantEvent bool
	}{
		{name: "landed", status: entity.RequestStatusLanded, version: 5, wantType: TypeRequestLanded, wantEvent: true},
		{name: "failed", status: entity.RequestStatusError, version: 3, wantType: TypeRequestFailed, wantEvent: true},
		{name: "failed before the pipeline saved the request", status: entity.RequestStatusError, version: 0, wantType: TypeRequestFailed, wantEvent: true},
		{name: "cancelled has no event", status: entity.RequestStatusCancelled, version: 4},
		{name: "unfinished status", status: entity.RequestStatusBatched, version: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := entity.RequestSummary{RequestID: "1149", Queue: "acme/monorepo", ChangeURIs: []string{"uri/1", "uri/2"}, Status: tt.status, RequestVersion: tt.version, StatusTimestampMs: 300}

			events := NewRequestFinished(summary)

			if !tt.wantEvent {
				assert.Empty(t, events)
				return
			}
			require.Len(t, events, len(summary.ChangeURIs))
			for i, event := range events {
				uri := summary.ChangeURIs[i]
				require.NoError(t, basehook.Validate(event))
				assert.Equal(t, string(tt.wantType), event.GetType())
				assert.Equal(t, tt.version, event.GetVersion())
				assert.Equal(t, summary.StatusTimestampMs, event.GetTimestampMs())
				assert.Equal(t, uri, event.GetPayload().GetFields()["change_uri"].GetStringValue())
				assert.Equal(t, basehook.NewEventID(Source, string(tt.wantType), changeSubject(summary, uri), tt.version), event.GetId())
			}
		})
	}
}

func TestChangeSubject(t *testing.T) {
	tests := []struct {
		name string
		a, b entity.RequestSummary
		aURI string
		bURI string
	}{
		{
			name: "same id in different queues",
			a:    entity.RequestSummary{RequestID: "1", Queue: "queue-a"},
			b:    entity.RequestSummary{RequestID: "1", Queue: "queue-b"},
			aURI: "uri/1", bURI: "uri/1",
		},
		{
			name: "boundary between queue and id moves",
			a:    entity.RequestSummary{RequestID: "23", Queue: "q1"},
			b:    entity.RequestSummary{RequestID: "3", Queue: "q12"},
			aURI: "uri/1", bURI: "uri/1",
		},
		{
			name: "different changes of one request",
			a:    entity.RequestSummary{RequestID: "1", Queue: "q"},
			b:    entity.RequestSummary{RequestID: "1", Queue: "q"},
			aURI: "uri/1", bURI: "uri/2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEqual(t, changeSubject(tt.a, tt.aURI), changeSubject(tt.b, tt.bURI))
			assert.NotContains(t, changeSubject(tt.a, tt.aURI), "/")
		})
	}
}

func eventIDs(events []*basehook.HookEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.GetId())
	}
	return ids
}
