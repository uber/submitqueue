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

package controller

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func explicitListWindow(lower, upper int64) entity.ListRequest {
	return entity.ListRequest{
		Queue: "monorepo/main", AcceptedAtOrAfterMs: lower, HasAcceptedAtOrAfterMs: true,
		AcceptedBeforeMs: upper, HasAcceptedBeforeMs: true,
	}
}

func TestResolveListRangeFirstPage(t *testing.T) {
	for _, tt := range []struct {
		name         string
		request      entity.ListRequest
		lower, upper int64
		limit        int
		clockCalls   int
	}{
		{"omitted bounds", entity.ListRequest{Queue: "monorepo/main"}, 0, 1000, 51, 1},
		{"lower only", entity.ListRequest{AcceptedAtOrAfterMs: 100, HasAcceptedAtOrAfterMs: true}, 100, 1000, 51, 1},
		{"upper only", entity.ListRequest{AcceptedBeforeMs: 2000, HasAcceptedBeforeMs: true}, 0, 2000, 51, 0},
		{"explicit window", explicitListWindow(100, 900), 100, 900, 51, 0},
		{"explicit zero lower", explicitListWindow(0, 900), 0, 900, 51, 0},
		{"future window", explicitListWindow(2000, 3000), 2000, 3000, 51, 0},
		{"one row", entity.ListRequest{PageSize: 1}, 0, 1000, 2, 1},
		{"maximum page", entity.ListRequest{PageSize: 200}, 0, 1000, 201, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clockCalls := 0
			controller := listController{now: func() time.Time {
				clockCalls++
				return time.UnixMilli(1000)
			}}
			query, err := controller.resolveListRange(tt.request)
			require.NoError(t, err)
			require.Equal(t, storage.RequestAcceptanceRange{
				AcceptedAtOrAfterMs: tt.lower, AcceptedBeforeMs: tt.upper, Limit: tt.limit,
			}, query)
			require.Equal(t, tt.clockCalls, clockCalls)
		})
	}
}

func TestResolveListRangeRejectsInvalidRequest(t *testing.T) {
	for _, tt := range []struct {
		name    string
		request entity.ListRequest
	}{
		{"negative lower", explicitListWindow(-1, 1000)},
		{"explicit zero upper", explicitListWindow(0, 0)},
		{"negative upper", explicitListWindow(0, -1)},
		{"equal bounds", explicitListWindow(1000, 1000)},
		{"inverted bounds", explicitListWindow(2000, 1000)},
		{"lower at default upper", entity.ListRequest{AcceptedAtOrAfterMs: 1000, HasAcceptedAtOrAfterMs: true}},
		{"negative page size", entity.ListRequest{PageSize: -1}},
		{"oversized page", entity.ListRequest{PageSize: 201}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			controller := listController{now: func() time.Time { return time.UnixMilli(1000) }}
			_, err := controller.resolveListRange(tt.request)
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func validListToken() listPageToken {
	return listPageToken{
		Version: listPageTokenVersion, Queue: "monorepo/main", AcceptedAtOrAfterMs: 0, AcceptedBeforeMs: 1000,
		LastAcceptedAtMs: 800, LastRequestID: "request/monorepo/main/9",
	}
}

func mustEncodeListToken(t *testing.T, token listPageToken) string {
	t.Helper()
	encoded, err := encodeListPageToken(token)
	require.NoError(t, err)
	return encoded
}

func TestResolveListRangeContinuation(t *testing.T) {
	token := validListToken()
	encoded := mustEncodeListToken(t, token)
	for _, tt := range []struct {
		name    string
		request entity.ListRequest
		limit   int
	}{
		{"bounds omitted", entity.ListRequest{Queue: token.Queue}, 51},
		{"explicit matching bounds", explicitListWindow(0, 1000), 51},
		{"explicit matching lower", entity.ListRequest{Queue: token.Queue, HasAcceptedAtOrAfterMs: true}, 51},
		{"explicit matching upper", entity.ListRequest{Queue: token.Queue, HasAcceptedBeforeMs: true, AcceptedBeforeMs: 1000}, 51},
		{"changed page size", entity.ListRequest{Queue: token.Queue, PageSize: 200}, 201},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.request
			req.PageToken = encoded
			clockCalls := 0
			controller := listController{now: func() time.Time {
				clockCalls++
				return time.UnixMilli(5000)
			}}
			query, err := controller.resolveListRange(req)
			require.NoError(t, err)
			require.Zero(t, clockCalls)
			require.Equal(t, storage.RequestAcceptanceRange{
				AcceptedAtOrAfterMs: 0, AcceptedBeforeMs: 1000, Limit: tt.limit,
				Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 800, RequestID: token.LastRequestID},
			}, query)
		})
	}
}

func TestResolveListRangeRejectsMismatchedContinuation(t *testing.T) {
	for _, tt := range []struct {
		name       string
		request    entity.ListRequest
		tokenLower int64
	}{
		{"queue", entity.ListRequest{Queue: "other/main"}, 0},
		{"lower", explicitListWindow(1, 1000), 0},
		{"upper", explicitListWindow(0, 1001), 0},
		{"explicit zero lower", explicitListWindow(0, 1000), 100},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.request
			token := validListToken()
			token.AcceptedAtOrAfterMs = tt.tokenLower
			req.PageToken = mustEncodeListToken(t, token)
			controller := listController{}
			_, err := controller.resolveListRange(req)
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func TestResolveListRangePreservesCursorBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		lower, upper, timestamp int64
		requestID               string
	}{
		{"inclusive lower", 800, 1000, 800, "request/monorepo/main/9"},
		{"opaque bytewise ID", 0, 1000, 800, "request/monorepo/main/é<&> "},
		{"large integer timestamps", 1<<63 - 100, 1<<63 - 1, 1<<63 - 2, "request/monorepo/main/9"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token := listPageToken{
				Version: listPageTokenVersion, Queue: "monorepo/main",
				AcceptedAtOrAfterMs: tt.lower, AcceptedBeforeMs: tt.upper,
				LastAcceptedAtMs: tt.timestamp, LastRequestID: tt.requestID,
			}
			controller := listController{}
			query, err := controller.resolveListRange(entity.ListRequest{Queue: token.Queue, PageToken: mustEncodeListToken(t, token)})
			require.NoError(t, err)
			require.Equal(t, storage.RequestAcceptanceRange{
				AcceptedAtOrAfterMs: tt.lower, AcceptedBeforeMs: tt.upper, Limit: 51,
				Before: storage.RequestAcceptanceCursor{AcceptedAtMs: tt.timestamp, RequestID: tt.requestID},
			}, query)
		})
	}
}

func TestDecodeListPageTokenRejectsMalformedInput(t *testing.T) {
	for _, contents := range []string{"not-json", "null", "{}", "[]", `{"version":"1"}`} {
		t.Run(contents, func(t *testing.T) {
			_, err := decodeListPageToken(base64.RawURLEncoding.EncodeToString([]byte(contents)))
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
	_, err := decodeListPageToken("%%%")
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestDecodeListPageTokenRejectsInvalidFields(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*listPageToken)
	}{
		{"unknown version", func(token *listPageToken) { token.Version++ }},
		{"missing queue", func(token *listPageToken) { token.Queue = "" }},
		{"missing ID", func(token *listPageToken) { token.LastRequestID = "" }},
		{"negative lower", func(token *listPageToken) { token.AcceptedAtOrAfterMs = -1 }},
		{"inverted window", func(token *listPageToken) { token.AcceptedBeforeMs = -1 }},
		{"unknown acceptance time", func(token *listPageToken) { token.LastAcceptedAtMs = 0 }},
		{"cursor below window", func(token *listPageToken) { token.AcceptedAtOrAfterMs = 900 }},
		{"cursor at upper", func(token *listPageToken) { token.LastAcceptedAtMs = 1000 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token := validListToken()
			tt.change(&token)
			_, err := decodeListPageToken(mustEncodeListToken(t, token))
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}
