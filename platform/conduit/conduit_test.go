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

package conduit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	phttp "github.com/uber/submitqueue/platform/http"
)

const testToken = "api-test-token"

// receivedCall is what the fake Conduit server saw.
type receivedCall struct {
	method      string
	path        string
	contentType string
	output      string
	params      map[string]any
}

// fakeConduit records every call it receives and answers each with one reply.
type fakeConduit struct {
	mu    sync.Mutex
	calls []receivedCall
}

func (f *fakeConduit) received() []receivedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]receivedCall(nil), f.calls...)
}

func newTestClient(t *testing.T, status int, body string) (*Client, *fakeConduit) {
	t.Helper()
	fake := &fakeConduit{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.NoError(t, r.ParseForm()) {
			return
		}
		var params map[string]any
		_ = json.Unmarshal([]byte(r.PostForm.Get("params")), &params)
		fake.mu.Lock()
		fake.calls = append(fake.calls, receivedCall{
			method:      r.Method,
			path:        r.URL.Path,
			contentType: r.Header.Get("Content-Type"),
			output:      r.PostForm.Get("output"),
			params:      params,
		})
		fake.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	httpClient, err := phttp.NewClient(server.URL)
	require.NoError(t, err)
	return NewClient(httpClient, testToken), fake
}

func TestClient_Call_SendsParamsWithToken(t *testing.T) {
	tests := []struct {
		name       string
		params     any
		wantParams map[string]any
	}{
		{
			name:   "map params",
			params: map[string]any{"ids": []int{72558007}},
			wantParams: map[string]any{
				"ids":         []any{float64(72558007)},
				"__conduit__": map[string]any{"token": testToken},
			},
		},
		{
			name: "struct params",
			params: struct {
				Constraints map[string][]int `json:"constraints"`
			}{Constraints: map[string][]int{"ids": {30026137}}},
			wantParams: map[string]any{
				"constraints": map[string]any{"ids": []any{float64(30026137)}},
				"__conduit__": map[string]any{"token": testToken},
			},
		},
		{
			name:       "nil params send only the token",
			wantParams: map[string]any{"__conduit__": map[string]any{"token": testToken}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, fake := newTestClient(t, http.StatusOK, `{"result": {}, "error_code": null, "error_info": null}`)

			require.NoError(t, client.Call(context.Background(), "differential.querydiffs", tt.params, nil))

			calls := fake.received()
			require.Len(t, calls, 1)
			call := calls[0]
			assert.Equal(t, http.MethodPost, call.method)
			assert.Equal(t, "/api/differential.querydiffs", call.path)
			assert.Equal(t, "application/x-www-form-urlencoded", call.contentType)
			assert.Equal(t, "json", call.output)
			assert.Equal(t, tt.wantParams, call.params)
		})
	}
}

func TestClient_Call_RejectsNonObjectParams(t *testing.T) {
	tests := []struct {
		name   string
		params any
	}{
		{name: "array", params: []int{1, 2}},
		{name: "string", params: "D123"},
		{name: "number", params: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, fake := newTestClient(t, http.StatusOK, `{"result": {}}`)

			require.Error(t, client.Call(context.Background(), "differential.querydiffs", tt.params, nil))
			assert.Empty(t, fake.received(), "a request with malformed params must not be sent")
		})
	}
}

func TestClient_Call_DecodesResponse(t *testing.T) {
	type diff struct {
		ID string `json:"id"`
	}

	tests := []struct {
		name          string
		status        int
		body          string
		want          map[string]diff
		wantErr       bool
		wantStatus    int
		wantErrorCode string
	}{
		{
			name:   "object result",
			status: http.StatusOK,
			body:   `{"result": {"72558007": {"id": "72558007"}}, "error_code": null, "error_info": null}`,
			want:   map[string]diff{"72558007": {ID: "72558007"}},
		},
		{
			name:   "empty array result is an empty map",
			status: http.StatusOK,
			body:   `{"result": [], "error_code": null, "error_info": null}`,
		},
		{
			name:   "null result",
			status: http.StatusOK,
			body:   `{"result": null, "error_code": null, "error_info": null}`,
		},
		{
			name:          "invalid token is a conduit error, not a status error",
			status:        http.StatusOK,
			body:          `{"result": null, "error_code": "ERR-INVALID-SESSION", "error_info": "Session key is not present."}`,
			wantErr:       true,
			wantErrorCode: "ERR-INVALID-SESSION",
		},
		{
			name:          "conduit error without info",
			status:        http.StatusOK,
			body:          `{"result": null, "error_code": "ERR-CONDUIT-CORE"}`,
			wantErr:       true,
			wantErrorCode: "ERR-CONDUIT-CORE",
		},
		{
			name:       "non-200 reply is a status error",
			status:     http.StatusBadGateway,
			body:       `upstream unavailable`,
			wantErr:    true,
			wantStatus: http.StatusBadGateway,
		},
		{
			name:    "non-JSON body",
			status:  http.StatusOK,
			body:    `<html>maintenance</html>`,
			wantErr: true,
		},
		{
			name:    "result of the wrong shape",
			status:  http.StatusOK,
			body:    `{"result": "72558007", "error_code": null}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := newTestClient(t, tt.status, tt.body)

			var got map[string]diff
			err := client.Call(context.Background(), "differential.querydiffs", map[string]any{"ids": []int{72558007}}, &got)

			if !tt.wantErr {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
				return
			}
			require.Error(t, err)

			var statusErr *phttp.StatusError
			if tt.wantStatus != 0 {
				require.True(t, errors.As(err, &statusErr))
				assert.Equal(t, tt.wantStatus, statusErr.StatusCode)
			} else {
				assert.False(t, errors.As(err, &statusErr))
			}

			var conduitErr *Error
			if tt.wantErrorCode != "" {
				require.True(t, errors.As(err, &conduitErr))
				assert.Equal(t, tt.wantErrorCode, conduitErr.Code)
			} else {
				assert.False(t, errors.As(err, &conduitErr))
			}
		})
	}
}

func TestClient_Call_HonorsContext(t *testing.T) {
	client, fake := newTestClient(t, http.StatusOK, `{"result": {}}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.Call(ctx, "conduit.ping", nil, nil)

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, fake.received())
}
