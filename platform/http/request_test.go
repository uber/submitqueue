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

package http

import (
	"context"
	"errors"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendRequest_ReturnsStatusAndBody(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(nethttp.StatusTeapot)
		_, _ = w.Write([]byte("hello"))
	}))
	t.Cleanup(srv.Close)

	status, body, err := SendRequest(context.Background(), srv.Client(), nethttp.MethodGet, srv.URL, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, nethttp.StatusTeapot, status)
	assert.Equal(t, "hello", string(body))
}

func TestSendRequest_SendsBody(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(nethttp.HandlerFunc(func(_ nethttp.ResponseWriter, r *nethttp.Request) {
		gotBody, _ = io.ReadAll(r.Body)
	}))
	t.Cleanup(srv.Close)

	_, _, err := SendRequest(context.Background(), srv.Client(), nethttp.MethodPost, srv.URL, []byte(`{"a":1}`), nil)
	require.NoError(t, err)
	assert.Equal(t, `{"a":1}`, string(gotBody))
}

func TestSendRequest_AppliesSetHeaders(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(_ nethttp.ResponseWriter, r *nethttp.Request) {
		gotHeader = r.Header.Get("X-Test")
	}))
	t.Cleanup(srv.Close)

	_, _, err := SendRequest(context.Background(), srv.Client(), nethttp.MethodGet, srv.URL, nil, func(req *nethttp.Request) {
		req.Header.Set("X-Test", "value")
	})
	require.NoError(t, err)
	assert.Equal(t, "value", gotHeader)
}

func TestSendRequest_InvalidMethod_ReturnsError(t *testing.T) {
	_, _, err := SendRequest(context.Background(), nethttp.DefaultClient, "invalid method", "http://example.com", nil, nil)
	require.Error(t, err)
}

func TestSendRequest_SendError_ReturnsError(t *testing.T) {
	client := &nethttp.Client{Transport: roundTripFunc(func(*nethttp.Request) (*nethttp.Response, error) {
		return nil, errors.New("boom")
	})}

	_, _, err := SendRequest(context.Background(), client, nethttp.MethodGet, "http://example.com", nil, nil)
	require.Error(t, err)
}
