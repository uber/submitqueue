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
	"fmt"
	"net"
	nethttp "net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	phttp "github.com/uber/submitqueue/platform/http"
)

func TestClassifier_StatusCodes(t *testing.T) {
	tests := []struct {
		name string
		code int
		want errs.Verdict
	}{
		// Server state: changes without us doing anything differently.
		{"bad gateway", nethttp.StatusBadGateway, errs.InfraDependencyRetryable},
		{"service unavailable", nethttp.StatusServiceUnavailable, errs.InfraDependencyRetryable},
		{"gateway timeout", nethttp.StatusGatewayTimeout, errs.InfraDependencyRetryable},
		{"internal server error", nethttp.StatusInternalServerError, errs.InfraDependencyRetryable},
		{"unassigned 5xx", 599, errs.InfraDependencyRetryable},
		{"request timeout", nethttp.StatusRequestTimeout, errs.InfraDependencyRetryable},
		{"too many requests", nethttp.StatusTooManyRequests, errs.InfraDependencyRetryable},

		// Verdicts on the request: replaying reproduces the same answer.
		{"not implemented", nethttp.StatusNotImplemented, errs.InfraDependency},
		{"http version not supported", nethttp.StatusHTTPVersionNotSupported, errs.InfraDependency},
		{"bad request", nethttp.StatusBadRequest, errs.InfraDependency},
		{"unauthorized", nethttp.StatusUnauthorized, errs.InfraDependency},
		{"forbidden", nethttp.StatusForbidden, errs.InfraDependency},
		{"not found", nethttp.StatusNotFound, errs.InfraDependency},
		{"unprocessable entity", nethttp.StatusUnprocessableEntity, errs.InfraDependency},
		{"unfollowed redirect", nethttp.StatusFound, errs.InfraDependency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(phttp.NewStatusError(tt.code, nil)))
		})
	}
}

func TestClassifier_TransportFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want errs.Verdict
	}{
		{
			name: "connection refused",
			err:  &url.Error{Op: "Get", URL: "http://api.example", Err: errors.New("connection refused")},
			want: errs.InfraDependencyRetryable,
		},
		{
			name: "dns failure",
			err:  &url.Error{Op: "Get", URL: "http://api.example", Err: &net.DNSError{Err: "no such host"}},
			want: errs.InfraDependencyRetryable,
		},
		{
			// Ours, not theirs: declining lets the walk reach context.Canceled,
			// where the generic classifier calls it plain retryable infra.
			name: "context cancelled",
			err:  &url.Error{Op: "Get", URL: "http://api.example", Err: context.Canceled},
			want: errs.Unknown,
		},
		{
			name: "context deadline exceeded",
			err:  &url.Error{Op: "Get", URL: "http://api.example", Err: context.DeadlineExceeded},
			want: errs.Unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_Unknown(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		// Per-node contract: a wrapped StatusError must not match here. The
		// classifier-processor walk reaches the inner node and asks again there.
		{"wrapped status error", fmt.Errorf("get build x: %w", phttp.NewStatusError(502, nil))},
		{"plain error", errors.New("anything")},
		{"bare context.Canceled", context.Canceled},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, errs.Unknown, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_AppliedViaProcessor(t *testing.T) {
	// The order services wire: generic first, this one before mysqlerrs.
	processor := errs.NewClassifierProcessor(genericerrs.Classifier, Classifier, mysqlerrs.Classifier)

	t.Run("wrapped 502 becomes a retryable dependency error", func(t *testing.T) {
		err := fmt.Errorf("get build org/pipeline/builds/1: %w", phttp.NewStatusError(nethttp.StatusBadGateway, []byte("proxy forward failed")))
		out := processor.Process(err)
		assert.True(t, errs.IsRetryable(out))
		assert.True(t, errs.IsDependencyError(out))
	})

	t.Run("wrapped 404 stays non-retryable", func(t *testing.T) {
		err := fmt.Errorf("get build org/pipeline/builds/1: %w", phttp.NewStatusError(nethttp.StatusNotFound, nil))
		out := processor.Process(err)
		assert.False(t, errs.IsRetryable(out))
		assert.True(t, errs.IsDependencyError(out))
	})

	t.Run("transport failure is attributed to the dependency not mysql", func(t *testing.T) {
		// mysqlerrs calls any net.Error retryable infra, and *url.Error is one,
		// so it would claim this node and drop the dependency attribution if it
		// were listed first.
		err := fmt.Errorf("send: %w", &url.Error{Op: "Get", URL: "http://api.example", Err: errors.New("connection reset by peer")})
		out := processor.Process(err)
		assert.True(t, errs.IsRetryable(out))
		assert.True(t, errs.IsDependencyError(out), "should be attributed to the HTTP dependency")
	})

	t.Run("our cancellation is retryable but not a dependency failure", func(t *testing.T) {
		err := fmt.Errorf("send: %w", &url.Error{Op: "Get", URL: "http://api.example", Err: context.Canceled})
		out := processor.Process(err)
		assert.True(t, errs.IsRetryable(out))
		assert.False(t, errs.IsDependencyError(out))
	})

	t.Run("a controller verdict wins over the classifier", func(t *testing.T) {
		// Pass 1 of the processor short-circuits on the existing framework wrap,
		// so a 502 a controller decided was fatal stays fatal.
		err := errs.NewDependencyError(phttp.NewStatusError(nethttp.StatusBadGateway, nil))
		out := processor.Process(err)
		assert.Same(t, err, out)
		assert.False(t, errs.IsRetryable(out))
	})
}
