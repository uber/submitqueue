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

package yarpc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/platform/errs"
	"go.uber.org/yarpc/yarpcerrors"
)

type customYARPCError struct {
	status *yarpcerrors.Status
}

func (e customYARPCError) Error() string {
	return e.status.Error()
}

func (e customYARPCError) YARPCError() *yarpcerrors.Status {
	return e.status
}

func TestClassifier_StatusCodes(t *testing.T) {
	tests := []struct {
		name string
		code yarpcerrors.Code
		want errs.Verdict
	}{
		{name: "cancelled", code: yarpcerrors.CodeCancelled, want: errs.InfraRetryable},
		{name: "unknown", code: yarpcerrors.CodeUnknown, want: errs.InfraDependencyRetryable},
		{name: "deadline exceeded", code: yarpcerrors.CodeDeadlineExceeded, want: errs.InfraDependencyRetryable},
		{name: "resource exhausted", code: yarpcerrors.CodeResourceExhausted, want: errs.InfraDependencyRetryable},
		{name: "aborted", code: yarpcerrors.CodeAborted, want: errs.InfraDependencyRetryable},
		{name: "internal", code: yarpcerrors.CodeInternal, want: errs.InfraDependencyRetryable},
		{name: "unavailable", code: yarpcerrors.CodeUnavailable, want: errs.InfraDependencyRetryable},
		{name: "invalid argument", code: yarpcerrors.CodeInvalidArgument, want: errs.InfraDependency},
		{name: "not found", code: yarpcerrors.CodeNotFound, want: errs.InfraDependency},
		{name: "already exists", code: yarpcerrors.CodeAlreadyExists, want: errs.InfraDependency},
		{name: "permission denied", code: yarpcerrors.CodePermissionDenied, want: errs.InfraDependency},
		{name: "failed precondition", code: yarpcerrors.CodeFailedPrecondition, want: errs.InfraDependency},
		{name: "out of range", code: yarpcerrors.CodeOutOfRange, want: errs.InfraDependency},
		{name: "unimplemented", code: yarpcerrors.CodeUnimplemented, want: errs.InfraDependency},
		{name: "data loss", code: yarpcerrors.CodeDataLoss, want: errs.InfraDependency},
		{name: "unauthenticated", code: yarpcerrors.CodeUnauthenticated, want: errs.InfraDependency},
		{name: "ok", code: yarpcerrors.CodeOK, want: errs.Unknown},
		{name: "unrecognized code", code: yarpcerrors.Code(99), want: errs.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(yarpcerrors.Newf(tt.code, "rpc failed")))
		})
	}
}

func TestClassifier_Unknown(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "wrapped status", err: fmt.Errorf("call failed: %w", yarpcerrors.DeadlineExceededErrorf("late"))},
		{name: "plain error", err: errors.New("anything")},
		{name: "nil", err: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, errs.Unknown, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_TransportSpecificYARPCError(t *testing.T) {
	err := customYARPCError{status: yarpcerrors.Newf(yarpcerrors.CodeUnavailable, "down")}
	assert.Equal(t, errs.InfraDependencyRetryable, Classifier.Classify(err))
}

func TestClassifier_AppliedViaProcessor(t *testing.T) {
	processor := errs.NewClassifierProcessor(Classifier)

	t.Run("wrapped deadline is a retryable dependency error", func(t *testing.T) {
		err := fmt.Errorf("set ref: %w", yarpcerrors.DeadlineExceededErrorf("context deadline exceeded"))
		out := processor.Process(err)
		assert.True(t, errs.IsRetryable(out))
		assert.True(t, errs.IsDependencyError(out))
	})

	t.Run("wrapped invalid argument is a non-retryable dependency error", func(t *testing.T) {
		err := fmt.Errorf("set ref: %w", yarpcerrors.InvalidArgumentErrorf("bad ref"))
		out := processor.Process(err)
		assert.False(t, errs.IsRetryable(out))
		assert.True(t, errs.IsDependencyError(out))
	})

	t.Run("cancelled is retryable without dependency attribution", func(t *testing.T) {
		out := processor.Process(yarpcerrors.CancelledErrorf("caller cancelled"))
		assert.True(t, errs.IsRetryable(out))
		assert.False(t, errs.IsDependencyError(out))
	})

	t.Run("a controller verdict wins over the classifier", func(t *testing.T) {
		err := errs.NewDependencyError(yarpcerrors.UnavailableErrorf("down"))
		out := processor.Process(err)
		assert.Same(t, err, out)
		assert.False(t, errs.IsRetryable(out))
	})
}
