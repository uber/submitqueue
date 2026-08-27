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

// Package yarpc classifies YARPC status errors by code.
package yarpc

import (
	"github.com/uber/submitqueue/platform/errs"
	"go.uber.org/yarpc/yarpcerrors"
)

// Classifier is the canonical YARPC error classifier.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

// yarpcError is YARPC's single-node status contract for transport-specific
// errors. The classifier processor still owns traversal to reach this node.
type yarpcError interface {
	YARPCError() *yarpcerrors.Status
}

func (classifier) Classify(err error) errs.Verdict {
	var status *yarpcerrors.Status
	switch e := err.(type) {
	case *yarpcerrors.Status:
		status = e
	case yarpcError:
		status = e.YARPCError()
	default:
		return errs.Unknown
	}
	if status == nil {
		return errs.Unknown
	}

	switch status.Code() {
	case yarpcerrors.CodeCancelled:
		// Cancellation belongs to the caller's operation rather than to the
		// downstream service, matching generic's context.Canceled verdict.
		return errs.InfraRetryable

	case yarpcerrors.CodeUnknown,
		yarpcerrors.CodeDeadlineExceeded,
		yarpcerrors.CodeResourceExhausted,
		yarpcerrors.CodeAborted,
		yarpcerrors.CodeInternal,
		yarpcerrors.CodeUnavailable:
		return errs.InfraDependencyRetryable

	case yarpcerrors.CodeInvalidArgument,
		yarpcerrors.CodeNotFound,
		yarpcerrors.CodeAlreadyExists,
		yarpcerrors.CodePermissionDenied,
		yarpcerrors.CodeFailedPrecondition,
		yarpcerrors.CodeOutOfRange,
		yarpcerrors.CodeUnimplemented,
		yarpcerrors.CodeDataLoss,
		yarpcerrors.CodeUnauthenticated:
		return errs.InfraDependency

	default:
		return errs.Unknown
	}
}
