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

package github

import "github.com/uber/submitqueue/platform/errs"

// Classifier implements errs.Classifier for this merger's own sentinel: an
// ErrMergePending is a retryable dependency failure, since GitHub is still
// working and a redelivery converges on the same merge. HTTP status and
// transport failures are left to platform/errs/http.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

// Classify inspects a single node; the classifier-processor walks the chain.
func (classifier) Classify(err error) errs.Verdict {
	if err == ErrMergePending {
		return errs.InfraDependencyRetryable
	}
	return errs.Unknown
}
