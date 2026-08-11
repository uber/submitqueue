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

package entity

import "github.com/uber/submitqueue/platform/base/failure"

// Subject types name what a failure is about. The queue layer treats the type
// as opaque; these are SubmitQueue's own vocabulary for it.
const (
	// SubjectTypeBatch identifies a subject by batch ID.
	SubjectTypeBatch = "batch"
	// SubjectTypeQueue identifies a subject by queue name. Used where no single
	// batch is at fault — a failure reading or planning the queue as a whole.
	SubjectTypeQueue = "queue"
	// SubjectTypeRequest identifies a subject by request ID.
	SubjectTypeRequest = "request"
)

// BatchSubject names a batch as what a failure is about.
func BatchSubject(batchID string) failure.Subject {
	return failure.Subject{Type: SubjectTypeBatch, ID: batchID}
}

// QueueSubject names a queue as what a failure is about. It is the honest
// subject for work that spans the queue — listing it, planning it — where
// blaming any one batch would be a guess.
func QueueSubject(queue string) failure.Subject {
	return failure.Subject{Type: SubjectTypeQueue, ID: queue}
}

// RequestSubject names a request as what a failure is about.
func RequestSubject(requestID string) failure.Subject {
	return failure.Subject{Type: SubjectTypeRequest, ID: requestID}
}
