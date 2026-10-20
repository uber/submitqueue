// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entity

// QueueRequestStatus is a membership record filing one in-queue request under its public
// status in its queue, so a queue's requests can be listed by status without a secondary
// index on the request summary.
//
// The (Queue, Status, RequestID) triple is the record's identity. A request changes status
// by a record appearing under the new status before the record under the old status
// disappears; a request that reaches a terminal status keeps no record.
//
// Records are advisory. The authoritative status is the Status of the RequestSummary
// identified by RequestID; a record may transiently, or after an interrupted write, file a
// request under a status it has already left. Readers load each listed request's summary
// and classify it by the summary's own Status.
type QueueRequestStatus struct {
	// Queue is the name of the queue the request belongs to. Queue name is defined in the
	// configuration and should be unique within the system.
	Queue string

	// Status is the public status this record files the request under. Always one of
	// InQueueRequestStatuses(). Advisory: the summary's Status may differ transiently.
	Status RequestStatus

	// RequestID is the request's identifier, unique within Queue.
	RequestID string

	// ReceivedAtMs is when the gateway received the request, in Unix milliseconds.
	ReceivedAtMs int64

	// EnteredAtMs is the timestamp of the status log that moved the request into Status, in
	// Unix milliseconds. Re-filing the request under the same status keeps it.
	EnteredAtMs int64
}

// InQueueRequestStatuses returns every public status a request holds while it is in its
// queue: every status except the internal accepting status and the terminal statuses. Each
// call returns a fresh slice.
func InQueueRequestStatuses() []RequestStatus {
	return []RequestStatus{
		RequestStatusAccepted,
		RequestStatusStarted,
		RequestStatusValidating,
		RequestStatusValidated,
		RequestStatusBatching,
		RequestStatusBatched,
		RequestStatusSpeculating,
		RequestStatusSpeculated,
		RequestStatusLanding,
		RequestStatusCancelling,
	}
}

// IsRequestStatusInQueue reports whether status is one of InQueueRequestStatuses().
func IsRequestStatusInQueue(status RequestStatus) bool {
	switch status {
	case RequestStatusAccepted, RequestStatusStarted, RequestStatusValidating, RequestStatusValidated,
		RequestStatusBatching, RequestStatusBatched, RequestStatusSpeculating, RequestStatusSpeculated,
		RequestStatusLanding, RequestStatusCancelling:
		return true
	default:
		return false
	}
}
