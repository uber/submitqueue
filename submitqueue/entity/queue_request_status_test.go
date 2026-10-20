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

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestInQueueRequestStatuses pins the vocabulary split: every status is in the queue,
// terminal, the internal accepting status, or the unknown sentinel — never more than one.
func TestInQueueRequestStatuses(t *testing.T) {
	tests := []struct {
		status  RequestStatus
		inQueue bool
	}{
		{status: RequestStatusUnknown},
		{status: RequestStatusAccepting},
		{status: RequestStatusAccepted, inQueue: true},
		{status: RequestStatusStarted, inQueue: true},
		{status: RequestStatusValidating, inQueue: true},
		{status: RequestStatusValidated, inQueue: true},
		{status: RequestStatusBatching, inQueue: true},
		{status: RequestStatusBatched, inQueue: true},
		{status: RequestStatusSpeculating, inQueue: true},
		{status: RequestStatusSpeculated, inQueue: true},
		{status: RequestStatusLanding, inQueue: true},
		{status: RequestStatusCancelling, inQueue: true},
		{status: RequestStatusLanded},
		{status: RequestStatusError},
		{status: RequestStatusCancelled},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			assert.Equal(t, tt.inQueue, slices.Contains(InQueueRequestStatuses(), tt.status))
			assert.Equal(t, tt.inQueue, IsRequestStatusInQueue(tt.status))
			if tt.inQueue {
				assert.False(t, IsRequestStateTerminal(RequestState(tt.status)), "an in-queue status is never terminal")
			}
		})
	}
}
