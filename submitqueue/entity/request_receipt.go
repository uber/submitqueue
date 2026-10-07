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

package entity

// RequestReceipt is an immutable lookup key for a publicly visible request.
type RequestReceipt struct {
	// Queue is the queue supplied at receipt.
	Queue string
	// ReceivedAtMs is the immutable, positive receipt timestamp in Unix milliseconds.
	ReceivedAtMs int64
	// RequestID identifies the request within Queue.
	RequestID string
}
