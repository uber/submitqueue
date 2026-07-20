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

// RequestBatch is the immutable assignment of one request to one batch.
// RequestID is unique, so retries resolve the same BatchID instead of creating
// another logical batch for the request.
type RequestBatch struct {
	// RequestID is the globally unique request identifier.
	RequestID string
	// BatchID is the globally unique batch identifier assigned to the request.
	BatchID string
	// Version is the version of the assignment. Immutable assignments start at 1.
	Version int32
}
