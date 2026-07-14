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

// IngestRequest represents the validated inputs of an ingest RPC call.
// The controller resolves the queue's head commit and mints a request ID internally.
type IngestRequest struct {
	// Queue is the name of the queue whose head commit should be ingested.
	Queue string
}

// IngestResult is the outcome of a successful ingest operation.
type IngestResult struct {
	// ID is the globally unique request identifier assigned to the ingested commit.
	// Format: "request/<queue>/<counter_value>".
	ID string
}
