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

import (
	"encoding/json"
	"fmt"

	"github.com/uber/submitqueue/platform/base/change"
)

// IngestRequest is the gateway-owned contract for a Stovepipe ingest request as it
// travels over the queue to the orchestrator. It carries the validated inputs and the
// generated request ID (the "spid"); downstream stages resolve any further detail.
type IngestRequest struct {
	// ID is the globally unique identifier for the ingest request (the "spid").
	// Format: "<queue>/<counter_value>".
	ID string `json:"id"`
	// Queue is the name of the queue processing the ingest request.
	Queue string `json:"queue"`
	// Change is the set of trunk commits to verify, identified by URI. The scheme names
	// the VCS; the rest is provider-specific (e.g. git://remote/repo/ref/commit_sha).
	Change change.Change `json:"change"`
}

// ToBytes serializes the IngestRequest to JSON bytes for queue message payload.
func (r IngestRequest) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// Validate checks that the request carries the identity and change URIs the pipeline
// needs to act on it. Selecting a resolver for each URI's scheme is the resolver/wiring
// layer's job, so Validate stays VCS-agnostic and does not inspect the scheme.
func (r IngestRequest) Validate() error {
	if r.ID == "" {
		return fmt.Errorf("ingest request requires an id")
	}
	if r.Queue == "" {
		return fmt.Errorf("ingest request requires a queue")
	}
	if len(r.Change.URIs) == 0 {
		return fmt.Errorf("ingest request requires at least one change URI")
	}
	for _, uri := range r.Change.URIs {
		if uri == "" {
			return fmt.Errorf("ingest request change URIs must be non-empty")
		}
	}
	return nil
}

// IngestRequestFromBytes deserializes an IngestRequest from JSON bytes and validates it.
func IngestRequestFromBytes(data []byte) (IngestRequest, error) {
	var req IngestRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return IngestRequest{}, err
	}
	if err := req.Validate(); err != nil {
		return IngestRequest{}, err
	}
	return req, nil
}
