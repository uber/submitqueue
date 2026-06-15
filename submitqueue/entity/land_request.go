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

	"github.com/uber/submitqueue/entity/change"
	"github.com/uber/submitqueue/entity/mergestrategy"
)

// LandRequest represents the gateway-owned fields of a land request sent over the queue
// to the orchestrator. It contains only the validated inputs and generated ID — the orchestrator
// is responsible for constructing the full Request entity with state machine fields.
type LandRequest struct {
	// ID is the globally unique identifier for the land request. Format: "<queue>/<counter_value>".
	ID string `json:"id"`
	// Queue is the name of the queue processing the land request.
	Queue string `json:"queue"`
	// Change is the set of code changes to land.
	Change change.Change `json:"change"`
	// LandStrategy is the source control integration strategy to use for this land operation.
	LandStrategy mergestrategy.MergeStrategy `json:"land_strategy"`
}

// ToBytes serializes the LandRequest to JSON bytes for queue message payload.
func (r LandRequest) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// LandRequestFromBytes deserializes a LandRequest from JSON bytes.
func LandRequestFromBytes(data []byte) (LandRequest, error) {
	var req LandRequest
	err := json.Unmarshal(data, &req)
	return req, err
}
