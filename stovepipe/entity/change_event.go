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

	entitygit "github.com/uber/submitqueue/stovepipe/entity/git"
)

// ChangeEvent represents a single new trunk change entering the pipeline, published to the
// start topic. A trunk change is one commit, so the event carries one git-backed URI. It is
// source-agnostic: both the webhook and the reconciliation poller emit it. Additional fields
// (e.g. source, committer time) can be added later as ingestion needs them.
type ChangeEvent struct {
	// URI identifies the commit that entered the pipeline (git://owner/repo/branch/revision).
	URI string `json:"uri"`
}

// ToBytes serializes the ChangeEvent to JSON bytes for queue message payload.
func (e ChangeEvent) ToBytes() ([]byte, error) {
	return json.Marshal(e)
}

// Validate checks that the change event carries a valid git-backed commit URI.
func (e ChangeEvent) Validate() error {
	if e.URI == "" {
		return fmt.Errorf("change event requires a commit URI")
	}
	if _, err := entitygit.ParseChangeID(e.URI); err != nil {
		return fmt.Errorf("change event URI: %w", err)
	}
	return nil
}

// ChangeEventFromBytes deserializes a ChangeEvent from JSON bytes.
func ChangeEventFromBytes(data []byte) (ChangeEvent, error) {
	var event ChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return ChangeEvent{}, err
	}
	if err := event.Validate(); err != nil {
		return ChangeEvent{}, err
	}
	return event, nil
}
