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
	"net/url"
)

// ChangeEvent represents a change in the pipeline. URI represents the identity
// of the change.
//
// The repository-scoped ordering key is carried on the queue message envelope
// (Message.PartitionKey), stamped once at ingestion and propagated unchanged
// across stages, so it is deliberately not duplicated here.
type ChangeEvent struct {
	// URI represents the identity of the change. The scheme names the VCS; the rest
	// is provider-specific (e.g. git://remote/repo/ref/commit_sha).
	URI string `json:"uri"`
}

// ToBytes serializes the ChangeEvent to JSON bytes for queue message payload.
func (e ChangeEvent) ToBytes() ([]byte, error) {
	return json.Marshal(e)
}

// Scheme returns the URI scheme identifying the VCS (e.g. "git"), or "" if the
// URI is empty or not scheme-qualified. It is the key used to select a resolver.
func (e ChangeEvent) Scheme() string {
	u, err := url.Parse(e.URI)
	if err != nil {
		return ""
	}
	return u.Scheme
}

// Validate checks that the change event carries a scheme-qualified commit URI.
func (e ChangeEvent) Validate() error {
	if e.URI == "" {
		return fmt.Errorf("change event requires a commit URI")
	}
	if e.Scheme() == "" {
		return fmt.Errorf("change event URI %q must be scheme-qualified", e.URI)
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
