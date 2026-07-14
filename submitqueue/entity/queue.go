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

import "encoding/json"

// QueueID is the queue-message payload for queue-scoped pipeline stages. It
// carries only the queue name.
type QueueID struct {
	// Name is the merge-queue name the message targets.
	Name string `json:"name"`
}

// ToBytes serializes the QueueID to JSON bytes for queue message payload.
func (q QueueID) ToBytes() ([]byte, error) {
	return json.Marshal(q)
}

// QueueIDFromBytes deserializes a QueueID from JSON bytes.
func QueueIDFromBytes(data []byte) (QueueID, error) {
	var qid QueueID
	err := json.Unmarshal(data, &qid)
	return qid, err
}
