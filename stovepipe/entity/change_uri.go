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

// ChangeURI is the lightweight reference passed between pipeline stages. It
// carries only the change identity; stages re-resolve any state they need.
type ChangeURI struct {
	// URI is the change identity (git://owner/repo/branch/revision).
	URI string `json:"uri"`
}

// ToBytes serializes the ChangeURI to JSON bytes for a queue message payload.
func (c ChangeURI) ToBytes() ([]byte, error) {
	return json.Marshal(c)
}

// ChangeURIFromBytes deserializes a ChangeURI from JSON bytes.
func ChangeURIFromBytes(data []byte) (ChangeURI, error) {
	var ref ChangeURI
	err := json.Unmarshal(data, &ref)
	return ref, err
}
