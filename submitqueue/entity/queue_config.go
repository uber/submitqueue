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

// QueueConfig identifies a single submit queue. It is the registry of valid
// queue names; the gateway validates that a land request targets a known queue.
// All behavioral and VCS configuration lives in the extension factory
// implementations, which are constructed per integrator deployment — the system
// hands a factory only the queue name. Immutable after creation.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	// Referenced by Request.Queue.
	Name string `json:"name" yaml:"name"`
}
