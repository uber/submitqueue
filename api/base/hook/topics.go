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

package hook

import "github.com/uber/submitqueue/platform/consumer"

// TopicKey looks up a queue backend, topic name, and subscription config in a
// consumer.TopicRegistry.
type TopicKey = consumer.TopicKey

// TopicKeyHook carries hook events. The key is per-host, not global: each domain
// runs its own hook topic, so two domains sharing a queue backend must map this
// to distinct topic names.
const TopicKeyHook TopicKey = "hook"
