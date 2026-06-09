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

// Package topickey defines Stovepipe pipeline stage identifiers.
package topickey

import "github.com/uber/submitqueue/core/consumer"

// TopicKey is the shared pipeline stage identifier type.
type TopicKey = consumer.TopicKey

const (
	// TopicKeyStart is the pipeline stage where trunk push events arrive from the gateway.
	TopicKeyStart TopicKey = "start"
	// TopicKeyValidate is the pipeline stage where commits are published for metadata resolution.
	TopicKeyValidate TopicKey = "validate"
)
