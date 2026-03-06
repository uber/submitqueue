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

package queue

//go:generate mockgen -source=publisher.go -destination=mock/publisher.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity/queue"
)

// Publisher publishes messages to topics.
// Implementations must be thread-safe.
type Publisher interface {
	// Publish sends a message to the specified topic.
	Publish(ctx context.Context, topic string, message queue.Message) error

	// Close gracefully shuts down the publisher, flushing pending messages.
	Close() error
}
