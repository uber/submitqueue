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

package messagequeue

import (
	"context"
)

// MetadataKeyQueueName carries the queue name independently of the
// transport partition key. Producers set it on Message.Metadata so consumers
// can attribute work before decoding the payload.
const MetadataKeyQueueName = "queue_name"

type queueNameContextKey struct{}

// WithQueueName returns a child context containing the queue name of the
// delivered message.
func WithQueueName(ctx context.Context, queueName string) context.Context {
	return context.WithValue(ctx, queueNameContextKey{}, queueName)
}

// QueueName returns the delivered message's queue name from ctx.
func QueueName(ctx context.Context) (string, bool) {
	queueName, ok := ctx.Value(queueNameContextKey{}).(string)
	return queueName, ok
}
