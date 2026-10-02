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

package counter

//go:generate mockgen -source=counter.go -destination=mock/counter_mock.go -package=mock

import "context"

// Config identifies the queue a Counter instance is resolved for. Like every
// other extension config, it carries only the queue name — everything an
// implementation needs beyond that is injected at construction by the
// integrator.
type Config struct {
	// QueueName is the name of the queue whose sequences the resolved Counter
	// is scoped to.
	QueueName string
}

// Factory resolves the queue-scoped Counter for a queue. Mirrors the extension
// contract: the host wiring decides which backend serves which queue;
// implementations bind the queue over their backend so a resolved instance can
// only read and advance that queue's sequences.
type Factory interface {
	// For returns the Counter bound to the queue named in config.
	For(config Config) (Counter, error)
}

// Counter provides atomic sequential number generation for a resource type within
// the queue the instance is bound to.
// Each call to Next returns the next value in the sequence for the specified resource type.
// The value is unique within the (queue, resource type) pair.
type Counter interface {
	// Next atomically increments the counter for the given resource type and returns the new value.
	// The first call for a new resource type returns 1.
	// The implementation should support resource types up to 255 bytes.
	// The function is safe to be called concurrently and will give unique results, but the order of the values is not guaranteed.
	Next(ctx context.Context, resourceType string) (int64, error)
}
