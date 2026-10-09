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

package storage

//go:generate mockgen -source=queue_policy_store.go -destination=mock/queue_policy_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// QueuePolicyStore holds a queue-scoped versioned pointer and immutable transitions by key.
// A transition is committed only when it is reachable from the current pointer.
type QueuePolicyStore interface {
	// GetCurrent retrieves the current pointer. Returns ErrNotFound when absent.
	GetCurrent(ctx context.Context) (entity.QueuePolicyHead, error)
	// CreateCurrent creates the pointer. Returns ErrAlreadyExists when present.
	CreateCurrent(ctx context.Context, head entity.QueuePolicyHead) error
	// UpdateCurrent conditionally replaces the pointer, guarding oldVersion and writing newVersion without arithmetic.
	UpdateCurrent(ctx context.Context, head entity.QueuePolicyHead, oldVersion, newVersion int64) error
	// GetTransition retrieves one immutable occurrence by its queue-scoped ID.
	GetTransition(ctx context.Context, id string) (entity.QueuePolicyTransition, error)
	// CreateTransition creates one occurrence. Returns ErrAlreadyExists for a duplicate ID; never overwrites it.
	CreateTransition(ctx context.Context, transition entity.QueuePolicyTransition) error
}
