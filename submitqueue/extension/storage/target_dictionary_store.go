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

//go:generate mockgen -source=target_dictionary_store.go -destination=mock/target_dictionary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// TargetDictionaryStore persists the target dictionary: one TargetLabel record
// for each 64-bit target ID. A record is immutable: it is created once under
// (Queue, ID) and never updated. Two labels can share an ID, so the first label
// recorded stays and the caller can read it back with Get to detect a collision.
type TargetDictionaryStore interface {
	// Create persists label in one write. Returns ErrAlreadyExists if the
	// bound queue already holds a label for label.ID, whether or not it is the
	// same label, so repeating a write is harmless.
	Create(ctx context.Context, label entity.TargetLabel) error

	// Get returns the label recorded for id in the bound queue. The bool is
	// false when none has been recorded.
	Get(ctx context.Context, id uint64) (entity.TargetLabel, bool, error)
}
