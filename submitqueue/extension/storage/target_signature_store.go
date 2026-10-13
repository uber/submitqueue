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

//go:generate mockgen -source=target_signature_store.go -destination=mock/target_signature_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// TargetSignatureStore persists the target signature of each batch. A
// signature is immutable: it is created once under (Queue, BatchID) and never
// updated. Reads and writes are per-batch point operations, so any backend can
// implement it.
type TargetSignatureStore interface {
	// Create persists signature in one write. Returns ErrAlreadyExists if the
	// bound queue already holds a signature for signature.BatchID, so the first
	// signature written stays and the caller can read it back with Get.
	Create(ctx context.Context, signature entity.TargetSignature) error

	// Get returns the signature of batchID in the bound queue. The bool is
	// false when no signature has been created for the batch.
	Get(ctx context.Context, batchID string) (entity.TargetSignature, bool, error)
}
