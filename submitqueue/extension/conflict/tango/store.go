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

package tango

//go:generate mockgen -source=store.go -destination=mock_store.go -package=tango

import "context"

// SignatureReader reads the target signature of a batch. It is the read-only
// view of SignatureStore for consumers that must not write.
type SignatureReader interface {
	// Get returns the signature of batchID in the bound queue. The bool is
	// false when no signature has been created for the batch.
	Get(ctx context.Context, batchID string) (TargetSignature, bool, error)
}

// SignatureStore persists target signatures. A signature is immutable: it is
// created once under (Queue, BatchID) and never updated. Reads and writes are
// per-batch point operations, so any backend can implement it.
type SignatureStore interface {
	SignatureReader

	// Create persists signature in one write. Returns an already-exists error
	// if the bound queue already holds a signature for signature.BatchID, so the
	// first signature written stays and the caller can read it back with Get.
	Create(ctx context.Context, signature TargetSignature) error
}

// DictionaryReader reads the label recorded for a target ID. It is the
// read-only view of DictionaryStore.
type DictionaryReader interface {
	// Get returns the label recorded for id in the bound queue. The bool is
	// false when none has been recorded.
	Get(ctx context.Context, id uint64) (TargetLabel, bool, error)
}

// DictionaryStore persists the target dictionary: one TargetLabel for each
// 64-bit target ID. A record is immutable: it is created once under
// (Queue, ID) and never updated. Two labels can share an ID, so the first label
// recorded stays and the caller can read it back with Get to detect a collision.
type DictionaryStore interface {
	DictionaryReader

	// Create persists label in one write. Returns an already-exists error if
	// the bound queue holds a label for label.ID, whether or not it is the same
	// label, so repeating a write is harmless.
	Create(ctx context.Context, label TargetLabel) error
}
