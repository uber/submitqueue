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

package storage

//go:generate mockgen -source=dlq_reconciliation_store.go -destination=mock/dlq_reconciliation_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// DLQReconciliationStore retains snapshots independently of transport message GC.
// Only the full source key within the bound queue addresses a record, never its ID.
// Callers clone BatchIDs before changes and preserve captured targets and completion.
type DLQReconciliationStore interface {
	// Get returns the source intent's snapshot, or ErrNotFound if none exists.
	Get(ctx context.Context, source entity.DLQReconciliationSource) (entity.DLQReconciliation, error)

	// Create returns ErrAlreadyExists if this source intent already has a record.
	// A losing creator must Get the winner rather than reuse its proposed record ID.
	Create(ctx context.Context, record entity.DLQReconciliation) error

	// Update replaces all mutable fields if ID and oldVersion match the stored record,
	// or returns ErrVersionMismatch without writing. Queue, Source and ID are immutable.
	// record.Version is ignored; the caller computes newVersion and assigns it only
	// after success. Callers retain Completed records; this contract has no Delete.
	Update(ctx context.Context, record entity.DLQReconciliation, oldVersion, newVersion int32) error
}
