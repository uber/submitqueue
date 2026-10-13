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

package request

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
)

func ensureRequestReceiptMapping(ctx context.Context, receipts basestorage.RequestReceiptStore, summary entity.RequestSummary) error {
	receipt := entity.RequestReceipt{
		Queue: summary.Queue, ReceivedAtMs: summary.ReceivedAtMs, RequestID: summary.RequestID,
	}
	// Unchanged summaries still need this write after a partially completed attempt.
	if err := receipts.Create(ctx, receipt); err != nil && !errors.Is(err, basestorage.ErrAlreadyExists) {
		return fmt.Errorf("failed to ensure request receipt mapping queue=%q request_id=%q: %w", summary.Queue, summary.RequestID, err)
	}
	return nil
}
