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

package requestlog

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func ensureRequestAcceptanceMapping(ctx context.Context, stores storage.Storage, summary entity.RequestSummary) error {
	if summary.AcceptedAtMs == 0 {
		return nil
	}
	acceptance := entity.RequestAcceptance{
		Queue: summary.Queue, AcceptedAtMs: summary.AcceptedAtMs, RequestID: summary.RequestID,
	}
	// This runs even for unchanged summaries: a prior attempt may have persisted only the summary.
	if err := stores.GetRequestAcceptanceStore().Create(ctx, acceptance); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("failed to ensure request acceptance mapping request_id=%q: %w", summary.RequestID, err)
	}
	return nil
}
