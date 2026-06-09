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

package request

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// PersistLog inserts the immutable request log entry and updates the request summary read model.
func PersistLog(ctx context.Context, store storage.Storage, log entity.RequestLog) error {
	if err := store.GetRequestLogStore().Insert(ctx, log); err != nil {
		return fmt.Errorf("failed to insert request log for request_id=%s: %w", log.RequestID, err)
	}
	if err := store.GetRequestSummaryStore().UpsertFromLog(ctx, log); err != nil {
		return fmt.Errorf("failed to upsert request summary for request_id=%s: %w", log.RequestID, err)
	}
	return nil
}
