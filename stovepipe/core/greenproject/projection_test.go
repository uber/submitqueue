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

package greenproject

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/page"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcemock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestBackfillDoesNotMarkAnIncompleteLineageReady(t *testing.T) {
	for _, interrupted := range []string{"projection write", "trunk rewrite"} {
		t.Run(interrupted, func(t *testing.T) {
			const queue = "repo/main"
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			sources := sourcemock.NewMockFactory(ctrl)
			source := sourcemock.NewMockSourceControl(ctrl)
			stores := storagemock.NewMockFactory(ctrl)
			store := storagemock.NewMockStorage(ctrl)
			facts := storagemock.NewMockValidationFactStore(ctrl)
			projection := storagemock.NewMockGreenProjectStore(ctrl)
			sources.EXPECT().For(sourcecontrol.Config{QueueName: queue}).Return(source, nil)
			stores.EXPECT().For(storage.Config{QueueName: queue}).Return(store, nil)
			store.EXPECT().GetGreenProjectStore().Return(projection)
			store.EXPECT().GetValidationFactStore().Return(facts)

			position := sourcecontrol.TrunkPosition{Lineage: "old", Position: 1}
			gomock.InOrder(
				source.EXPECT().Latest(ctx).Return("A", nil),
				source.EXPECT().TrunkPosition(ctx, "A").Return(position, nil),
				source.EXPECT().History(ctx, "", backfillPageSize).Return(page.Page[string]{Items: []string{"A"}}, nil),
				source.EXPECT().TrunkPosition(ctx, "A").Return(position, nil),
				facts.EXPECT().ListProjects(ctx, "A", "", backfillPageSize).Return([]entity.ValidationFact{{URI: "A", Project: "payments"}}, nil),
			)
			write := projection.EXPECT().CreateForCommit(ctx, "old", int64(1), "A", []string{"payments"})
			if interrupted == "projection write" {
				write.Return(context.DeadlineExceeded)
				require.ErrorIs(t, Backfill(ctx, stores, sources, queue), context.DeadlineExceeded)
			} else {
				gomock.InOrder(
					write.Return(nil),
					source.EXPECT().Latest(ctx).Return("B", nil),
					source.EXPECT().TrunkPosition(ctx, "B").Return(sourcecontrol.TrunkPosition{Lineage: "new", Position: 1}, nil),
				)
				require.True(t, errs.IsRetryable(Backfill(ctx, stores, sources, queue)))
			}
		})
	}
}
