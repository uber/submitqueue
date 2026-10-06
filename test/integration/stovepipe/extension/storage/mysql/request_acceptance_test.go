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

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testRequestAcceptanceStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	bound, err := factory.For(storage.Config{QueueName: "monorepo/main"})
	require.NoError(t, err)
	store := bound.GetRequestAcceptanceStore()
	other, err := factory.For(storage.Config{QueueName: "other/main"})
	require.NoError(t, err)
	foreign := entity.RequestAcceptance{Queue: "other/main", AcceptedAtMs: 1500, RequestID: "request/other/main/1"}
	require.Error(t, store.Create(ctx, foreign))
	require.NoError(t, other.GetRequestAcceptanceStore().Create(ctx, foreign))

	acceptance := func(timestamp int64, suffix string) entity.RequestAcceptance {
		return entity.RequestAcceptance{Queue: "monorepo/main", AcceptedAtMs: timestamp, RequestID: "request/monorepo/main/" + suffix}
	}
	unicodeID := strings.Repeat("é", 150)
	fixtures := []entity.RequestAcceptance{
		acceptance(500, "old"), acceptance(1000, "lower"),
		acceptance(1500, "9"), acceptance(1500, "10"),
		acceptance(1500, "A"), acceptance(1500, "a"), acceptance(1500, "a "),
		acceptance(1500, "z"), acceptance(1500, unicodeID),
		acceptance(2000, "upper"),
	}
	for _, mapping := range fixtures {
		require.NoError(t, store.Create(ctx, mapping))
	}
	require.ErrorIs(t, store.Create(ctx, fixtures[0]), storage.ErrAlreadyExists)
	require.Error(t, store.Create(ctx, acceptance(0, "unknown")))
	for _, tt := range []struct {
		name         string
		lower, upper int64
		want         []entity.RequestAcceptance
	}{
		{"half-open window", 1000, 2000, []entity.RequestAcceptance{
			acceptance(1500, unicodeID), acceptance(1500, "z"),
			acceptance(1500, "a "), acceptance(1500, "a"), acceptance(1500, "A"),
			acceptance(1500, "9"), acceptance(1500, "10"), acceptance(1000, "lower"),
		}},
		{"all known times", 0, 3000, []entity.RequestAcceptance{
			acceptance(2000, "upper"), acceptance(1500, unicodeID), acceptance(1500, "z"),
			acceptance(1500, "a "), acceptance(1500, "a"),
			acceptance(1500, "A"), acceptance(1500, "9"), acceptance(1500, "10"),
			acceptance(1000, "lower"), acceptance(500, "old"),
		}},
		{"no matches", 2001, 3000, []entity.RequestAcceptance{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bounds := storage.RequestAcceptanceRange{AcceptedAtOrAfterMs: tt.lower, AcceptedBeforeMs: tt.upper, Limit: 100}
			got, err := store.List(ctx, bounds)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			bounds.Limit = 1
			paged := make([]entity.RequestAcceptance, 0)
			for {
				page, err := store.List(ctx, bounds)
				require.NoError(t, err)
				require.LessOrEqual(t, len(page), bounds.Limit)
				if len(page) == 0 {
					break
				}
				require.Less(t, len(paged), len(tt.want), "cursor must make progress")
				paged = append(paged, page...)
				last := page[len(page)-1]
				bounds.Before = storage.RequestAcceptanceCursor{AcceptedAtMs: last.AcceptedAtMs, RequestID: last.RequestID}
			}
			require.Equal(t, tt.want, paged)
		})
	}
}

func testRequestAcceptanceProjection(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	stores, err := factory.For(storage.Config{QueueName: "monorepo/main"})
	require.NoError(t, err)
	request := entity.Request{
		ID: "request/monorepo/main/1", Queue: "monorepo/main", URI: "git://repo/head",
		State: entity.RequestStateAccepted, Version: 1,
	}
	require.NoError(t, stores.GetRequestStore().Create(ctx, request))
	materializer := requestlog.NewMaterializer(tally.NoopScope)
	accepted := requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)
	accepted.TimestampMs = 1000
	require.NoError(t, materializer.PersistLog(ctx, stores, accepted))
	retry := accepted
	retry.TimestampMs = 2000
	require.NoError(t, materializer.PersistLog(ctx, stores, retry))

	processing := request
	processing.State = entity.RequestStateProcessing
	require.NoError(t, stores.GetRequestStore().Update(ctx, processing, request.Version, request.Version+1))
	processing.Version = request.Version + 1
	log := requestlog.NewRequestStateLog(processing, entity.RequestOutcomeReasonUnknown)
	log.TimestampMs = 3000
	require.NoError(t, materializer.PersistLog(ctx, stores, log))
	require.NoError(t, materializer.PersistLog(ctx, stores, accepted))

	summary, err := stores.GetRequestSummaryStore().Get(ctx, request.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1000), summary.AcceptedAtMs)
	require.Equal(t, entity.RequestStateProcessing, summary.State)
	require.Equal(t, int32(2), summary.RequestVersion)
	mappings, err := stores.GetRequestAcceptanceStore().List(ctx, storage.RequestAcceptanceRange{AcceptedBeforeMs: 4000, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []entity.RequestAcceptance{{Queue: request.Queue, AcceptedAtMs: 1000, RequestID: request.ID}}, mappings)
}
