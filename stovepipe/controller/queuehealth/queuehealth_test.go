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

package queuehealth

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	configmock "github.com/uber/submitqueue/stovepipe/extension/queueconfig/mock"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcemock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

const sampleTimeMs int64 = 1000000

type fixture struct {
	queue         entity.Queue
	ids           []string
	requests      map[string]entity.Request
	logs          map[string][]entity.RequestLog
	builds        map[string]entity.Build
	deleted       []string
	maxConcurrent int32
	changed       bool
	failure       string
}

type fixedFactory struct{ store storage.Storage }

func (f fixedFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func request(id string, state entity.RequestState) entity.Request {
	return entity.Request{ID: id, Queue: "q", URI: "head", State: state, Version: 2}
}

func admission(id string) entity.RequestLog {
	return entity.RequestLog{Queue: "q", RequestID: id, State: entity.RequestStateProcessing, TimestampMs: sampleTimeMs - 180000}
}

func trigger(id, buildID string, age int64) entity.RequestLog {
	return entity.RequestLog{Queue: "q", RequestID: id, Event: entity.RequestEventBuildTriggered, TimestampMs: sampleTimeMs - age,
		Metadata: map[string]string{requestlog.MetadataKeyBuildID: buildID}}
}

func (f *fixture) observe(t *testing.T) (Observation, error) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	queues := storagemock.NewMockQueueStore(ctrl)
	active := storagemock.NewMockActiveRequestStore(ctrl)
	requests := storagemock.NewMockRequestStore(ctrl)
	logs := storagemock.NewMockRequestLogStore(ctrl)
	builds := storagemock.NewMockBuildStore(ctrl)
	configs := configmock.NewMockStore(ctrl)
	sources := sourcemock.NewMockFactory(ctrl)
	source := sourcemock.NewMockSourceControl(ctrl)
	failed := func(stage string) error {
		if f.failure == stage {
			return errors.New(stage)
		}
		return nil
	}
	store.EXPECT().GetQueueStore().Return(queues).AnyTimes()
	store.EXPECT().GetActiveRequestStore().Return(active).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logs).AnyTimes()
	store.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	configs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{MaxConcurrent: f.maxConcurrent}, failed("config")).AnyTimes()
	sources.EXPECT().For(sourcecontrol.Config{QueueName: "q"}).Return(source, failed("source resolution")).AnyTimes()
	source.EXPECT().Latest(gomock.Any()).Return("head", failed("head")).AnyTimes()
	reads := 0
	queues.EXPECT().Get(gomock.Any(), "q").DoAndReturn(func(context.Context, string) (entity.Queue, error) {
		reads++
		row := f.queue
		if f.changed && reads > 1 {
			row.Version++
		}
		if f.failure == "missing queue" {
			return entity.Queue{}, storage.ErrNotFound
		}
		return row, failed("queue")
	}).AnyTimes()
	active.EXPECT().List(gomock.Any(), gomock.Any(), pageSize).DoAndReturn(func(_ context.Context, after string, limit int) ([]string, error) {
		var ids []string
		for _, id := range f.ids {
			if id > after && len(ids) < limit {
				ids = append(ids, id)
			}
		}
		return ids, failed("index")
	}).AnyTimes()
	active.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) error {
		f.deleted = append(f.deleted, id)
		return failed("delete")
	}).AnyTimes()
	requests.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.Request, error) {
		if err := failed("request"); err != nil {
			return entity.Request{}, err
		}
		row, ok := f.requests[id]
		if !ok {
			return entity.Request{}, storage.ErrNotFound
		}
		return row, nil
	}).AnyTimes()
	logs.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) ([]entity.RequestLog, error) { return f.logs[id], failed("history") }).AnyTimes()
	builds.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.Build, error) {
		if err := failed("build"); err != nil {
			return entity.Build{}, err
		}
		build, ok := f.builds[id]
		if !ok {
			return entity.Build{}, storage.ErrNotFound
		}
		return build, nil
	}).AnyTimes()
	c := New(fixedFactory{store}, configs, sources).(*controller)
	c.now = func() time.Time { return time.UnixMilli(sampleTimeMs) }
	return c.Observe(context.Background(), "q")
}

func TestObserve(t *testing.T) {
	for _, tt := range []struct {
		name      string
		configure func(*fixture)
		want      Observation
		wantErr   bool
		deleted   []string
	}{
		{name: "healthy idle"},
		{name: "new head awaits ingestion", configure: func(f *fixture) { r := f.requests["2"]; r.URI = "old"; f.requests["2"] = r }, want: Observation{HeadNotIngested: true}},
		{name: "cold start", configure: func(f *fixture) { f.failure = "missing queue" }, want: Observation{HeadNotIngested: true}},
		{name: "obsolete accepted request needs no build", configure: func(f *fixture) { f.ids = []string{"1"}; f.requests["1"] = request("1", entity.RequestStateAccepted) }},
		{name: "pending with capacity", configure: func(f *fixture) { f.ids = []string{"2"}; f.requests["2"] = request("2", entity.RequestStateAccepted) }, want: Observation{PendingWithCapacity: true}},
		{name: "normal active build blocks pending work", configure: func(f *fixture) {
			f.queue.InFlightCount = 1
			f.ids = []string{"1", "2"}
			f.requests["1"] = request("1", entity.RequestStateProcessing)
			f.requests["2"] = request("2", entity.RequestStateAccepted)
			f.logs["1"] = []entity.RequestLog{admission("1"), trigger("1", "b", 120000)}
		}, want: Observation{BuildAgeMs: 120000, ActiveBuildCount: 1}},
		{name: "dispatch gap", configure: func(f *fixture) {
			f.queue.InFlightCount = 1
			f.ids = []string{"2"}
			f.requests["2"] = request("2", entity.RequestStateProcessing)
			f.logs["2"] = []entity.RequestLog{admission("2")}
		}, want: Observation{Dispatches: []PendingDispatch{{RequestID: "2", AgeMs: 180000}}}},
		{name: "queued build counts as active", configure: func(f *fixture) {
			f.ids = []string{"1"}
			f.requests["1"] = request("1", entity.RequestStateProcessing)
			f.logs["1"] = []entity.RequestLog{admission("1"), trigger("1", "b", 120000)}
			b := f.builds["b"]
			b.Status = entity.BuildStatusAccepted
			f.builds["b"] = b
		}, want: Observation{BuildAgeMs: 120000, ActiveBuildCount: 1}},
		{name: "oldest build across requests", configure: func(f *fixture) {
			f.maxConcurrent = 2
			f.queue.InFlightCount = 2
			f.ids = []string{"1", "2"}
			f.requests["1"] = request("1", entity.RequestStateProcessing)
			f.requests["2"] = request("2", entity.RequestStateProcessing)
			f.logs["1"] = []entity.RequestLog{admission("1"), trigger("1", "b", 120000)}
			f.logs["2"] = []entity.RequestLog{admission("2"), trigger("2", "c", 60000)}
			f.builds["c"] = entity.Build{ID: "c", RequestID: "2", Status: entity.BuildStatusRunning}
		}, want: Observation{BuildAgeMs: 120000, ActiveBuildCount: 2}},
		{name: "unowned occupied slot", configure: func(f *fixture) { f.queue.InFlightCount = 1 }, want: Observation{UnaccountedSlots: 1}},
		{name: "terminal cleanup", configure: func(f *fixture) { f.ids = []string{"2"} }, deleted: []string{"2"}},
		{name: "missing indexed request is unknown", configure: func(f *fixture) { f.ids = []string{"missing"} }, wantErr: true},
		{name: "unindexed active latest is unknown", configure: func(f *fixture) { f.requests["2"] = request("2", entity.RequestStateProcessing) }, wantErr: true},
		{name: "missing admission timestamp is unknown", configure: func(f *fixture) { f.ids = []string{"2"}; f.requests["2"] = request("2", entity.RequestStateProcessing) }, wantErr: true},
		{name: "clock skew is unknown", configure: func(f *fixture) {
			f.ids = []string{"2"}
			f.requests["2"] = request("2", entity.RequestStateProcessing)
			log := admission("2")
			log.TimestampMs = sampleTimeMs + 1
			f.logs["2"] = []entity.RequestLog{log}
		}, wantErr: true},
		{name: "concurrent queue update is unknown", configure: func(f *fixture) { f.changed = true }, wantErr: true},
		{name: "invalid configuration is unknown", configure: func(f *fixture) { f.maxConcurrent = 0 }, wantErr: true},
		{name: "finalization stalled", configure: func(f *fixture) {
			f.ids = []string{"1"}
			f.requests["1"] = request("1", entity.RequestStateProcessing)
			f.logs["1"] = []entity.RequestLog{admission("1"), trigger("1", "b", 120000), {Queue: "q", RequestID: "1", Event: entity.RequestEventBuildFinished, TimestampMs: sampleTimeMs - 30000, Metadata: map[string]string{requestlog.MetadataKeyBuildID: "b"}}}
			b := f.builds["b"]
			b.Status = entity.BuildStatusSucceeded
			f.builds["b"] = b
		}, want: Observation{FinalizationAgeMs: 30000}},
		{name: "terminal build without completion timestamp is unknown", configure: func(f *fixture) {
			f.ids = []string{"1"}
			f.requests["1"] = request("1", entity.RequestStateProcessing)
			f.logs["1"] = []entity.RequestLog{admission("1"), trigger("1", "b", 120000)}
			b := f.builds["b"]
			b.Status = entity.BuildStatusSucceeded
			f.builds["b"] = b
		}, wantErr: true},
		{name: "pagination", configure: func(f *fixture) {
			f.requests["2"] = request("2", entity.RequestStateAccepted)
			f.ids = []string{"2"}
			for i := 0; i < pageSize+1; i++ {
				id := fmt.Sprintf("%05d", i)
				f.ids = append(f.ids, id)
				f.requests[id] = request(id, entity.RequestStateAccepted)
			}
			sort.Strings(f.ids)
		}, want: Observation{PendingWithCapacity: true}},
		{name: "bounded scan cannot report healthy", configure: func(f *fixture) {
			for i := 0; i <= maxObservedRequests; i++ {
				id := fmt.Sprintf("%05d", i)
				f.ids = append(f.ids, id)
				f.requests[id] = request(id, entity.RequestStateAccepted)
			}
			sort.Strings(f.ids)
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := fixture{queue: entity.Queue{Name: "q", LatestRequestID: "2", Version: 1}, maxConcurrent: 1,
				requests: map[string]entity.Request{"2": request("2", entity.RequestStateSucceeded)},
				logs:     map[string][]entity.RequestLog{}, builds: map[string]entity.Build{"b": {ID: "b", RequestID: "1", Status: entity.BuildStatusRunning}}}
			if tt.configure != nil {
				tt.configure(&f)
			}
			got, err := f.observe(t)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, Observation{}, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.Equal(t, tt.deleted, f.deleted)
		})
	}
}

func TestObserveFailures(t *testing.T) {
	for _, failure := range []string{"config", "source resolution", "head", "queue", "index", "request", "history", "build", "delete"} {
		t.Run(failure, func(t *testing.T) {
			f := fixture{queue: entity.Queue{Name: "q", LatestRequestID: "1", Version: 1}, maxConcurrent: 1, failure: failure,
				ids: []string{"1"}, requests: map[string]entity.Request{"1": request("1", entity.RequestStateProcessing)},
				logs:   map[string][]entity.RequestLog{"1": {admission("1"), trigger("1", "b", 120000)}},
				builds: map[string]entity.Build{"b": {ID: "b", RequestID: "1", Status: entity.BuildStatusRunning}}}
			if failure == "delete" {
				f.requests["1"] = request("1", entity.RequestStateSucceeded)
			}
			got, err := f.observe(t)
			require.Error(t, err)
			require.Equal(t, Observation{}, got)
		})
	}
}
