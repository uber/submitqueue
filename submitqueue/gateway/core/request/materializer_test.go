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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/mock/gomock"
)

func TestMaterializer_PersistLog(t *testing.T) {
	base := testRequestSummary()
	log := entity.RequestLog{Queue: base.Queue, RequestID: base.RequestID, TimestampMs: 20, Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusLanded, RequestVersion: 2, Metadata: map[string]string{"build_id": "b/7"}}

	t.Run("winning log updates the authoritative summary", func(t *testing.T) {
		f := newMaterializerReceiptFixture(gomock.NewController(t))
		f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil)
		f.summaries.EXPECT().Get(gomock.Any(), base.RequestID).Return(base, nil)
		f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).DoAndReturn(func(_ context.Context, updated entity.RequestSummary, _, _ int32) error {
			want := base
			want.Status, want.RequestVersion, want.StatusTimestampMs = log.Status, log.RequestVersion, log.TimestampMs
			want.Metadata = log.Metadata
			assert.Equal(t, want, updated)
			updated.Metadata["build_id"] = "other"
			return nil
		})
		f.expectURIMappings(base, nil)
		f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(base)).Return(nil)
		require.NoError(t, f.materializer.PersistLog(context.Background(), log))
		assert.Equal(t, "b/7", log.Metadata["build_id"])
		assert.Equal(t, testRequestSummary(), base)
	})

	t.Run("CAS conflict reloads the concurrent winner", func(t *testing.T) {
		f := newMaterializerReceiptFixture(gomock.NewController(t))
		advanced := base
		advanced.Status, advanced.RequestVersion, advanced.StatusTimestampMs, advanced.Version = entity.RequestStatusError, 3, 30, 2
		gomock.InOrder(
			f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil),
			f.summaries.EXPECT().Get(gomock.Any(), base.RequestID).Return(base, nil),
			f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(storage.ErrVersionMismatch),
			f.summaries.EXPECT().Get(gomock.Any(), base.RequestID).Return(advanced, nil),
			f.expectURIMappings(advanced, nil),
			f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(advanced)).Return(nil),
		)
		require.NoError(t, f.materializer.PersistLog(context.Background(), log))
	})
}

func TestMaterializer_RecordsLifecycleMetrics(t *testing.T) {
	started := testRequestSummary()
	started.Status, started.RequestVersion, started.StatusTimestampMs = entity.RequestStatusStarted, 1, 15
	landed := started
	landed.Status, landed.RequestVersion, landed.StatusTimestampMs = entity.RequestStatusLanded, 2, 20
	unreceived := started
	unreceived.ReceivedAtMs = 0
	statusLog := func(status entity.RequestStatus, version int32, timestampMs int64) entity.RequestLog {
		return entity.RequestLog{Queue: "q", RequestID: "1", Type: entity.RequestLogTypeStatus, Status: status, RequestVersion: version, TimestampMs: timestampMs}
	}

	tests := []struct {
		name           string
		before         entity.RequestSummary
		log            entity.RequestLog
		wantUpdate     bool
		wantCounters   map[string]int64
		wantHistograms []string
	}{
		{
			name:         "accepted log moves the request out of accepting",
			before:       testRequestSummary(),
			log:          statusLog(entity.RequestStatusAccepted, 0, 20),
			wantUpdate:   true,
			wantCounters: map[string]int64{"request_lifecycle.accepted+queue=q": 1},
		},
		{
			name:         "a later status overtaking the accepted log still counts acceptance once",
			before:       testRequestSummary(),
			log:          statusLog(entity.RequestStatusStarted, 1, 20),
			wantUpdate:   true,
			wantCounters: map[string]int64{"request_lifecycle.accepted+queue=q": 1},
		},
		{
			name:           "terminal log finishes the request and records its land time",
			before:         started,
			log:            statusLog(entity.RequestStatusLanded, 2, 20),
			wantUpdate:     true,
			wantCounters:   map[string]int64{"request_lifecycle.terminated+from_status=started,queue=q,status=landed": 1},
			wantHistograms: []string{"request_lifecycle.end_to_end_latency+queue=q,status=landed"},
		},
		{
			name:         "missing receipt time counts the finish without a land time",
			before:       unreceived,
			log:          statusLog(entity.RequestStatusError, 2, 20),
			wantUpdate:   true,
			wantCounters: map[string]int64{"request_lifecycle.terminated+from_status=started,queue=q,status=error": 1},
		},
		{
			name:       "re-published terminal log with a newer timestamp rewrites the summary without counting again",
			before:     landed,
			log:        statusLog(entity.RequestStatusLanded, 2, 30),
			wantUpdate: true,
		},
		{
			name:   "identical terminal redelivery leaves the summary and the metrics alone",
			before: landed,
			log:    statusLog(entity.RequestStatusLanded, 2, 20),
		},
		{
			name:       "cancelling is not a finished status",
			before:     started,
			log:        statusLog(entity.RequestStatusCancelling, 0, 20),
			wantUpdate: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMaterializerReceiptFixture(gomock.NewController(t))
			f.logs.EXPECT().Insert(gomock.Any(), tt.log).Return(nil)
			f.summaries.EXPECT().Get(gomock.Any(), tt.before.RequestID).Return(tt.before, nil)
			if tt.wantUpdate {
				f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), tt.before.Version, tt.before.Version+1).Return(nil)
				if entity.IsRequestStatusInQueue(tt.log.Status) {
					f.expectRecheck(tt.log.Status)
				}
			}
			f.uris.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			f.receipts.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			require.NoError(t, f.materializer.PersistLog(context.Background(), tt.log))

			snapshot := f.metrics.Snapshot()
			counters := map[string]int64{}
			for key, counter := range snapshot.Counters() {
				counters[key] = counter.Value()
			}
			if tt.wantCounters == nil {
				tt.wantCounters = map[string]int64{}
			}
			assert.Equal(t, tt.wantCounters, counters)
			histograms := []string{}
			for key := range snapshot.Histograms() {
				histograms = append(histograms, key)
			}
			assert.ElementsMatch(t, tt.wantHistograms, histograms)
		})
	}
}

// recordCall is one expected status-record write, in order: a put (followed by the summary
// re-read when recheck is set) or a delete.
type recordCall struct {
	put     bool
	status  entity.RequestStatus
	entered int64
	// recheck is the status the summary re-read after a put reports.
	recheck entity.RequestStatus
}

func putRecord(status entity.RequestStatus, entered int64, recheck entity.RequestStatus) recordCall {
	return recordCall{put: true, status: status, entered: entered, recheck: recheck}
}

func deleteRecord(status entity.RequestStatus) recordCall {
	return recordCall{status: status}
}

// sweepRecords is the terminal sweep: a delete under every in-queue status.
func sweepRecords() []recordCall {
	var calls []recordCall
	for _, status := range entity.InQueueRequestStatuses() {
		calls = append(calls, deleteRecord(status))
	}
	return calls
}

// expectRecordCalls declares calls in order, each put followed by its summary re-read. The
// call at failAt returns err, and nothing after it is expected.
func (f materializerReceiptFixture) expectRecordCalls(calls []recordCall, failAt int, err error) {
	var ordered []any
	for i, call := range calls {
		failed := i == failAt
		var callErr error
		if failed {
			callErr = err
		}
		if call.put {
			record := entity.QueueRequestStatus{Queue: "q", Status: call.status, RequestID: "1", ReceivedAtMs: 10, EnteredAtMs: call.entered}
			ordered = append(ordered, f.statuses.EXPECT().Put(gomock.Any(), record).Return(callErr))
			if !failed {
				ordered = append(ordered, f.expectRecheck(call.recheck))
			}
		} else {
			ordered = append(ordered, f.statuses.EXPECT().Delete(gomock.Any(), call.status, "1").Return(callErr))
		}
		if failed {
			break
		}
	}
	gomock.InOrder(ordered...)
}

func TestMaterializer_FilesStatusRecords(t *testing.T) {
	summaryAt := func(status entity.RequestStatus, timestampMs int64) entity.RequestSummary {
		summary := testRequestSummary()
		summary.Status, summary.StatusTimestampMs = status, timestampMs
		return summary
	}
	statusLog := func(status entity.RequestStatus, version int32, timestampMs int64) entity.RequestLog {
		return entity.RequestLog{Queue: "q", RequestID: "1", Type: entity.RequestLogTypeStatus, Status: status, RequestVersion: version, TimestampMs: timestampMs}
	}
	landed := summaryAt(entity.RequestStatusLanded, 20)
	landed.RequestVersion = 2

	tests := []struct {
		name       string
		before     entity.RequestSummary
		log        entity.RequestLog
		wantUpdate bool
		wantCalls  []recordCall
	}{
		{
			name:       "accepting the request files its first record",
			before:     testRequestSummary(),
			log:        statusLog(entity.RequestStatusAccepted, 0, 20),
			wantUpdate: true,
			wantCalls:  []recordCall{putRecord(entity.RequestStatusAccepted, 20, entity.RequestStatusAccepted)},
		},
		{
			name:       "a status change files the new record before removing the old one",
			before:     summaryAt(entity.RequestStatusStarted, 15),
			log:        statusLog(entity.RequestStatusValidating, 0, 20),
			wantUpdate: true,
			wantCalls: []recordCall{
				putRecord(entity.RequestStatusValidating, 20, entity.RequestStatusValidating),
				deleteRecord(entity.RequestStatusStarted),
			},
		},
		{
			name:       "a newer log at the same status re-files without removing anything",
			before:     summaryAt(entity.RequestStatusSpeculating, 15),
			log:        statusLog(entity.RequestStatusSpeculating, 0, 20),
			wantUpdate: true,
			wantCalls:  []recordCall{putRecord(entity.RequestStatusSpeculating, 20, entity.RequestStatusSpeculating)},
		},
		{
			name:      "a redelivery that loses re-files the current record to repair an interrupted write",
			before:    summaryAt(entity.RequestStatusBatched, 20),
			log:       statusLog(entity.RequestStatusBatched, 0, 20),
			wantCalls: []recordCall{putRecord(entity.RequestStatusBatched, 20, entity.RequestStatusBatched)},
		},
		{
			name:       "a record filed after another writer moved the summary on is removed again",
			before:     testRequestSummary(),
			log:        statusLog(entity.RequestStatusAccepted, 0, 20),
			wantUpdate: true,
			wantCalls: []recordCall{
				putRecord(entity.RequestStatusAccepted, 20, entity.RequestStatusStarted),
				deleteRecord(entity.RequestStatusAccepted),
			},
		},
		{
			name:       "a cancel overtaken by its terminal log removes both its own and the previous record",
			before:     summaryAt(entity.RequestStatusSpeculating, 15),
			log:        statusLog(entity.RequestStatusCancelling, 0, 20),
			wantUpdate: true,
			wantCalls: []recordCall{
				putRecord(entity.RequestStatusCancelling, 20, entity.RequestStatusCancelled),
				deleteRecord(entity.RequestStatusCancelling),
				deleteRecord(entity.RequestStatusSpeculating),
			},
		},
		{
			name:       "a terminal status sweeps every in-queue record and files none",
			before:     summaryAt(entity.RequestStatusLanding, 15),
			log:        statusLog(entity.RequestStatusLanded, 2, 20),
			wantUpdate: true,
			wantCalls:  sweepRecords(),
		},
		{
			name:      "a terminal redelivery sweeps again to retry a failed removal",
			before:    landed,
			log:       statusLog(entity.RequestStatusLanded, 2, 20),
			wantCalls: sweepRecords(),
		},
		{
			name:   "an event log leaves the record alone",
			before: summaryAt(entity.RequestStatusSpeculating, 15),
			log:    entity.RequestLog{Queue: "q", RequestID: "1", Type: entity.RequestLogTypeEvent, Event: entity.RequestEventBuilding, TimestampMs: 20},
		},
		{
			name:   "a request still accepting has no record",
			before: testRequestSummary(),
			log:    statusLog(entity.RequestStatusAccepting, 0, 20),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newStrictMaterializerFixture(gomock.NewController(t))
			f.logs.EXPECT().Insert(gomock.Any(), tt.log).Return(nil)
			f.summaries.EXPECT().Get(gomock.Any(), tt.before.RequestID).Return(tt.before, nil)
			if tt.wantUpdate {
				f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), tt.before.Version, tt.before.Version+1).Return(nil)
			}
			f.uris.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			f.receipts.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			f.expectRecordCalls(tt.wantCalls, -1, nil)

			require.NoError(t, f.materializer.PersistLog(context.Background(), tt.log))
		})
	}
}

// TestMaterializer_StatusRecordFailures covers every record write and the re-read failing:
// each is returned so the delivery is retried, and nothing after it runs.
func TestMaterializer_StatusRecordFailures(t *testing.T) {
	writeErr := errors.New("storage unavailable")
	moving := []recordCall{
		putRecord(entity.RequestStatusValidating, 20, entity.RequestStatusValidating),
		deleteRecord(entity.RequestStatusStarted),
	}
	superseded := []recordCall{
		putRecord(entity.RequestStatusValidating, 20, entity.RequestStatusValidated),
		deleteRecord(entity.RequestStatusValidating),
	}
	tests := []struct {
		name       string
		log        entity.RequestStatus
		calls      []recordCall
		failAt     int
		recheckErr bool
	}{
		{name: "filing the new record", log: entity.RequestStatusValidating, calls: moving, failAt: 0},
		{name: "removing the previous record", log: entity.RequestStatusValidating, calls: moving, failAt: 1},
		{name: "removing a superseded record", log: entity.RequestStatusValidating, calls: superseded, failAt: 1},
		{name: "sweeping a finished request", log: entity.RequestStatusError, calls: sweepRecords()[:3], failAt: 2},
		{name: "re-reading the summary", log: entity.RequestStatusValidating, recheckErr: true, failAt: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newStrictMaterializerFixture(gomock.NewController(t))
			before := testRequestSummary()
			before.Status = entity.RequestStatusStarted
			log := entity.RequestLog{Queue: "q", RequestID: "1", Type: entity.RequestLogTypeStatus, Status: tt.log, TimestampMs: 20}
			f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil)
			f.summaries.EXPECT().Get(gomock.Any(), "1").Return(before, nil)
			f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
			f.uris.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			f.receipts.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			if tt.recheckErr {
				record := entity.QueueRequestStatus{Queue: "q", Status: tt.log, RequestID: "1", ReceivedAtMs: 10, EnteredAtMs: 20}
				gomock.InOrder(
					f.statuses.EXPECT().Put(gomock.Any(), record).Return(nil),
					f.summaries.EXPECT().Get(gomock.Any(), "1").Return(entity.RequestSummary{}, writeErr),
				)
			} else {
				f.expectRecordCalls(tt.calls, tt.failAt, writeErr)
			}

			require.ErrorIs(t, f.materializer.PersistLog(context.Background(), log), writeErr)
		})
	}
}

func TestMaterializer_PersistenceFailures(t *testing.T) {
	writeErr := errors.New("storage unavailable")
	tests := []struct {
		name                         string
		insertErr, getErr, updateErr error
	}{
		{name: "audit insert", insertErr: writeErr},
		{name: "summary read", getErr: writeErr},
		{name: "missing summary", getErr: storage.ErrNotFound},
		{name: "summary update", updateErr: writeErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMaterializerReceiptFixture(gomock.NewController(t))
			current := testRequestSummary()
			log := entity.RequestLog{Queue: current.Queue, RequestID: current.RequestID, TimestampMs: 20, Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusStarted}
			f.logs.EXPECT().Insert(gomock.Any(), log).Return(tt.insertErr)
			wantErr := tt.insertErr
			if tt.insertErr == nil {
				f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, tt.getErr)
				wantErr = tt.getErr
				if tt.getErr == nil {
					f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(tt.updateErr)
					wantErr = tt.updateErr
				}
			}
			require.ErrorIs(t, f.materializer.PersistLog(context.Background(), log), wantErr)
		})
	}
}

func TestLogWins(t *testing.T) {
	base := testRequestSummary()
	tests := []struct {
		name     string
		current  entity.RequestSummary
		incoming entity.RequestLog
		want     bool
	}{
		{
			name:    "accepted activates accepting receipt",
			current: entity.RequestSummary{Status: entity.RequestStatusAccepting, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusAccepted, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "started activates accepting receipt despite older timestamp",
			current: entity.RequestSummary{Status: entity.RequestStatusAccepting, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusStarted, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "late accepted does not replace started",
			current: entity.RequestSummary{Status: entity.RequestStatusStarted, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusAccepted, TimestampMs: 200,
			},
		},
		{
			name:    "versioned terminal beats newer unversioned status",
			current: entity.RequestSummary{Status: entity.RequestStatusSpeculating, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 1, TimestampMs: 100,
			},
			want: true,
		},
		{
			// A head funds several paths and builds them at once, so one build
			// starting says nothing about where the request is: it is still
			// speculating. Letting this win would also freeze the summary,
			// since nothing publishes again until the next position.
			name:    "a build event never becomes the current status",
			current: entity.RequestSummary{Status: entity.RequestStatusSpeculating, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Type: entity.RequestLogTypeEvent, Event: entity.RequestEventBuilding, TimestampMs: 200,
			},
		},
		{
			name:    "a completed build event does not report the request finished",
			current: entity.RequestSummary{Status: entity.RequestStatusSpeculating, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Type: entity.RequestLogTypeEvent, Event: entity.RequestEventBuilt, TimestampMs: 200,
			},
		},
		{
			// The position after the events still moves normally, so the
			// exclusion is of the events themselves, not of that whole window.
			name:    "the position after the build still wins",
			current: entity.RequestSummary{Status: entity.RequestStatusSpeculating, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusSpeculated, TimestampMs: 200,
			},
			want: true,
		},
		{
			name:    "unversioned terminal status has no terminal precedence",
			current: entity.RequestSummary{Status: entity.RequestStatusLanded, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusSpeculating, TimestampMs: 200,
			},
			want: true,
		},
		{
			name:    "nonterminal cannot replace versioned terminal",
			current: entity.RequestSummary{Status: entity.RequestStatusLanded, RequestVersion: 1, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusSpeculating, TimestampMs: 200,
			},
		},
		{
			name:    "higher terminal request version wins",
			current: entity.RequestSummary{Status: entity.RequestStatusError, RequestVersion: 1, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 2, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "lower terminal request version loses",
			current: entity.RequestSummary{Status: entity.RequestStatusLanded, RequestVersion: 2, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusError, RequestVersion: 1, TimestampMs: 200,
			},
		},
		{
			name:    "equal terminal version uses later timestamp",
			current: entity.RequestSummary{Status: entity.RequestStatusError, RequestVersion: 2, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 2, TimestampMs: 200,
			},
			want: true,
		},
		{
			name:    "without terminal winner later timestamp wins",
			current: base,
			incoming: entity.RequestLog{
				Status: entity.RequestStatusStarted, TimestampMs: base.StatusTimestampMs + 1,
			},
			want: true,
		},
		{
			name:    "exact version and timestamp tie keeps current winner",
			current: entity.RequestSummary{Status: entity.RequestStatusError, RequestVersion: 2, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 2, TimestampMs: 200,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Cases above leave Type unset when the entry is a status, so the
			// table reads as being about the ordering rules under test. An
			// untyped entry is a status, matching UnmarshalRequestLog.
			incoming := tt.incoming
			if incoming.Type == "" {
				incoming.Type = entity.RequestLogTypeStatus
			}
			assert.Equal(t, tt.want, logWins(incoming, tt.current))
		})
	}
}

func testRequestSummary() entity.RequestSummary {
	return entity.RequestSummary{
		RequestID: "1", Queue: "q", ChangeURIs: []string{"uri/1", "uri/2"}, ReceivedAtMs: 10,
		Status: entity.RequestStatusAccepting, StatusTimestampMs: 10, Version: 1, Metadata: map[string]string{},
	}
}

func ptr[T any](v T) *T { return &v }
