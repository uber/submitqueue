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

package client

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/errs"
)

type historyReaderFunc func(context.Context, string, string) ([]*pb.HistoryEvent, error)

func (f historyReaderFunc) History(ctx context.Context, queue, sqid string) ([]*pb.HistoryEvent, error) {
	return f(ctx, queue, sqid)
}

type historySourceFunc func(context.Context, *pb.GetRequestHistoryByIDRequest) (*pb.GetRequestHistoryByIDResponse, error)

func (f historySourceFunc) GetRequestHistoryByID(
	ctx context.Context, request *pb.GetRequestHistoryByIDRequest, _ ...grpc.CallOption,
) (*pb.GetRequestHistoryByIDResponse, error) {
	return f(ctx, request)
}

func TestPollHistoryPermanentFailures(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			row := &Row{SQID: "opaque/id", Status: "speculating", Trail: []string{"accepted", "speculating"}}
			tracker := NewTracker([]*Row{row})
			failure := status.Error(code, "read rejected")
			reader := historyReaderFunc(func(_ context.Context, queue, sqid string) ([]*pb.HistoryEvent, error) {
				assert.Equal(t, "dummy-queue", queue)
				assert.Equal(t, "opaque/id", sqid)
				return nil, failure
			})
			var err error
			captureStdout(t, func() {
				tracker.Seal()
				err = tracker.PollHistory(context.Background(), reader, "dummy-queue")
			})
			require.ErrorIs(t, err, failure)
			assert.Equal(t, code, status.Code(err))
			assert.Equal(t, "speculating", row.Status)
			assert.Equal(t, []string{"accepted", "speculating"}, row.Trail)
			assert.False(t, row.Done)
			assert.True(t, row.Settled.IsZero())
			assert.False(t, isClosed(tracker.Settled()))
			assert.NotEmpty(t, row.Note)
		})
	}
}

func TestHistoryTransientDiagnosticsPreserveRecordedError(t *testing.T) {
	for _, failure := range []error{
		status.Error(codes.Unavailable, "temporarily unavailable"),
		status.Error(codes.DeadlineExceeded, "request deadline exceeded"),
		status.Error(codes.ResourceExhausted, "temporarily throttled"),
		errs.NewRetryableError(errors.New("temporary storage failure")),
	} {
		t.Run(fmt.Sprintf("%T-%v", failure, status.Code(failure)), func(t *testing.T) {
			row := &Row{SQID: "1"}
			tracker := NewTracker([]*Row{row})
			reader := historyReaderFunc(func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
				return []*pb.HistoryEvent{{Status: "speculating", LastError: "build failed"}}, nil
			})
			captureStdout(t, func() {
				require.NoError(t, tracker.refreshHistory(context.Background(), reader, "queue", true))
			})
			reader = func(context.Context, string, string) ([]*pb.HistoryEvent, error) { return nil, failure }
			captureStdout(t, func() {
				require.NoError(t, tracker.refreshHistory(context.Background(), reader, "queue", true))
				require.NoError(t, tracker.refreshHistory(context.Background(), reader, "queue", true))
			})
			assert.Equal(t, "build failed; history read: "+failure.Error(), row.Note)
			assert.Equal(t, "speculating", row.Status)
			assert.False(t, row.Done)

			reader = func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
				return []*pb.HistoryEvent{{Status: "landed"}}, nil
			}
			captureStdout(t, func() {
				tracker.Seal()
				require.NoError(t, tracker.refreshHistory(context.Background(), reader, "queue", true))
			})
			assert.Empty(t, row.Note)
			assert.Equal(t, "landed", row.Status)
			assert.True(t, row.Done)
			assert.True(t, isClosed(tracker.Settled()))
		})
	}
}

func TestPollHistoryRetriesUntilSettled(t *testing.T) {
	tracker := NewTracker([]*Row{{SQID: "1"}})
	calls := 0
	reader := historyReaderFunc(func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
		calls++
		if calls == 1 {
			return nil, status.Error(codes.Unavailable, "retry")
		}
		return []*pb.HistoryEvent{{Status: "landed"}}, nil
	})
	captureStdout(t, func() {
		tracker.Seal()
		require.NoError(t, tracker.PollHistory(context.Background(), reader, "queue"))
	})
	assert.Equal(t, 2, calls)
	assert.Equal(t, "landed", tracker.SnapshotRows()[0].Status)
}

func TestHistoryPendingReadPreservesStatus(t *testing.T) {
	for _, failure := range []error{nil, status.Error(codes.NotFound, "not materialized")} {
		t.Run(status.Code(failure).String(), func(t *testing.T) {
			row := &Row{SQID: "1", Status: "speculating", Note: "build failed"}
			tracker := NewTracker([]*Row{row})
			unavailable := historyReaderFunc(func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
				return nil, status.Error(codes.Unavailable, "temporary")
			})
			pending := historyReaderFunc(func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
				return nil, failure
			})
			captureStdout(t, func() {
				require.NoError(t, tracker.refreshHistory(context.Background(), unavailable, "queue", true))
				require.NoError(t, tracker.refreshHistory(context.Background(), pending, "queue", true))
			})
			assert.Equal(t, "speculating", row.Status)
			assert.Equal(t, "build failed", row.Note)
			assert.False(t, row.Done)
		})
	}
}

func TestPollHistoryContextCancellation(t *testing.T) {
	for _, cancelDuringRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel-during-read-%t", cancelDuringRead), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tracker := NewTracker([]*Row{{SQID: "1"}})
			calls := 0
			reader := historyReaderFunc(func(ctx context.Context, _, _ string) ([]*pb.HistoryEvent, error) {
				calls++
				cancel()
				return nil, ctx.Err()
			})
			if !cancelDuringRead {
				cancel()
			}
			captureStdout(t, func() {
				require.ErrorIs(t, tracker.PollHistory(ctx, reader, "queue"), context.Canceled)
			})
			expectedCalls := 0
			if cancelDuringRead {
				expectedCalls = 1
			}
			assert.Equal(t, expectedCalls, calls)
			assert.False(t, tracker.SnapshotRows()[0].Done)
		})
	}
}

func TestLegacyRefreshStillToleratesPermanentErrors(t *testing.T) {
	row := &Row{SQID: "1", Status: "accepted", Note: "existing note"}
	tracker := NewTracker([]*Row{row})
	source := historySourceFunc(func(context.Context, *pb.GetRequestHistoryByIDRequest) (*pb.GetRequestHistoryByIDResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "legacy ignored")
	})
	captureStdout(t, func() { tracker.refresh(context.Background(), source, "queue") })
	assert.Equal(t, "accepted", row.Status)
	assert.Equal(t, "existing note", row.Note)
	assert.False(t, row.Done)
}

func TestTrackerSnapshotRowsDoNotAliasTrackedValues(t *testing.T) {
	tracker := NewTracker([]*Row{{
		SQID:  "1",
		Cells: []Cell{{Text: "#12", URL: "https://example.test/12"}},
		Trail: []string{"accepted"},
	}})
	snapshot := tracker.SnapshotRows()
	snapshot[0].SQID = "mutated"
	snapshot[0].Cells[0].Text = "mutated"
	snapshot[0].Trail[0] = "mutated"

	next := tracker.SnapshotRows()
	assert.Equal(t, "1", next[0].SQID)
	assert.Equal(t, "#12", next[0].Cells[0].Text)
	assert.Equal(t, []string{"accepted"}, next[0].Trail)

	captureStdout(t, func() {
		tracker.Update(func() {
			tracker.Rows()[0].Cells[0].Text = "#13"
			tracker.Rows()[0].Trail[0] = "started"
		})
	})
	assert.Equal(t, "#12", next[0].Cells[0].Text)
	assert.Equal(t, []string{"accepted"}, next[0].Trail)
}
