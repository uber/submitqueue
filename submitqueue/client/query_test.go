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

package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

func TestListRequiresAQueue(t *testing.T) {
	sq, stop := dial(t, &pagingGateway{})
	defer stop()

	_, err := sq.List(context.Background(), ListQuery{})
	require.Error(t, err)
}

func TestListFollowsPages(t *testing.T) {
	gw := &pagingGateway{pages: [][]string{
		{"q/1", "q/2"},
		{"q/3", "q/4"},
		{"q/5"},
	}}
	sq, stop := dial(t, gw)
	defer stop()

	got, err := sq.List(context.Background(), ListQuery{Queue: "q"})
	require.NoError(t, err)

	assert.Equal(t, []string{"q/1", "q/2", "q/3", "q/4", "q/5"}, sqidsOf(got))
	assert.Equal(t, 3, gw.calls, "every page is fetched")
}

func TestListStopsAtTheLimit(t *testing.T) {
	// The limit is across pages, not per page, so it has to cut a page short
	// and stop asking for more rather than over-fetching the whole queue.
	gw := &pagingGateway{pages: [][]string{
		{"q/1", "q/2"},
		{"q/3", "q/4"},
		{"q/5"},
	}}
	sq, stop := dial(t, gw)
	defer stop()

	got, err := sq.List(context.Background(), ListQuery{Queue: "q", Limit: 3})
	require.NoError(t, err)

	assert.Equal(t, []string{"q/1", "q/2", "q/3"}, sqidsOf(got))
	assert.Equal(t, 2, gw.calls, "the third page is never asked for")
}

func TestListStopsOnAnEmptyPage(t *testing.T) {
	// A server that keeps handing back a token with nothing in the page would
	// otherwise spin forever.
	gw := &pagingGateway{pages: [][]string{{"q/1"}, {}}, alwaysToken: true}
	sq, stop := dial(t, gw)
	defer stop()

	got, err := sq.List(context.Background(), ListQuery{Queue: "q"})
	require.NoError(t, err)

	assert.Equal(t, []string{"q/1"}, sqidsOf(got))
	assert.Equal(t, 2, gw.calls)
}

func TestListSinceBecomesAReceiptBound(t *testing.T) {
	gw := &pagingGateway{pages: [][]string{{"q/1"}}}
	sq, stop := dial(t, gw)
	defer stop()

	before := time.Now().Add(-time.Hour).UnixMilli()
	_, err := sq.List(context.Background(), ListQuery{Queue: "q", Since: time.Hour})
	require.NoError(t, err)
	after := time.Now().Add(-time.Hour).UnixMilli()

	assert.GreaterOrEqual(t, gw.lastRequest.GetReceivedAtOrAfterMs(), before)
	assert.LessOrEqual(t, gw.lastRequest.GetReceivedAtOrAfterMs(), after)
}

func TestListWithoutSinceLeavesTheWindowOpen(t *testing.T) {
	gw := &pagingGateway{pages: [][]string{{"q/1"}}}
	sq, stop := dial(t, gw)
	defer stop()

	_, err := sq.List(context.Background(), ListQuery{Queue: "q"})
	require.NoError(t, err)

	assert.Zero(t, gw.lastRequest.GetReceivedAtOrAfterMs(),
		"no window means all retained history, not a bound of zero-time")
}

func TestRowsFromSummaries(t *testing.T) {
	received := time.Now().Add(-5 * time.Minute)
	rows := RowsFromSummaries([]*pb.RequestSummary{
		{
			Sqid:         "q/1",
			Status:       "landed",
			ChangeUris:   []string{"github://h/o/r/pull/1/abc"},
			ReceivedAtMs: received.UnixMilli(),
		},
		nil, // a hole in the page is skipped rather than becoming a blank row
		{Sqid: "q/2", Status: "batched", LastError: "still going"},
	})

	require.Len(t, rows, 2)

	assert.Equal(t, "q/1", rows[0].SQID)
	assert.Equal(t, []Cell{{Text: "github://h/o/r/pull/1/abc"}}, rows[0].Cells)
	assert.True(t, rows[0].Done, "a terminal status arrives already settled")
	assert.Equal(t, received.UnixMilli(), rows[0].Submitted.UnixMilli())

	assert.Equal(t, "q/2", rows[1].SQID)
	assert.False(t, rows[1].Done, "an active request is still outstanding")
	assert.Equal(t, "still going", rows[1].Note)
}

// pagingGateway answers List from a fixed set of pages, handing out a
// continuation token until they run out.
type pagingGateway struct {
	pb.UnimplementedSubmitQueueGatewayServer
	pages [][]string
	// alwaysToken keeps returning a token even past the last page, standing for
	// a server that never signals the end.
	alwaysToken bool

	calls       int
	lastRequest *pb.ListRequest
}

func (g *pagingGateway) List(_ context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	g.lastRequest = req
	page := g.calls
	g.calls++

	if page >= len(g.pages) {
		return &pb.ListResponse{}, nil
	}

	requests := make([]*pb.RequestSummary, 0, len(g.pages[page]))
	for _, sqid := range g.pages[page] {
		requests = append(requests, &pb.RequestSummary{Sqid: sqid, Queue: req.GetQueue()})
	}

	token := ""
	if g.alwaysToken || page+1 < len(g.pages) {
		token = fmt.Sprintf("page-%d", page+1)
	}
	return &pb.ListResponse{Requests: requests, NextPageToken: token}, nil
}

func dial(t *testing.T, gw pb.SubmitQueueGatewayServer) (*Client, func()) {
	t.Helper()

	addr, stop := serve(t, gw)
	sq, err := New(Options{Addr: addr})
	require.NoError(t, err)

	return sq, func() {
		_ = sq.Close()
		stop()
	}
}

func sqidsOf(summaries []*pb.RequestSummary) []string {
	out := make([]string, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, s.GetSqid())
	}
	return out
}
