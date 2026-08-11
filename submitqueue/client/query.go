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
	"time"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

// ListQuery selects a page range of a queue's receipt history.
type ListQuery struct {
	// Queue is the exact queue to read. Required: the gateway has no
	// cross-queue listing, because a request id is only resolvable within its
	// own queue.
	Queue string

	// Since bounds the window to requests received within it. Zero reads from
	// the beginning of retained history.
	Since time.Duration

	// Limit caps how many requests are returned across all pages. Zero means
	// every request in the window, which for a busy queue is a lot of paging.
	Limit int

	// PageSize is what each page requests. Zero takes the server default.
	PageSize int
}

// List reads a queue's requests, newest first, following continuation tokens
// until the limit is reached or the queue is exhausted.
//
// Paging is followed here rather than exposed, because a caller asking what a
// queue is doing wants the answer, not a cursor. A caller that needs the cursor
// can reach the generated client through Gateway.
func (c *Client) List(ctx context.Context, q ListQuery) ([]*pb.RequestSummary, error) {
	if q.Queue == "" {
		return nil, fmt.Errorf("queue must not be empty")
	}

	req := &pb.ListRequest{Queue: q.Queue, PageSize: int32(q.PageSize)}
	if q.Since > 0 {
		req.ReceivedAtOrAfterMs = time.Now().Add(-q.Since).UnixMilli()
	}

	var out []*pb.RequestSummary
	for {
		resp, err := c.gw.List(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("list %s failed: %w", q.Queue, err)
		}

		out = append(out, resp.GetRequests()...)
		if q.Limit > 0 && len(out) >= q.Limit {
			return out[:q.Limit], nil
		}

		// An empty token is the last page. An empty page with a token would
		// otherwise spin, so a page that added nothing also ends the walk.
		if resp.GetNextPageToken() == "" || len(resp.GetRequests()) == 0 {
			return out, nil
		}
		req.PageToken = resp.GetNextPageToken()
	}
}

// Summary reads one request's current status.
func (c *Client) Summary(ctx context.Context, queue, sqid string) (*pb.RequestSummary, error) {
	if queue == "" || sqid == "" {
		return nil, fmt.Errorf("queue and sqid are both required")
	}
	resp, err := c.gw.GetRequestSummaryByID(ctx, &pb.GetRequestSummaryByIDRequest{Sqid: sqid, Queue: queue})
	if err != nil {
		return nil, fmt.Errorf("status of %s failed: %w", sqid, err)
	}
	if resp.GetRequest() == nil {
		return nil, fmt.Errorf("no request found for %q", sqid)
	}
	return resp.GetRequest(), nil
}

// History reads the events recorded for one request, oldest first.
func (c *Client) History(ctx context.Context, queue, sqid string) ([]*pb.HistoryEvent, error) {
	if queue == "" || sqid == "" {
		return nil, fmt.Errorf("queue and sqid are both required")
	}
	resp, err := c.gw.GetRequestHistoryByID(ctx, &pb.GetRequestHistoryByIDRequest{Sqid: sqid, Queue: queue})
	if err != nil {
		return nil, fmt.Errorf("history of %s failed: %w", sqid, err)
	}
	return resp.GetEvents(), nil
}

// RowsFromSummaries builds watchable rows from a listing, newest first.
//
// The changes column carries each request's change URIs, which is all a client
// watching a queue it did not create knows about them. A caller that knows
// more — a tool that just opened the pull requests, say — sets richer cells of
// its own.
//
// A summary records when a request was received but not when it settled, so a
// row built here has no settle time and its elapsed column reads as age since
// receipt. For a request still in flight that is the same number; for one that
// finished long ago it is how long ago, which is the useful thing to show in a
// listing anyway.
func RowsFromSummaries(summaries []*pb.RequestSummary) []*Row {
	rows := make([]*Row, 0, len(summaries))
	for _, s := range summaries {
		if s == nil {
			continue
		}
		cells := make([]Cell, 0, len(s.GetChangeUris()))
		for _, uri := range s.GetChangeUris() {
			cells = append(cells, Cell{Text: uri})
		}
		rows = append(rows, &Row{
			SQID:      s.GetSqid(),
			Cells:     cells,
			Status:    s.GetStatus(),
			Note:      s.GetLastError(),
			Submitted: time.UnixMilli(s.GetReceivedAtMs()),
			Done:      terminalStatuses[s.GetStatus()],
		})
	}
	return rows
}
