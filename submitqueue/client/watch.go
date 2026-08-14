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
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

// HistorySource is the part of the gateway a watch reads from.
//
// It is narrowed to the one call rather than taking the generated client whole,
// so a caller watching requests can be handed something that only knows how to
// answer for them. The generated client satisfies it as it stands.
type HistorySource interface {
	GetRequestHistoryByID(
		ctx context.Context,
		in *pb.GetRequestHistoryByIDRequest,
		opts ...grpc.CallOption,
	) (*pb.GetRequestHistoryByIDResponse, error)
}

// Tracker owns the rows and the table drawn from them.
//
// Two things move a run forward at once — whatever is producing requests, and
// the poll that reads their statuses — and both draw the same table, so the
// mutex is what keeps one from redrawing halfway through the other's update.
type Tracker struct {
	mu     sync.Mutex
	rows   []*Row
	r      *renderer
	status string
	// sealed records that every request that will be watched is known. Without
	// it, polling would find nothing outstanding before the first request
	// existed and call the run finished.
	sealed bool

	// settled closes once every request has reached a terminal status.
	settled chan struct{}
	once    sync.Once
}

// NewTracker returns a Tracker over the given rows.
func NewTracker(rows []*Row) *Tracker {
	return &Tracker{rows: rows, r: newRenderer(), settled: make(chan struct{})}
}

// Rows are the rows the tracker draws. Mutate them only from inside Update,
// which holds the lock the poll also takes.
func (t *Tracker) Rows() []*Row {
	return t.rows
}

// Settled closes once every request has reached a terminal status.
func (t *Tracker) Settled() <-chan struct{} {
	return t.settled
}

// Note replaces the line under the table and redraws.
func (t *Tracker) Note(format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = fmt.Sprintf(format, args...)
	t.r.draw(t.rows, t.status)
}

// Update applies a change to the rows and redraws with it.
func (t *Tracker) Update(fn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fn()
	t.r.draw(t.rows, t.status)
}

// Conclude draws the verdict and reports whether everything landed. It reads
// the rows under the lock because a poll may still be applying its last round.
func (t *Tracker) Conclude() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = outcome(t.rows)
	t.r.draw(t.rows, t.status)
	return summarize(t.rows)
}

// Seal declares that nothing further will be watched, which is what lets an
// otherwise-finished run conclude.
func (t *Tracker) Seal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sealed = true
	t.signalLocked()
}

// signalLocked closes settled once there is nothing left to wait for.
func (t *Tracker) signalLocked() {
	if !t.sealed {
		return
	}
	for _, rw := range t.rows {
		if rw.SQID == "" || !rw.Done {
			return
		}
	}
	t.once.Do(func() { close(t.settled) })
}

// Poll re-reads statuses until the run finishes or the context ends.
func (t *Tracker) Poll(ctx context.Context, src HistorySource, queue string) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.settled:
			return
		case <-ticker.C:
		}
		t.refresh(ctx, src, queue)
	}
}

// refresh re-reads every request that has been accepted but has not settled.
//
// The reads happen outside the lock. Holding it across a round of RPCs would
// stall whatever is producing requests behind the network, and letting that run
// ahead is the whole point of watching each request the moment it exists.
func (t *Tracker) refresh(ctx context.Context, src HistorySource, queue string) {
	t.mu.Lock()
	outstanding := make([]*Row, 0, len(t.rows))
	for _, rw := range t.rows {
		if rw.SQID != "" && !rw.Done {
			outstanding = append(outstanding, rw)
		}
	}
	total := len(t.rows)
	t.mu.Unlock()

	type reading struct {
		rw     *Row
		trail  []string
		status string
		note   string
	}
	readings := make([]reading, 0, len(outstanding))
	for _, rw := range outstanding {
		// SQID is written once, before the row becomes outstanding, so reading
		// it here without the lock is safe.
		resp, err := src.GetRequestHistoryByID(ctx, &pb.GetRequestHistoryByIDRequest{Sqid: rw.SQID, Queue: queue})
		if err != nil || resp == nil || len(resp.Events) == 0 {
			// A history that is not readable yet is normal right after Land;
			// the next tick picks it up.
			continue
		}
		trail, status, note := digest(resp.Events)
		readings = append(readings, reading{rw: rw, trail: trail, status: status, note: note})
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	settled := 0
	for _, got := range readings {
		got.rw.Trail, got.rw.Status, got.rw.Note = got.trail, got.status, got.note
		if terminalStatuses[got.status] && !got.rw.Done {
			// Stamped from the local clock rather than the event timestamp so
			// the elapsed column is measured end to end against one clock.
			got.rw.Done, got.rw.Settled = true, time.Now()
		}
	}
	for _, rw := range t.rows {
		if rw.Done {
			settled++
		}
	}

	t.status = fmt.Sprintf("%d of %d settled", settled, total)
	t.r.draw(t.rows, t.status)
	t.signalLocked()
}
