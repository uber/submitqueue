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

package demo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// Run generates a workload. Returned artifacts remain available on failure.
func Run(ctx context.Context, opts Options, deps Dependencies) (RunResult, error) {
	if opts.Prefix == "" {
		opts.Prefix = "demo"
	}
	if err := opts.validate(); err != nil {
		return RunResult{}, err
	}
	if deps.Source == nil {
		return RunResult{}, fmt.Errorf("a change source is required")
	}
	if opts.RunID == "" {
		var suffix [4]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			return RunResult{}, fmt.Errorf("create run identifier: %w", err)
		}
		opts.RunID = time.Now().Format("0102-150405") + "-" + hex.EncodeToString(suffix[:])
	}
	opts.Folders = resolveFolders(opts.RunID, opts.Folders)
	return runWorkload(ctx, opts, deps, nil)
}

// RunExisting submits existing changes in caller order without invoking Source.
// Each input must pin its revision. Stack ancestry is the caller's responsibility.
func RunExisting(ctx context.Context, opts Options, changes []Change, deps Dependencies) (RunResult, error) {
	opts.Count = len(changes)
	if opts.Files == 0 {
		opts.Files = 1
	}
	if err := opts.validate(); err != nil {
		return RunResult{}, err
	}
	for _, change := range changes {
		if change.URI == "" || change.HeadSHA == "" {
			return RunResult{}, fmt.Errorf("existing changes must include a pinned URI and head revision")
		}
	}
	return runWorkload(ctx, opts, deps, append([]Change(nil), changes...))
}

func (opts Options) validate() error {
	switch {
	case opts.Count < 1:
		return fmt.Errorf("count must be at least 1")
	case opts.Concurrency < 1:
		return fmt.Errorf("concurrency must be at least 1")
	case opts.Files < 1:
		return fmt.Errorf("files must be at least 1")
	case opts.Folders < 0:
		return fmt.Errorf("folders must not be negative")
	case opts.Land && opts.Queue == "":
		return fmt.Errorf("queue is required for submission")
	}
	if strings.ContainsAny(opts.RunID, "/\\") || opts.RunID == "." || opts.RunID == ".." {
		return fmt.Errorf("run identifier must be a single path component")
	}
	for _, part := range strings.Split(opts.Prefix, "/") {
		if part == "." || part == ".." {
			return fmt.Errorf("prefix must not contain relative path components")
		}
	}
	return nil
}

type workload struct {
	opts     Options
	deps     Dependencies
	tracker  *client.Tracker
	changes  []Change
	requests [][]Change
}

func runWorkload(ctx context.Context, opts Options, deps Dependencies, existing []Change) (RunResult, error) {
	if opts.Land && deps.Gateway == nil {
		return RunResult{}, fmt.Errorf("a gateway is required for submission")
	}
	rows := opts.Count
	if opts.Stacked {
		rows = 1
	}
	tracker := client.NewTracker(client.NewRows(rows))
	w := workload{
		opts: opts, deps: deps, tracker: tracker,
		changes: make([]Change, opts.Count), requests: make([][]Change, rows),
	}
	tracker.Note("starting")
	// Polling has its own context: a failed watch must never cancel an
	// in-flight Open or Land, whose outcome would then be unknown.
	pollCtx, stopPolling := context.WithCancel(ctx)
	defer stopPolling()
	recorder := &historyRecorder{gateway: deps.Gateway, histories: make(map[string][]*pb.HistoryEvent)}
	pollDone := make(chan error, 1)
	polling := opts.Land && opts.Watch
	if polling {
		go func() {
			err := tracker.PollHistory(pollCtx, recorder, opts.Queue)
			if err != nil && pollCtx.Err() == nil {
				tracker.Note("watch stopped: %v; submission continues", err)
			}
			pollDone <- err
		}()
	}

	err := w.createAndSubmit(ctx, existing)
	tracker.Seal()
	var pollErr error
	pollJoined := false
	interacted := false
	if err == nil && polling {
		interacted = true
		stop, quit := tracker.Interact(ctx)
		select {
		case <-tracker.Settled():
		case pollErr = <-pollDone:
			pollJoined = true
		case <-ctx.Done():
		case <-quit:
			err = fmt.Errorf("watch interrupted; submitted requests continue remotely")
		}
		stop()
	}
	stopPolling()
	if polling && !pollJoined {
		pollErr = <-pollDone
	}
	if errors.Is(pollErr, context.Canceled) {
		pollErr = nil
	}
	err = errors.Join(err, pollErr)
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		err = errors.Join(err, ctxErr)
	}
	// The interactive view took the table with it; draw it into the scrollback
	// even when the watch ended early, but only let its verdict stand alone.
	if interacted {
		if verdict := tracker.Conclude(); err == nil {
			err = verdict
		}
	}
	if err != nil {
		tracker.Note("run stopped: %v", err)
	} else if !opts.Land {
		tracker.Note("created %d change(s), not enqueued", opts.Count)
	} else if !opts.Watch {
		tracker.Note("enqueued, not watching")
	}
	return w.snapshotResult(recorder), err
}

func (w *workload) createAndSubmit(ctx context.Context, existing []Change) error {
	if w.opts.Stacked {
		return w.createAndSubmitStack(ctx, existing)
	}
	if w.opts.Burst && w.opts.Land {
		if err := w.forEachChange(ctx, func(gate context.Context, i int) error {
			if err := w.createChange(ctx, gate, i, Change{}, false, existing); err != nil {
				return err
			}
			return w.prepareChange(gate, i, i)
		}); err != nil {
			return err
		}
		w.tracker.Note("enqueuing %d prepared changes", w.opts.Count)
		return w.forEachChange(ctx, func(gate context.Context, i int) error {
			return w.submitRequest(ctx, gate, i, []Change{w.changes[i]})
		})
	}
	return w.forEachChange(ctx, func(gate context.Context, i int) error {
		if err := w.createChange(ctx, gate, i, Change{}, false, existing); err != nil {
			return err
		}
		if !w.opts.Land {
			return nil
		}
		if err := w.prepareChange(gate, i, i); err != nil {
			return err
		}
		return w.submitRequest(ctx, gate, i, []Change{w.changes[i]})
	})
}

// forEachChange passes fn a gate that closes when any sibling fails. Mutating
// calls check the gate before starting but run on the caller's context, so a
// sibling failure never cancels an Open or Land whose outcome would be unknown.
func (w *workload) forEachChange(ctx context.Context, fn func(gate context.Context, i int) error) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(w.opts.Concurrency)
	for i := range w.changes {
		if groupCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			if err := groupCtx.Err(); err != nil {
				return err
			}
			return fn(groupCtx, i)
		})
	}
	return errors.Join(group.Wait(), ctx.Err())
}

func (w *workload) createAndSubmitStack(ctx context.Context, existing []Change) error {
	for i := range w.changes {
		parent := Change{}
		if i > 0 {
			parent = w.changes[i-1]
		}
		if err := w.createChange(ctx, ctx, i, parent, i > 0, existing); err != nil {
			return err
		}
	}
	if !w.opts.Land {
		return nil
	}
	// Finish the chain before checks can block its creation.
	w.setReadinessNote(0, "waiting for readiness")
	if err := w.forEachChange(ctx, func(gate context.Context, i int) error {
		return w.waitForReadiness(gate, i)
	}); err != nil {
		return err
	}
	w.setReadinessNote(0, "")
	return w.submitRequest(ctx, ctx, 0, w.changes)
}

func (w *workload) createChange(ctx, gate context.Context, i int, parent Change, hasParent bool, existing []Change) error {
	if err := gate.Err(); err != nil {
		return err
	}
	var opened Change
	var err error
	if existing != nil {
		opened = existing[i]
	} else {
		branch := fmt.Sprintf("%s/%s/%d", w.opts.Prefix, w.opts.RunID, i+1)
		files := make([]File, changeFileCount(w.opts.RunID, i+1, w.opts.Files))
		for k := range files {
			files[k] = File{
				Path:    changeFilePath(w.opts.RunID, w.opts.Folders, i+1, k+1),
				Body:    fmt.Sprintf("change %d of run %s\nfile %d of %d\n", i+1, w.opts.RunID, k+1, len(files)),
				Message: fmt.Sprintf("demo change %d (run %s): file %d of %d", i+1, w.opts.RunID, k+1, len(files)),
			}
		}
		opened, err = w.deps.Source.Open(ctx, ChangeSpec{
			Branch: branch, Title: fmt.Sprintf("demo change %d (run %s)", i+1, w.opts.RunID),
			Files: files, Parent: parent, HasParent: hasParent, Note: w.tracker.Note,
		})
	}
	w.changes[i] = opened
	row := i
	if w.opts.Stacked {
		row = 0
	}
	if opened.URI != "" || opened.Branch != "" {
		w.tracker.Update(func() {
			w.tracker.Rows()[row].Cells = append(w.tracker.Rows()[row].Cells,
				client.Cell{Text: opened.Label, URL: opened.URL})
		})
	}
	if err != nil {
		return err
	}
	if opened.URI == "" || opened.HeadSHA == "" {
		return fmt.Errorf("source returned an unpinned change for %s", opened.Branch)
	}
	return nil
}

func (w *workload) prepareChange(ctx context.Context, i, row int) error {
	w.setReadinessNote(row, "waiting for readiness")
	if err := w.waitForReadiness(ctx, i); err != nil {
		return err
	}
	w.setReadinessNote(row, "")
	return nil
}

func (w *workload) waitForReadiness(ctx context.Context, i int) error {
	if w.deps.Readiness == nil {
		return nil
	}
	if err := w.deps.Readiness.Wait(ctx, w.changes[i]); err != nil {
		return fmt.Errorf("prepare %s: %w", w.changes[i].Label, err)
	}
	return nil
}

func (w *workload) setReadinessNote(row int, note string) {
	if w.deps.Readiness == nil {
		return
	}
	w.tracker.Update(func() { w.tracker.Rows()[row].Note = note })
}

func (w *workload) submitRequest(ctx, gate context.Context, row int, changes []Change) error {
	if err := gate.Err(); err != nil {
		return err
	}
	uris := make([]string, len(changes))
	for i, change := range changes {
		uris[i] = change.URI
	}
	w.tracker.Note("enqueuing %s", strings.Join(uris, ", "))
	id, err := w.deps.Gateway.Land(ctx, w.opts.Queue, uris, w.opts.Strategy)
	if err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("gateway accepted a request without returning its identifier")
	}
	w.requests[row] = append([]Change(nil), changes...)
	w.tracker.Update(func() {
		r := w.tracker.Rows()[row]
		r.SQID, r.Submitted, r.Status = id, time.Now(), "accepted"
	})
	return nil
}

func (w *workload) snapshotResult(recorder *historyRecorder) RunResult {
	result := RunResult{}
	for _, change := range w.changes {
		if change.URI != "" || change.Branch != "" {
			result.Changes = append(result.Changes, change)
		}
	}
	for i, row := range w.tracker.SnapshotRows() {
		if row.SQID != "" {
			result.Requests = append(result.Requests, RequestResult{
				Changes: append([]Change(nil), w.requests[i]...), ID: row.SQID,
				Status: row.Status, History: recorder.histories[row.SQID],
			})
		}
	}
	return result
}

type historyRecorder struct {
	gateway   Gateway
	mu        sync.Mutex
	histories map[string][]*pb.HistoryEvent
}

func (r *historyRecorder) History(ctx context.Context, queue, id string) ([]*pb.HistoryEvent, error) {
	events, err := r.gateway.History(ctx, queue, id)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return events, nil
	}
	cloned := make([]*pb.HistoryEvent, len(events))
	for i, event := range events {
		if event != nil {
			cloned[i] = proto.Clone(event).(*pb.HistoryEvent)
		}
	}
	r.mu.Lock()
	r.histories[id] = cloned
	r.mu.Unlock()
	return events, nil
}
