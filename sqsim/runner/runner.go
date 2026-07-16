// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package runner submits and verifies SQSim scenarios against SubmitQueue.
package runner

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/uber/submitqueue/sqsim"
	"github.com/uber/submitqueue/sqsim/model"
)

const (
	maxSubmissionsPerCycle     = 25
	maxSubmissionsBetweenPolls = 100
)

// Gateway is the public SubmitQueue surface used by SQSim.
type Gateway interface {
	// Land submits one synthetic change.
	Land(ctx context.Context, queue, changeURI string) (string, error)
	// List returns one page of request summaries for a queue.
	List(ctx context.Context, queue string, receivedAtOrAfterMs, receivedBeforeMs int64, pageToken string) ([]Summary, string, error)
	// Summary returns the current summary for one sqid.
	Summary(ctx context.Context, sqid string) (Summary, error)
	// History returns retained lifecycle events for one sqid.
	History(ctx context.Context, sqid string) ([]HistoryEvent, error)
}

// Summary is the public current view of one request.
type Summary struct {
	// SQID is the request identifier returned by Land.
	SQID string
	// Status is the customer-facing request status.
	Status string
	// LastError is the latest reported failure.
	LastError string
	// Metadata is display and debugging context.
	Metadata map[string]string
}

// HistoryEvent is one retained public lifecycle event.
type HistoryEvent struct {
	// TimestampMs is the event creation time in Unix milliseconds.
	TimestampMs int64
	// Status is the customer-facing request status.
	Status string
	// LastError is the error associated with the event.
	LastError string
	// Metadata is event-specific context.
	Metadata map[string]string
}

// Request is the runner's current view of one scenario Land.
type Request struct {
	// Name is the Land name from the scenario.
	Name string
	// SQID is the request identifier returned by Gateway Land.
	SQID string
	// Status is the latest public request status.
	Status string
	// Expected is the required terminal status.
	Expected string
	// LastError is the latest public failure.
	LastError string
	// Metadata is the latest public display context.
	Metadata map[string]string
	// History is populated lazily after a terminal status is confirmed.
	History []HistoryEvent
}

// Snapshot is one observable point in a scenario run.
type Snapshot struct {
	// Scenario is the public scenario name.
	Scenario string
	// StartedAt is the run start time.
	StartedAt time.Time
	// Now is the observation time.
	Now time.Time
	// Requests are in scenario declaration order.
	Requests []Request
	// Done reports whether every request is terminal.
	Done bool
}

// Observer receives snapshots when visible state changes.
type Observer interface {
	// Observe receives an immutable snapshot.
	Observe(Snapshot)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(Snapshot)

// Observe calls the wrapped function.
func (f ObserverFunc) Observe(snapshot Snapshot) {
	f(snapshot)
}

// Report is the verified result of a completed run.
type Report struct {
	// Scenario is the public scenario name.
	Scenario string
	// Requests are the final request views.
	Requests []Request
	// Passed is true when every terminal status matched its expectation.
	Passed bool
}

// Options configure one engine run.
type Options struct {
	// ScenarioName is the selected public scenario name.
	ScenarioName string
	// Scenario is the immutable workload.
	Scenario sqsim.Scenario
	// Gateway is the public API client.
	Gateway Gateway
	// Clock supplies wall time and cancellable waits.
	Clock model.Clock
	// PollInterval bounds public API polling frequency.
	PollInterval time.Duration
	// Observer receives visible state changes.
	Observer Observer
}

type requestState struct {
	request          Request
	queue            string
	changeURI        string
	submitAfter      time.Duration
	submitted        bool
	terminalObserved bool
	historyLoaded    bool
	historyDirty     bool
}

// Run executes and verifies one scenario.
func Run(ctx context.Context, options Options) (Report, error) {
	if options.Gateway == nil {
		return Report{}, fmt.Errorf("gateway is required")
	}
	if options.Clock == nil {
		return Report{}, fmt.Errorf("clock is required")
	}
	if options.PollInterval <= 0 {
		return Report{}, fmt.Errorf("poll interval must be positive")
	}
	if err := sqsim.Validate(options.Scenario); err != nil {
		return Report{}, fmt.Errorf("validate scenario: %w", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(options.Scenario.TimeoutMs)*time.Millisecond)
	defer cancel()

	startedAt := options.Clock.Now()
	states := make([]requestState, len(options.Scenario.Lands))
	for i, land := range options.Scenario.Lands {
		changeURI, err := model.ChangeURI(options.ScenarioName, land.Name)
		if err != nil {
			return Report{}, err
		}
		states[i] = requestState{
			request: Request{
				Name:     land.Name,
				Expected: string(land.Expectation.Status),
				Metadata: map[string]string{},
			},
			queue:       land.Queue,
			changeURI:   changeURI,
			submitAfter: time.Duration(land.SubmitAfterMs) * time.Millisecond,
		}
	}

	emit(options, startedAt, states, false)
	submitted := 0
	submittedSincePoll := 0
	for {
		now := options.Clock.Now()
		submittedThisCycle := 0
		for i := range states {
			state := &states[i]
			if state.submitted {
				continue
			}
			if now.Sub(startedAt) < state.submitAfter {
				continue
			}
			sqid, err := options.Gateway.Land(runCtx, state.queue, state.changeURI)
			if err != nil {
				return Report{}, fmt.Errorf("land %q: %w", state.request.Name, err)
			}
			if sqid == "" {
				return Report{}, fmt.Errorf("land %q returned an empty sqid", state.request.Name)
			}
			state.request.SQID = sqid
			state.request.Status = "submitted"
			state.submitted = true
			submitted++
			submittedThisCycle++
			if submittedThisCycle == maxSubmissionsPerCycle {
				break
			}
		}
		if submittedThisCycle > 0 {
			submittedSincePoll += submittedThisCycle
			emit(options, startedAt, states, false)
		}

		pollChanged := false
		shouldPoll := submitted == len(states) ||
			submittedThisCycle == 0 ||
			submittedSincePoll >= maxSubmissionsBetweenPolls
		if shouldPoll {
			var err error
			pollChanged, err = poll(runCtx, options, startedAt, states)
			if err != nil {
				if runCtx.Err() != nil {
					return Report{}, fmt.Errorf("scenario %q timed out: %w", options.ScenarioName, runCtx.Err())
				}
				return Report{}, err
			}
			submittedSincePoll = 0
		}

		done := submitted == len(states) && allTerminal(states)
		if pollChanged || done {
			emit(options, startedAt, states, done)
		}
		if done {
			requests := requestsFrom(states)
			passed := true
			for _, request := range requests {
				if request.Status != request.Expected {
					passed = false
				}
			}
			return Report{Scenario: options.ScenarioName, Requests: requests, Passed: passed}, nil
		}

		wait := options.PollInterval
		for i := range states {
			if states[i].submitted {
				continue
			}
			untilSubmit := states[i].submitAfter - options.Clock.Now().Sub(startedAt)
			if untilSubmit < wait {
				wait = untilSubmit
			}
		}
		if wait < 0 {
			wait = 0
		}
		if err := options.Clock.Wait(runCtx, wait); err != nil {
			return Report{}, fmt.Errorf("scenario %q timed out: %w", options.ScenarioName, err)
		}
	}
}

func poll(ctx context.Context, options Options, startedAt time.Time, states []requestState) (bool, error) {
	if len(states) == 0 {
		return false, nil
	}
	bySQID := make(map[string]*requestState, len(states))
	queues := make(map[string]struct{})
	for i := range states {
		if !states[i].submitted {
			continue
		}
		bySQID[states[i].request.SQID] = &states[i]
		queues[states[i].queue] = struct{}{}
	}

	found := make(map[string]struct{}, len(states))
	queueNames := make([]string, 0, len(queues))
	for queue := range queues {
		queueNames = append(queueNames, queue)
	}
	sort.Strings(queueNames)

	changed := false
	for _, queue := range queueNames {
		pageToken := ""
		for {
			summaries, nextToken, err := options.Gateway.List(
				ctx,
				queue,
				startedAt.Add(-time.Second).UnixMilli(),
				options.Clock.Now().Add(time.Minute).UnixMilli(),
				pageToken,
			)
			if err != nil {
				break
			}
			for _, summary := range summaries {
				state, ok := bySQID[summary.SQID]
				if !ok {
					continue
				}
				found[summary.SQID] = struct{}{}
				if applySummary(state, summary) {
					changed = true
					state.historyDirty = true
				}
			}
			if nextToken == "" {
				break
			}
			pageToken = nextToken
		}
	}

	for i := range states {
		state := &states[i]
		if !state.submitted {
			continue
		}
		_, listed := found[state.request.SQID]
		if !listed || (isTerminal(state.request.Status) && !state.terminalObserved) {
			summary, err := options.Gateway.Summary(ctx, state.request.SQID)
			if err == nil {
				if applySummary(state, summary) {
					changed = true
					state.historyDirty = true
				}
			}
		}
		if isTerminal(state.request.Status) {
			state.terminalObserved = true
		}
		if state.historyDirty || (isTerminal(state.request.Status) && !state.historyLoaded) {
			history, err := options.Gateway.History(ctx, state.request.SQID)
			if err == nil {
				if !equalHistory(state.request.History, history) {
					state.request.History = cloneHistory(history)
					changed = true
				}
				state.historyDirty = false
				if isTerminal(state.request.Status) {
					state.historyLoaded = true
				}
			}
		}
	}
	return changed, nil
}

func applySummary(state *requestState, summary Summary) bool {
	if summary.Status == "" {
		return false
	}
	changed := state.request.Status != summary.Status ||
		state.request.LastError != summary.LastError ||
		!equalMetadata(state.request.Metadata, summary.Metadata)
	state.request.Status = summary.Status
	state.request.LastError = summary.LastError
	state.request.Metadata = cloneMetadata(summary.Metadata)
	return changed
}

func allTerminal(states []requestState) bool {
	for _, state := range states {
		if !state.submitted || !isTerminal(state.request.Status) || !state.historyLoaded {
			return false
		}
	}
	return true
}

func isTerminal(status string) bool {
	return status == string(sqsim.RequestLanded) ||
		status == string(sqsim.RequestError) ||
		status == string(sqsim.RequestCancelled)
}

func emit(options Options, startedAt time.Time, states []requestState, done bool) {
	if options.Observer == nil {
		return
	}
	options.Observer.Observe(Snapshot{
		Scenario:  options.ScenarioName,
		StartedAt: startedAt,
		Now:       options.Clock.Now(),
		Requests:  requestsFrom(states),
		Done:      done,
	})
}

func requestsFrom(states []requestState) []Request {
	requests := make([]Request, len(states))
	for i, state := range states {
		requests[i] = state.request
		requests[i].Metadata = cloneMetadata(state.request.Metadata)
		requests[i].History = cloneHistory(state.request.History)
	}
	return requests
}

func cloneHistory(history []HistoryEvent) []HistoryEvent {
	cloned := make([]HistoryEvent, len(history))
	for i, event := range history {
		cloned[i] = event
		cloned[i].Metadata = cloneMetadata(event.Metadata)
	}
	return cloned
}

func cloneMetadata(metadata map[string]string) map[string]string {
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func equalMetadata(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func equalHistory(left, right []HistoryEvent) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].TimestampMs != right[i].TimestampMs ||
			left[i].Status != right[i].Status ||
			left[i].LastError != right[i].LastError ||
			!equalMetadata(left[i].Metadata, right[i].Metadata) {
			return false
		}
	}
	return true
}
