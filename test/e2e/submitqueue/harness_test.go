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

package e2e_test

// Reusable e2e helpers so tests read as intent, not plumbing. They drive the
// stack through the real gateway gRPC surface (Land / Cancel / Status) and
// observe outcomes on four read-only planes:
//
//   - event plane: pushed events from the queue observer (observer_test.go) —
//     status transitions on the log topic, runway's check/merge answers on the
//     signal topics. await* helpers block on these; no polling.
//   - control plane: the Status RPC, the customer-facing view. It reads the
//     request_log the gateway's log consumer persists, so it lags the published
//     event; awaitStatusRPC bridges the gap with a ctx-bounded poll.
//   - state plane: the operating stores on mysql-app (request, batch). The
//     orchestrator CAS-writes state *before* publishing the corresponding log
//     event, so a state read placed after an observed event is deterministic.
//   - timeline: the request_log audit trail on mysql-app, persisted
//     asynchronously by the gateway's log consumer; awaitStatusesInOrder polls
//     it to convergence.
//
// Every wait is bounded by the suite context deadline (derived from the test
// binary's own deadline in suite_test.go) — there are no per-wait timeout
// constants. pollInterval below is a poll cadence, not a timeout.

import (
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaymqpb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// pollInterval is the cadence of the ctx-bounded polls against the control
// plane and the request_log (both are eventually consistent with the event
// plane). It bounds how often we re-query, never how long we wait.
const pollInterval = 250 * time.Millisecond

// fallbackWaitBudget bounds eventually() only when the test binary runs with
// no deadline at all (go test -timeout=0); under Bazel the deadline is always
// set from the test target's timeout and this value is unused.
const fallbackWaitBudget = time.Hour

// land submits a request with the default REBASE strategy and returns its sqid.
// URIs may carry "sq-fake=<token>" markers to steer negative paths (see
// submitqueue/core/fakemarker); the happy path uses a plain change URI.
func (s *E2EIntegrationSuite) land(queue string, uris ...string) string {
	t := s.T()
	resp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue:    queue,
		Change:   &changepb.Change{Uris: uris},
		Strategy: mergestrategypb.Strategy_REBASE,
	})
	require.NoError(t, err, "Land failed for queue %s", queue)
	require.NotEmpty(t, resp.Sqid, "Land returned an empty sqid for queue %s", queue)
	return resp.Sqid
}

// cancel requests cancellation of a request via the gateway.
func (s *E2EIntegrationSuite) cancel(sqid, reason string) {
	_, err := s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{Sqid: sqid, Reason: reason})
	require.NoError(s.T(), err, "Cancel failed for %s", sqid)
}

// eventually polls cond at pollInterval until it returns true, bounded by the
// suite context deadline (the single wait budget for the whole suite).
func (s *E2EIntegrationSuite) eventually(cond func() bool, msgAndArgs ...interface{}) {
	s.T().Helper()
	waitFor := fallbackWaitBudget
	if deadline, ok := s.ctx.Deadline(); ok {
		waitFor = time.Until(deadline)
	}
	require.Eventually(s.T(), cond, waitFor, pollInterval, msgAndArgs...)
}

// awaitEvent blocks until the observer has recorded an event matching match.
// desc names the awaited condition in the failure message.
func (s *E2EIntegrationSuite) awaitEvent(desc string, match func(pipelineEvent) bool) pipelineEvent {
	s.T().Helper()
	ev, err := s.observer.recorder.await(s.ctx, match)
	require.NoErrorf(s.T(), err, "no %s observed before the suite deadline", desc)
	return ev
}

// awaitStatusEvent blocks until the pipeline publishes the given status for the
// request on the log topic. Because every orchestrator controller CAS-writes
// request state before publishing the matching log entry, state-plane reads
// placed after this call observe the corresponding state deterministically.
func (s *E2EIntegrationSuite) awaitStatusEvent(sqid string, want entity.RequestStatus) {
	s.T().Helper()
	s.log.Logf("Awaiting %q status event for %s", want, sqid)
	s.awaitEvent(string(want)+" status event for "+sqid, func(e pipelineEvent) bool {
		return e.topic == topickey.TopicKeyLog && e.requestID == sqid && e.status == want
	})
}

// awaitCheckRequested blocks until the orchestrator hands the request's
// merge-conflict check to runway (validate published to the runway-owned
// merge-conflict-check topic). This is the "parked at the runway boundary"
// marker: with runway paused, a request that reached this point can make no
// further forward progress until runway resumes.
func (s *E2EIntegrationSuite) awaitCheckRequested(sqid string) {
	s.T().Helper()
	s.log.Logf("Awaiting merge-conflict-check request for %s", sqid)
	s.awaitEvent("merge-conflict-check request for "+sqid, func(e pipelineEvent) bool {
		return e.topic == runwaymq.TopicKeyMergeConflictCheck && e.requestID == sqid
	})
}

// awaitCheckSignal blocks until runway publishes the merge-conflict-check
// result for the request and returns it. Proof on the wire that runway consumed
// and answered the check.
func (s *E2EIntegrationSuite) awaitCheckSignal(sqid string) pipelineEvent {
	s.T().Helper()
	s.log.Logf("Awaiting merge-conflict-check signal for %s", sqid)
	return s.awaitEvent("merge-conflict-check signal for "+sqid, func(e pipelineEvent) bool {
		return e.topic == runwaymq.TopicKeyMergeConflictCheckSignal && e.requestID == sqid
	})
}

// awaitMergeSignal blocks until runway publishes a committing-merge result
// covering the request and returns it. Merge results carry the batch ID as the
// top-level correlation ID; the request is matched through the step IDs, which
// the orchestrator sets to the request sqid.
func (s *E2EIntegrationSuite) awaitMergeSignal(sqid string) pipelineEvent {
	s.T().Helper()
	s.log.Logf("Awaiting merge signal covering %s", sqid)
	return s.awaitEvent("merge signal covering "+sqid, func(e pipelineEvent) bool {
		if e.topic != runwaymq.TopicKeyMergeSignal {
			return false
		}
		for _, id := range e.stepIDs {
			if id == sqid {
				return true
			}
		}
		return false
	})
}

// currentStatus reads the request's current customer-facing status via the
// Status RPC. A transport error is returned so callers can keep polling.
func (s *E2EIntegrationSuite) currentStatus(sqid string) (entity.RequestStatus, error) {
	resp, err := s.gatewayClient.Status(s.ctx, &gatewaypb.StatusRequest{Sqid: sqid})
	if err != nil {
		return entity.RequestStatusUnknown, err
	}
	return entity.RequestStatus(resp.Status), nil
}

// awaitStatusRPC polls the Status RPC until it reports want. The Status RPC
// reads the request_log persisted by the gateway's log consumer, which lags the
// published event the test already observed — this bridges that persistence
// gap; it is not the primary wait.
func (s *E2EIntegrationSuite) awaitStatusRPC(sqid string, want entity.RequestStatus) {
	s.T().Helper()
	s.eventually(func() bool {
		got, err := s.currentStatus(sqid)
		if err != nil {
			s.log.Logf("Status(%s) not ready yet: %v", sqid, err)
			return false
		}
		s.log.Logf("Status(%s) = %q (want %q)", sqid, got, want)
		return got == want
	}, "request %s should reach status %q on the Status RPC", sqid, want)
}

// timeline returns the ordered status history from the request_log (the audit
// trail persisted by the gateway log consumer on mysql-app). These are the
// customer-facing RequestStatus values — the only ordered history in the system
// (the internal RequestState is point-in-time, see terminalState).
func (s *E2EIntegrationSuite) timeline(sqid string) []entity.RequestStatus {
	t := s.T()
	logs, err := s.requestLog.List(s.ctx, sqid)
	require.NoError(t, err, "failed to list request_log for %s", sqid)
	statuses := make([]entity.RequestStatus, len(logs))
	for i, l := range logs {
		statuses[i] = l.Status
	}
	return statuses
}

// containsInOrder reports whether want appears as an ordered subsequence of got.
func containsInOrder(got []entity.RequestStatus, want []entity.RequestStatus) bool {
	matched := 0
	for _, st := range got {
		if matched < len(want) && st == want[matched] {
			matched++
		}
	}
	return matched == len(want)
}

// awaitStatusesInOrder polls the request_log until want appears as an ordered
// subsequence of the persisted status timeline. It tolerates intermediate
// statuses (so it is not a change-detector), asserting only the relative order
// of the statuses that matter. Polling (rather than a pure event await) is
// needed because persistence into the request_log is the gateway log consumer's
// job and lags the events the observer sees.
func (s *E2EIntegrationSuite) awaitStatusesInOrder(sqid string, want ...entity.RequestStatus) {
	s.T().Helper()
	s.eventually(func() bool {
		got := s.timeline(sqid)
		if containsInOrder(got, want) {
			return true
		}
		s.log.Logf("request_log for %s not yet %v; currently %v", sqid, want, got)
		return false
	}, "request_log for %s should contain %v as an ordered subsequence", sqid, want)
}

// assertStatusNeverLogged asserts that the persisted timeline contains no entry
// with the given status. Call it after the terminal status has been persisted
// (awaitStatusRPC/awaitStatusesInOrder), when the timeline is complete.
func (s *E2EIntegrationSuite) assertStatusNeverLogged(sqid string, status entity.RequestStatus) {
	s.T().Helper()
	got := s.timeline(sqid)
	assert.NotContainsf(s.T(), got, status,
		"request_log for %s should never contain %q; got %v", sqid, status, got)
}

// terminalState reads the request's current internal RequestState from the
// operating store (mysql-app). Unlike the status timeline, RequestState is
// point-in-time — the Request entity is updated in place under optimistic
// locking, so only the current (terminal, once settled) value is observable.
func (s *E2EIntegrationSuite) terminalState(sqid string) entity.RequestState {
	t := s.T()
	req, err := s.requestStore.Get(s.ctx, sqid)
	require.NoError(t, err, "failed to get request %s from operating store", sqid)
	return req.State
}

// assertNoBatchContains asserts that no batch in the queue — active or terminal
// — ever enrolled the request.
func (s *E2EIntegrationSuite) assertNoBatchContains(queue, sqid string) {
	t := s.T()
	allStates := append(entity.ActiveBatchStates(),
		entity.BatchStateSucceeded, entity.BatchStateFailed, entity.BatchStateCancelled)
	batches, err := s.batchStore.GetByQueueAndStates(s.ctx, queue, allStates)
	require.NoError(t, err, "failed to list batches for queue %s", queue)
	for _, b := range batches {
		assert.NotContainsf(t, b.Contains, sqid,
			"batch %s should not contain request %s", b.ID, sqid)
	}
}

// lastError returns the LastError reported by the Status RPC (populated on the
// error path).
func (s *E2EIntegrationSuite) lastError(sqid string) string {
	t := s.T()
	resp, err := s.gatewayClient.Status(s.ctx, &gatewaypb.StatusRequest{Sqid: sqid})
	require.NoError(t, err, "Status failed for %s", sqid)
	return resp.LastError
}

// assertOutcomeSucceeded asserts a runway signal event reports SUCCEEDED.
func (s *E2EIntegrationSuite) assertOutcomeSucceeded(ev pipelineEvent, what string) {
	assert.Equalf(s.T(), runwaymqpb.Outcome_SUCCEEDED, ev.outcome,
		"%s should report SUCCEEDED, got %s", what, ev.outcome)
}
