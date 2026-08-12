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
// stack through the real gateway gRPC surface (Land / Cancel / GetRequestSummaryByID) and
// observe outcomes two ways:
//
//   - black-box, by polling the GetRequestSummaryByID RPC to a target/terminal status; and
//   - black-box, by reading the ordered stage progression through GetRequestHistoryByID.
//
// The pipeline consumers run inside containers, so there is no in-process
// signal to await. Polling continues until the condition holds or Bazel's test
// timeout terminates a genuinely stuck suite.

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func pollUntil(interval time.Duration, condition func() bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		<-ticker.C
	}
}

// request identifies one landed request. A sqid is only resolvable within its own
// queue, so the read APIs take both and the harness carries the pair together
// rather than threading a bare sqid.
type request struct {
	queue string
	sqid  string
}

// land submits a request with the default REBASE strategy and returns its identity.
// URIs may carry "sq-fake=<token>" markers to steer negative paths (see
// platform/fakemarker); the happy path uses a plain change URI.
func (s *E2EIntegrationSuite) land(queue string, uris ...string) request {
	t := s.T()
	resp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue:    queue,
		Change:   &changepb.Change{Uris: uris},
		Strategy: mergestrategypb.Strategy_REBASE,
	})
	require.NoError(t, err, "Land failed for queue %s", queue)
	require.NotEmpty(t, resp.Sqid, "Land returned an empty sqid for queue %s", queue)
	return request{queue: queue, sqid: resp.Sqid}
}

// currentStatus reads the request's current customer-facing status via
// GetRequestSummaryByID. A transport error is returned so callers can keep polling.
func (s *E2EIntegrationSuite) currentStatus(req request) (entity.RequestStatus, error) {
	resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: req.sqid, Queue: req.queue})
	if err != nil {
		return entity.RequestStatusUnknown, err
	}
	if resp.Request == nil {
		return entity.RequestStatusUnknown, fmt.Errorf("GetRequestSummaryByID(%s) returned no request", req.sqid)
	}
	return entity.RequestStatus(resp.Request.Status), nil
}

// awaitStatus polls GetRequestSummaryByID until the request reaches exactly want.
// It returns every distinct status it observed on the way, so a caller can check
// what the summary did and did not report while the request was in flight.
func (s *E2EIntegrationSuite) awaitStatus(req request, want entity.RequestStatus) []entity.RequestStatus {
	var seen []entity.RequestStatus
	pollUntil(persistPollInterval, func() bool {
		got, err := s.currentStatus(req)
		if err != nil {
			s.log.Logf("GetRequestSummaryByID(%s) not ready yet: %v", req.sqid, err)
			return false
		}
		if len(seen) == 0 || seen[len(seen)-1] != got {
			seen = append(seen, got)
		}
		s.log.Logf("GetRequestSummaryByID(%s) = %q (want %q)", req.sqid, got, want)
		return got == want
	})
	return seen
}

// awaitTerminal polls GetRequestSummaryByID until the request reaches a terminal status
// (landed, error, or cancelled) and returns it.
func (s *E2EIntegrationSuite) awaitTerminal(req request) entity.RequestStatus {
	var last entity.RequestStatus
	pollUntil(persistPollInterval, func() bool {
		got, err := s.currentStatus(req)
		if err != nil {
			s.log.Logf("GetRequestSummaryByID(%s) not ready yet: %v", req.sqid, err)
			return false
		}
		last = got
		s.log.Logf("GetRequestSummaryByID(%s) = %q (awaiting terminal)", req.sqid, got)
		return isTerminalStatus(got)
	})
	return last
}

// timeline returns the ordered customer-facing status history through
// GetRequestHistoryByID. Only status entries are included: build events share
// the stream but are not positions, and eventTimeline reads those.
func (s *E2EIntegrationSuite) timeline(req request) []entity.RequestStatus {
	t := s.T()
	resp, err := s.gatewayClient.GetRequestHistoryByID(s.ctx, &gatewaypb.GetRequestHistoryByIDRequest{Sqid: req.sqid, Queue: req.queue})
	require.NoError(t, err, "GetRequestHistoryByID failed for %s", req.sqid)
	var statuses []entity.RequestStatus
	for _, event := range resp.Events {
		if entity.RequestLogType(event.Type) != entity.RequestLogTypeStatus {
			continue
		}
		statuses = append(statuses, entity.RequestStatus(event.Status))
	}
	return statuses
}

// eventTimeline returns the ordered build events recorded against the request.
func (s *E2EIntegrationSuite) eventTimeline(req request) []entity.RequestEvent {
	t := s.T()
	resp, err := s.gatewayClient.GetRequestHistoryByID(s.ctx, &gatewaypb.GetRequestHistoryByIDRequest{Sqid: req.sqid, Queue: req.queue})
	require.NoError(t, err, "GetRequestHistoryByID failed for %s", req.sqid)
	var events []entity.RequestEvent
	for _, event := range resp.Events {
		if entity.RequestLogType(event.Type) != entity.RequestLogTypeEvent {
			continue
		}
		events = append(events, entity.RequestEvent(event.Event))
	}
	return events
}

// assertStatusesInOrder asserts that want appears as an ordered subsequence of
// the GetRequestHistoryByID status timeline. It tolerates intermediate statuses (so it is
// not a change-detector), asserting only the relative order of the statuses that
// matter.
func (s *E2EIntegrationSuite) assertStatusesInOrder(req request, want ...entity.RequestStatus) {
	t := s.T()
	got := s.timeline(req)
	matched := 0
	for _, st := range got {
		if matched < len(want) && st == want[matched] {
			matched++
		}
	}
	assert.Equalf(t, len(want), matched,
		"GetRequestHistoryByID for %s should contain %v as an ordered subsequence; got %v",
		req.sqid, want, got)
}

// assertStatusesNever asserts that none of the banned statuses ever appeared
// in the GetRequestHistoryByID status timeline.
func (s *E2EIntegrationSuite) assertStatusesNever(req request, banned ...entity.RequestStatus) {
	t := s.T()
	got := s.timeline(req)
	for _, b := range banned {
		assert.NotContainsf(t, got, b,
			"GetRequestHistoryByID for %s must never contain %q; got %v", req.sqid, b, got)
	}
}

// awaitBatchID polls the operating store until the request has been claimed by
// a batch and returns that batch's ID.
//
// Messages about a batch are keyed by the batch ID, not the sqid the test
// holds, so a test that wants to name one has to resolve it. Polling because
// the claim happens asynchronously, several stages after Land returns.
func (s *E2EIntegrationSuite) awaitBatchID(req request) string {
	t := s.T()
	store, err := s.appStorage.For(req.queue)
	require.NoError(t, err, "failed to resolve operating store for queue %s", req.queue)

	var batchID string
	pollUntil(persistPollInterval, func() bool {
		associations, err := store.GetRequestBatchStore().GetByRequestID(s.ctx, req.sqid)
		if err != nil || len(associations) == 0 {
			return false
		}
		batchID = associations[0].BatchID
		return true
	})
	s.log.Logf("Request %s is carried by batch %s", req.sqid, batchID)
	return batchID
}

// closeGate closes the consumer gate for the consumer group, scoped to one
// partition (the queue name for pipeline topics). The gate must be closed
// before the message that must be caught is published — that makes the stop
// exact by construction rather than a timing race.
func (s *E2EIntegrationSuite) closeGate(consumerGroup, partitionKey, reason string) {
	t := s.T()
	key := consumergate.Key{ConsumerGroup: consumerGroup, PartitionKey: partitionKey}
	require.NoError(t, s.gate.Close(s.ctx, key, consumergate.Metadata{
		Reason:      reason,
		CreatedBy:   "e2e-suite",
		CreatedAtMs: time.Now().UnixMilli(),
	}), "failed to close gate %+v", key)
	s.log.Logf("Closed consumer gate %s (partition %q)", consumerGroup, partitionKey)
}

// openGate opens the consumer gate for the consumer group and partition.
// Opening an already-open gate is a no-op, so it is safe to call from a defer
// after an explicit open.
func (s *E2EIntegrationSuite) openGate(consumerGroup, partitionKey string) {
	t := s.T()
	key := consumergate.Key{ConsumerGroup: consumerGroup, PartitionKey: partitionKey}
	require.NoError(t, s.gate.Open(s.ctx, key), "failed to open gate %+v", key)
	s.log.Logf("Opened consumer gate %s (partition %q)", consumerGroup, partitionKey)
}

// awaitParked polls the shared gate directory until the delivery identified by
// (consumer group, topic key, message ID) has a parked record, and returns it.
// The record is written by the gated service before it postpones the delivery
// (and refreshed on every re-check while the gate stays closed), so observing
// it proves the stopped controller caught exactly this message — as opposed
// to the message simply not having arrived yet.
func (s *E2EIntegrationSuite) awaitParked(consumerGroup, topic, messageID string) consumergate.Parked {
	t := s.T()
	var found consumergate.Parked
	pollUntil(persistPollInterval, func() bool {
		records, err := s.gate.ListParked(s.ctx, consumerGroup)
		require.NoError(t, err, "failed to list parked deliveries for gate %s", consumerGroup)
		for _, r := range records {
			if r.Topic == topic && r.MessageID == messageID {
				found = r
				return true
			}
		}
		return false
	})
	return found
}

// awaitUnparked polls until the previously observed parked record is absent.
// The gated service removes the record when the redelivered message clears the
// open gate, so disappearance proves the delivery was admitted after the open.
func (s *E2EIntegrationSuite) awaitUnparked(consumerGroup, topic, messageID string) {
	t := s.T()
	pollUntil(persistPollInterval, func() bool {
		records, err := s.gate.ListParked(s.ctx, consumerGroup)
		require.NoError(t, err, "failed to list parked deliveries for gate %s", consumerGroup)
		for _, r := range records {
			if r.Topic == topic && r.MessageID == messageID {
				return false
			}
		}
		return true
	})
}

// terminalState reads the request's current internal RequestState from the
// operating store (mysql-app). Unlike the status timeline, RequestState is
// point-in-time — the Request entity is updated in place under optimistic
// locking, so only the current (terminal, once settled) value is observable.
func (s *E2EIntegrationSuite) terminalState(req request) entity.RequestState {
	t := s.T()
	store, err := s.appStorage.For(req.queue)
	require.NoError(t, err, "failed to resolve operating store for queue %s", req.queue)
	got, err := store.GetRequestStore().Get(s.ctx, req.sqid)
	require.NoError(t, err, "failed to get request %s from operating store", req.sqid)
	return got.State
}

// lastError returns the LastError reported by GetRequestSummaryByID.
func (s *E2EIntegrationSuite) lastError(req request) string {
	t := s.T()
	resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: req.sqid, Queue: req.queue})
	require.NoError(t, err, "GetRequestSummaryByID failed for %s", req.sqid)
	require.NotNil(t, resp.Request)
	return resp.Request.LastError
}

// isTerminalStatus reports whether a customer-facing status is terminal.
func isTerminalStatus(status entity.RequestStatus) bool {
	switch status {
	case entity.RequestStatusLanded, entity.RequestStatusError, entity.RequestStatusCancelled:
		return true
	default:
		return false
	}
}
