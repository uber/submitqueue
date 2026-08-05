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

// currentStatus reads the request's current customer-facing status via
// GetRequestSummaryByID. A transport error is returned so callers can keep polling.
func (s *E2EIntegrationSuite) currentStatus(sqid string) (entity.RequestStatus, error) {
	resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: sqid})
	if err != nil {
		return entity.RequestStatusUnknown, err
	}
	if resp.Request == nil {
		return entity.RequestStatusUnknown, fmt.Errorf("GetRequestSummaryByID(%s) returned no request", sqid)
	}
	return entity.RequestStatus(resp.Request.Status), nil
}

// awaitStatus polls GetRequestSummaryByID until the request reaches exactly want.
func (s *E2EIntegrationSuite) awaitStatus(sqid string, want entity.RequestStatus) {
	pollUntil(persistPollInterval, func() bool {
		got, err := s.currentStatus(sqid)
		if err != nil {
			s.log.Logf("GetRequestSummaryByID(%s) not ready yet: %v", sqid, err)
			return false
		}
		s.log.Logf("GetRequestSummaryByID(%s) = %q (want %q)", sqid, got, want)
		return got == want
	})
}

// awaitTerminal polls GetRequestSummaryByID until the request reaches a terminal status
// (landed, error, or cancelled) and returns it.
func (s *E2EIntegrationSuite) awaitTerminal(sqid string) entity.RequestStatus {
	var last entity.RequestStatus
	pollUntil(persistPollInterval, func() bool {
		got, err := s.currentStatus(sqid)
		if err != nil {
			s.log.Logf("GetRequestSummaryByID(%s) not ready yet: %v", sqid, err)
			return false
		}
		last = got
		s.log.Logf("GetRequestSummaryByID(%s) = %q (awaiting terminal)", sqid, got)
		return isTerminalStatus(got)
	})
	return last
}

// timeline returns the ordered customer-facing status history through
// GetRequestHistoryByID.
func (s *E2EIntegrationSuite) timeline(sqid string) []entity.RequestStatus {
	t := s.T()
	resp, err := s.gatewayClient.GetRequestHistoryByID(s.ctx, &gatewaypb.GetRequestHistoryByIDRequest{Sqid: sqid})
	require.NoError(t, err, "GetRequestHistoryByID failed for %s", sqid)
	statuses := make([]entity.RequestStatus, len(resp.Events))
	for i, event := range resp.Events {
		statuses[i] = entity.RequestStatus(event.Status)
	}
	return statuses
}

// assertStatusesInOrder asserts that want appears as an ordered subsequence of
// the GetRequestHistoryByID status timeline. It tolerates intermediate statuses (so it is
// not a change-detector), asserting only the relative order of the statuses that
// matter.
func (s *E2EIntegrationSuite) assertStatusesInOrder(sqid string, want ...entity.RequestStatus) {
	t := s.T()
	got := s.timeline(sqid)
	matched := 0
	for _, st := range got {
		if matched < len(want) && st == want[matched] {
			matched++
		}
	}
	assert.Equalf(t, len(want), matched,
		"GetRequestHistoryByID for %s should contain %v as an ordered subsequence; got %v",
		sqid, want, got)
}

// assertStatusesNever asserts that none of the banned statuses ever appeared
// in the GetRequestHistoryByID status timeline.
func (s *E2EIntegrationSuite) assertStatusesNever(sqid string, banned ...entity.RequestStatus) {
	t := s.T()
	got := s.timeline(sqid)
	for _, b := range banned {
		assert.NotContainsf(t, got, b,
			"GetRequestHistoryByID for %s must never contain %q; got %v", sqid, b, got)
	}
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
// The record is written by the gated service before it blocks, so observing it
// proves the stopped controller is holding exactly this message — as opposed
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
// The gate removes the record before releasing the delivery, so disappearance
// proves the delivery cleared the gate after it opened.
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
func (s *E2EIntegrationSuite) terminalState(sqid string) entity.RequestState {
	t := s.T()
	req, err := s.requestStore.Get(s.ctx, sqid)
	require.NoError(t, err, "failed to get request %s from operating store", sqid)
	return req.State
}

// lastError returns the LastError reported by GetRequestSummaryByID.
func (s *E2EIntegrationSuite) lastError(sqid string) string {
	t := s.T()
	resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: sqid})
	require.NoError(t, err, "GetRequestSummaryByID failed for %s", sqid)
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
