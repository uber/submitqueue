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
// observe outcomes two ways:
//
//   - black-box, by polling the Status RPC to a target/terminal status; and
//   - white-box, by reading the request_log timeline (RequestLogStore.List on
//     mysql-app) to assert the ordered stage progression.
//
// Convergence is bounded by require.Eventually (persistTimeout /
// persistPollInterval) rather than time.Sleep: the pipeline consumers run inside
// containers, so there is no in-process signal to await; a timeout here means a
// stage is genuinely stuck, not a timing race.

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
)

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

// list calls the gateway List RPC and fails the test on unexpected errors.
func (s *E2EIntegrationSuite) list(req *gatewaypb.ListRequest) *gatewaypb.ListResponse {
	t := s.T()
	resp, err := s.gatewayClient.List(s.ctx, req)
	require.NoError(t, err, "List failed for queue %s", req.Queue)
	return resp
}

// awaitListContains polls List until all expected sqids are visible in one
// response page. The gateway updates request summaries synchronously for Land
// and asynchronously from the log topic for later statuses, so callers use the
// same bounded polling style as Status.
func (s *E2EIntegrationSuite) awaitListContains(req *gatewaypb.ListRequest, want ...string) *gatewaypb.ListResponse {
	t := s.T()
	var resp *gatewaypb.ListResponse
	require.Eventually(t, func() bool {
		var err error
		resp, err = s.gatewayClient.List(s.ctx, req)
		if err != nil {
			s.log.Logf("List(%s) not ready yet: %v", req.Queue, err)
			return false
		}
		got := summarySQIDs(resp.Requests)
		s.log.Logf("List(%s) = %v (want %v)", req.Queue, got, want)
		return containsAll(got, want)
	}, persistTimeout, persistPollInterval,
		"List(%s) should contain sqids %v", req.Queue, want)
	return resp
}

func summarySQIDs(summaries []*gatewaypb.RequestSummary) []string {
	out := make([]string, len(summaries))
	for i, summary := range summaries {
		out[i] = summary.Sqid
	}
	return out
}

func containsAll(got []string, want []string) bool {
	seen := make(map[string]struct{}, len(got))
	for _, sqid := range got {
		seen[sqid] = struct{}{}
	}
	for _, sqid := range want {
		if _, ok := seen[sqid]; !ok {
			return false
		}
	}
	return true
}

func mapFromStrings(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
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

// awaitStatus polls Status until the request reaches exactly want.
func (s *E2EIntegrationSuite) awaitStatus(sqid string, want entity.RequestStatus) {
	t := s.T()
	require.Eventually(t, func() bool {
		got, err := s.currentStatus(sqid)
		if err != nil {
			s.log.Logf("Status(%s) not ready yet: %v", sqid, err)
			return false
		}
		s.log.Logf("Status(%s) = %q (want %q)", sqid, got, want)
		return got == want
	}, persistTimeout, persistPollInterval,
		"request %s should reach status %q", sqid, want)
}

// awaitTerminal polls Status until the request reaches a terminal status
// (landed, error, or cancelled) and returns it.
func (s *E2EIntegrationSuite) awaitTerminal(sqid string) entity.RequestStatus {
	t := s.T()
	var last entity.RequestStatus
	require.Eventually(t, func() bool {
		got, err := s.currentStatus(sqid)
		if err != nil {
			s.log.Logf("Status(%s) not ready yet: %v", sqid, err)
			return false
		}
		last = got
		s.log.Logf("Status(%s) = %q (awaiting terminal)", sqid, got)
		return isTerminalStatus(got)
	}, persistTimeout, persistPollInterval,
		"request %s should reach a terminal status", sqid)
	return last
}

// timeline returns the ordered status history from the request_log (the audit
// trail persisted by the gateway log consumer on mysql-app).
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

// assertStatusesInOrder asserts that want appears as an ordered subsequence of
// the request_log status timeline. It tolerates intermediate statuses (so it is
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
		"request_log for %s should contain %v as an ordered subsequence; got %v",
		sqid, want, got)
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

// lastError returns the LastError reported by the Status RPC (populated on the
// error path).
func (s *E2EIntegrationSuite) lastError(sqid string) string {
	t := s.T()
	resp, err := s.gatewayClient.Status(s.ctx, &gatewaypb.StatusRequest{Sqid: sqid})
	require.NoError(t, err, "Status failed for %s", sqid)
	return resp.LastError
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
