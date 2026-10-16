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

package e2e_test

import (
	"fmt"
	"os"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	queuePauseFileName = "pause.yaml"

	// pauseQueue is owned by these tests, so switching its pause state cannot
	// disturb another test's queue.
	pauseQueue = "e2e-pause-queue"

	// sentinelQueue is never paused. Its request landing proves the
	// orchestrator is running every stage, so a frozen queue that has not
	// advanced was held and not merely slow.
	sentinelQueue = "e2e-test-queue"
)

// writeQueuePauseFile replaces the pause file by rename inside its directory.
// The services read the file while it is rewritten, so a reader must see either
// the old or the new contents and never a partial file. The directory is
// bind-mounted, so a renamed file is visible in the containers; a single
// bind-mounted file would keep the old inode.
func (s *E2EIntegrationSuite) writeQueuePauseFile(contents string) {
	t := s.T()
	tmp := s.pausePath + ".tmp"
	require.NoError(t, os.WriteFile(tmp, []byte(contents), 0o644), "failed to write pause file")
	require.NoError(t, os.Rename(tmp, s.pausePath), "failed to replace pause file")
}

// setQueuePause sets the pause state of pauseQueue, including its reason, and
// leaves every other queue unpaused.
func (s *E2EIntegrationSuite) setQueuePause(state entity.QueuePause) {
	s.writeQueuePauseFile(fmt.Sprintf(
		"queues:\n  - name: %s\n    reject_lands: %t\n    freeze_processing: %t\n    reason: %q\n",
		pauseQueue, state.RejectLands, state.FreezeProcessing, state.Reason))
	s.log.Logf("Set pause state for %s: reject_lands=%t freeze_processing=%t reason=%q", pauseQueue, state.RejectLands, state.FreezeProcessing, state.Reason)
}

// clearQueuePause unpauses every queue. Tests defer it so a failed assertion
// cannot leave the queue paused for the tests after it.
func (s *E2EIntegrationSuite) clearQueuePause() {
	s.writeQueuePauseFile("queues: []\n")
}

// assertLandRejectedAsPaused lands directly on the gateway and expects the
// paused-queue rejection, which carries no sqid because no request is created.
// A non-empty wantReason must appear in the rejection the caller sees.
func (s *E2EIntegrationSuite) assertLandRejectedAsPaused(queue string, uri string, wantReason string) {
	resp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue:    queue,
		Change:   &changepb.Change{Uris: []string{uri}},
		Strategy: mergestrategypb.Strategy_REBASE,
	})
	require.Error(s.T(), err, "Land on paused queue %s should be rejected", queue)
	assert.Equal(s.T(), codes.FailedPrecondition, status.Code(err), "paused queue should map to FailedPrecondition")
	assert.Empty(s.T(), resp.GetSqid(), "a rejected land must not return a request")
	if wantReason != "" {
		assert.Contains(s.T(), status.Convert(err).Message(), wantReason, "rejection should carry the pause reason")
	}
}

// assertHeldWhileSentinelLands proves a frozen request did not advance. The
// sentinel queue shares the orchestrator but not the frozen queue's partitions,
// so once it has landed every stage has run and the frozen request, which was
// published earlier, would have moved if the freeze had not held it. The freeze
// gate records no parked delivery, so this ordering is the only witness.
func (s *E2EIntegrationSuite) assertHeldWhileSentinelLands(held request, sentinelURI string) {
	sentinel := s.land(sentinelQueue, sentinelURI)
	s.awaitStatus(sentinel, entity.RequestStatusLanded)
	assert.Equal(s.T(), entity.RequestStatusAccepted, s.mustStatus(held),
		"request %s on a frozen queue must stay accepted while another queue lands", held.sqid)
}

func uriForPull(repo string, pull int) string {
	return fmt.Sprintf("github://github.example.com/uber/%s/pull/%d/abcdef0123456789abcdef0123456789abcdef01", repo, pull)
}

// Neither switch set: an explicit all-off entry must not pause anything.
func (s *E2EIntegrationSuite) TestQueuePause_Running_LandsAndAdvances() {
	s.setQueuePause(entity.QueuePause{})
	defer s.clearQueuePause()

	req := s.land(pauseQueue, uriForPull("e2e-pause", 1001))

	s.awaitStatus(req, entity.RequestStatusLanded)
}

// Reject only (drain): in-flight work finishes and new lands are refused.
// The request is held by a freeze first so it is provably in flight when the
// freeze is lifted and rejection begins.
func (s *E2EIntegrationSuite) TestQueuePause_Drain_FinishesInFlightAndRejectsNewLands() {
	s.setQueuePause(entity.QueuePause{FreezeProcessing: true})
	defer s.clearQueuePause()
	inFlight := s.land(pauseQueue, uriForPull("e2e-pause", 2001))
	s.assertHeldWhileSentinelLands(inFlight, uriForPull("e2e-pause-sentinel", 2002))

	s.setQueuePause(entity.QueuePause{RejectLands: true})

	s.assertLandRejectedAsPaused(pauseQueue, uriForPull("e2e-pause", 2003), "")
	s.awaitStatus(inFlight, entity.RequestStatusLanded)
}

// Freeze only (hold): requests are accepted and durable, and nothing advances
// until the freeze is lifted.
func (s *E2EIntegrationSuite) TestQueuePause_Hold_AcceptsButDoesNotAdvance() {
	s.setQueuePause(entity.QueuePause{FreezeProcessing: true})
	defer s.clearQueuePause()
	held := s.land(pauseQueue, uriForPull("e2e-pause", 3001))
	s.assertHeldWhileSentinelLands(held, uriForPull("e2e-pause-sentinel", 3002))

	s.clearQueuePause()

	s.awaitStatus(held, entity.RequestStatusLanded)
}

// Both switches set: new lands are refused with the operator's reason and
// accepted work stays held, and clearing both lets the held request finish.
func (s *E2EIntegrationSuite) TestQueuePause_FullPause_RejectsAndHolds() {
	s.setQueuePause(entity.QueuePause{FreezeProcessing: true})
	defer s.clearQueuePause()
	held := s.land(pauseQueue, uriForPull("e2e-pause", 4001))

	const reason = "incident 4242, back at 17:00"
	s.setQueuePause(entity.QueuePause{RejectLands: true, FreezeProcessing: true, Reason: reason})

	s.assertLandRejectedAsPaused(pauseQueue, uriForPull("e2e-pause", 4002), reason)
	s.assertHeldWhileSentinelLands(held, uriForPull("e2e-pause-sentinel", 4003))

	s.clearQueuePause()

	s.awaitStatus(held, entity.RequestStatusLanded)
}
