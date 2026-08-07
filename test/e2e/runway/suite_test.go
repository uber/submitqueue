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

// Runway end-to-end tests.
//
// These tests use docker-compose from service/runway/server/docker-compose.yml.
// They are hermetic: the runway image is built from a staged context whose
// inputs (Bazel-built Linux binary, Dockerfile) are all declared data
// dependencies of the test target.
//
// Run with:
//
//	make e2e-test
//
// or only this package:
//
//	bazel test //test/e2e/runway:go_default_test
//
// The stack runs the Runway service plus a queue MySQL. Runway is consumer-only
// — it has no gateway — so the suite drives it the way its real client does: it
// publishes a MergeRequest to an inbound merge topic and listens on the
// corresponding signal topic for the MergeResult carrying its correlation id.
// That is the seam no other test covers: topic wiring, the protojson round-trip
// over the wire, partition-key propagation, the ack-and-publish-FAILED path for
// expected outcomes, and DLQ reconciliation.
//
// The service runs with MERGER=fake, so what a merge *does* is out of scope
// here; the fake's marker tokens only pick which outcome Runway has to carry
// back. Merge behavior proper — strategies, real conflicts, staleness, author
// attribution — is covered against a real git remote in
// runway/extension/merger/git/git_merger_test.go.

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap/zaptest"
)

type RunwayE2ESuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	queueDB *sql.DB
	queue   extqueue.Queue

	// Signal-topic observers, standing in for the client awaiting its results.
	mergeSignal *observer
	checkSignal *observer

	// seq mints unique correlation ids across the suite.
	seq int
}

func TestRunwayE2E(t *testing.T) {
	suite.Run(t, new(RunwayE2ESuite))
}

func (s *RunwayE2ESuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Runway e2e test suite using docker-compose")

	// Outcomes have to be steerable from the request payload, so the service
	// runs the marker-driven fake merger instead of noop or git. Set before the
	// stack is created: ComposeStack snapshots the environment at construction.
	t.Setenv("SQ_RUNWAY_MERGER", "fake")

	composeFile := testutil.Runfile("service/runway/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-runway",
		testutil.WithBuildContext(map[string]string{
			".docker-bin/runway":               "service/runway/server/runway_linux",
			"service/runway/server/Dockerfile": "service/runway/server/Dockerfile",
		}))

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	// Applied after the stack is up; the service connects lazily and its
	// consumers retry, so the boot ordering is tolerated.
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	// The suite talks to the same queue backend Runway does, as its client.
	s.queue, err = queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.queueDB,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err, "failed to create queue client")
	t.Cleanup(func() { s.queue.Close() })

	s.mergeSignal = s.observe(topicMergeSignal)
	s.checkSignal = s.observe(topicCheckSignal)

	s.log.Logf("Runway e2e test suite ready")
}

func (s *RunwayE2ESuite) TearDownSuite() {
	// Compose stack cleanup is handled automatically by t.Cleanup (registered in
	// NewComposeStack).
	s.log.Logf("Tearing down Runway e2e test suite")
}

// TestMerge_HappyPath_PublishesMergedResult drives the committing merge: a
// two-step request in, a SUCCEEDED result out on the merge-signal topic. It
// asserts the whole envelope the client depends on — the correlation id echoed,
// every step attributable by its step id in application order, a produced
// revision per step, and the partition key carried from request to result so
// the client's per-queue ordering survives the round trip.
func (s *RunwayE2ESuite) TestMerge_HappyPath_PublishesMergedResult() {
	t := s.T()
	const queue = "e2e-runway/merge"

	request := s.mergeRequest(queue,
		step("base", baseURI),
		step("candidate", baseURI),
	)
	s.publish(topicMerge, request)

	observed := s.mergeSignal.await(t, s.ctx, request.GetId())
	result := observed.result

	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.GetOutcome())
	assert.Empty(t, result.GetReason(), "a successful merge carries no reason")
	assert.Equal(t, queue, observed.partitionKey,
		"the result must stay on the request's partition")

	require.Len(t, result.GetSteps(), 2)
	assert.Equal(t, "base", result.GetSteps()[0].GetStepId())
	assert.Equal(t, "candidate", result.GetSteps()[1].GetStepId())
	for i, stepResult := range result.GetSteps() {
		assert.NotEmpty(t, stepResult.GetOutputs(),
			"step %d of a committing merge must report the revision it produced", i)
	}
}

// TestMergeConflictCheck_HappyPath_PublishesMergeableResult drives the dry-run
// check on its own topic pair. The distinguishing assertion against the
// committing merge is that a check commits nothing, so it reports no outputs.
func (s *RunwayE2ESuite) TestMergeConflictCheck_HappyPath_PublishesMergeableResult() {
	t := s.T()
	const queue = "e2e-runway/check"

	request := s.mergeRequest(queue, step("candidate", baseURI))
	s.publish(topicCheck, request)

	observed := s.checkSignal.await(t, s.ctx, request.GetId())
	result := observed.result

	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.GetOutcome())
	assert.Equal(t, queue, observed.partitionKey)
	require.Len(t, result.GetSteps(), 1)
	assert.Equal(t, "candidate", result.GetSteps()[0].GetStepId())
	assert.Empty(t, result.GetSteps()[0].GetOutputs(), "a dry run produces no revisions")
}

// TestTerminalFailure_AcksAndSignalsFailed covers the outcomes Runway treats as
// answers rather than faults: a conflict and an unapplicable request. Both must
// come back as a FAILED result on the signal topic, published by the controller
// that handled the request — not by the dead-letter reconciler. Dead-lettering
// an expected outcome would burn the retry budget on a request that can never
// succeed and resolve the client through the backstop instead of the answer.
func (s *RunwayE2ESuite) TestTerminalFailure_AcksAndSignalsFailed() {
	tests := []struct {
		name   string
		marker string
	}{
		{name: "conflict", marker: markerConflict},
		{name: "invalid request", marker: markerInvalid},
	}

	topics := []struct {
		name     string
		inbound  string
		observer func() *observer
	}{
		{"merge", topicMerge, func() *observer { return s.mergeSignal }},
		{"conflict check", topicCheck, func() *observer { return s.checkSignal }},
	}

	for _, topic := range topics {
		for _, tt := range tests {
			s.Run(topic.name+"/"+tt.name, func() {
				t := s.T()
				queue := "e2e-runway/failed"

				request := s.mergeRequest(queue, step("candidate", markedURI(tt.marker)))
				s.publish(topic.inbound, request)

				observed := topic.observer().await(t, s.ctx, request.GetId())
				result := observed.result

				assert.Equal(t, runwaypb.Outcome_FAILED, result.GetOutcome())
				assert.NotEmpty(t, result.GetReason(),
					"a failed result must tell the client why")
				assert.Equal(t, queue, observed.partitionKey)
				assert.False(t, reconciled(result),
					"an expected outcome must be answered by the controller, not dead-lettered")
			})
		}
	}
}

// TestUnexpectedFailure_ReconcilesFromDLQ covers the backstop. An unexpected
// merger fault is not a terminal outcome, so the controller does not answer it;
// the consumer rejects the message to the dead-letter topic instead. Runway is
// the sole responder on the client's correlation id, so a request that stopped
// there would leave the client waiting forever — the DLQ reconciler is what
// resolves it, republishing a FAILED result to the same signal topic.
func (s *RunwayE2ESuite) TestUnexpectedFailure_ReconcilesFromDLQ() {
	t := s.T()
	const queue = "e2e-runway/dlq"

	request := s.mergeRequest(queue, step("candidate", markedURI(markerError)))
	s.publish(topicMerge, request)

	observed := s.mergeSignal.await(t, s.ctx, request.GetId())

	assert.Equal(t, runwaypb.Outcome_FAILED, observed.result.GetOutcome())
	assert.True(t, reconciled(observed.result),
		"an unexpected fault should be resolved by the dead-letter reconciler")
	assert.Equal(t, queue, observed.partitionKey,
		"the reconciled result must stay on the request's partition")
}

// TestUndecodablePayload_DropsWithoutSignalling covers the one request Runway
// cannot answer: bytes that do not decode carry no correlation id, so there is
// nothing to resolve and both the controller and the reconciler drop them.
//
// The absence is asserted without sleeping. The garbage and a following
// sentinel share a partition, and a partition is consumed in order, so the
// sentinel's result arriving proves the garbage was already processed. Anything
// the garbage had signalled would sit ahead of the sentinel on the same
// partition of the same signal topic.
func (s *RunwayE2ESuite) TestUndecodablePayload_DropsWithoutSignalling() {
	t := s.T()
	const queue = "e2e-runway/undecodable"

	mark := s.mergeSignal.mark()

	s.publishRaw(topicMerge, "e2e-runway/garbage", queue, []byte("{not a merge request"))

	sentinel := s.mergeRequest(queue, step("candidate", baseURI))
	s.publish(topicMerge, sentinel)

	observed := s.mergeSignal.await(t, s.ctx, sentinel.GetId())
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, observed.result.GetOutcome())

	arrived := s.mergeSignal.newSince(mark)
	require.Len(t, arrived, 1,
		"the undecodable payload must not signal; only the sentinel should have arrived")
	assert.Equal(t, sentinel.GetId(), arrived[0].result.GetId())
}
