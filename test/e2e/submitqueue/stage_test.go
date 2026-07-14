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

// Stage catalog: every queue consumer controller across all three services,
// enumerated from the actual wiring in the server mains. This knowledge is
// duplicated here so the test can plant pre-holds on any stage; see
// NewStageHold in test/testutil for the mechanism.
//
// Source of truth for each service's topic→consumer-group mapping:
//   - Gateway:      service/submitqueue/gateway/server/main.go
//   - Orchestrator: service/submitqueue/orchestrator/server/main.go
//   - Runway:       service/runway/server/main.go

import (
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/test/testutil"
)

// stage identifies a queue consumer controller by its topic wire name and
// consumer group, sufficient to plant a StageHold on a specific partition.
type stage struct {
	// topic is the wire name registered in the topic registry (the Name field
	// of consumer.TopicConfig, not the TopicKey constant).
	topic string
	// consumerGroup is the group suffix the service subscribes with.
	consumerGroup string
}

// ---------------------------------------------------------------------------
// Gateway stages (source: service/submitqueue/gateway/server/main.go)
// ---------------------------------------------------------------------------

var (
	// stageGatewayLog is the gateway's log consumer that persists request_log
	// entries published by the orchestrator.
	stageGatewayLog = stage{topic: "log", consumerGroup: "gateway-log"}
)

// ---------------------------------------------------------------------------
// Orchestrator primary stages (source: service/submitqueue/orchestrator/server/main.go)
// ---------------------------------------------------------------------------

var (
	stageOrchestratorStart               = stage{topic: "start", consumerGroup: "orchestrator-start"}
	stageOrchestratorCancel              = stage{topic: "cancel", consumerGroup: "orchestrator-cancel"}
	stageOrchestratorValidate            = stage{topic: "validate", consumerGroup: "orchestrator-validate"}
	stageOrchestratorMergeConflictSignal = stage{topic: "merge-conflict-check-signal", consumerGroup: "orchestrator-mergeconflictsignal"}
	stageOrchestratorBatch               = stage{topic: "batch", consumerGroup: "orchestrator-batch"}
	stageOrchestratorScore               = stage{topic: "score", consumerGroup: "orchestrator-score"}
	stageOrchestratorSpeculate           = stage{topic: "speculate", consumerGroup: "orchestrator-speculate"}
	stageOrchestratorBuild               = stage{topic: "build", consumerGroup: "orchestrator-build"}
	stageOrchestratorBuildSignal         = stage{topic: "buildsignal", consumerGroup: "orchestrator-buildsignal"}
	stageOrchestratorMerge               = stage{topic: "mergebatch", consumerGroup: "orchestrator-merge"}
	stageOrchestratorMergeSignal         = stage{topic: "merge-signal", consumerGroup: "orchestrator-mergesignal"}
	stageOrchestratorConclude            = stage{topic: "conclude", consumerGroup: "orchestrator-conclude"}
)

// ---------------------------------------------------------------------------
// Orchestrator DLQ stages (source: service/submitqueue/orchestrator/server/main.go)
// Each DLQ consumer group is the primary group suffixed with "-dlq", and each
// DLQ topic is the primary topic suffixed with "_dlq".
// ---------------------------------------------------------------------------

var (
	stageOrchestratorStartDLQ               = stage{topic: "start_dlq", consumerGroup: "orchestrator-start-dlq"}
	stageOrchestratorCancelDLQ              = stage{topic: "cancel_dlq", consumerGroup: "orchestrator-cancel-dlq"}
	stageOrchestratorValidateDLQ            = stage{topic: "validate_dlq", consumerGroup: "orchestrator-validate-dlq"}
	stageOrchestratorMergeConflictSignalDLQ = stage{topic: "merge-conflict-check-signal_dlq", consumerGroup: "orchestrator-mergeconflictsignal-dlq"}
	stageOrchestratorBatchDLQ               = stage{topic: "batch_dlq", consumerGroup: "orchestrator-batch-dlq"}
	stageOrchestratorScoreDLQ               = stage{topic: "score_dlq", consumerGroup: "orchestrator-score-dlq"}
	stageOrchestratorSpeculateDLQ           = stage{topic: "speculate_dlq", consumerGroup: "orchestrator-speculate-dlq"}
	stageOrchestratorBuildDLQ               = stage{topic: "build_dlq", consumerGroup: "orchestrator-build-dlq"}
	stageOrchestratorBuildSignalDLQ         = stage{topic: "buildsignal_dlq", consumerGroup: "orchestrator-buildsignal-dlq"}
	stageOrchestratorMergeDLQ               = stage{topic: "mergebatch_dlq", consumerGroup: "orchestrator-merge-dlq"}
	stageOrchestratorMergeSignalDLQ         = stage{topic: "merge-signal_dlq", consumerGroup: "orchestrator-mergesignal-dlq"}
	stageOrchestratorConcludeDLQ            = stage{topic: "conclude_dlq", consumerGroup: "orchestrator-conclude-dlq"}
)

// ---------------------------------------------------------------------------
// Runway stages (source: service/runway/server/main.go)
// ---------------------------------------------------------------------------

var (
	// stageRunwayMergeConflictCheck is runway's merge-conflict-check consumer
	// (dry-run merge attempts).
	stageRunwayMergeConflictCheck = stage{topic: "merge-conflict-check", consumerGroup: "runway-mergeconflictcheck"}
	// stageRunwayMerge is runway's committing-merge consumer.
	stageRunwayMerge = stage{topic: "merge", consumerGroup: "runway-merge"}
)

// holdStage plants a phantom partition lease that starves the given stage's
// consumer for the specified partition key. The hold is released automatically
// via t.Cleanup; callers may also call Release() on the returned handle to
// resume consumption earlier.
//
// This must be called BEFORE the partition's first message is published (the
// hold is a pre-hold — see testutil.StageHold for the limitation).
func (s *E2EIntegrationSuite) holdStage(st stage, partitionKey string) *testutil.StageHold {
	s.T().Helper()
	hold, err := testutil.NewStageHold(
		s.log,
		s.queueDB,
		st.consumerGroup,
		st.topic,
		partitionKey,
		s.T().Cleanup,
	)
	require.NoError(s.T(), err, "failed to plant stage hold on %s/%s for partition %s",
		st.topic, st.consumerGroup, partitionKey)
	return hold
}
