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

// Package orchestrator declares the SubmitQueue orchestrator's pipeline
// topology, extension seams, and controller set. The host (main.go) fills
// Deps and passes Stages to pipeline.Construct; no assembly logic lives here.
package orchestrator

import (
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/pipeline"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/batch"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/build"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/buildsignal"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/cancel"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/conclude"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/dlq"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/merge"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/mergeconflictsignal"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/mergesignal"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/speculate"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/start"
	"github.com/uber/submitqueue/submitqueue/orchestrator/controller/validate"
	"go.uber.org/zap"
)

// Deps is the full set of dependencies the orchestrator pipeline needs.
// This struct IS the service's public API toward deployers: fill every
// field, pass it and Stages to pipeline.Construct, and you get a running
// orchestrator pipeline.
type Deps struct {
	// Logger is the structured logger for all controllers.
	Logger *zap.SugaredLogger

	// Scope is the metrics scope for all controllers.
	Scope tally.Scope

	// Storage provides request, batch, and change stores.
	Storage storage.Storage

	// Counter provides distributed batch counters.
	Counter counter.Counter

	// BuildRunner resolves the build runner for each queue.
	BuildRunner buildrunner.Factory

	// ChangeProvider resolves the change provider for each queue.
	ChangeProvider changeprovider.Factory

	// Analyzer resolves the conflict analyzer for each queue.
	Analyzer conflict.Factory

	// Validator resolves the validator for each queue.
	Validator validator.Factory
}

// Stages is the orchestrator's pipeline topology as a typed table.
// Adding a stage = adding one row. Nothing else, anywhere.
//
// Pipeline:
//
//	start → cancel → validate ⇢ (runway) ⇢ mergeconflictsignal → batch → speculate → build → buildsignal ─┐
//	                                                                       ↑  ↘             ↻ poll       │
//	                                                                       │   merge → conclude          │
//	                                                                       │     │                       │
//	                                                                       └─────┴───────────────────────┘
var Stages = []pipeline.Stage[Deps]{
	{
		Key:           topickey.TopicKeyStart,
		Name:          "start",
		ConsumerGroup: "orchestrator-start",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return start.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQRequestController(d.Logger, d.Scope, d.Storage, sc.Registry, dlq.DecodeLandRequestID, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyCancel,
		Name:          "cancel",
		ConsumerGroup: "orchestrator-cancel",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return cancel.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQRequestController(d.Logger, d.Scope, d.Storage, sc.Registry, dlq.DecodeCancelRequestID, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyValidate,
		Name:          "validate",
		ConsumerGroup: "orchestrator-validate",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return validate.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, d.ChangeProvider, d.Validator, runwaymq.TopicKeyMergeConflictCheck, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQRequestController(d.Logger, d.Scope, d.Storage, sc.Registry, dlq.DecodeRequestID, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           runwaymq.TopicKeyMergeConflictCheckSignal,
		Name:          "merge-conflict-check-signal",
		ConsumerGroup: "orchestrator-mergeconflictsignal",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return mergeconflictsignal.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQMergeConflictSignalController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyBatch,
		Name:          "batch",
		ConsumerGroup: "orchestrator-batch",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return batch.NewController(d.Logger, d.Scope, sc.Registry, d.Counter, d.Storage, d.Analyzer, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQRequestController(d.Logger, d.Scope, d.Storage, sc.Registry, dlq.DecodeRequestID, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeySpeculate,
		Name:          "speculate",
		ConsumerGroup: "orchestrator-speculate",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return speculate.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQBatchController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyBuild,
		Name:          "build",
		ConsumerGroup: "orchestrator-build",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return build.NewController(d.Logger, d.Scope, d.Storage, d.BuildRunner, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQBatchController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyBuildSignal,
		Name:          "buildsignal",
		ConsumerGroup: "orchestrator-buildsignal",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return buildsignal.NewController(d.Logger, d.Scope, d.Storage, d.BuildRunner, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQBuildSignalController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyMerge,
		Name:          "submitqueue-merge",
		ConsumerGroup: "orchestrator-merge",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return merge.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, runwaymq.TopicKeyMerge, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQBatchController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           runwaymq.TopicKeyMergeSignal,
		Name:          "merge-signal",
		ConsumerGroup: "orchestrator-mergesignal",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return mergesignal.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQMergeSignalController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
	{
		Key:           topickey.TopicKeyConclude,
		Name:          "conclude",
		ConsumerGroup: "orchestrator-conclude",
		New: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return conclude.NewController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
		DLQ: func(d Deps, sc pipeline.StageContext) (consumer.Controller, error) {
			return dlq.NewDLQBatchController(d.Logger, d.Scope, d.Storage, sc.Registry, sc.TopicKey, sc.ConsumerGroup), nil
		},
	},
}

// PublishOnlyTopics declares topics the orchestrator publishes to but does
// not consume. These are registered in the TopicRegistry so controllers
// can look up topic names for publishing.
var PublishOnlyTopics = []pipeline.PublishOnlyTopic{
	// Log: the orchestrator emits request-log entries; the gateway consumes them.
	{Key: topickey.TopicKeyLog, Name: "log"},
	// Merge-conflict check: the orchestrator publishes check requests to runway.
	{Key: runwaymq.TopicKeyMergeConflictCheck, Name: "merge-conflict-check"},
	// Merge: the orchestrator publishes merge requests to runway.
	{Key: runwaymq.TopicKeyMerge, Name: "runway-merge"},
}

// Controllers holds the orchestrator's RPC-facing controllers, constructed
// but NOT bound to any wire contract. Binding to a proto service + transport
// is host glue, because deployers may use different protos or transports.
type Controllers struct {
	// Ping is the health-check controller.
	Ping *controller.PingController
}

// NewControllers creates the orchestrator's RPC controllers from the given Deps.
// The PingController takes a base *zap.Logger, so we desugar the SugaredLogger.
func NewControllers(d Deps) Controllers {
	return Controllers{
		Ping: controller.NewPingController(d.Logger.Desugar(), d.Scope),
	}
}
