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

// Package buildrunner adapts SQSim Build Runner behavior to the production extension.
package buildrunner

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber/submitqueue/sqsim/adapter/internal/operationlock"
	simentity "github.com/uber/submitqueue/sqsim/entity"
	"github.com/uber/submitqueue/sqsim/model"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	buildrunnerext "github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

var _ buildrunnerext.BuildRunner = (*Runner)(nil)

// Runner executes SQSim Build Runner behavior.
type Runner struct {
	runtime    *model.Runtime
	resolver   changeset.Resolver
	queue      string
	operations *operationlock.Locker
	triggered  *model.State[entity.BuildID]
	builds     *model.State[*buildExecution]
}

type buildExecution struct {
	mu          sync.Mutex
	changeURI   string
	createdAt   time.Time
	execution   simentity.BuildExecution
	statusCalls int
	cancelled   bool
}

// New returns an SQSim Build Runner bound to one queue.
func New(runtime *model.Runtime, resolver changeset.Resolver, queue string) *Runner {
	return &Runner{
		runtime:    runtime,
		resolver:   resolver,
		queue:      queue,
		operations: operationlock.New(),
		triggered:  model.NewState[entity.BuildID](),
		builds:     model.NewState[*buildExecution](),
	}
}

// Trigger creates the modeled build selected by the head batch's SQSim URI.
func (r *Runner) Trigger(ctx context.Context, base []entity.Batch, head entity.Batch, _ entity.BuildMetadata) (entity.BuildID, error) {
	if head.Queue != r.queue {
		return entity.BuildID{}, fmt.Errorf("head batch queue %q does not match SQSim Build Runner queue %q", head.Queue, r.queue)
	}
	changeURI, err := r.resolveHeadBehavior(ctx, head)
	if err != nil {
		return entity.BuildID{}, err
	}
	key := triggerKey(r.queue, base, head)
	unlock := r.operations.Lock(key)
	defer unlock()

	if buildID, ok := r.triggered.Get(key); ok {
		return buildID, nil
	}
	invocation, err := r.runtime.NextBuildTrigger(changeURI)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("next Build Runner Trigger invocation: %w", err)
	}
	if err := r.runtime.Clock().Wait(ctx, time.Duration(invocation.DelayMs)*time.Millisecond); err != nil {
		return entity.BuildID{}, err
	}
	if invocation.Fault.Phase == simentity.FaultBeforeSideEffect {
		return entity.BuildID{}, model.ErrorForFault(invocation.Fault)
	}

	buildID := entity.BuildID{ID: buildIDFor(key)}
	execution := &buildExecution{
		changeURI: changeURI,
		createdAt: r.runtime.Clock().Now(),
		execution: cloneBuildExecution(invocation.Outcome.Build),
	}
	r.builds.Set(buildID.ID, execution)
	r.triggered.Set(key, buildID)
	if err := model.ErrorForFault(invocation.Fault); err != nil {
		return entity.BuildID{}, err
	}
	return buildID, nil
}

// Status returns status from the build's real-time scenario timeline.
func (r *Runner) Status(_ context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	execution, ok := r.builds.Get(buildID.ID)
	if !ok {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("SQSim build %q does not exist", buildID.ID)
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()

	execution.statusCalls++
	for _, fault := range execution.execution.StatusFaults {
		if fault.Call == execution.statusCalls {
			return entity.BuildStatusUnknown, nil, model.ErrorForFault(fault.Fault)
		}
	}

	elapsed := r.runtime.Clock().Now().Sub(execution.createdAt)
	status := toBuildStatus(model.BuildStatusAt(execution.execution.Timeline, elapsed))
	if execution.cancelled {
		status = entity.BuildStatusCancelled
	}
	return status, entity.BuildMetadata{
		"sqsim.change_uri": execution.changeURI,
		"sqsim.elapsed_ms": strconv.FormatInt(elapsed.Milliseconds(), 10),
	}, nil
}

// Cancel marks a non-terminal modeled build as cancelled.
func (r *Runner) Cancel(_ context.Context, buildID entity.BuildID) error {
	execution, ok := r.builds.Get(buildID.ID)
	if !ok {
		return fmt.Errorf("SQSim build %q does not exist", buildID.ID)
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if execution.cancelled {
		return nil
	}
	status := model.BuildStatusAt(execution.execution.Timeline, r.runtime.Clock().Now().Sub(execution.createdAt))
	if status.IsTerminal() {
		return nil
	}
	execution.cancelled = true
	return nil
}

func (r *Runner) resolveHeadBehavior(ctx context.Context, head entity.Batch) (string, error) {
	changes, err := r.resolver.ChangesForBatch(ctx, head)
	if err != nil {
		return "", fmt.Errorf("resolve head batch %s: %w", head.ID, err)
	}
	var selectedURI string
	var selected simentity.BuildRunnerBehavior
	for _, change := range changes {
		for _, changeURI := range change.URIs {
			land, err := r.runtime.Resolve(changeURI)
			if err != nil {
				return "", fmt.Errorf("resolve head change URI %q: %w", changeURI, err)
			}
			if land.Queue != r.queue {
				return "", fmt.Errorf("land %q queue %q does not match SQSim Build Runner queue %q", land.Name, land.Queue, r.queue)
			}
			if selectedURI == "" {
				selectedURI = changeURI
				selected = land.Behavior.BuildRunner
				continue
			}
			if !reflect.DeepEqual(selected, land.Behavior.BuildRunner) {
				return "", fmt.Errorf("head batch %q contains incompatible SQSim Build Runner behavior", head.ID)
			}
		}
	}
	if selectedURI == "" {
		return "", fmt.Errorf("head batch %q has no SQSim change URI", head.ID)
	}
	return selectedURI, nil
}

func triggerKey(queue string, base []entity.Batch, head entity.Batch) string {
	var key strings.Builder
	appendKeyPart(&key, queue)
	for _, batch := range base {
		appendKeyPart(&key, batch.ID)
	}
	appendKeyPart(&key, head.ID)
	return key.String()
}

func appendKeyPart(key *strings.Builder, value string) {
	fmt.Fprintf(key, "%d:%s", len(value), value)
}

func buildIDFor(key string) string {
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("sqsim-%x", sum[:])
}

func cloneBuildExecution(execution simentity.BuildExecution) simentity.BuildExecution {
	execution.Timeline = append([]simentity.BuildStatusPoint(nil), execution.Timeline...)
	execution.StatusFaults = append([]simentity.FaultOnCall(nil), execution.StatusFaults...)
	return execution
}

func toBuildStatus(status simentity.BuildStatus) entity.BuildStatus {
	switch status {
	case simentity.BuildAccepted:
		return entity.BuildStatusAccepted
	case simentity.BuildRunning:
		return entity.BuildStatusRunning
	case simentity.BuildSucceeded:
		return entity.BuildStatusSucceeded
	case simentity.BuildFailed:
		return entity.BuildStatusFailed
	case simentity.BuildCancelled:
		return entity.BuildStatusCancelled
	default:
		return entity.BuildStatusUnknown
	}
}
