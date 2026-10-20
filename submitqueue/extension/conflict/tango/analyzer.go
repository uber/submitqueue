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

package tango

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// ErrNoSignature means an in-flight batch has no recorded signature. Tango is
// never asked for it: a signature computed later could differ from the one the
// batch was admitted with.
var ErrNoSignature = errors.New("no target signature recorded for in-flight batch")

// Params are the dependencies of one queue's Analyzer.
type Params struct {
	// Signatures persists signatures. It must be bound to the queue the
	// analyzer serves.
	Signatures SignatureStore

	// Dictionary records the label behind each target ID, in the background and
	// best-effort. It must be bound to the queue the analyzer serves.
	Dictionary DictionaryStore

	// Cache holds recently used signatures and may be shared across queues.
	Cache *SignatureCache

	// Client returns the targets a set of changes affects.
	Client ChangedTargetsClient

	// BaseRevision returns the full commit SHA of the target branch tip that a
	// new batch of the named queue is analyzed against.
	BaseRevision func(ctx context.Context, queue string) (string, error)

	// Changes resolves a batch into the changes it contains.
	Changes changeset.Resolver

	// Logger reports best-effort failures. It defaults to a no-op logger.
	Logger *zap.SugaredLogger
}

type analyzer struct {
	Params
}

// New returns a conflict.Analyzer that reports an in-flight batch as a
// ConflictTypeTargetOverlap conflict when its signature shares a target ID with
// the analyzed batch's. It fails closed: any signature it cannot obtain fails
// the analysis.
func New(params Params) conflict.Analyzer {
	if params.Logger == nil {
		params.Logger = zap.NewNop().Sugar()
	}
	return &analyzer{Params: params}
}

// Analyze signs batch, which is the only step that can call Tango, and compares
// it with the recorded signature of each in-flight batch. The new batch is
// signed even when nothing is in flight, so later batches find its record.
func (a *analyzer) Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]entity.Conflict, error) {
	mine, err := a.signNewBatch(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to sign batch %s: %w", batch.ID, err)
	}

	// TODO(tango): read the in-flight signatures concurrently with a bound.
	// New dependency edges are not in the signatures yet; see the TODO in signNewBatch.
	var conflicts []entity.Conflict
	for _, other := range inFlight {
		theirs, found, err := a.recordedSignature(ctx, other.Queue, other.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to read the signature of in-flight batch %s: %w", other.ID, err)
		}
		if !found {
			return nil, fmt.Errorf("in-flight batch %s: %w", other.ID, ErrNoSignature)
		}
		if sharesTarget(mine.Targets, theirs.Targets) {
			conflicts = append(conflicts, entity.Conflict{BatchID: other.ID, Type: entity.ConflictTypeTargetOverlap})
		}
	}
	return conflicts, nil
}

func (a *analyzer) recordedSignature(ctx context.Context, queue, batchID string) (TargetSignature, bool, error) {
	if signature, found := a.Cache.get(queue, batchID); found {
		return signature, true, nil
	}
	signature, found, err := a.Signatures.Get(ctx, batchID)
	if err != nil || !found {
		return TargetSignature{}, false, err
	}
	a.Cache.put(signature)
	return signature, true, nil
}

// recordLabels is best-effort: signatures compare IDs alone, and a label that
// fails to record is written again by any later batch that touches it.
func (a *analyzer) recordLabels(ctx context.Context, queue string, labels []hashedLabel) {
	createdAt := time.Now().UnixMilli()
	for _, l := range labels {
		err := a.Dictionary.Create(ctx, TargetLabel{Queue: queue, ID: l.id, Label: l.label, CreatedAt: createdAt})
		if err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			a.Logger.Warnw("failed to record target label", "queue", queue, "id", l.id, "label", l.label, "error", err)
		}
	}
}

// signNewBatch returns the batch's own record if an earlier attempt created
// one; otherwise it creates the signature first, so racing attempts agree.
func (a *analyzer) signNewBatch(ctx context.Context, batch entity.Batch) (TargetSignature, error) {
	signature, found, err := a.recordedSignature(ctx, batch.Queue, batch.ID)
	if err != nil || found {
		return signature, err
	}

	changes, err := a.Changes.ChangesForBatch(ctx, batch)
	if err != nil {
		return TargetSignature{}, fmt.Errorf("failed to resolve the changes: %w", err)
	}
	baseRevision, err := a.BaseRevision(ctx, batch.Queue)
	if err != nil {
		return TargetSignature{}, fmt.Errorf("failed to resolve the base revision of queue %s: %w", batch.Queue, err)
	}
	affected, err := a.Client.ChangedTargets(ctx, changedTargetsRequestFor(baseRevision, changes))
	if err != nil {
		return TargetSignature{}, fmt.Errorf("failed to get the changed targets: %w", err)
	}

	// TODO(tango): add the destination of each new dependency edge to the signature, the RFC's known gap. The adapter must return direct dependencies first.
	targets, labels := toSignatureTargets(affected)
	signature = TargetSignature{
		Queue:        batch.Queue,
		BatchID:      batch.ID,
		BaseRevision: baseRevision,
		Targets:      targets,
		CreatedAt:    time.Now().UnixMilli(),
	}

	if err := a.Signatures.Create(ctx, signature); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return TargetSignature{}, fmt.Errorf("failed to create the signature: %w", err)
		}
		signature, found, err = a.Signatures.Get(ctx, batch.ID)
		if err != nil {
			return TargetSignature{}, fmt.Errorf("failed to read the existing signature: %w", err)
		}
		if !found {
			return TargetSignature{}, fmt.Errorf("signature reported as existing but not found")
		}
	} else {
		// Only the attempt that created the signature records its labels, off the
		// request path and detached so a finished request does not cancel it.
		go a.recordLabels(context.WithoutCancel(ctx), batch.Queue, labels)
	}
	a.Cache.put(signature)
	return signature, nil
}
