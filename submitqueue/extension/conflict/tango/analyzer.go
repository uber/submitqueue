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

	"github.com/uber-go/tally"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/baserevision"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// ErrNoSignature means an in-flight batch has no recorded signature. Tango is
// never asked for it: a signature computed later could differ from the one the
// batch was admitted with.
var ErrNoSignature = errors.New("no target signature recorded for in-flight batch")

// signatureTargetBuckets reaches past coremetrics.CountBuckets, which stops at
// 144, because a signature can hold millions of targets.
var signatureTargetBuckets = tally.ValueBuckets{0, 10, 100, 1000, 10000, 100000, 1000000}

// Params are the dependencies of one queue's Analyzer.
type Params struct {
	// Signatures persists signatures. It must be bound to the queue the
	// analyzer serves.
	Signatures SignatureStore

	// Cache holds recently used signatures and may be shared across queues.
	Cache *SignatureCache

	// Client returns the targets a set of changes affects.
	Client ChangedTargetsClient

	// BaseRevisions returns the commit a new batch is analyzed against.
	BaseRevisions baserevision.Resolver

	// Changes resolves a batch into the changes it contains.
	Changes changeset.Resolver

	// MetricsScope receives the analyzer's metrics. Nil discards them.
	MetricsScope tally.Scope
}

type analyzer struct {
	Params
	metricsScope tally.Scope
}

// New returns a conflict.Analyzer that reports an in-flight batch as a
// ConflictTypeTargetOverlap conflict when its signature shares a target ID with
// the analyzed batch's. It fails closed: any signature it cannot obtain fails
// the analysis.
func New(params Params) conflict.Analyzer {
	scope := params.MetricsScope
	if scope == nil {
		scope = tally.NoopScope
	}
	return &analyzer{Params: params, metricsScope: scope.SubScope("tango_conflict")}
}

// Analyze signs batch, which is the only step that can call Tango, and compares
// it with the recorded signature of each in-flight batch. The new batch is
// signed even when nothing is in flight, so later batches find its record.
func (a *analyzer) Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) (conflicts []entity.Conflict, retErr error) {
	tags := []coremetrics.Tag{coremetrics.NewTag("queue", batch.Queue)}
	op := coremetrics.Begin(a.metricsScope, "analyze", coremetrics.LongLatencyBuckets, tags...)
	defer func() { op.Complete(retErr) }()
	coremetrics.NamedHistogram(a.metricsScope, "analyze", "in_flight", coremetrics.CountBuckets, tags...).RecordValue(float64(len(inFlight)))

	mine, err := a.signNewBatch(ctx, batch, tags)
	if err != nil {
		return nil, fmt.Errorf("failed to sign batch %s: %w", batch.ID, err)
	}

	// TODO(tango): read the in-flight signatures concurrently with a bound.
	// New dependency edges are not in the signatures yet; see the TODO in signNewBatch.
	for _, other := range inFlight {
		theirs, found, cached, err := a.recordedSignature(ctx, other.Queue, other.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to read the signature of in-flight batch %s: %w", other.ID, err)
		}
		if cached {
			coremetrics.NamedCounter(a.metricsScope, "cache", "hit", 1, tags...)
		} else {
			coremetrics.NamedCounter(a.metricsScope, "cache", "miss", 1, tags...)
		}
		if !found {
			coremetrics.NamedCounter(a.metricsScope, "no_signature", "total", 1, tags...)
			return nil, fmt.Errorf("in-flight batch %s: %w", other.ID, ErrNoSignature)
		}
		if sharesTarget(mine.Targets, theirs.Targets) {
			conflicts = append(conflicts, entity.Conflict{BatchID: other.ID, Type: entity.ConflictTypeTargetOverlap})
		}
	}
	if len(conflicts) > 0 {
		coremetrics.NamedCounter(a.metricsScope, "conflicts", "total", int64(len(conflicts)),
			append(tags, coremetrics.NewTag("type", string(entity.ConflictTypeTargetOverlap)))...)
	}
	return conflicts, nil
}

// recordedSignature reports whether the signature was found and, if so,
// whether it came from the cache.
func (a *analyzer) recordedSignature(ctx context.Context, queue, batchID string) (signature TargetSignature, found, cached bool, err error) {
	if signature, found := a.Cache.get(queue, batchID); found {
		return signature, true, true, nil
	}
	signature, found, err = a.Signatures.Get(ctx, batchID)
	if err != nil || !found {
		return TargetSignature{}, false, false, err
	}
	a.Cache.put(signature)
	return signature, true, false, nil
}

// signNewBatch returns the batch's own record if an earlier attempt created
// one; otherwise it creates the signature first, so racing attempts agree.
func (a *analyzer) signNewBatch(ctx context.Context, batch entity.Batch, tags []coremetrics.Tag) (TargetSignature, error) {
	signature, found, _, err := a.recordedSignature(ctx, batch.Queue, batch.ID)
	if err != nil {
		return TargetSignature{}, err
	}
	if found {
		a.countSignatureOutcome("reused", tags)
		return signature, nil
	}

	changes, err := a.Changes.ChangesForBatch(ctx, batch)
	if err != nil {
		return TargetSignature{}, fmt.Errorf("failed to resolve the changes: %w", err)
	}
	baseOp := coremetrics.Begin(a.metricsScope, "base_revision", coremetrics.LongLatencyBuckets, tags...)
	baseRevision, err := a.BaseRevisions.Resolve(ctx, batch.Queue)
	baseOp.Complete(err)
	if err != nil {
		return TargetSignature{}, fmt.Errorf("failed to resolve the base revision of queue %s: %w", batch.Queue, err)
	}
	affected, err := a.Client.ChangedTargets(ctx, changedTargetsRequestFor(baseRevision, changes))
	if err != nil {
		return TargetSignature{}, fmt.Errorf("failed to get the changed targets: %w", err)
	}

	// TODO(tango): record the labels in the target dictionary before creating the signature.
	// TODO(tango): add the destination of each new dependency edge to the signature, the RFC's known gap. The adapter must return direct dependencies first.
	targets, _ := toSignatureTargets(affected)
	coremetrics.NamedHistogram(a.metricsScope, "signature", "targets", signatureTargetBuckets, tags...).RecordValue(float64(len(targets)))
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
		a.countSignatureOutcome("race_lost", tags)
	} else {
		a.countSignatureOutcome("created", tags)
	}
	a.Cache.put(signature)
	return signature, nil
}

func (a *analyzer) countSignatureOutcome(outcome string, tags []coremetrics.Tag) {
	coremetrics.NamedCounter(a.metricsScope, "signature", "outcomes", 1, append(tags, coremetrics.NewTag("outcome", outcome))...)
}
