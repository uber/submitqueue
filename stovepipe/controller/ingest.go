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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// ErrInvalidRequest is returned when the request fails validation.
// This error should be mapped to codes.InvalidArgument at the gRPC layer.
var ErrInvalidRequest = errs.NewUserError(errors.New("invalid request"))

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// IngestController handles ingest business logic for stovepipe: it admits a queue's newly
// observed commit into the validation pipeline.
//
// It resolves the queue's head commit URI via the SourceControl extension, dedups on the
// (queue, URI) pair, persists the Request and its URI mapping via storage, and publishes the
// request ID onto the process stage. Ingestion is idempotent: a re-reported head resolves to the
// already-minted request and no new work is published.
type IngestController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	counter       counter.Counter
	sourceControl sourcecontrol.Factory
	store         storage.Storage
	registry      consumer.TopicRegistry
}

// NewIngestController creates a new instance of the stovepipe ingest controller. It publishes
// accepted requests to the topic registered under messagequeue.TopicKeyProcess in the registry.
func NewIngestController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	counter counter.Counter,
	sourceControl sourcecontrol.Factory,
	store storage.Storage,
	registry consumer.TopicRegistry,
) *IngestController {
	return &IngestController{
		logger:        logger,
		metricsScope:  scope.SubScope("ingest_controller"),
		counter:       counter,
		sourceControl: sourceControl,
		store:         store,
		registry:      registry,
	}
}

// Ingest admits a queue's newly observed commit into the validation pipeline and returns the
// request ID validating it.
//
// It is idempotent and runs to completion on every call, each step tolerant of having already
// run: it resolves (or claims) the (queue, URI) mapping, ensures the Request row exists, and
// publishes the request to the process stage. A retry after a partial failure — for example the
// URI mapping committed but the request write failed — completes the missing steps instead of
// returning a dangling reference. The (queue, URI) mapping is the dedup gate, so concurrent
// ingests of the same head converge on one request.
func (c *IngestController) Ingest(ctx context.Context, req *pb.IngestRequest) (resp *pb.IngestResponse, retErr error) {
	const opName = "ingest"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	if req.Queue == "" {
		return nil, fmt.Errorf("IngestController requires the request to have a queue name specified: %w", ErrInvalidRequest)
	}
	queue := req.Queue

	// Resolve the queue's current head commit to its opaque URI via SourceControl.
	// An unresolvable queue/ref is a caller error (unknown queue), not infrastructure.
	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		return nil, fmt.Errorf("IngestController failed to resolve source control for queue=%s: %w", queue, err)
	}
	uri, err := sc.Latest(ctx)
	if err != nil {
		if sourcecontrol.IsNotFound(err) {
			return nil, fmt.Errorf("IngestController could not resolve head for queue=%s: %w: %w", queue, err, ErrInvalidRequest)
		}
		return nil, fmt.Errorf("IngestController failed to resolve head for queue=%s: %w", queue, err)
	}

	// The (queue, URI) mapping is the dedup gate and the source of truth for "does this head
	// have a request id".
	id, mintedSeq, err := c.resolveID(ctx, queue, uri)
	if err != nil {
		return nil, err
	}

	// Ensure the request row exists, healing a prior partial write where the mapping committed
	// but the request did not.
	request, created, err := c.ensureRequest(ctx, id, queue, uri, mintedSeq)
	if err != nil {
		return nil, err
	}

	// Stamp the queue's latest_request_seq only after a successful new request create so the
	// pointer stays aligned with minted sequences (dedup and heal paths skip this).
	if created {
		if err := c.stampLatestRequestSeq(ctx, queue, request.Sequence); err != nil {
			return nil, err
		}
	}

	// Publish while the request is still pre-pipeline (Accepted). The process consumer is
	// idempotent (keyed on the request id, at-least-once), so re-publishing on a retry or a
	// duplicate report is safe and closes the "request created but publish failed" gap. Once
	// process advances the request past Accepted, ingest stops re-publishing.
	if request.State == entity.RequestStateAccepted {
		if err := c.publishProcess(ctx, id, queue); err != nil {
			return nil, fmt.Errorf("IngestController failed to publish request %s to process: %w", id, err)
		}
	}

	c.logger.Infow("ingested request",
		"id", request.ID,
		"queue", request.Queue,
		"uri", request.URI,
		"state", request.State,
	)

	return &pb.IngestResponse{Id: id}, nil
}

// resolveID returns the request id mapped to (queue, URI), minting and claiming a new one if the
// pair is not yet mapped. mintedSeq is the counter value when this call claimed a new id; it is
// zero when the id came from an existing mapping (dedup or create race).
func (c *IngestController) resolveID(ctx context.Context, queue, uri string) (id string, mintedSeq int64, retErr error) {
	uriStore := c.store.GetRequestURIStore()

	if existing, err := uriStore.GetIDByURI(ctx, queue, uri); err == nil {
		return existing, 0, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return "", 0, fmt.Errorf("IngestController failed to look up existing request for queue=%s: %w", queue, err)
	}

	// Mint a globally unique request ID namespaced by the queue. The counter domain
	// ("request/<queue>") doubles as the ID prefix, so the ID is "<domain>/<counter>".
	domain := "request/" + queue
	seq, err := c.counter.Next(ctx, domain)
	if err != nil {
		return "", 0, fmt.Errorf("IngestController failed to generate request ID for queue=%s: %w", queue, err)
	}
	id = fmt.Sprintf("%s/%d", domain, seq)

	if err := uriStore.Create(ctx, queue, uri, id); err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			existing, getErr := uriStore.GetIDByURI(ctx, queue, uri)
			if getErr != nil {
				return "", 0, fmt.Errorf("IngestController failed to resolve raced request for queue=%s: %w", queue, getErr)
			}
			return existing, 0, nil
		}
		return "", 0, fmt.Errorf("IngestController failed to map URI for queue=%s: %w", queue, err)
	}
	return id, seq, nil
}

// ensureRequest returns the request for id, creating it in the Accepted state if it does not yet
// exist. created is true only when this call inserted the row. A concurrent creator
// (ErrAlreadyExists) is resolved by re-reading the canonical row.
func (c *IngestController) ensureRequest(
	ctx context.Context,
	id, queue, uri string,
	mintedSeq int64,
) (entity.Request, bool, error) {
	reqStore := c.store.GetRequestStore()

	got, err := reqStore.Get(ctx, id)
	if err == nil {
		return got, false, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return entity.Request{}, false, fmt.Errorf("IngestController failed to load request %s: %w", id, err)
	}

	sequence := mintedSeq
	if sequence == 0 {
		var parseErr error
		sequence, parseErr = parseRequestSequence(id, queue)
		if parseErr != nil {
			return entity.Request{}, false, fmt.Errorf("IngestController failed to parse sequence from request %s: %w", id, parseErr)
		}
	}

	request := entity.Request{
		ID:       id,
		Queue:    queue,
		URI:      uri,
		Sequence: sequence,
		State:    entity.RequestStateAccepted,
		Version:  1,
	}
	if err := reqStore.Create(ctx, request); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return entity.Request{}, false, fmt.Errorf("IngestController failed to persist request %s: %w", id, err)
		}
		// Raced with a concurrent creator; read the canonical row.
		got, getErr := reqStore.Get(ctx, id)
		return got, false, getErr
	}
	return request, true, nil
}

// stampLatestRequestSeq advances the queue row's latest_request_seq pointer after a new request
// is created. Retries on optimistic-lock conflicts so concurrent ingests converge.
func (c *IngestController) stampLatestRequestSeq(ctx context.Context, queueName string, seq int64) error {
	queueStore := c.store.GetQueueStore()

	for {
		queue, err := queueStore.GetOrCreate(ctx, queueName, entity.Queue{Version: 1})
		if err != nil {
			return fmt.Errorf("IngestController failed to load queue %s: %w", queueName, err)
		}
		if seq <= queue.LatestRequestSeq {
			return nil
		}

		updated := queue
		updated.LatestRequestSeq = seq
		newVersion := queue.Version + 1
		if err := queueStore.Update(ctx, updated, queue.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("IngestController failed to stamp latest_request_seq for queue %s: %w", queueName, err)
		}
		return nil
	}
}

// parseRequestSequence extracts the per-queue counter value from a request id of the form
// request/<queue>/<n>.
func parseRequestSequence(id, queue string) (int64, error) {
	prefix := "request/" + queue + "/"
	if !strings.HasPrefix(id, prefix) {
		return 0, fmt.Errorf("id %q does not match queue %q", id, queue)
	}
	return strconv.ParseInt(id[len(prefix):], 10, 64)
}

// publishProcess publishes the request ID to the process stage, partitioned by queue so a
// queue's requests stay ordered.
func (c *IngestController) publishProcess(ctx context.Context, id, queue string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id})
	if err != nil {
		return fmt.Errorf("failed to serialize process request: %w", err)
	}

	msg := entityqueue.NewMessage(id, payload, queue, nil)

	q, ok := c.registry.Queue(stovepipemq.TopicKeyProcess)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", stovepipemq.TopicKeyProcess)
	}
	topicName, ok := c.registry.TopicName(stovepipemq.TopicKeyProcess)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", stovepipemq.TopicKeyProcess)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish process request: %w", err)
	}
	return nil
}
