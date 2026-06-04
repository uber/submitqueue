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

// Package buildkite implements buildrunner.BuildRunner backed by the Buildkite
// CI platform.
//
// Trigger is non-blocking: it generates a build ID, enqueues the job on a
// buffered channel, and returns immediately. This keeps the orchestrator's
// queue loop decoupled from Buildkite availability — provider-side work
// happens asynchronously, per the BuildRunner contract. A background worker
// drains the channel, submits the build to Buildkite (retrying transient
// failures with backoff), and stamps the SQ build ID into the build's
// metadata. If submission fails after all retries, the build is recorded as a
// submission failure and Status reports it as terminal Failed. Cancel is
// similarly async.
//
// The in-memory map from SQ build ID to Buildkite reference is a pure latency
// cache, not the source of truth. Because every build carries its SQ build ID
// in Buildkite metadata, Status and Cancel re-derive the reference with a
// metadata-filtered build lookup whenever the cache misses — including after a
// process restart that empties the map. Nothing about a build's identity lives
// only in memory.
//
// The Buildkite build receives base and head change URIs as JSON-encoded
// environment variables (SQ_BASE_URIS, SQ_HEAD_URIS, SQ_QUEUE). The pipeline
// script fetches each PR's diff with the GitHub API, applies them with
// `git apply -3`, produces one commit per layer (base, head), then runs CI.
package buildkite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// Env var keys set on every triggered Buildkite build.
const (
	// EnvKeyBaseURIs carries the JSON-encoded ordered list of change URIs from
	// the dependency batches. The pipeline script applies these first and
	// commits the result as the "base" layer.
	EnvKeyBaseURIs = "SQ_BASE_URIS"

	// EnvKeyHeadURIs carries the JSON-encoded ordered list of change URIs from
	// the batch under test. Applied on top of the base layer, committed
	// separately.
	EnvKeyHeadURIs = "SQ_HEAD_URIS"

	// EnvKeyQueue carries the SQ queue name so the pipeline script can select
	// queue-specific test targets.
	EnvKeyQueue = "SQ_QUEUE"
)

// metaKeyBuildID is the Buildkite build-metadata key under which the SQ build
// ID is stored at create time. Status and Cancel filter builds by this key to
// recover the Buildkite reference when the in-memory cache misses (e.g. after
// a restart), which keeps that cache from being a durable source of truth.
const metaKeyBuildID = "sq_build_id"

const (
	defaultTriggerQueueSize  = 256
	defaultCancelQueueSize   = 256
	defaultSubmitTimeout     = 30 * time.Second
	defaultMaxSubmitAttempts = 5
	defaultSubmitBackoff     = 1 * time.Second
)

// triggerJob carries everything the background worker needs to submit one build
// to Buildkite. It is enqueued by Trigger and consumed by processTrigger.
type triggerJob struct {
	buildID  string
	baseURIs []string
	headURIs []string
}

// cancelJob carries the build ID the background worker should cancel in
// Buildkite. It is enqueued by Cancel and consumed by processCancel.
type cancelJob struct {
	buildID string
}

// runner implements buildrunner.BuildRunner. A background goroutine drains
// triggerCh and cancelCh for the lifetime of the runner, dispatching each job
// to its own goroutine so that a slow submit (retry/backoff) never head-of-line
// blocks other builds.
type runner struct {
	cfg    Config
	client *client

	// mu protects refs and submitFailures.
	mu sync.RWMutex
	// refs maps our internal build ID to the encoded Buildkite reference
	// ("{org}/{pipeline}/{number}"). It is a pure latency cache: the durable
	// record is the SQ build ID stamped into the Buildkite build's metadata,
	// so a missing entry is recovered via the client's metadata lookup rather
	// than treated as authoritative.
	refs map[string]string
	// submitFailures records build IDs whose Buildkite submission permanently
	// failed (all retries exhausted), mapped to a short reason. Status reports
	// these as terminal Failed so the build does not poll Accepted forever.
	// This is in-memory only: a restart that loses it leaves the build Accepted,
	// which the orchestrator's (out-of-scope) Accepted deadline must catch.
	submitFailures map[string]string

	// triggerCh queues pending build-creation jobs from Trigger.
	triggerCh chan triggerJob
	// cancelCh queues pending build-cancellation jobs from Cancel.
	cancelCh chan cancelJob
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// Params holds the dependencies for a Buildkite BuildRunner. The caller is
// responsible for configuring HTTPClient with the base URL (via
// httpclient.BaseURLTransport) and auth (via an Authorization-header transport).
type Params struct {
	// Config holds Buildkite-specific configuration for a single queue.
	Config Config
	// HTTPClient is a pre-configured HTTP client. The caller is responsible
	// for the base URL (via httpclient.BaseURLTransport) and auth (via a
	// transport layer). If nil, http.DefaultClient is used.
	HTTPClient *http.Client
}

// NewBuildRunner constructs a Buildkite-backed BuildRunner bound to a single
// pipeline and starts its background worker goroutine. The goroutine runs for
// the lifetime of the process.
//
// Returns an error if OrgSlug, PipelineSlug, or Branch are empty.
func NewBuildRunner(params Params) (buildrunner.BuildRunner, error) {
	cfg := params.Config
	if cfg.OrgSlug == "" {
		return nil, fmt.Errorf("buildkite: OrgSlug is required")
	}
	if cfg.PipelineSlug == "" {
		return nil, fmt.Errorf("buildkite: PipelineSlug is required")
	}
	if cfg.Branch == "" {
		return nil, fmt.Errorf("buildkite: Branch is required")
	}

	httpClient := params.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	triggerSize := cfg.TriggerQueueSize
	if triggerSize == 0 {
		triggerSize = defaultTriggerQueueSize
	}
	cancelSize := cfg.CancelQueueSize
	if cancelSize == 0 {
		cancelSize = defaultCancelQueueSize
	}

	r := newRunner(cfg, &client{
		httpClient: httpClient,
	}, triggerSize, cancelSize)
	go r.work()
	return r, nil
}

// newRunner constructs a runner without starting the background goroutine.
// Used by New and by tests (which drive the goroutine via drainTrigger /
// drainCancel to avoid timing races).
func newRunner(cfg Config, c *client, triggerSize, cancelSize int) *runner {
	return &runner{
		cfg:            cfg,
		client:         c,
		refs:           make(map[string]string),
		submitFailures: make(map[string]string),
		triggerCh:      make(chan triggerJob, triggerSize),
		cancelCh:       make(chan cancelJob, cancelSize),
	}
}

// Trigger generates a unique build ID, enqueues an async job to submit the
// build to Buildkite, and returns immediately. The build is visible to Status
// as Accepted until the background worker contacts Buildkite and the build
// becomes discoverable by its stamped metadata.
//
// Returns an error (retryable by the controller) if the trigger queue is full.
func (r *runner) Trigger(_ context.Context, base, head []entity.Change, _ entity.BuildMetadata) (entity.BuildID, error) {
	id := newBuildID()
	job := triggerJob{
		buildID:  id,
		baseURIs: flattenURIs(base),
		headURIs: flattenURIs(head),
	}
	select {
	case r.triggerCh <- job:
		return entity.BuildID{ID: id}, nil
	default:
		return entity.BuildID{}, fmt.Errorf("buildkite: trigger queue full; backpressure from Buildkite API")
	}
}

// Status returns the current build status. While the async submission is still
// in flight (no Buildkite build carries this build ID yet) it returns Accepted.
// Once the build exists, Status fetches the live state from Buildkite and
// returns it with the build URL in BuildMetadata["url"].
//
// If the submission permanently failed (all retries exhausted), Status reports
// terminal Failed with the reason in BuildMetadata["error"].
//
// On a cache miss Status re-derives the Buildkite reference by filtering builds
// on the stamped SQ build ID, so it works after a restart that emptied the
// in-memory cache.
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	ref, cached := r.lookupRef(buildID.ID)

	var resp buildResponse
	switch {
	case cached:
		org, pipeline, number, err := parseBuildRef(ref)
		if err != nil {
			return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: malformed build ref: %w", err)
		}
		resp, err = r.client.getBuild(ctx, org, pipeline, number)
		if err != nil {
			return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: get build: %w", err)
		}
	default:
		if reason, failed := r.lookupSubmitFailure(buildID.ID); failed {
			// The async submission exhausted its retries; nothing exists in
			// Buildkite to poll. Report terminal Failed so the build does not
			// loop in Accepted forever.
			return entity.BuildStatusFailed, entity.BuildMetadata{"error": reason}, nil
		}
		found, exists, err := r.client.findBuildByMeta(ctx, r.cfg.OrgSlug, r.cfg.PipelineSlug, metaKeyBuildID, buildID.ID)
		if err != nil {
			return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: find build: %w", err)
		}
		if !exists {
			// Not yet visible in Buildkite — the submission is still in flight
			// (or queued for retry). Report Accepted so the buildsignal poll
			// loop keeps waiting without treating this as terminal.
			return entity.BuildStatusAccepted, nil, nil
		}
		ref = encodeBuildRef(r.cfg.OrgSlug, r.cfg.PipelineSlug, found.Number)
		r.storeRef(buildID.ID, ref)
		resp = found
	}

	return mapState(resp.State), entity.BuildMetadata{"url": resp.WebURL}, nil
}

// Cancel enqueues an async cancellation and returns immediately, keeping the
// caller's queue loop decoupled from Buildkite availability. The background
// worker delivers the cancel to Buildkite, recovering the build reference from
// the stamped metadata if it is not cached (e.g. after a restart). If the build
// was never submitted, the cancel is a no-op.
//
// Returns an error if the cancel queue is full; the caller should retry.
func (r *runner) Cancel(_ context.Context, buildID entity.BuildID) error {
	select {
	case r.cancelCh <- cancelJob{buildID: buildID.ID}:
		return nil
	default:
		return fmt.Errorf("buildkite: cancel queue full; try again later")
	}
}

// work is the background consumer goroutine. It dispatches each trigger and
// cancel job to its own goroutine so that a job's retry/backoff does not block
// the others. processTrigger and processCancel guard shared state with r.mu and
// are safe to run concurrently.
func (r *runner) work() {
	for {
		select {
		case job := <-r.triggerCh:
			go r.processTrigger(job)
		case job := <-r.cancelCh:
			go r.processCancel(job)
		}
	}
}

// processTrigger submits one build to Buildkite, retrying transient failures
// with backoff. On success it caches the Buildkite reference so subsequent
// Status calls skip the metadata lookup. If every attempt fails the build was
// never created, so it is recorded as a submission failure and Status reports
// it as terminal Failed rather than polling Accepted forever.
func (r *runner) processTrigger(job triggerJob) {
	baseJSON, _ := json.Marshal(job.baseURIs)
	headJSON, _ := json.Marshal(job.headURIs)

	req := createBuildRequest{
		Branch:  r.cfg.Branch,
		Message: "submitqueue speculative build",
		Env: map[string]string{
			EnvKeyBaseURIs: string(baseJSON),
			EnvKeyHeadURIs: string(headJSON),
			EnvKeyQueue:    r.cfg.QueueName,
		},
		MetaData: map[string]string{
			metaKeyBuildID: job.buildID,
		},
	}

	var resp buildResponse
	err := r.withRetry(func() error {
		ctx, cancel := r.opCtx()
		defer cancel()
		var e error
		resp, e = r.client.createBuild(ctx, r.cfg.OrgSlug, r.cfg.PipelineSlug, req)
		return e
	})
	if err != nil {
		r.markSubmitFailed(job.buildID, fmt.Sprintf("buildkite submission failed after retries: %v", err))
		return
	}

	r.storeRef(job.buildID, encodeBuildRef(r.cfg.OrgSlug, r.cfg.PipelineSlug, resp.Number))
}

// processCancel cancels the Buildkite build, recovering its reference from the
// stamped metadata when the cache misses. No-ops when no build carries this
// build ID yet (trigger not yet processed, or submission failed).
func (r *runner) processCancel(job cancelJob) {
	ref, cached := r.lookupRef(job.buildID)
	if !cached {
		ctx, cancel := r.opCtx()
		found, exists, err := r.client.findBuildByMeta(ctx, r.cfg.OrgSlug, r.cfg.PipelineSlug, metaKeyBuildID, job.buildID)
		cancel()
		if err != nil || !exists {
			// Nothing to cancel (not yet submitted) or a transient lookup
			// failure; the caller may re-issue Cancel.
			return
		}
		ref = encodeBuildRef(r.cfg.OrgSlug, r.cfg.PipelineSlug, found.Number)
		r.storeRef(job.buildID, ref)
	}

	org, pipeline, number, err := parseBuildRef(ref)
	if err != nil {
		return
	}

	_ = r.withRetry(func() error {
		ctx, cancel := r.opCtx()
		defer cancel()
		return r.client.cancelBuild(ctx, org, pipeline, number)
	})
}

// withRetry runs fn up to MaxSubmitAttempts times with linear backoff, returning
// the last error. Used for the background submit and cancel API calls so a
// transient Buildkite failure does not abandon the work.
func (r *runner) withRetry(fn func() error) error {
	attempts := r.cfg.MaxSubmitAttempts
	if attempts <= 0 {
		attempts = defaultMaxSubmitAttempts
	}
	backoff := r.cfg.SubmitBackoff
	if backoff <= 0 {
		backoff = defaultSubmitBackoff
	}

	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt < attempts {
			time.Sleep(backoff * time.Duration(attempt))
		}
	}
	return err
}

// opCtx returns a context bounded by SubmitTimeout for a single background API
// call. The caller must invoke the returned CancelFunc.
func (r *runner) opCtx() (context.Context, context.CancelFunc) {
	timeout := r.cfg.SubmitTimeout
	if timeout == 0 {
		timeout = defaultSubmitTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

// lookupRef returns the cached Buildkite reference for a build ID.
func (r *runner) lookupRef(buildID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ref, ok := r.refs[buildID]
	return ref, ok
}

// storeRef caches the Buildkite reference for a build ID.
func (r *runner) storeRef(buildID, ref string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs[buildID] = ref
}

// markSubmitFailed records that a build's Buildkite submission permanently
// failed, so Status reports it as terminal Failed.
func (r *runner) markSubmitFailed(buildID, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.submitFailures[buildID] = reason
}

// lookupSubmitFailure reports whether a build's submission permanently failed,
// returning the recorded reason.
func (r *runner) lookupSubmitFailure(buildID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reason, ok := r.submitFailures[buildID]
	return reason, ok
}

// newBuildID returns a cryptographically random hex string prefixed with "bk-"
// that uniquely identifies a build within this runner implementation.
func newBuildID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand.Read only fails when the OS entropy source is broken.
		panic(fmt.Sprintf("buildkite: crypto/rand.Read failed: %v", err))
	}
	return "bk-" + hex.EncodeToString(b)
}

// flattenURIs concatenates the URI lists from all changes into a single slice.
func flattenURIs(changes []entity.Change) []string {
	uris := make([]string, 0, len(changes))
	for _, c := range changes {
		uris = append(uris, c.URIs...)
	}
	return uris
}

// encodeBuildRef encodes org, pipeline, and build number into the internal
// reference string stored in r.refs.
// Format: "{org}/{pipeline}/{number}". Buildkite slugs are [a-z0-9-] so "/"
// is unambiguous as a separator.
func encodeBuildRef(org, pipeline string, number int) string {
	return fmt.Sprintf("%s/%s/%d", org, pipeline, number)
}

// parseBuildRef is the inverse of encodeBuildRef.
func parseBuildRef(ref string) (org, pipeline string, number int, err error) {
	last := strings.LastIndex(ref, "/")
	if last < 1 {
		return "", "", 0, fmt.Errorf("invalid build ref %q", ref)
	}
	number, err = strconv.Atoi(ref[last+1:])
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid build ref %q: non-numeric number segment", ref)
	}
	prefix := ref[:last]
	first := strings.Index(prefix, "/")
	if first < 1 {
		return "", "", 0, fmt.Errorf("invalid build ref %q", ref)
	}
	return prefix[:first], prefix[first+1:], number, nil
}

// mapState maps a Buildkite build state string to a BuildStatus.
//
// Buildkite states: creating, scheduled, running, blocked, passed, failed,
// canceling, canceled, skipped, not_run.
func mapState(state string) entity.BuildStatus {
	switch state {
	case "creating", "scheduled":
		return entity.BuildStatusAccepted
	case "running", "blocked":
		// blocked = waiting on a block step; still live, not yet terminal.
		return entity.BuildStatusRunning
	case "passed":
		return entity.BuildStatusSucceeded
	case "failed", "not_run", "skipped":
		// not_run/skipped never produced a passing result; treat them as
		// terminal failure so the batch is not merged on a non-success verdict.
		return entity.BuildStatusFailed
	case "canceling", "canceled":
		return entity.BuildStatusCancelled
	default:
		// Unrecognised Buildkite state. Do NOT assume terminal: Unknown is
		// non-terminal, so the buildsignal poll loop keeps waiting rather than
		// failing the batch on a state this code does not understand.
		return entity.BuildStatusUnknown
	}
}
