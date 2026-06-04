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
// Trigger calls the Buildkite API directly: it generates a build ID, stamps it
// into the build's metadata, and returns the ID on success. Cancel calls the
// Buildkite API directly as well. Both propagate errors to the caller, which can
// nack and retry via the normal queue consumer path.
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
	"sync"

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

// runner implements buildrunner.BuildRunner.
type runner struct {
	cfg    Config
	client *client

	// mu protects refs.
	mu sync.RWMutex
	// refs maps our internal build ID to the encoded Buildkite reference
	// ("{number}"). It is a pure latency cache: the durable record is the SQ
	// build ID stamped into the Buildkite build's metadata, so a missing entry
	// is recovered via the client's metadata lookup rather than treated as
	// authoritative.
	refs map[string]string
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
// pipeline.
//
// The HTTPClient must have BaseURLTransport configured to the pipeline's API
// root (e.g. "https://api.buildkite.com/v2/organizations/{org}/pipelines/{slug}"),
// and an auth transport that injects the Authorization header.
func NewBuildRunner(params Params) (buildrunner.BuildRunner, error) {
	httpClient := params.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return newRunner(params.Config, &client{httpClient: httpClient}), nil
}

// newRunner constructs a runner. Used by NewBuildRunner and by tests.
func newRunner(cfg Config, c *client) *runner {
	return &runner{
		cfg:    cfg,
		client: c,
		refs:   make(map[string]string),
	}
}

// Trigger generates a unique build ID, calls the Buildkite API to create the
// build, caches the Buildkite reference, and returns the build ID. Errors are
// propagated to the caller so the queue consumer can nack and retry.
func (r *runner) Trigger(ctx context.Context, base, head []entity.Change, _ entity.BuildMetadata) (entity.BuildID, error) {
	id := newBuildID()
	baseJSON, _ := json.Marshal(flattenURIs(base))
	headJSON, _ := json.Marshal(flattenURIs(head))

	req := createBuildRequest{
		Message: "submitqueue speculative build",
		Env: map[string]string{
			EnvKeyBaseURIs: string(baseJSON),
			EnvKeyHeadURIs: string(headJSON),
			EnvKeyQueue:    r.cfg.QueueName,
		},
		MetaData: map[string]string{
			metaKeyBuildID: id,
		},
	}

	resp, err := r.client.createBuild(ctx, req)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("buildkite: create build: %w", err)
	}

	r.storeRef(id, encodeBuildRef(resp.Number))
	return entity.BuildID{ID: id}, nil
}

// Status returns the current build status. On a cache miss it recovers the
// Buildkite reference by filtering builds on the stamped SQ build ID, so it
// works after a restart that emptied the in-memory cache. Returns Accepted when
// the build is not yet visible in Buildkite.
//
// On a cache hit, Status fetches the live state and returns it with the build
// URL in BuildMetadata["url"].
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	ref, cached := r.lookupRef(buildID.ID)

	var resp buildResponse
	switch {
	case cached:
		number, err := parseBuildRef(ref)
		if err != nil {
			return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: malformed build ref: %w", err)
		}
		resp, err = r.client.getBuild(ctx, number)
		if err != nil {
			return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: get build: %w", err)
		}
	default:
		found, exists, err := r.client.findBuildByMeta(ctx, metaKeyBuildID, buildID.ID)
		if err != nil {
			return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: find build: %w", err)
		}
		if !exists {
			// Not yet visible in Buildkite — e.g. after a restart where the cache
			// was lost but the build was created before. Report Accepted so the
			// buildsignal poll loop keeps waiting.
			return entity.BuildStatusAccepted, nil, nil
		}
		ref = encodeBuildRef(found.Number)
		r.storeRef(buildID.ID, ref)
		resp = found
	}

	return mapState(resp.State), entity.BuildMetadata{"url": resp.WebURL}, nil
}

// Cancel calls the Buildkite API to cancel the build. On a cache miss it
// recovers the reference from the stamped metadata. If no build carries this
// build ID (not yet submitted), Cancel is a no-op.
func (r *runner) Cancel(ctx context.Context, buildID entity.BuildID) error {
	ref, cached := r.lookupRef(buildID.ID)
	if !cached {
		found, exists, err := r.client.findBuildByMeta(ctx, metaKeyBuildID, buildID.ID)
		if err != nil {
			return fmt.Errorf("buildkite: find build for cancel: %w", err)
		}
		if !exists {
			return nil
		}
		ref = encodeBuildRef(found.Number)
		r.storeRef(buildID.ID, ref)
	}

	number, err := parseBuildRef(ref)
	if err != nil {
		return fmt.Errorf("buildkite: malformed build ref: %w", err)
	}

	if err := r.client.cancelBuild(ctx, number); err != nil {
		return fmt.Errorf("buildkite: cancel build: %w", err)
	}
	return nil
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

// encodeBuildRef encodes a Buildkite build number as the internal reference
// string stored in r.refs.
func encodeBuildRef(number int) string {
	return strconv.Itoa(number)
}

// parseBuildRef is the inverse of encodeBuildRef.
func parseBuildRef(ref string) (int, error) {
	n, err := strconv.Atoi(ref)
	if err != nil {
		return 0, fmt.Errorf("invalid build ref %q", ref)
	}
	return n, nil
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
