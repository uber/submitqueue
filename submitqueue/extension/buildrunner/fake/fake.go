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

// Package fake provides a buildrunner.BuildRunner whose outcome is driven by the
// triggered changes. With no marker every build immediately succeeds, behaving
// as a best-case stub for wiring and baselines. Failures are injected by
// embedding a marker token in a head change URI of the form "sq-fake=<token>":
//
//	sq-fake=trigger-error -> Trigger returns a non-nil error
//	sq-fake=build-fail    -> Status reports BuildStatusFailed
//	sq-fake=build-error   -> Status returns a non-nil error
//
// The runner is stateless: Trigger encodes the desired terminal outcome into the
// returned BuildID, and Status decides the result purely from the BuildID it is
// given — no per-build bookkeeping. This means any runner instance can answer
// Status for an ID minted by any other (Trigger and Status can even live in
// different controllers/processes), and a single running stack can exercise the
// negative paths purely by varying request payloads. It is intended for examples
// and tests only, never production.
package fake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/uber/submitqueue/submitqueue/core/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// Recognized marker tokens. See the package doc for the convention.
const (
	tokenTriggerError = "trigger-error"
	tokenFail         = "build-fail"
	tokenError        = "build-error"
)

// outcomeOK is the BuildID outcome segment for a build that should succeed.
const outcomeOK = "ok"

// runner is a buildrunner.BuildRunner that reports every build as succeeded
// unless a marker token in a head change URI requests otherwise. It holds no
// per-build state: the outcome is encoded in the BuildID at Trigger and read
// back out at Status. Uniqueness comes from a random suffix per ID, so it needs
// no shared counter and never collides across instances or processes.
type runner struct{}

// New returns a buildrunner.BuildRunner that defaults to succeeding and honors
// marker tokens embedded in head change URIs.
func New() buildrunner.BuildRunner {
	return &runner{}
}

// Trigger fails when a head change URI carries the trigger-error marker;
// otherwise it returns a unique BuildID that encodes the terminal outcome the
// build should report at Status time (decided from the head marker). The base
// changes and metadata are ignored.
func (r *runner) Trigger(_ context.Context, _ []entity.Change, head []entity.Change, _ entity.BuildMetadata) (entity.BuildID, error) {
	outcome := outcomeOK
	switch fakemarker.TokenInChanges(head) {
	case tokenTriggerError:
		return entity.BuildID{}, fmt.Errorf("fake: marked trigger error")
	case tokenFail:
		outcome = tokenFail
	case tokenError:
		outcome = tokenError
	}

	// Encode the outcome in the ID (e.g. "fake-build-fail-a1b2c3d4") so Status is
	// stateless. The random suffix keeps IDs globally unique across instances and
	// processes — the BuildID uniqueness contract — without any shared state.
	suffix, err := randomSuffix()
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("fake: generating build id: %w", err)
	}
	id := fmt.Sprintf("fake-%s-%s", outcome, suffix)
	return entity.BuildID{ID: id}, nil
}

// randomSuffix returns a short random hex string used to keep fake BuildIDs
// globally unique. Hex digits never spell the outcome marker tokens, so the
// suffix cannot interfere with Status decoding the outcome via substring match.
func randomSuffix() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Status decides the result purely from the BuildID's encoded outcome. IDs that
// carry no recognized outcome (including those not minted by this fake) default
// to succeeded, keeping the runner best-case.
func (r *runner) Status(_ context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	switch {
	case strings.Contains(buildID.ID, tokenError):
		return entity.BuildStatusUnknown, nil, fmt.Errorf("fake: marked build error")
	case strings.Contains(buildID.ID, tokenFail):
		return entity.BuildStatusFailed, nil, nil
	default:
		return entity.BuildStatusSucceeded, nil, nil
	}
}

// Cancel is a no-op and always succeeds.
func (r *runner) Cancel(_ context.Context, _ entity.BuildID) error {
	return nil
}
