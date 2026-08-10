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

// Package fake provides a buildrunner.BuildRunner whose outcome is driven by
// construction-time Params and by the triggered head URI. With zero-value Params
// and no marker every build immediately succeeds, behaving as a best-case stub for
// local-stack/e2e wiring.
//
// Params set the behavior for every build: FailurePercent makes a share of builds
// report BuildStatusFailed, BuildDuration makes builds report BuildStatusRunning
// for a while before turning terminal, and DurationJitterPercent spreads that
// duration per build within stated bounds. Per-build behavior is
// injected instead by embedding a marker token in headURI of the form
// "buildrunner-fake=<token>":
//
//	buildrunner-fake=trigger-error -> Trigger returns a non-nil error
//	buildrunner-fake=build-fail    -> Status reports BuildStatusFailed
//	buildrunner-fake=build-error   -> Status returns a non-nil error
//	buildrunner-fake=build-slow    -> Status reports BuildStatusRunning for a
//	                                  short window after Trigger, then reports
//	                                  the terminal outcome
//
// A marker overrides the configured behavior for the build that carries it: the
// outcome markers pin the outcome the configured rate would otherwise draw, and
// build-slow pins the running window regardless of BuildDuration.
//
// The runner is stateless: Trigger encodes the desired terminal outcome and the
// instant the build reaches it into the returned BuildID, and Status decides the
// result purely from the BuildID it is given — no per-build bookkeeping. This
// means any runner instance can answer Status for an id minted by any other
// (Trigger and Status can even live in different processes), and a single running
// stack can exercise the negative paths purely by varying request payloads. It is
// intended for examples and tests only, never production.
package fake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

// _defaultSlowBuildDuration is how long a build-slow build reports
// BuildStatusRunning before turning terminal when Params leaves BuildDuration
// unset. It must be long enough for the caller's poll loop to observe at least
// one non-terminal status.
const _defaultSlowBuildDuration = 3 * time.Second

// _markerPrefix introduces a marker token in headURI: "buildrunner-fake=<token>".
const _markerPrefix = "buildrunner-fake="

// Recognized marker tokens. See the package doc for the convention.
const (
	_tokenTriggerError = "trigger-error"
	_tokenFail         = "build-fail"
	_tokenError        = "build-error"
	_tokenSlow         = "build-slow"
)

// _outcomeOK is the BuildID outcome segment for a build that should succeed.
const _outcomeOK = "ok"

// _idPrefix introduces every BuildID this fake mints.
const _idPrefix = "fake-"

// _percentScale is the draw range for a percentage sample: a draw in [0,100)
// compared against a percentage in [0,100] yields that percentage of hits.
const _percentScale = 100

// Params configures how a fake runner behaves for builds whose head URI carries
// no marker. The zero value is the best-case stub: every build succeeds
// immediately.
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config

	// FailurePercent is the share of builds, in percent, whose Status reports
	// BuildStatusFailed rather than BuildStatusSucceeded. Valid range is 0-100.
	// The outcome is drawn independently per Trigger, so this is a rate over
	// many builds rather than a guarantee about any fixed number of them. The
	// zero value never fails.
	FailurePercent int

	// BuildDuration is how long a build reports BuildStatusRunning, measured
	// from Trigger, before it reports its terminal outcome. The zero value makes
	// builds terminal on the first Status call. It also becomes the window for
	// the build-slow marker, which otherwise uses its own default.
	BuildDuration time.Duration

	// DurationJitterPercent spreads each build's duration around BuildDuration,
	// as a percentage of it: 25 draws uniformly from [0.75×BuildDuration,
	// 1.25×BuildDuration], so builds vary but stay within bounds the integrator
	// can state in one line. Valid range is 0-100, which keeps the lower bound at
	// or above zero; at 100 a build may come back terminal immediately or take
	// twice BuildDuration. The spread is drawn independently per Trigger and
	// applies to the build-slow marker's window too. The zero value makes every
	// build take exactly its window.
	DurationJitterPercent int
}

// runner is a buildrunner.BuildRunner that reports builds as succeeded unless
// its configured failure rate or a marker token in headURI requests otherwise.
// It holds no per-build state: the outcome and the instant it becomes terminal
// are encoded in the BuildID at Trigger and read back out at Status. Uniqueness
// comes from a random suffix per id, so it needs no shared counter and never
// collides across instances or processes.
type runner struct {
	// cfg is the per-queue identity this runner was built for.
	cfg buildrunner.Config

	// failurePercent is the share of builds, in percent, that report failed.
	// Configuration, not per-build state: it is read at Trigger to draw the
	// outcome baked into the id, never mutated.
	failurePercent int

	// buildDuration is how long every build reports running before it turns
	// terminal. Zero leaves builds terminal immediately.
	buildDuration time.Duration

	// slowBuildDuration is the running window for a build-slow build, which
	// applies even when buildDuration leaves other builds terminal immediately.
	slowBuildDuration time.Duration

	// durationJitterPercent is how far, in percent, a build's duration may fall
	// either side of its window. Zero pins every build to its window exactly.
	durationJitterPercent int

	// intn draws the outcome and duration samples from [0,n). A field rather than
	// a direct call to math/rand so tests can pin the draws.
	intn func(n int) int
}

// New returns a buildrunner.BuildRunner bound to the queue named in
// params.Config, whose failure rate and build duration come from params, and
// which honors marker tokens embedded in the triggered headURI. The zero-value
// Params ask for the best-case stub: every build succeeds immediately. It rejects a percentage outside 0-100 and a negative
// BuildDuration so a misconfigured stack fails at wiring time rather than
// silently running different builds than the integrator asked for.
func New(params Params) (buildrunner.BuildRunner, error) {
	if params.FailurePercent < 0 || params.FailurePercent > _percentScale {
		return nil, fmt.Errorf("fake: failure percent %d outside 0-100", params.FailurePercent)
	}
	if params.BuildDuration < 0 {
		return nil, fmt.Errorf("fake: build duration %s is negative", params.BuildDuration)
	}
	if params.DurationJitterPercent < 0 || params.DurationJitterPercent > _percentScale {
		return nil, fmt.Errorf("fake: duration jitter percent %d outside 0-100", params.DurationJitterPercent)
	}
	return newRunner(params), nil
}

// newRunner constructs a runner from validated params. Used by New and by tests
// that need to override a field the Params do not expose.
func newRunner(params Params) runner {
	slowBuildDuration := params.BuildDuration
	if slowBuildDuration <= 0 {
		slowBuildDuration = _defaultSlowBuildDuration
	}
	return runner{
		cfg:                   params.Config,
		failurePercent:        params.FailurePercent,
		buildDuration:         params.BuildDuration,
		slowBuildDuration:     slowBuildDuration,
		durationJitterPercent: params.DurationJitterPercent,
		intn:                  mathrand.IntN,
	}
}

// Trigger fails when headURI carries the trigger-error marker; otherwise it
// returns a unique BuildID that encodes both the terminal outcome the build
// should report at Status time and the instant it reaches it. The outcome comes
// from the configured failure rate unless a marker in headURI pins it. baseURI
// and metadata are ignored.
func (r runner) Trigger(_ context.Context, _, headURI string, _ entity.BuildMetadata) (entity.BuildID, error) {
	outcome := r.drawOutcome()
	readyAt := terminalAt(r.drawDuration(r.buildDuration))
	switch marker(headURI) {
	case _tokenTriggerError:
		return entity.BuildID{}, fmt.Errorf("fake: marked trigger error")
	case _tokenFail:
		outcome = _tokenFail
	case _tokenError:
		outcome = _tokenError
	case _tokenSlow:
		// Only the timing is pinned; the outcome still comes from the draw
		// above, so a slow build fails at the configured rate like any other.
		readyAt = terminalAt(r.drawDuration(r.slowBuildDuration))
	}

	// Encode the outcome and terminal instant in the id (e.g.
	// "fake-build-fail-0-a1b2c3d4") so Status is stateless. The random suffix
	// keeps ids globally unique across instances and processes without any
	// shared state.
	suffix, err := randomSuffix()
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("fake: generating build id: %w", err)
	}
	return entity.BuildID{ID: fmt.Sprintf("%s%s-%d-%s", _idPrefix, outcome, readyAt, suffix)}, nil
}

// Status decides the result purely from the BuildID's encoded outcome and
// terminal instant. Ids that carry no recognized outcome (including those not
// minted by this fake) default to succeeded, keeping the runner best-case.
func (r runner) Status(_ context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	outcome, readyAt := decodeID(buildID.ID)
	// A marked error is a failure of the Status call itself, so it surfaces
	// whether or not the build would still be running.
	if outcome == _tokenError {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("fake: marked build error")
	}
	if readyAt > time.Now().UnixMilli() {
		return entity.BuildStatusRunning, nil, nil
	}
	if outcome == _tokenFail {
		return entity.BuildStatusFailed, nil, nil
	}
	return entity.BuildStatusSucceeded, nil, nil
}

// Cancel is a no-op and always succeeds.
func (r runner) Cancel(_ context.Context, _ entity.BuildID) error {
	return nil
}

// drawOutcome samples the terminal outcome for a build from the configured
// failure rate.
func (r runner) drawOutcome() string {
	if r.failurePercent <= 0 {
		return _outcomeOK
	}
	if r.intn(_percentScale) < r.failurePercent {
		return _tokenFail
	}
	return _outcomeOK
}

// drawDuration samples how long one build runs: window spread uniformly by up to
// ±durationJitterPercent of itself, leaving window untouched when there is no
// window or no jitter configured. The draw covers the 2j+1 whole percentage
// points from -j to +j inclusive, so it is centered on window and, with the
// percentage capped at 100, never yields a negative duration.
func (r runner) drawDuration(window time.Duration) time.Duration {
	if window <= 0 || r.durationJitterPercent <= 0 {
		return window
	}
	offset := r.intn(2*r.durationJitterPercent+1) - r.durationJitterPercent
	return window + window*time.Duration(offset)/_percentScale
}

// marker returns the marker token embedded in uri, or "" if none is present.
// The token ends at the first "&", "#", or "/" delimiter, so a marker may sit
// among other query parameters, before a fragment, or ahead of a further path
// segment (as when a URI is built as "git://<queue>/HEAD" and the marker rides
// in on the queue name).
func marker(uri string) string {
	_, rest, found := strings.Cut(uri, _markerPrefix)
	if !found {
		return ""
	}
	if i := strings.IndexAny(rest, "&#/"); i >= 0 {
		rest = rest[:i]
	}
	return rest
}

// terminalAt converts a running window into the epoch-millisecond instant at
// which the build turns terminal, or 0 when it is terminal immediately. Baking
// the instant into the id is what keeps Status stateless: any instance, in any
// process, decodes the same deadline.
func terminalAt(window time.Duration) int64 {
	if window <= 0 {
		return 0
	}
	return time.Now().Add(window).UnixMilli()
}

// decodeID recovers the outcome and terminal instant that Trigger encoded into
// an id of the form "fake-<outcome>-<readyAtMs>-<suffix>". An id carrying no
// parsable instant — one minted before the instant became part of the id, or not
// minted by this fake at all — falls back to reading the outcome as a substring
// and to being terminal immediately.
func decodeID(id string) (string, int64) {
	// Trim the random suffix, then the instant, leaving "fake-<outcome>".
	if suffixAt := strings.LastIndex(id, "-"); suffixAt > 0 {
		head := id[:suffixAt]
		if instantAt := strings.LastIndex(head, "-"); instantAt > 0 {
			if readyAt, err := strconv.ParseInt(head[instantAt+1:], 10, 64); err == nil {
				return strings.TrimPrefix(head[:instantAt], _idPrefix), readyAt
			}
		}
	}
	return substringOutcome(id), 0
}

// substringOutcome reads the outcome out of an id that carries no encoded
// instant, matching the outcome tokens anywhere in the id.
func substringOutcome(id string) string {
	switch {
	case strings.Contains(id, _tokenError):
		return _tokenError
	case strings.Contains(id, _tokenFail):
		return _tokenFail
	default:
		return _outcomeOK
	}
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
