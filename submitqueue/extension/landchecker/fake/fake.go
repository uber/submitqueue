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

// Package fake provides a landchecker.LandChecker whose outcome is driven by
// the input change. With no marker it reports every change as landable,
// behaving as a best-case stub for wiring and baselines. A failure can be
// injected end-to-end (e.g. from an e2e land request) by embedding a marker
// token in a change URI of the form "sq-fake=<token>":
//
//	sq-fake=unlandable     -> Result{Landable: false}
//	sq-fake=landcheck-error -> non-nil error
//
// This lets a single running stack exercise negative paths purely by varying
// request payloads. It is intended for examples and tests only, never
// production.
package fake

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/platform/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/landchecker"
)

// Recognized marker tokens. See the package doc for the convention.
const (
	tokenUnlandable = "unlandable"
	tokenError      = "landcheck-error"
)

// checker is a landchecker.LandChecker that reports changes as landable
// unless a marker token in a change URI requests otherwise.
type checker struct {
	// cfg is the per-queue identity this checker was built for.
	cfg landchecker.Config
}

// New returns a landchecker.LandChecker bound to the queue named in cfg that
// defaults to landable and honors marker tokens embedded in change URIs.
func New(cfg landchecker.Config) landchecker.LandChecker {
	return checker{cfg: cfg}
}

// Check reports the change as landable unless a recognized marker token is
// present in one of the request's change URIs.
func (checker) Check(_ context.Context, request entity.Request) (entity.LandCheckResult, error) {
	switch fakemarker.Token(request.Change.URIs) {
	case tokenUnlandable:
		return entity.LandCheckResult{Landable: false, Reason: "fake: marked unlandable"}, nil
	case tokenError:
		return entity.LandCheckResult{}, fmt.Errorf("fake: marked land-check error")
	default:
		return entity.LandCheckResult{Landable: true}, nil
	}
}
