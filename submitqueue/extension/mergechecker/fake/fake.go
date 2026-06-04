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

// Package fake provides a mergechecker.MergeChecker whose outcome is driven by
// the input change. With no marker it reports every change as mergeable,
// behaving as a best-case stub for wiring and baselines. A failure can be
// injected end-to-end (e.g. from an e2e land request) by embedding a marker
// token in a change URI of the form "sq-fake=<token>":
//
//	sq-fake=unmergeable     -> Result{Mergeable: false}
//	sq-fake=mergecheck-error -> non-nil error
//
// This lets a single running stack exercise negative paths purely by varying
// request payloads. It is intended for examples and tests only, never
// production.
package fake

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/core/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/mergechecker"
)

// Recognized marker tokens. See the package doc for the convention.
const (
	tokenUnmergeable = "unmergeable"
	tokenError       = "mergecheck-error"
)

// checker is a mergechecker.MergeChecker that reports changes as mergeable
// unless a marker token in a change URI requests otherwise.
type checker struct{}

// New returns a mergechecker.MergeChecker that defaults to mergeable and honors
// marker tokens embedded in change URIs.
func New() mergechecker.MergeChecker {
	return checker{}
}

// Check reports the change as mergeable unless a recognized marker token is
// present in one of its URIs.
func (checker) Check(_ context.Context, change entity.Change) (mergechecker.Result, error) {
	switch fakemarker.Token(change.URIs) {
	case tokenUnmergeable:
		return mergechecker.Result{Mergeable: false, Reason: "fake: marked unmergeable"}, nil
	case tokenError:
		return mergechecker.Result{}, fmt.Errorf("fake: marked merge-check error")
	default:
		return mergechecker.Result{Mergeable: true}, nil
	}
}
