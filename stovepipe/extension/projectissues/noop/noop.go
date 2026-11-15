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

// Package noop skips project attribution when no backend is configured.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectissues"
)

var _ projectissues.AttributerFactory = AttributerFactory{}

// AttributerFactory returns an attributer that performs no assessment.
type AttributerFactory struct{}

// New returns a no-op attribution factory.
func New() AttributerFactory {
	return AttributerFactory{}
}

// For returns an attributer that skips attribution.
func (AttributerFactory) For(projectissues.AttributerConfig) (projectissues.Attributer, error) {
	return attributer{}, nil
}

type attributer struct{}

func (attributer) Attribute(context.Context, entity.Request) ([]projectissues.Attribution, error) {
	return nil, nil
}
