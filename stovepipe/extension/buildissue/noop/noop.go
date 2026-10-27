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

// Package noop provides a detector that returns no issue observations.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildissue"
)

// DetectorFactory returns a no-op detector for every queue.
type DetectorFactory struct{}

// New returns a no-op detector factory.
func New() DetectorFactory { return DetectorFactory{} }

func (DetectorFactory) For(buildissue.DetectorConfig) (buildissue.Detector, error) {
	return detector{}, nil
}

type detector struct{}

func (detector) Detect(context.Context, entity.BuildID) ([]entity.BuildIssue, error) {
	return nil, nil
}

var _ buildissue.DetectorFactory = DetectorFactory{}
var _ buildissue.Detector = detector{}
