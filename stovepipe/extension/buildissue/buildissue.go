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

// Package buildissue defines confirmed-issue discovery for active or
// completed builds, independently of build status polling.
package buildissue

//go:generate mockgen -source=buildissue.go -destination=mock/buildissue_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Detector discovers issues retained in a build's accumulated results.
// Calls may repeat or omit earlier issues; absence does not retract them.
// A successful terminal call must discover all remaining reportable issues.
// Implementations must be concurrency-safe and must not consume evidence.
// Return plain dependency errors; issue results returned with an error must not be used.
type Detector interface {
	Detect(ctx context.Context, buildID entity.BuildID) ([]entity.BuildIssue, error)
}

// DetectorConfig identifies the queue whose issue detector is requested.
type DetectorConfig struct {
	// QueueName identifies the queue.
	QueueName string
}

// DetectorFactory resolves a detector for a validation queue.
type DetectorFactory interface {
	For(cfg DetectorConfig) (Detector, error)
}
