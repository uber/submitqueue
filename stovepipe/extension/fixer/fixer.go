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

// Package fixer defines repair submission for an open issue.
package fixer

//go:generate mockgen -source=fixer.go -destination=mock/fixer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RepairExecutor submits repair work for one open issue within a configured queue.
// Implementations must be concurrency-safe and return promptly after submission.
type RepairExecutor interface {
	// Submit returns a nonempty opaque provider attempt ID on acceptance.
	// Acceptance does not establish recovery.
	// Repeated calls may start additional attempts. A plain error can follow acceptance,
	// including on context cancellation; callers must not automatically repeat Submit.
	Submit(context.Context, entity.OpenIssue) (string, error)
}

// Config identifies the queue whose repair executor is requested.
type Config struct {
	// QueueName identifies the validation queue.
	QueueName string
}

// Factory resolves queue-bound executors; routing belongs to service wiring.
type Factory interface {
	For(Config) (RepairExecutor, error)
}
