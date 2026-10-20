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

// Package fixer defines one-shot repair submission.
package fixer

//go:generate mockgen -source=fixer.go -destination=mock/fixer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RepairExecutor submits one repair attempt and returns its provider reference.
// A nonempty reference means submission was accepted, not that the issue recovered.
// An error can follow acceptance. Callers must not automatically repeat Submit.
type RepairExecutor interface {
	Submit(context.Context, SubmitRequest) (string, error)
}

// SubmitRequest identifies the validation failure to repair within the configured queue.
type SubmitRequest struct {
	// BuildID identifies the build whose failure initiated this submission.
	BuildID string
	// Issue identifies the selected condition and its continuous opening.
	Issue entity.OpenIssue
}

// Config identifies the queue whose repair extension is being resolved.
type Config struct {
	// QueueName is the validation queue.
	QueueName string
}

// Factory resolves configured repair executors.
type Factory interface {
	For(Config) (RepairExecutor, error)
}
