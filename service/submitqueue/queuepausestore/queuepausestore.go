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

// Package queuepausestore selects the queuepause.Store a SubmitQueue service
// runs with, so the gateway and orchestrator resolve it the same way.
package queuepausestore

import (
	"fmt"
	"os"

	"github.com/uber/submitqueue/submitqueue/extension/queuepause"
	queuepausefile "github.com/uber/submitqueue/submitqueue/extension/queuepause/file"
	queuepausenoop "github.com/uber/submitqueue/submitqueue/extension/queuepause/noop"
	"go.uber.org/zap"
)

const pathEnv = "QUEUE_PAUSE_CONFIG_PATH"

// FromEnv returns the file-backed store when QUEUE_PAUSE_CONFIG_PATH is set,
// and otherwise a store under which no queue is ever paused. A path that
// cannot be loaded is an error, so a misconfigured service fails at startup.
func FromEnv(logger *zap.Logger) (queuepause.Store, error) {
	path := os.Getenv(pathEnv)
	if path == "" {
		logger.Info("queue pause disabled")
		return queuepausenoop.New(), nil
	}
	store, err := queuepausefile.NewStore(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load queue pause config: %w", err)
	}
	logger.Info("queue pause configured", zap.String("path", path))
	return store, nil
}
