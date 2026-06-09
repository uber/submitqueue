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

// Package changeingester provides ChangeIngester implementations.
package changeingester

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension"
	"go.uber.org/zap"
)

// LoggingHandler is a stub ChangeHandler that logs received changes.
// Replace with real persistence logic once DB schema is ready.
type LoggingHandler struct {
	logger *zap.Logger
}

// New constructs a new LoggingHandler.
// The return type enforces interface compliance at compile time.
func New(logger *zap.Logger) extension.ChangeHandler {
	return LoggingHandler{logger: logger}
}

func (h LoggingHandler) IngestChange(ctx context.Context, info entity.ChangeInfo) error {
	h.logger.Info("ingested change",
		zap.String("uri", info.URI),
		zap.String("previous_uri", info.PreviousURI),
		zap.String("author_name", info.AuthorName),
		zap.String("author_email", info.AuthorEmail),
	)
	return nil
}
