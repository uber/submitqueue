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

package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const sentinelQueueDSN = "sentinel-user:super-secret@tcp(mysql:3306)/submitqueue?tls=sentinel-query-secret"

type queueInitializationLogFixture struct {
	logger *zap.Logger
	logs   *observer.ObservedLogs
}

func newQueueInitializationLogFixture(t *testing.T) *queueInitializationLogFixture {
	t.Helper()

	core, logs := observer.New(zap.InfoLevel)
	return &queueInitializationLogFixture{
		logger: zap.New(core),
		logs:   logs,
	}
}

func TestLogQueueInitialized(t *testing.T) {
	tests := []struct {
		name             string
		queueDSN         string
		forbiddenSecrets []string
	}{
		{
			name:     "omits credentials and retains backend",
			queueDSN: sentinelQueueDSN,
			forbiddenSecrets: []string{
				"sentinel-user",
				"super-secret",
				"sentinel-query-secret",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("QUEUE_MYSQL_DSN", tt.queueDSN)
			fixture := newQueueInitializationLogFixture(t)

			logQueueInitialized(fixture.logger)

			entries := fixture.logs.All()
			require.Len(t, entries, 1)
			assert.Equal(t, "initialized queue", entries[0].Message)
			assert.Equal(t, queueBackendMySQL, entries[0].ContextMap()["backend"])
			assert.NotContains(t, entries[0].ContextMap(), "dsn")

			renderedEntry := fmt.Sprintf("%s %v", entries[0].Message, entries[0].ContextMap())
			for _, secret := range tt.forbiddenSecrets {
				assert.NotContains(t, renderedEntry, secret)
			}
		})
	}
}
