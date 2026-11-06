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

package request

import (
	"context"
	"fmt"
	"maps"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// PublishTerminalRequestLog replays the terminal facts of a persisted request snapshot.
// Legacy snapshots without terminal details publish an explicitly incomplete diagnostic.
func PublishTerminalRequestLog(ctx context.Context, registry consumer.TopicRegistry, request entity.Request) error {
	status, err := terminalStateToStatus(request.State)
	if err != nil {
		return err
	}
	details := request.TerminalDetails
	logEntry := entity.NewRequestStatusLog(request.Queue, request.ID, status, request.Version, details.LastError, maps.Clone(details.Metadata))
	if details.TimestampMs == 0 {
		logEntry.LastError = ""
		logEntry.Metadata = map[string]string{entity.TerminalDetailsUnavailable: "true"}
	} else {
		logEntry.TimestampMs = details.TimestampMs
	}
	// No occurrence: repeats are redeliveries of the same terminal outcome.
	if err := PublishLog(ctx, registry, logEntry, request.ID, ""); err != nil {
		return fmt.Errorf("failed to publish request log for %s: %w", request.ID, err)
	}
	return nil
}
