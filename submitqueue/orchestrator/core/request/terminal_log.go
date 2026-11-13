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

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// ResendTerminalRequestLog resends the request's terminal status without another state write.
// The log identifies the resend; original diagnostics may be unavailable.
// For example, validation can store Error and crash before publishing its status log.
// Redelivery must resend the status without another request-state write.
func ResendTerminalRequestLog(ctx context.Context, registry consumer.TopicRegistry, request entity.Request) error {
	status, err := terminalStateToStatus(request.State)
	if err != nil {
		return err
	}
	logEntry := entity.NewRequestStatusLog(request.Queue, request.ID, status, request.Version, "terminal status resent for consistency; original details may not have been retained", nil)
	if err := PublishLog(ctx, registry, logEntry, request.ID, ""); err != nil {
		return fmt.Errorf("failed to publish request log for %s: %w", request.ID, err)
	}
	return nil
}
