// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqsim

import "github.com/uber/submitqueue/sqsim/entity"

// RetryableErrorBeforeSideEffect returns a transient fault with no applied outcome.
func RetryableErrorBeforeSideEffect() Fault {
	return entity.Fault{Kind: entity.FaultRetryable, Phase: entity.FaultBeforeSideEffect}
}

// RetryableErrorAfterSideEffect returns a transient fault after applying the outcome.
func RetryableErrorAfterSideEffect() Fault {
	return entity.Fault{Kind: entity.FaultRetryable, Phase: entity.FaultAfterSideEffect}
}

// NonRetryableErrorBeforeSideEffect returns a permanent fault with no applied outcome.
func NonRetryableErrorBeforeSideEffect() Fault {
	return entity.Fault{Kind: entity.FaultNonRetryable, Phase: entity.FaultBeforeSideEffect}
}
