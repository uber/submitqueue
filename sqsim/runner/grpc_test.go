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

package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

func TestFromProtoSummaryCopiesPublicFields(t *testing.T) {
	got := fromProtoSummary(&gatewaypb.RequestSummary{
		Sqid:      "sqsim/1",
		Status:    "building",
		LastError: "none",
		Metadata:  map[string]string{"controller": "build"},
	})
	assert.Equal(t, "sqsim/1", got.SQID)
	assert.Equal(t, "building", got.Status)
	assert.Equal(t, "build", got.Metadata["controller"])
}
