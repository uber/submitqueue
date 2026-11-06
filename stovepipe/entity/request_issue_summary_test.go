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

package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestIssueValidate(t *testing.T) {
	valid := RequestIssueSummary{RequestID: "12", TaskID: "test", BuildID: "build/12", FirstSeenBuildID: "build/10", Kind: RequestIssueKindExisting}
	for _, kind := range []RequestIssueKind{RequestIssueKindNew, RequestIssueKindExisting, RequestIssueKindClosed} {
		issue := valid
		issue.Kind = kind
		require.NoError(t, issue.Validate())
	}
	for _, change := range []func(*RequestIssueSummary){
		func(i *RequestIssueSummary) { i.RequestID = "" }, func(i *RequestIssueSummary) { i.TaskID = "" },
		func(i *RequestIssueSummary) { i.BuildID = "" }, func(i *RequestIssueSummary) { i.FirstSeenBuildID = "" },
		func(i *RequestIssueSummary) { i.Kind = "open" },
	} {
		issue := valid
		change(&issue)
		require.Error(t, issue.Validate())
	}
}
