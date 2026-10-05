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

package e2e_test

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
	cpfake "github.com/uber/submitqueue/submitqueue/extension/changeprovider/fake"
	"github.com/uber/submitqueue/test/testutil"
)

func (s *E2EIntegrationSuite) TestFakeDemo_IndependentAndStackedChanges() {
	t := s.T()
	const queue = "e2e-test-queue"
	started := time.Now().UnixMilli()
	addr, err := s.stack.ServiceHost("gateway-service", 8080)
	require.NoError(t, err)
	for _, stacked := range []bool{false, true} {
		cmd := exec.CommandContext(s.ctx, testutil.Runfile("service/submitqueue/demo/requests/requests_/requests"),
			"-provider=fake", "-addr="+addr, "-queue="+queue, "-count=3", "-watch=false",
			fmt.Sprintf("-stacked=%t", stacked), fmt.Sprintf("-prefix=fake-e2e-%t", stacked))
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	var summaries []*gatewaypb.RequestSummary
	pollUntil(persistPollInterval, func() bool {
		resp, err := s.gatewayClient.List(s.ctx, &gatewaypb.ListRequest{
			Queue: queue, ReceivedAtOrAfterMs: started, ReceivedBeforeMs: time.Now().UnixMilli() + 1, PageSize: 10,
		})
		require.NoError(t, err)
		summaries = resp.Requests
		return len(summaries) == 4
	})
	store, err := s.appStorage.For(queue)
	require.NoError(t, err)
	var sizes []int
	for _, summary := range summaries {
		req := request{queue: queue, sqid: summary.Sqid}
		require.Equal(t, entity.RequestStatusLanded, s.awaitTerminal(req))
		sizes = append(sizes, len(summary.ChangeUris))
		infos, err := cpfake.New(changeprovider.Config{QueueName: queue}).Get(s.ctx,
			entity.Request{Change: change.Change{URIs: summary.ChangeUris}})
		require.NoError(t, err)
		for _, info := range infos {
			assert.NotContains(t, info.URI, "?")
			require.NotEmpty(t, info.Details.ChangedFiles)
			records, err := store.GetChangeStore().GetByURI(s.ctx, info.URI)
			require.NoError(t, err)
			require.Len(t, records, 1)
			assert.Equal(t, info.Details, records[0].Details,
				"separate processes must resolve the same synthetic files")
		}
	}
	assert.ElementsMatch(t, []int{1, 1, 1, 3}, sizes)
}
