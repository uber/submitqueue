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

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/errs"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/runway/extension/merger"
)

const (
	testHost   = "github.com"
	testOwner  = "uber"
	testRepo   = "submitqueue"
	testTarget = "main"
)

func headSHA(n int) string { return fmt.Sprintf("%040x", n) }

func mergeSHA(n int) string { return fmt.Sprintf("%040x", 0xa000+n) }

func uri(n int) string {
	return fmt.Sprintf("github://%s/%s/%s/pull/%d/%s", testHost, testOwner, testRepo, n, headSHA(n))
}

// fakeGitHub serves the endpoints the merger calls from in-memory state.
type fakeGitHub struct {
	mu     sync.Mutex
	pulls  map[int]*pullRequest
	stacks []stack

	// submitStatus overrides the merge-async response code (default 202).
	submitStatus int
	// submitBody overrides the merge-async response body.
	submitBody string
	// pollsUntilDone is how many status reads stay pending.
	pollsUntilDone int
	// finalStatus is what a settled merge reports (default merged).
	finalStatus string
	// mergeableNullReads is how many pull reads report mergeable as null.
	mergeableNullReads int
	// unmergeable names pull requests reported as conflicting.
	unmergeable map[int]bool
	// pullErrStatus makes every pull read fail with this code.
	pullErrStatus int
	// mergedReadLag is how many event reads of a pull request landed by a merge
	// still lack its merged event, as github.com does briefly after a stack
	// merge settles.
	mergedReadLag int
	// eventPadding is how many other events precede a merged event, to push it
	// past the first page.
	eventPadding int
	// failMerges names top pull requests whose merge settles as failed.
	failMerges map[int]bool

	mergeCommits map[int]string
	lagging      map[int]int
	submits      []submitCall
	polls        int
}

type submitCall struct {
	number int
	req    asyncMergeRequest
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{pulls: make(map[int]*pullRequest), mergeCommits: make(map[int]string), lagging: make(map[int]int)}
}

// addPull adds an open pull request with the given base branch.
func (f *fakeGitHub) addPull(n int, base string) {
	f.pulls[n] = &pullRequest{
		Number: n,
		State:  pullStateOpen,
		Head:   gitRef{Ref: "branch-" + strconv.Itoa(n), SHA: headSHA(n)},
		Base:   gitRef{Ref: base},
	}
}

// addStack adds open pull requests forming a GitHub stack on the target,
// bottom to top.
func (f *fakeGitHub) addStack(number int, prs ...int) {
	s := stack{Number: number, Open: true, Base: gitRef{Ref: testTarget}}
	base := testTarget
	for _, n := range prs {
		f.addPull(n, base)
		base = "branch-" + strconv.Itoa(n)
		s.PullRequests = append(s.PullRequests, stackPull{Number: n, State: pullStateOpen, Head: gitRef{Ref: base, SHA: headSHA(n)}})
	}
	f.stacks = append(f.stacks, s)
}

func (f *fakeGitHub) markMerged(n int) {
	pr := f.pulls[n]
	pr.Merged = true
	pr.State = "closed"
	f.mergeCommits[n] = mergeSHA(n)
	mergedAt := "2026-10-07T00:00:00Z"
	for si := range f.stacks {
		for pi := range f.stacks[si].PullRequests {
			if f.stacks[si].PullRequests[pi].Number == n {
				f.stacks[si].PullRequests[pi].State = "closed"
				f.stacks[si].PullRequests[pi].MergedAt = &mergedAt
			}
		}
	}
}

// landStack merges top and every unmerged pull request below it, as GitHub's
// async merge does for a stacked pull request.
func (f *fakeGitHub) landStack(top int) {
	for _, s := range f.stacks {
		for i, p := range s.PullRequests {
			if p.Number != top {
				continue
			}
			for _, below := range s.PullRequests[:i+1] {
				if !f.pulls[below.Number].Merged {
					f.markMerged(below.Number)
				}
			}
			return
		}
	}
	f.markMerged(top)
}

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	prefix := "/repos/" + testOwner + "/" + testRepo + "/"
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, prefix), "/")
	switch {
	case r.Method == http.MethodGet && parts[0] == "stacks":
		n, _ := strconv.Atoi(r.URL.Query().Get("pull_request"))
		var out []stack
		for _, s := range f.stacks {
			for _, p := range s.PullRequests {
				if p.Number == n {
					out = append(out, s)
				}
			}
		}
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodGet && parts[0] == "issues" && len(parts) == 3 && parts[2] == "events":
		n, _ := strconv.Atoi(parts[1])
		var events []issueEvent
		for i := 0; i < f.eventPadding; i++ {
			events = append(events, issueEvent{Event: "labeled"})
		}
		if sha, ok := f.mergeCommits[n]; ok {
			if f.lagging[n] > 0 {
				f.lagging[n]--
			} else {
				events = append(events, issueEvent{Event: "merged", CommitID: sha})
			}
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		start := min((page-1)*perPage, len(events))
		writeJSON(w, http.StatusOK, events[start:min(start+perPage, len(events))])

	case parts[0] == "pulls" && len(parts) == 2 && r.Method == http.MethodGet:
		if f.pullErrStatus != 0 {
			w.WriteHeader(f.pullErrStatus)
			return
		}
		n, _ := strconv.Atoi(parts[1])
		pr, ok := f.pulls[n]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		out := *pr
		if !out.Merged {
			if f.mergeableNullReads > 0 {
				f.mergeableNullReads--
			} else {
				mergeable := !f.unmergeable[n]
				out.Mergeable = &mergeable
				if !mergeable {
					out.MergeableState = mergeableStateDirty
				}
			}
		}
		writeJSON(w, http.StatusOK, out)

	case parts[0] == "pulls" && len(parts) == 3 && r.Method == http.MethodPut:
		n, _ := strconv.Atoi(parts[1])
		var req asyncMergeRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.submits = append(f.submits, submitCall{number: n, req: req})
		if f.submitStatus != 0 {
			w.WriteHeader(f.submitStatus)
			_, _ = w.Write([]byte(f.submitBody))
			return
		}
		if f.pollsUntilDone == 0 {
			f.settle(w, n)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": mergeStatusPending, "details": map[string]string{"uuid": "uuid-1"}})

	case parts[0] == "pulls" && len(parts) == 4 && r.Method == http.MethodGet:
		n, _ := strconv.Atoi(parts[1])
		f.polls++
		if f.polls < f.pollsUntilDone {
			writeJSON(w, http.StatusOK, map[string]any{"status": mergeStatusPending})
			return
		}
		f.settle(w, n)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeGitHub) settle(w http.ResponseWriter, top int) {
	status := f.finalStatus
	if status == "" {
		status = mergeStatusMerged
	}
	if f.failMerges[top] {
		status = mergeStatusFailed
	}
	if status == mergeStatusMerged {
		f.landStack(top)
		for n, pr := range f.pulls {
			if pr.Merged {
				f.lagging[n] = f.mergedReadLag
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": status, "details": map[string]string{"message": "merge " + status, "sha": mergeSHA(top)}})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func newTestMerger(t *testing.T, gh *fakeGitHub) merger.Merger {
	t.Helper()
	srv := httptest.NewServer(gh)
	t.Cleanup(srv.Close)
	httpClient, err := phttp.NewClient(srv.URL)
	require.NoError(t, err)
	m, err := New(Params{
		HTTPClient:      httpClient,
		Host:            testHost,
		Owner:           testOwner,
		Repo:            testRepo,
		Target:          testTarget,
		DefaultStrategy: mergestrategypb.Strategy_SQUASH_REBASE,
		PollInterval:    -1,
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
	})
	require.NoError(t, err)
	return m
}

func step(id string, strategy mergestrategypb.Strategy, uris ...string) *runwaymq.MergeStep {
	return &runwaymq.MergeStep{StepId: id, Change: &changepb.Change{Uris: uris}, Strategy: strategy}
}

func request(steps ...*runwaymq.MergeStep) *runwaymq.MergeRequest {
	return &runwaymq.MergeRequest{Id: "req-1", QueueName: "q", Steps: steps}
}

func outputIDs(r *runwaymq.StepResult) []string {
	var ids []string
	for _, o := range r.GetOutputs() {
		ids = append(ids, o.GetId())
	}
	return ids
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*fakeGitHub)
		req         *runwaymq.MergeRequest
		wantErr     error
		wantOutputs map[string][]string
		wantSubmits []submitCall
		// wantFailedStep, on an error, names the step reported as failed after
		// the steps in wantOutputs landed; empty means no result is returned.
		wantFailedStep string
	}{
		{
			name:        "single pull request on trunk",
			setup:       func(f *fakeGitHub) { f.addPull(1, testTarget) },
			req:         request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1)}},
			wantSubmits: []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}}},
		},
		{
			name:        "stack lands with one merge on its top pull request",
			setup:       func(f *fakeGitHub) { f.addStack(7, 1, 2, 3) },
			req:         request(step("s1", mergestrategypb.Strategy_DEFAULT, uri(1), uri(2), uri(3))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1), mergeSHA(2), mergeSHA(3)}},
			wantSubmits: []submitCall{{number: 3, req: asyncMergeRequest{SHA: headSHA(3), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
		{
			name:        "bottom of a stack lands without the pull requests above it",
			setup:       func(f *fakeGitHub) { f.addStack(7, 1, 2, 3) },
			req:         request(step("s1", mergestrategypb.Strategy_MERGE, uri(1), uri(2))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1), mergeSHA(2)}},
			wantSubmits: []submitCall{{number: 2, req: asyncMergeRequest{SHA: headSHA(2), MergeMethod: "merge", MergeAction: mergeActionDirect}}},
		},
		{
			name: "redelivery skips the already-merged bottom of a stack",
			setup: func(f *fakeGitHub) {
				f.addStack(7, 1, 2)
				f.markMerged(1)
				f.pulls[2].Base.Ref = testTarget
			},
			req:         request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1), uri(2))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1), mergeSHA(2)}},
			wantSubmits: []submitCall{{number: 2, req: asyncMergeRequest{SHA: headSHA(2), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
		{
			name: "fully merged step reports outputs without merging",
			setup: func(f *fakeGitHub) {
				f.addStack(7, 1, 2)
				f.landStack(2)
			},
			req:         request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1), uri(2))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1), mergeSHA(2)}},
		},
		{
			name: "merge commits not yet recorded after the merge settles are re-read",
			setup: func(f *fakeGitHub) {
				f.addStack(7, 1, 2)
				f.mergedReadLag = 2
			},
			req:         request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1), uri(2))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1), mergeSHA(2)}},
			wantSubmits: []submitCall{{number: 2, req: asyncMergeRequest{SHA: headSHA(2), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
		{
			name: "merge commit past the first page of events",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.eventPadding = issueEventsPageSize + 5
			},
			req:         request(step("s1", mergestrategypb.Strategy_MERGE, uri(1))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1)}},
			wantSubmits: []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "merge", MergeAction: mergeActionDirect}}},
		},
		{
			name: "pending merge is polled until it lands",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.pollsUntilDone = 3
			},
			req:         request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1)}},
			wantSubmits: []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
		{
			name: "existing in-flight request is adopted",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.pollsUntilDone = 2
				f.submitStatus = http.StatusConflict
				f.submitBody = `{"status":"pending","details":{"uuid":"uuid-0","expected_head_sha":"` + headSHA(1) + `","merge_method":"squash"}}`
			},
			req:         request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1))),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1)}},
			wantSubmits: []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
		{
			name: "steps land in order",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.addStack(7, 2, 3)
			},
			req: request(
				step("s1", mergestrategypb.Strategy_REBASE, uri(1)),
				step("s2", mergestrategypb.Strategy_SQUASH_REBASE, uri(2), uri(3)),
			),
			wantOutputs: map[string][]string{"s1": {mergeSHA(1)}, "s2": {mergeSHA(2), mergeSHA(3)}},
			wantSubmits: []submitCall{
				{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}},
				{number: 3, req: asyncMergeRequest{SHA: headSHA(3), MergeMethod: "squash", MergeAction: mergeActionDirect}},
			},
		},
		{
			name: "failed merge is a conflict",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.finalStatus = mergeStatusFailed
			},
			req:            request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
			wantErr:        merger.ErrConflict,
			wantFailedStep: "s1",
			wantSubmits:    []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}}},
		},
		{
			name: "refused merge is an invalid request",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.submitStatus = http.StatusUnprocessableEntity
				f.submitBody = `{"details":{"message":"required status checks"}}`
			},
			req:            request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
			wantErr:        merger.ErrInvalidRequest,
			wantFailedStep: "s1",
			wantSubmits:    []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}}},
		},
		{
			name: "head moved since the URI was minted",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.pulls[1].Head.SHA = headSHA(99)
			},
			req:            request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
			wantErr:        merger.ErrInvalidRequest,
			wantFailedStep: "s1",
		},
		{
			name: "closed pull request",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.pulls[1].State = "closed"
			},
			req:            request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
			wantErr:        merger.ErrInvalidRequest,
			wantFailedStep: "s1",
		},
		{
			name: "stack unstacked after validation is refused before merging",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.addPull(2, testTarget)
			},
			req:            request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(2))),
			wantErr:        merger.ErrInvalidRequest,
			wantFailedStep: "s1",
		},
		{
			name: "invalid later step stops the request after earlier steps land",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.addPull(2, "feature")
			},
			req: request(
				step("s1", mergestrategypb.Strategy_REBASE, uri(1)),
				step("s2", mergestrategypb.Strategy_REBASE, uri(2)),
			),
			wantErr:        merger.ErrInvalidRequest,
			wantOutputs:    map[string][]string{"s1": {mergeSHA(1)}},
			wantFailedStep: "s2",
			wantSubmits:    []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}}},
		},
		{
			name: "merge GitHub fails after a landed step is a conflict",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.addPull(2, testTarget)
				f.failMerges = map[int]bool{2: true}
			},
			req: request(
				step("s1", mergestrategypb.Strategy_REBASE, uri(1)),
				step("s2", mergestrategypb.Strategy_REBASE, uri(2)),
			),
			wantErr:        merger.ErrConflict,
			wantOutputs:    map[string][]string{"s1": {mergeSHA(1)}},
			wantFailedStep: "s2",
			wantSubmits: []submitCall{
				{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}},
				{number: 2, req: asyncMergeRequest{SHA: headSHA(2), MergeMethod: "rebase", MergeAction: mergeActionDirect}},
			},
		},
		{
			name: "steps after a failed one are not attempted",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.addPull(2, testTarget)
				f.addPull(3, testTarget)
				f.failMerges = map[int]bool{2: true}
			},
			req: request(
				step("s1", mergestrategypb.Strategy_REBASE, uri(1)),
				step("s2", mergestrategypb.Strategy_REBASE, uri(2)),
				step("s3", mergestrategypb.Strategy_REBASE, uri(3)),
			),
			wantErr:        merger.ErrConflict,
			wantOutputs:    map[string][]string{"s1": {mergeSHA(1)}},
			wantFailedStep: "s2",
			wantSubmits: []submitCall{
				{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "rebase", MergeAction: mergeActionDirect}},
				{number: 2, req: asyncMergeRequest{SHA: headSHA(2), MergeMethod: "rebase", MergeAction: mergeActionDirect}},
			},
		},
		{
			name: "in-flight merge for a different head is not adopted",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.submitStatus = http.StatusConflict
				f.submitBody = `{"status":"pending","details":{"uuid":"uuid-0","expected_head_sha":"` + headSHA(99) + `","merge_method":"squash"}}`
			},
			req:            request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1))),
			wantErr:        merger.ErrInvalidRequest,
			wantFailedStep: "s1",
			wantSubmits:    []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
		{
			name: "in-flight merge with a different method is not adopted",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.submitStatus = http.StatusConflict
				f.submitBody = `{"status":"pending","details":{"uuid":"uuid-0","expected_head_sha":"` + headSHA(1) + `","merge_method":"merge"}}`
			},
			req:            request(step("s1", mergestrategypb.Strategy_SQUASH_REBASE, uri(1))),
			wantErr:        merger.ErrInvalidRequest,
			wantFailedStep: "s1",
			wantSubmits:    []submitCall{{number: 1, req: asyncMergeRequest{SHA: headSHA(1), MergeMethod: "squash", MergeAction: mergeActionDirect}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gh := newFakeGitHub()
			tt.setup(gh)
			m := newTestMerger(t, gh)

			result, err := m.Merge(context.Background(), tt.req)
			assert.Equal(t, tt.wantSubmits, gh.submits)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				if tt.wantFailedStep == "" {
					assert.Nil(t, result)
					return
				}
				assert.Equal(t, runwaypb.Outcome_FAILED, result.GetOutcome())
				steps := result.GetSteps()
				require.Len(t, steps, len(tt.wantOutputs)+1)
				for _, sr := range steps[:len(steps)-1] {
					assert.Equal(t, tt.wantOutputs[sr.GetStepId()], outputIDs(sr), sr.GetStepId())
				}
				failed := steps[len(steps)-1]
				assert.Equal(t, tt.wantFailedStep, failed.GetStepId())
				assert.NotEmpty(t, failed.GetReason())
				assert.Empty(t, failed.GetOutputs())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.GetOutcome())
			assert.Equal(t, tt.req.GetId(), result.GetId())
			require.Len(t, result.GetSteps(), len(tt.req.GetSteps()))
			for _, sr := range result.GetSteps() {
				assert.Equal(t, tt.wantOutputs[sr.GetStepId()], outputIDs(sr), sr.GetStepId())
			}
		})
	}
}

func TestMergeErrorClassification(t *testing.T) {
	t.Run("server error stays a retryable status error", func(t *testing.T) {
		gh := newFakeGitHub()
		gh.addPull(1, testTarget)
		gh.pullErrStatus = http.StatusBadGateway
		m := newTestMerger(t, gh)

		_, err := m.Merge(context.Background(), request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))))
		require.Error(t, err)
		assert.False(t, merger.IsTerminal(err))
		var se *phttp.StatusError
		require.ErrorAs(t, err, &se)
		assert.Equal(t, http.StatusBadGateway, se.StatusCode)
	})

	t.Run("merge outlasting the poll budget is pending and retryable", func(t *testing.T) {
		gh := newFakeGitHub()
		gh.addPull(1, testTarget)
		gh.pollsUntilDone = 1 << 30
		srv := httptest.NewServer(gh)
		t.Cleanup(srv.Close)
		httpClient, err := phttp.NewClient(srv.URL)
		require.NoError(t, err)
		m, err := New(Params{
			HTTPClient:      httpClient,
			Host:            testHost,
			Owner:           testOwner,
			Repo:            testRepo,
			Target:          testTarget,
			DefaultStrategy: mergestrategypb.Strategy_REBASE,
			PollInterval:    -1,
			MaxPollDuration: time.Nanosecond,
			Logger:          zaptest.NewLogger(t).Sugar(),
			MetricsScope:    tally.NoopScope,
		})
		require.NoError(t, err)

		_, err = m.Merge(context.Background(), request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))))
		require.ErrorIs(t, err, ErrMergePending)
		assert.False(t, merger.IsTerminal(err))
		assert.True(t, errs.IsRetryable(errs.NewClassifierProcessor(Classifier).Process(err)))
	})
}

func TestCheckMergeability(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*fakeGitHub)
		req     *runwaymq.MergeRequest
		wantErr error
	}{
		{
			name:  "single pull request on trunk is mergeable",
			setup: func(f *fakeGitHub) { f.addPull(1, testTarget) },
			req:   request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
		},
		{
			name:  "stack in order is mergeable",
			setup: func(f *fakeGitHub) { f.addStack(7, 1, 2, 3) },
			req:   request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(2), uri(3))),
		},
		{
			name: "mergeability still being computed is re-read",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.mergeableNullReads = 3
			},
			req: request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
		},
		{
			name: "conflicting pull request is a conflict",
			setup: func(f *fakeGitHub) {
				f.addStack(7, 1, 2)
				f.unmergeable = map[int]bool{2: true}
			},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(2))),
			wantErr: merger.ErrConflict,
		},
		{
			name: "independent pull requests on trunk are not a stack",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.addPull(2, testTarget)
			},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(2))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "stack listed top first",
			setup:   func(f *fakeGitHub) { f.addStack(7, 1, 2) },
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(2), uri(1))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "stack listed with a gap",
			setup:   func(f *fakeGitHub) { f.addStack(7, 1, 2, 3) },
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(3))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name: "pull requests from two stacks",
			setup: func(f *fakeGitHub) {
				f.addStack(7, 1, 2)
				f.addStack(8, 3, 4)
			},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(4))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "upper stacked pull request without the ones below it",
			setup:   func(f *fakeGitHub) { f.addStack(7, 1, 2) },
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(2))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name: "stack based on another branch",
			setup: func(f *fakeGitHub) {
				f.addStack(7, 1, 2)
				f.stacks[0].Base.Ref = "release"
				f.pulls[1].Base.Ref = "release"
			},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(1), uri(2))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name: "draft pull request",
			setup: func(f *fakeGitHub) {
				f.addPull(1, testTarget)
				f.pulls[1].Draft = true
			},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, uri(1))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "change in another repository",
			setup:   func(f *fakeGitHub) {},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, "github://github.com/uber/other/pull/1/"+headSHA(1))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "change on another host",
			setup:   func(f *fakeGitHub) {},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, "github://github.example.com/uber/submitqueue/pull/1/"+headSHA(1))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "non-github change",
			setup:   func(f *fakeGitHub) {},
			req:     request(step("s1", mergestrategypb.Strategy_REBASE, "git://github.com/uber/submitqueue/refs%2Fheads%2Fmain/"+headSHA(1))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "promote is unsupported",
			setup:   func(f *fakeGitHub) { f.addPull(1, testTarget) },
			req:     request(step("s1", mergestrategypb.Strategy_PROMOTE, uri(1))),
			wantErr: merger.ErrInvalidRequest,
		},
		{
			name:    "request without steps",
			setup:   func(f *fakeGitHub) {},
			req:     request(),
			wantErr: merger.ErrInvalidRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gh := newFakeGitHub()
			tt.setup(gh)
			m := newTestMerger(t, gh)

			result, err := m.CheckMergeability(context.Background(), tt.req)
			assert.Empty(t, gh.submits, "a dry run must not merge")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.GetOutcome())
			for _, sr := range result.GetSteps() {
				assert.Empty(t, sr.GetOutputs())
			}
		})
	}
}

func TestNewRejectsUnusableParams(t *testing.T) {
	valid := Params{
		HTTPClient:      http.DefaultClient,
		Host:            testHost,
		Owner:           testOwner,
		Repo:            testRepo,
		Target:          testTarget,
		DefaultStrategy: mergestrategypb.Strategy_REBASE,
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
	}
	tests := []struct {
		name   string
		mutate func(*Params)
	}{
		{name: "missing http client", mutate: func(p *Params) { p.HTTPClient = nil }},
		{name: "missing repo", mutate: func(p *Params) { p.Repo = "" }},
		{name: "missing target", mutate: func(p *Params) { p.Target = "" }},
		{name: "promote default", mutate: func(p *Params) { p.DefaultStrategy = mergestrategypb.Strategy_PROMOTE }},
		{name: "default default", mutate: func(p *Params) { p.DefaultStrategy = mergestrategypb.Strategy_DEFAULT }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := valid
			tt.mutate(&p)
			_, err := New(p)
			require.Error(t, err)
		})
	}

	_, err := New(valid)
	require.NoError(t, err)
}
