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

package baserevision

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/submitqueue/platform/extension/gitworkspace"
	"github.com/uber/submitqueue/platform/extension/gitworkspace/mock"
)

const (
	testQueue = "monorepo"
	testRepo  = "org/repo"
	testSHA   = "9f2c1d0e3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d"
)

var errTestFailure = errors.New("dependency down")

func TestFromGit(t *testing.T) {
	tests := []struct {
		name      string
		queue     string
		target    Target
		factoryOK bool
		execOut   []gitworkspace.Output
		execErr   error
		wantArgs  [][]string
		want      string
		wantErr   bool
	}{
		{
			name:      "returns the fetched tip",
			queue:     testQueue,
			target:    Target{Repo: testRepo, Remote: "upstream", Branch: "main"},
			factoryOK: true,
			execOut:   []gitworkspace.Output{{Alias: "fetch"}, {Alias: "tip", Stdout: testSHA + "\n"}},
			wantArgs:  [][]string{{"fetch", "upstream", "main"}, {"rev-parse", "FETCH_HEAD^{commit}"}},
			want:      testSHA,
		},
		{
			name:      "defaults the remote to origin",
			queue:     testQueue,
			target:    Target{Repo: testRepo, Branch: "main"},
			factoryOK: true,
			execOut:   []gitworkspace.Output{{Alias: "fetch"}, {Alias: "tip", Stdout: testSHA}},
			wantArgs:  [][]string{{"fetch", "origin", "main"}, {"rev-parse", "FETCH_HEAD^{commit}"}},
			want:      testSHA,
		},
		{
			name:    "queue without a target fails",
			queue:   "other",
			target:  Target{Repo: testRepo, Branch: "main"},
			wantErr: true,
		},
		{
			name:    "workspace that cannot be opened fails",
			queue:   testQueue,
			target:  Target{Repo: testRepo, Branch: "main"},
			wantErr: true,
		},
		{
			name:      "batch that cannot run fails",
			queue:     testQueue,
			target:    Target{Repo: testRepo, Branch: "main"},
			factoryOK: true,
			execErr:   errTestFailure,
			wantArgs:  [][]string{{"fetch", "origin", "main"}, {"rev-parse", "FETCH_HEAD^{commit}"}},
			wantErr:   true,
		},
		{
			name:      "failed fetch fails",
			queue:     testQueue,
			target:    Target{Repo: testRepo, Branch: "main"},
			factoryOK: true,
			execOut:   []gitworkspace.Output{{Alias: "fetch", ExitCode: 128, Stderr: "auth"}, {Alias: "tip", Skipped: true, ExitCode: -1}},
			wantArgs:  [][]string{{"fetch", "origin", "main"}, {"rev-parse", "FETCH_HEAD^{commit}"}},
			wantErr:   true,
		},
		{
			name:      "abbreviated SHA fails",
			queue:     testQueue,
			target:    Target{Repo: testRepo, Branch: "main"},
			factoryOK: true,
			execOut:   []gitworkspace.Output{{Alias: "fetch"}, {Alias: "tip", Stdout: "9f2c1d0\n"}},
			wantArgs:  [][]string{{"fetch", "origin", "main"}, {"rev-parse", "FETCH_HEAD^{commit}"}},
			wantErr:   true,
		},
		{
			name:      "missing output fails",
			queue:     testQueue,
			target:    Target{Repo: testRepo, Branch: "main"},
			factoryOK: true,
			execOut:   []gitworkspace.Output{{Alias: "fetch"}},
			wantArgs:  [][]string{{"fetch", "origin", "main"}, {"rev-parse", "FETCH_HEAD^{commit}"}},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			factory := mock.NewMockFactory(ctrl)
			if tt.queue == testQueue {
				if tt.factoryOK {
					workspace := mock.NewMockWorkspace(ctrl)
					factory.EXPECT().For(gomock.Any(), testRepo).Return(workspace, nil)
					workspace.EXPECT().Exec(gomock.Cond(func(commands []gitworkspace.Command) bool {
						if len(commands) != len(tt.wantArgs) {
							return false
						}
						for i, command := range commands {
							if command.Bin != "git" || !assert.ObjectsAreEqual(tt.wantArgs[i], command.Args) {
								return false
							}
						}
						return true
					})).Return(tt.execOut, tt.execErr)
					workspace.EXPECT().Close().Return(nil)
				} else {
					factory.EXPECT().For(gomock.Any(), testRepo).Return(nil, errTestFailure)
				}
			}

			got, err := FromGit(factory, map[string]Target{testQueue: tt.target})(context.Background(), tt.queue)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFromGit_RepoLocks(t *testing.T) {
	validOutputs := []gitworkspace.Output{{Alias: "fetch"}, {Alias: "tip", Stdout: testSHA}}

	t.Run("calls on one repo never overlap", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		factory := mock.NewMockFactory(ctrl)
		workspace := mock.NewMockWorkspace(ctrl)
		factory.EXPECT().For(gomock.Any(), testRepo).Return(workspace, nil).AnyTimes()
		workspace.EXPECT().Close().Return(nil).AnyTimes()
		var inFlight, maxInFlight atomic.Int32
		workspace.EXPECT().Exec(gomock.Any()).DoAndReturn(func([]gitworkspace.Command) ([]gitworkspace.Output, error) {
			n := inFlight.Add(1)
			for m := maxInFlight.Load(); n > m && !maxInFlight.CompareAndSwap(m, n); m = maxInFlight.Load() {
			}
			runtime.Gosched()
			inFlight.Add(-1)
			return validOutputs, nil
		}).AnyTimes()
		baseRevision := FromGit(factory, map[string]Target{
			"main-queue":    {Repo: testRepo, Branch: "main"},
			"release-queue": {Repo: testRepo, Branch: "release"},
		})

		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			queue := "main-queue"
			if i%2 == 1 {
				queue = "release-queue"
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := baseRevision(context.Background(), queue)
				assert.NoError(t, err)
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), maxInFlight.Load())
	})

	t.Run("calls on different repos do not block each other", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		factory := mock.NewMockFactory(ctrl)
		entered := map[string]chan struct{}{"repo-x": make(chan struct{}), "repo-y": make(chan struct{})}
		for repo, other := range map[string]string{"repo-x": "repo-y", "repo-y": "repo-x"} {
			workspace := mock.NewMockWorkspace(ctrl)
			factory.EXPECT().For(gomock.Any(), repo).Return(workspace, nil)
			workspace.EXPECT().Exec(gomock.Any()).DoAndReturn(func([]gitworkspace.Command) ([]gitworkspace.Output, error) {
				close(entered[repo])
				<-entered[other]
				return validOutputs, nil
			})
			workspace.EXPECT().Close().Return(nil)
		}
		baseRevision := FromGit(factory, map[string]Target{
			"queue-x": {Repo: "repo-x", Branch: "main"},
			"queue-y": {Repo: "repo-y", Branch: "main"},
		})

		var wg sync.WaitGroup
		for _, queue := range []string{"queue-x", "queue-y"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := baseRevision(context.Background(), queue)
				assert.NoError(t, err)
			}()
		}
		wg.Wait()
	})
}
