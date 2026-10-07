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

package demo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sourceFunc func(context.Context, ChangeSpec) (Change, error)

func (f sourceFunc) Open(ctx context.Context, spec ChangeSpec) (Change, error) {
	return f(ctx, spec)
}

type readinessFunc func(context.Context, Change) error

func (f readinessFunc) Wait(ctx context.Context, change Change) error {
	return f(ctx, change)
}

type gatewayFixture struct {
	land    func(context.Context, string, []string, mergestrategypb.Strategy) (string, error)
	history func(context.Context, string, string) ([]*pb.HistoryEvent, error)
}

func (g gatewayFixture) Land(ctx context.Context, queue string, uris []string, strategy mergestrategypb.Strategy) (string, error) {
	return g.land(ctx, queue, uris, strategy)
}

func (g gatewayFixture) History(ctx context.Context, queue, id string) ([]*pb.HistoryEvent, error) {
	if g.history == nil {
		return nil, nil
	}
	return g.history(ctx, queue, id)
}

func fixtureOptions() Options {
	return Options{
		Count: 3, Files: 1, Folders: 2, Concurrency: 1, Prefix: "demo",
		RunID: "run", Queue: "q", Land: true, Strategy: mergestrategypb.Strategy_SQUASH_REBASE,
	}
}

func fixtureChange(spec ChangeSpec) Change {
	return Change{
		Branch: spec.Branch, HeadSHA: strings.Repeat("a", 40),
		URI: "change:" + spec.Branch, Label: spec.Branch,
	}
}

func fixtureGateway() gatewayFixture {
	return gatewayFixture{land: func(_ context.Context, _ string, uris []string, _ mergestrategypb.Strategy) (string, error) {
		return strings.Join(uris, ","), nil
	}}
}

func TestWorkloadSubmissionModes(t *testing.T) {
	tests := []struct {
		name          string
		burst         bool
		stacked       bool
		wantLandSizes []int
		wantCreated   []int
	}{
		{name: "immediate", wantLandSizes: []int{1, 1, 1}, wantCreated: []int{1, 2, 3}},
		{name: "burst", burst: true, wantLandSizes: []int{1, 1, 1}, wantCreated: []int{3, 3, 3}},
		{name: "stack", stacked: true, wantLandSizes: []int{3}, wantCreated: []int{3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := fixtureOptions()
			opts.Burst, opts.Stacked = tt.burst, tt.stacked
			var specs []ChangeSpec
			var prepared []string
			var landSizes, createdAtLand []int
			src := sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) {
				specs = append(specs, spec)
				return fixtureChange(spec), nil
			})
			gateway := fixtureGateway()
			gateway.land = func(_ context.Context, _ string, uris []string, _ mergestrategypb.Strategy) (string, error) {
				landSizes = append(landSizes, len(uris))
				createdAtLand = append(createdAtLand, len(specs))
				return fmt.Sprintf("id-%d", len(landSizes)), nil
			}
			result, err := Run(context.Background(), opts, Dependencies{
				Source: src, Gateway: gateway, Readiness: readinessFunc(func(_ context.Context, change Change) error {
					prepared = append(prepared, change.URI)
					return nil
				}),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantLandSizes, landSizes)
			assert.Equal(t, tt.wantCreated, createdAtLand)
			require.Len(t, result.Changes, 3)
			assert.Len(t, prepared, 3)
			assert.Len(t, result.Requests, len(tt.wantLandSizes))
			for i, spec := range specs {
				assert.Equal(t, tt.stacked && i > 0, spec.HasParent)
				if spec.HasParent {
					assert.Equal(t, result.Changes[i-1], spec.Parent)
				}
			}
		})
	}
}

func TestBurstWaitsForAllReadiness(t *testing.T) {
	opts := fixtureOptions()
	opts.Burst = true
	var prepared, calls int
	expected := errors.New("readiness failed")
	gateway := fixtureGateway()
	gateway.land = func(context.Context, string, []string, mergestrategypb.Strategy) (string, error) {
		calls++
		return "id", nil
	}
	result, err := Run(context.Background(), opts, Dependencies{
		Source:  sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) { return fixtureChange(spec), nil }),
		Gateway: gateway, Readiness: readinessFunc(func(context.Context, Change) error {
			prepared++
			if prepared == 3 {
				return expected
			}
			return nil
		}),
	})
	require.ErrorIs(t, err, expected)
	assert.Len(t, result.Changes, 3)
	assert.Empty(t, result.Requests)
	assert.Zero(t, calls)
}

func TestIndependentCreationIsBoundedAndOverlapsSubmission(t *testing.T) {
	opts := fixtureOptions()
	opts.Concurrency = 2
	opts.Count = 4
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	landed := make(chan struct{}, 4)
	done := make(chan error, 1)
	src := sourceFunc(func(ctx context.Context, spec ChangeSpec) (Change, error) {
		entered <- struct{}{}
		if strings.HasSuffix(spec.Branch, "/1") {
			return fixtureChange(spec), nil
		}
		select {
		case <-release:
			return fixtureChange(spec), nil
		case <-ctx.Done():
			return Change{}, ctx.Err()
		}
	})
	gateway := fixtureGateway()
	gateway.land = func(_ context.Context, _ string, uris []string, _ mergestrategypb.Strategy) (string, error) {
		landed <- struct{}{}
		return uris[0], nil
	}
	go func() {
		_, err := Run(context.Background(), opts, Dependencies{Source: src, Gateway: gateway})
		done <- err
	}()
	<-landed
	<-entered
	<-entered
	<-entered
	select {
	case <-entered:
		assert.Fail(t, "creation exceeded the two active workers")
	default:
	}
	close(release)
	require.NoError(t, <-done)
}

func TestPartialFailurePreservesAcceptedArtifacts(t *testing.T) {
	opts := fixtureOptions()
	expected := errors.New("creation failed")
	calls := 0
	result, err := Run(context.Background(), opts, Dependencies{
		Gateway: fixtureGateway(),
		Source: sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) {
			calls++
			if calls == 2 {
				return Change{Branch: spec.Branch}, expected
			}
			return fixtureChange(spec), nil
		}),
	})
	require.ErrorIs(t, err, expected)
	assert.Equal(t, 2, calls)
	assert.Len(t, result.Changes, 2)
	require.Len(t, result.Requests, 1)
	assert.Equal(t, "accepted", result.Requests[0].Status)
	assert.Equal(t, "change:demo/run/1", result.Requests[0].ID)
}

func TestExistingChangesAreNotCreatedOrMutated(t *testing.T) {
	opts := fixtureOptions()
	opts.Stacked = true
	changes := []Change{fixtureChange(ChangeSpec{Branch: "first"}), fixtureChange(ChangeSpec{Branch: "second"})}
	want := append([]Change(nil), changes...)
	var uris []string
	gateway := fixtureGateway()
	gateway.land = func(_ context.Context, _ string, in []string, _ mergestrategypb.Strategy) (string, error) {
		uris = append([]string(nil), in...)
		return "opaque/id", nil
	}
	result, err := RunExisting(context.Background(), opts, changes, Dependencies{
		Gateway: gateway, Source: sourceFunc(func(context.Context, ChangeSpec) (Change, error) {
			return Change{}, errors.New("existing changes must not invoke Source")
		}),
	})
	require.NoError(t, err)
	assert.Equal(t, want, changes)
	assert.Equal(t, []string{changes[0].URI, changes[1].URI}, uris)
	require.Len(t, result.Requests, 1)
	result.Changes[0].URI = "mutated"
	assert.Equal(t, want, changes)
	assert.Equal(t, want, result.Requests[0].Changes)
}

func TestCreateOnlySkipsGatewayAndReadiness(t *testing.T) {
	opts := fixtureOptions()
	opts.Land, opts.Watch = false, true
	result, err := Run(context.Background(), opts, Dependencies{
		Source: sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) { return fixtureChange(spec), nil }),
		Readiness: readinessFunc(func(context.Context, Change) error {
			return errors.New("create-only must not wait")
		}),
	})
	require.NoError(t, err)
	assert.Len(t, result.Changes, 3)
	assert.Empty(t, result.Requests)
}

func TestWatchReturnsHistoryAndTerminalVerdict(t *testing.T) {
	for _, terminal := range []string{"landed", "error", "cancelled"} {
		t.Run(terminal, func(t *testing.T) {
			opts := fixtureOptions()
			opts.Count, opts.Watch = 1, true
			gateway := fixtureGateway()
			events := []*pb.HistoryEvent{{Status: "accepted"}, {Status: terminal, Metadata: map[string]string{"build_url": "build"}}}
			gateway.history = func(context.Context, string, string) ([]*pb.HistoryEvent, error) { return events, nil }
			result, err := Run(context.Background(), opts, Dependencies{
				Source:  sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) { return fixtureChange(spec), nil }),
				Gateway: gateway,
			})
			if terminal == "landed" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Len(t, result.Requests, 1)
			assert.Equal(t, terminal, result.Requests[0].Status)
			assert.Equal(t, events, result.Requests[0].History)
			result.Requests[0].History[1].Metadata["build_url"] = "changed"
			assert.Equal(t, "build", events[1].Metadata["build_url"])
		})
	}
}

func TestWatchFailureDoesNotCancelCreationOrSubmission(t *testing.T) {
	opts := fixtureOptions()
	opts.Count, opts.Concurrency, opts.Watch = 2, 2, true
	historyFailed := make(chan struct{})
	var once sync.Once
	gateway := fixtureGateway()
	gateway.history = func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
		once.Do(func() { close(historyFailed) })
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	result, err := Run(context.Background(), opts, Dependencies{
		Source: sourceFunc(func(ctx context.Context, spec ChangeSpec) (Change, error) {
			if !strings.HasSuffix(spec.Branch, "/1") {
				<-historyFailed
			}
			if err := ctx.Err(); err != nil {
				return Change{}, err
			}
			return fixtureChange(spec), nil
		}),
		Gateway: gateway,
	})
	require.Error(t, err)
	var grpcErr interface{ GRPCStatus() *status.Status }
	require.ErrorAs(t, err, &grpcErr)
	assert.Equal(t, codes.PermissionDenied, grpcErr.GRPCStatus().Code())
	require.Len(t, result.Changes, 2)
	require.Len(t, result.Requests, 2)
	for _, request := range result.Requests {
		assert.Equal(t, "accepted", request.Status)
	}
}

func TestInvalidWorkloadsFailBeforeCreation(t *testing.T) {
	for _, mutate := range []func(*Options){
		func(o *Options) { o.Count = 0 },
		func(o *Options) { o.Concurrency = 0 },
		func(o *Options) { o.Files = 0 },
		func(o *Options) { o.Folders = -1 },
		func(o *Options) { o.Queue = "" },
		func(o *Options) { o.RunID = "../escape" },
		func(o *Options) { o.Prefix = "../escape" },
	} {
		opts := fixtureOptions()
		mutate(&opts)
		_, err := Run(context.Background(), opts, Dependencies{})
		require.Error(t, err)
	}
}

func TestCallerCancellationDuringWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := fixtureOptions()
	opts.Count, opts.Watch = 1, true
	gateway := fixtureGateway()
	gateway.history = func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
		cancel()
		return nil, context.Canceled
	}
	result, err := Run(ctx, opts, Dependencies{
		Source:  sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) { return fixtureChange(spec), nil }),
		Gateway: gateway,
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, result.Requests, 1)
	assert.Equal(t, "accepted", result.Requests[0].Status)
}

func TestWatchRetainsKnownHistoryAfterEmptyRead(t *testing.T) {
	opts := fixtureOptions()
	opts.Count, opts.Watch = 1, true
	gateway := fixtureGateway()
	calls := 0
	known := []*pb.HistoryEvent{{Status: "started", Metadata: map[string]string{"build_url": "build"}}}
	gateway.history = func(context.Context, string, string) ([]*pb.HistoryEvent, error) {
		calls++
		switch calls {
		case 1:
			return known, nil
		case 2:
			return nil, nil
		default:
			return nil, status.Error(codes.PermissionDenied, "denied")
		}
	}
	result, err := Run(context.Background(), opts, Dependencies{
		Source:  sourceFunc(func(_ context.Context, spec ChangeSpec) (Change, error) { return fixtureChange(spec), nil }),
		Gateway: gateway,
	})
	require.Error(t, err)
	require.Len(t, result.Requests, 1)
	assert.Equal(t, "started", result.Requests[0].Status)
	assert.Equal(t, known, result.Requests[0].History)
}
