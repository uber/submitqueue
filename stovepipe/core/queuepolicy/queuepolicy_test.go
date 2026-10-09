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

package queuepolicy

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type memoryPolicyStore struct {
	head          entity.QueuePolicyHead
	transitions   map[string]entity.QueuePolicyTransition
	beforeUpdate  func()
	updateErr     error
	createHeadErr error
}

func (s *memoryPolicyStore) GetCurrent(context.Context) (entity.QueuePolicyHead, error) {
	if s.head.Queue == "" {
		return entity.QueuePolicyHead{}, storage.ErrNotFound
	}
	return s.head, nil
}
func (s *memoryPolicyStore) CreateCurrent(_ context.Context, head entity.QueuePolicyHead) error {
	if s.createHeadErr != nil {
		return s.createHeadErr
	}
	if s.head.Queue != "" {
		return storage.ErrAlreadyExists
	}
	s.head = head
	return nil
}
func (s *memoryPolicyStore) UpdateCurrent(_ context.Context, head entity.QueuePolicyHead, oldVersion, newVersion int64) error {
	if s.beforeUpdate != nil {
		fn := s.beforeUpdate
		s.beforeUpdate = nil
		fn()
	}
	if s.updateErr != nil {
		return s.updateErr
	}
	if oldVersion != s.head.Version {
		return storage.ErrVersionMismatch
	}
	head.Version = newVersion
	s.head = head
	return nil
}
func (s *memoryPolicyStore) GetTransition(_ context.Context, id string) (entity.QueuePolicyTransition, error) {
	transition, ok := s.transitions[id]
	if !ok {
		return transition, storage.ErrNotFound
	}
	return transition, nil
}
func (s *memoryPolicyStore) CreateTransition(_ context.Context, transition entity.QueuePolicyTransition) error {
	if _, ok := s.transitions[transition.ID]; ok {
		return storage.ErrAlreadyExists
	}
	s.transitions[transition.ID] = transition
	return nil
}

type testPolicyStorage struct {
	storage.Storage
	store storage.QueuePolicyStore
}

func (s testPolicyStorage) GetQueuePolicyStore() storage.QueuePolicyStore { return s.store }

type testPolicyFactory struct{ store storage.QueuePolicyStore }

func (f testPolicyFactory) For(storage.Config) (storage.Storage, error) {
	return testPolicyStorage{store: f.store}, nil
}

type testSourceFactory struct{ sc sourcecontrol.SourceControl }

func (f testSourceFactory) For(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return f.sc, nil
}

func newPolicyWriterFixture() (*writer, *memoryPolicyStore) {
	store := &memoryPolicyStore{transitions: make(map[string]entity.QueuePolicyTransition)}
	sc := scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{"G", "F", "E", "D", "C", "B", "A", "before"})
	w := NewWriter(testPolicyFactory{store}, testSourceFactory{sc}).(*writer)
	w.now = func() time.Time { return time.UnixMilli(1000) }
	return w, store
}
func policyChange(id string, revision int64, state entity.QueuePolicyState, boundary string) entity.ApplyQueuePolicyRequest {
	return entity.ApplyQueuePolicyRequest{Queue: "repo/main", OperationID: id, ExpectedRevision: revision, State: state, EffectiveFromCommitURI: boundary}
}

func TestInitializeIsExplicitAndRecoversPartialWrite(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "head write failed"}[partial], func(t *testing.T) {
			w, store := newPolicyWriterFixture()
			require.Empty(t, store.transitions)
			if partial {
				store.createHeadErr = errors.New("storage unavailable")
				_, err := w.Initialize(context.Background(), "repo/main")
				require.ErrorIs(t, err, store.createHeadErr)
				require.Len(t, store.transitions, 1)
				store.createHeadErr = nil
				w.now = func() time.Time { return time.UnixMilli(2000) }
			}
			policy, err := w.Initialize(context.Background(), "repo/main")
			require.NoError(t, err)
			require.Equal(t, entity.QueuePolicy{Revision: 1, State: entity.QueuePolicyStateDisabled, ChangedAtMs: 1000}, policy)
			enabled, err := w.Apply(context.Background(), policyChange("enable", 1, entity.QueuePolicyStateEnabled, "A"))
			require.NoError(t, err)
			again, err := w.Initialize(context.Background(), "repo/main")
			require.NoError(t, err)
			require.Equal(t, enabled, again)
			require.Len(t, store.transitions, 2)
		})
	}
}

func TestPolicyHistoryAndCommittedReplay(t *testing.T) {
	w, store := newPolicyWriterFixture()
	ctx := context.Background()
	_, err := w.Initialize(ctx, "repo/main")
	require.NoError(t, err)
	changes := []entity.ApplyQueuePolicyRequest{
		policyChange("enable", 1, entity.QueuePolicyStateEnabled, "A"),
		policyChange("disable", 2, entity.QueuePolicyStateDisabled, "D"),
		policyChange("reenable", 3, entity.QueuePolicyStateEnabled, "G"),
	}
	policies := make([]entity.QueuePolicy, 0, len(changes))
	for _, change := range changes {
		p, err := w.Apply(ctx, change)
		require.NoError(t, err)
		policies = append(policies, p)
	}
	for i, change := range changes {
		p, err := w.Apply(ctx, change)
		require.NoError(t, err)
		require.Equal(t, policies[i], p)
	}
	require.Equal(t, int64(4), store.head.Revision)
	require.Equal(t, int64(4), store.head.Version)
	collision := changes[0]
	collision.EffectiveFromCommitURI = "B"
	_, err = w.Apply(ctx, collision)
	require.ErrorIs(t, err, ErrOperationConflict)
}

func TestPolicyPartialCommitRetryPinsOccurrence(t *testing.T) {
	w, store := newPolicyWriterFixture()
	ctx := context.Background()
	_, err := w.Initialize(ctx, "repo/main")
	require.NoError(t, err)
	change := policyChange("enable", 1, entity.QueuePolicyStateEnabled, "B")
	store.updateErr = errors.New("connection failed")
	_, err = w.Apply(ctx, change)
	require.ErrorIs(t, err, store.updateErr)
	require.Equal(t, int64(1), store.head.Revision)
	proposal := store.transitions["enable"]
	store.updateErr = nil
	w.now = func() time.Time { return time.UnixMilli(9999) }
	p, err := w.Apply(ctx, change)
	require.NoError(t, err)
	require.Equal(t, proposal.Policy, p)
	require.Equal(t, int64(2), store.head.Version)
}

func TestPolicyConcurrentCommitConverges(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(map[bool]string{false: "competing operation", true: "same operation"}[same], func(t *testing.T) {
			w, store := newPolicyWriterFixture()
			ctx := context.Background()
			_, err := w.Initialize(ctx, "repo/main")
			require.NoError(t, err)
			change := policyChange("enable", 1, entity.QueuePolicyStateEnabled, "A")
			store.beforeUpdate = func() {
				competitor := change
				if !same {
					competitor.OperationID = "winner"
					competitor.EffectiveFromCommitURI = "B"
				}
				_, err := w.Apply(ctx, competitor)
				require.NoError(t, err)
			}
			p, err := w.Apply(ctx, change)
			if same {
				require.NoError(t, err)
				require.Equal(t, int64(2), p.Revision)
			} else {
				require.ErrorIs(t, err, storage.ErrVersionMismatch)
				require.Equal(t, "winner", store.head.TransitionID)
				// The losing immutable proposal is not in committed history.
				_, err = w.Apply(ctx, change)
				require.ErrorIs(t, err, storage.ErrVersionMismatch)
			}
			require.Equal(t, int64(2), store.head.Revision)
			require.Equal(t, int64(2), store.head.Version)
		})
	}
}

func TestPolicyBoundaryAndRequestValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change entity.ApplyQueuePolicyRequest
		want   error
	}{
		{"unknown boundary", policyChange("off", 2, entity.QueuePolicyStateDisabled, "other"), sourcecontrol.ErrNotFound},
		{"backward boundary", policyChange("off", 2, entity.QueuePolicyStateDisabled, "before"), ErrInvalidChange},
		{"same boundary", policyChange("off", 2, entity.QueuePolicyStateDisabled, "A"), nil},
		{"forward boundary", policyChange("off", 2, entity.QueuePolicyStateDisabled, "D"), nil},
		{"stale revision", policyChange("off", 1, entity.QueuePolicyStateDisabled, "D"), storage.ErrVersionMismatch},
		{"same state", policyChange("on", 2, entity.QueuePolicyStateEnabled, "D"), ErrInvalidChange},
		{"missing boundary", policyChange("off", 2, entity.QueuePolicyStateDisabled, ""), ErrInvalidChange},
		{"invalid state", policyChange("off", 2, entity.QueuePolicyStateUnknown, "D"), ErrInvalidChange},
		{"reserved identity", policyChange("initial", 2, entity.QueuePolicyStateDisabled, "D"), ErrInvalidChange},
		{"overflow", policyChange("off", math.MaxInt64, entity.QueuePolicyStateDisabled, "D"), ErrInvalidChange},
		{"long identity", policyChange(strings.Repeat("x", 256), 2, entity.QueuePolicyStateDisabled, "D"), ErrInvalidChange},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w, store := newPolicyWriterFixture()
			ctx := context.Background()
			_, err := w.Initialize(ctx, "repo/main")
			require.NoError(t, err)
			_, err = w.Apply(ctx, policyChange("enable", 1, entity.QueuePolicyStateEnabled, "A"))
			require.NoError(t, err)
			_, err = w.Apply(ctx, tt.change)
			if tt.want == nil {
				require.NoError(t, err)
				require.Equal(t, int64(3), store.head.Revision)
			} else {
				require.ErrorIs(t, err, tt.want)
				require.Equal(t, int64(2), store.head.Revision)
			}
		})
	}
}

func TestPolicyBrokenCurrentAndHistoryAreNotLookupMisses(t *testing.T) {
	for _, tt := range []struct {
		name    string
		corrupt func(*memoryPolicyStore)
	}{
		{"missing current occurrence", func(s *memoryPolicyStore) { delete(s.transitions, "initial") }},
		{"wrong queue", func(s *memoryPolicyStore) { s.head.Queue = "other" }},
		{"invalid version", func(s *memoryPolicyStore) { s.head.Version = 0 }},
		{"invalid initial policy", func(s *memoryPolicyStore) {
			p := s.transitions["initial"]
			p.Policy.State = entity.QueuePolicyStateEnabled
			s.transitions[p.ID] = p
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w, store := newPolicyWriterFixture()
			ctx := context.Background()
			_, err := w.Initialize(ctx, "repo/main")
			require.NoError(t, err)
			tt.corrupt(store)
			_, err = w.Initialize(ctx, "repo/main")
			require.ErrorIs(t, err, ErrInconsistentHistory)
			require.False(t, storage.IsNotFound(err))
		})
	}
}
