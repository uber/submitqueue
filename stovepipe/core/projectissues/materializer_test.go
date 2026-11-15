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

package projectissues

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestPersistAttribution(t *testing.T) {
	for _, tt := range []struct {
		name          string
		sourceErr     error
		projectionErr error
		wantErr       error
	}{
		{name: "new pair"},
		{name: "retained source", sourceErr: fmt.Errorf("duplicate source: %w", storage.ErrAlreadyExists)},
		{name: "existing projection", projectionErr: fmt.Errorf("duplicate lookup: %w", storage.ErrAlreadyExists)},
		{name: "replay", sourceErr: storage.ErrAlreadyExists, projectionErr: storage.ErrAlreadyExists},
		{name: "source failure", sourceErr: assert.AnError, wantErr: assert.AnError},
		{name: "projection failure", projectionErr: assert.AnError, wantErr: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stores := storagemock.NewMockStorage(ctrl)
			source := storagemock.NewMockRequestProjectsByIssueStore(ctrl)
			projection := storagemock.NewMockRequestIssuesByProjectStore(ctrl)
			entry := entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//app:test", Project: "app"}

			calls := []any{
				stores.EXPECT().GetRequestProjectsByIssueStore().Return(source),
				source.EXPECT().Create(gomock.Any(), "42", entry).Return(tt.sourceErr),
			}
			if tt.sourceErr == nil || errors.Is(tt.sourceErr, storage.ErrAlreadyExists) {
				calls = append(calls,
					stores.EXPECT().GetRequestIssuesByProjectStore().Return(projection),
					projection.EXPECT().Create(gomock.Any(), "42", entry).Return(tt.projectionErr),
				)
			}
			gomock.InOrder(calls...)

			err := NewMaterializer(tally.NoopScope).PersistAttribution(context.Background(), stores, "42", entry)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPersistAttributionRetriesPartialResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	stores := storagemock.NewMockStorage(ctrl)
	source := storagemock.NewMockRequestProjectsByIssueStore(ctrl)
	projection := storagemock.NewMockRequestIssuesByProjectStore(ctrl)
	stores.EXPECT().GetRequestProjectsByIssueStore().Return(source).AnyTimes()
	stores.EXPECT().GetRequestIssuesByProjectStore().Return(projection).AnyTimes()
	entries := []entity.ProjectIssueAttribution{
		{TaskID: "test", UnitID: "//app:test", Project: "app"},
		{TaskID: "test", UnitID: "//app:test", Project: "dependent"},
		{TaskID: "lint", UnitID: "//lib:lint", Project: "library"},
	}

	gomock.InOrder(
		source.EXPECT().Create(gomock.Any(), "42", entries[0]).Return(nil),
		projection.EXPECT().Create(gomock.Any(), "42", entries[0]).Return(nil),
		source.EXPECT().Create(gomock.Any(), "42", entries[1]).Return(nil),
		projection.EXPECT().Create(gomock.Any(), "42", entries[1]).Return(assert.AnError),
		source.EXPECT().Create(gomock.Any(), "42", entries[0]).Return(storage.ErrAlreadyExists),
		projection.EXPECT().Create(gomock.Any(), "42", entries[0]).Return(storage.ErrAlreadyExists),
		source.EXPECT().Create(gomock.Any(), "42", entries[1]).Return(storage.ErrAlreadyExists),
		projection.EXPECT().Create(gomock.Any(), "42", entries[1]).Return(nil),
		source.EXPECT().Create(gomock.Any(), "42", entries[2]).Return(nil),
		projection.EXPECT().Create(gomock.Any(), "42", entries[2]).Return(nil),
	)

	m := NewMaterializer(tally.NoopScope)
	ctx := context.Background()
	require.NoError(t, m.PersistAttribution(ctx, stores, "42", entries[0]))
	require.ErrorIs(t, m.PersistAttribution(ctx, stores, "42", entries[1]), assert.AnError)
	for _, entry := range entries {
		require.NoError(t, m.PersistAttribution(ctx, stores, "42", entry))
	}
}

func TestPersistAttributionRejectsInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name      string
		requestID string
		entry     entity.ProjectIssueAttribution
	}{
		{name: "missing request", entry: entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//app:test", Project: "app"}},
		{name: "invalid request", requestID: "not-a-request", entry: entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//app:test", Project: "app"}},
		{name: "missing task", requestID: "42", entry: entity.ProjectIssueAttribution{UnitID: "//app:test", Project: "app"}},
		{name: "task wide issue", requestID: "42", entry: entity.ProjectIssueAttribution{TaskID: "test", Project: "app"}},
		{name: "missing project", requestID: "42", entry: entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//app:test"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stores := storagemock.NewMockStorage(gomock.NewController(t))
			err := NewMaterializer(tally.NoopScope).PersistAttribution(context.Background(), stores, tt.requestID, tt.entry)
			require.Error(t, err)
		})
	}
}

func TestPersistAttributionCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stores := storagemock.NewMockStorage(gomock.NewController(t))
	entry := entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//app:test", Project: "app"}
	require.ErrorIs(t, NewMaterializer(tally.NoopScope).PersistAttribution(ctx, stores, "42", entry), context.Canceled)
}
