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

package validationfact

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestPersist(t *testing.T) {
	for _, project := range []string{"", "project"} {
		t.Run(project, func(t *testing.T) {
			for _, tt := range []struct {
				name        string
				createErr   error
				loadErr     error
				otherOwner  bool
				wantCreated bool
				wantErr     error
			}{
				{name: "creates fact", wantCreated: true},
				{name: "adopts existing fact", createErr: storage.ErrAlreadyExists},
				{name: "rejects different owner", createErr: storage.ErrAlreadyExists, otherOwner: true},
				{name: "create fails", createErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded},
				{name: "read fails", createErr: storage.ErrAlreadyExists, loadErr: context.Canceled, wantErr: context.Canceled},
			} {
				t.Run(tt.name, func(t *testing.T) {
					ctrl := gomock.NewController(t)
					store := storagemock.NewMockValidationFactStore(ctrl)
					incoming := entity.ValidationFact{
						URI: "git://repository/main/commit", Project: project,
						RequestID: "7", Degree: entity.DegreeBroken, CreatedAt: 20,
					}
					retained := incoming
					retained.Degree, retained.CreatedAt = entity.DegreeGreen, 10
					if tt.otherOwner {
						retained.RequestID = "6"
					}
					store.EXPECT().Create(gomock.Any(), incoming).Return(tt.createErr)
					if tt.createErr == storage.ErrAlreadyExists {
						store.EXPECT().Get(gomock.Any(), incoming.URI, project).Return(retained, tt.loadErr)
					}

					fact, created, err := Persist(context.Background(), zap.NewNop().Sugar(), tally.NoopScope, store, incoming)

					if tt.wantErr != nil {
						require.ErrorIs(t, err, tt.wantErr)
					} else if tt.otherOwner {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						if tt.wantCreated {
							require.Equal(t, incoming, fact)
						} else {
							require.Equal(t, retained, fact)
						}
					}
					require.Equal(t, tt.wantCreated, created)
					if err != nil {
						require.Empty(t, fact)
					}
				})
			}
		})
	}
}
