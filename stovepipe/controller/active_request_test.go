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

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestTrackActiveRequest(t *testing.T) {
	unavailable := errors.New("unavailable")
	for _, tt := range []struct {
		name      string
		state     entity.RequestState
		createErr error
		wantErr   error
	}{
		{name: "accepted", state: entity.RequestStateAccepted},
		{name: "processing repair", state: entity.RequestStateProcessing},
		{name: "duplicate", state: entity.RequestStateAccepted, createErr: storage.ErrAlreadyExists},
		{name: "failed write", state: entity.RequestStateAccepted, createErr: unavailable, wantErr: unavailable},
		{name: "succeeded", state: entity.RequestStateSucceeded},
		{name: "failed", state: entity.RequestStateFailed},
		{name: "cancelled", state: entity.RequestStateCancelled},
		{name: "superseded", state: entity.RequestStateSuperseded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockStorage(ctrl)
			active := storagemock.NewMockActiveRequestStore(ctrl)
			if !tt.state.IsTerminal() {
				store.EXPECT().GetActiveRequestStore().Return(active)
				active.EXPECT().Create(gomock.Any(), "42").Return(tt.createErr)
			}
			err := trackActiveRequest(context.Background(), store, entity.Request{ID: "42", State: tt.state})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
