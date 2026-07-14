// Copyright (c) 2025 Uber Technologies, Inc.
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

package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ buildrunner.BuildRunner = New()
}

func TestTrigger(t *testing.T) {
	tests := []struct {
		name    string
		headURI string
		wantErr bool
	}{
		{name: "no marker succeeds", headURI: "git://repo/ref/deadbeef"},
		{name: "unrelated query params succeed", headURI: "git://repo/ref/deadbeef?attempt=2"},
		{name: "trigger-error marker fails", headURI: "git://repo/ref/deadbeef?buildrunner-fake=trigger-error", wantErr: true},
		{name: "build-fail marker still triggers", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-fail"},
		{name: "build-error marker still triggers", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := New().Trigger(context.Background(), tt.headURI, "", nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, id.ID)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, id.ID)
		})
	}
}

func TestTrigger_UniqueIDs(t *testing.T) {
	a, err := New().Trigger(context.Background(), "git://repo/ref/deadbeef", "", nil)
	require.NoError(t, err)
	b, err := New().Trigger(context.Background(), "git://repo/ref/deadbeef", "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, b.ID)
}

func TestStatus(t *testing.T) {
	tests := []struct {
		name       string
		headURI    string
		wantStatus entity.BuildStatus
		wantErr    bool
	}{
		{name: "no marker succeeds", headURI: "git://repo/ref/deadbeef", wantStatus: entity.BuildStatusSucceeded},
		{name: "build-fail marker fails", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-fail", wantStatus: entity.BuildStatusFailed},
		{name: "build-error marker errors", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-error", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := New().Trigger(context.Background(), tt.headURI, "", nil)
			require.NoError(t, err)

			status, metadata, err := New().Status(context.Background(), id)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
			assert.Nil(t, metadata)
		})
	}
}

func TestStatus_UnrecognizedIDSucceeds(t *testing.T) {
	status, metadata, err := New().Status(context.Background(), entity.BuildID{ID: "not-minted-by-this-fake"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
	assert.Nil(t, metadata)
}

func TestStatus_StatelessAcrossInstances(t *testing.T) {
	id, err := New().Trigger(context.Background(), "git://repo/ref/deadbeef?buildrunner-fake=build-fail", "", nil)
	require.NoError(t, err)

	status, _, err := New().Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusFailed, status)
}

func TestCancel_NoOp(t *testing.T) {
	err := New().Cancel(context.Background(), entity.BuildID{ID: "anything"})
	assert.NoError(t, err)
}
