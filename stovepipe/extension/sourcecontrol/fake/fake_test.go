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
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ sourcecontrol.SourceControl = New(nil)
}

// history is ordered newest-first: c is the latest, a is the oldest ancestor.
var history = []string{"git://repo/ref/c", "git://repo/ref/b", "git://repo/ref/a"}

func TestLatest(t *testing.T) {
	tests := []struct {
		name    string
		history []string
		want    string
		wantErr bool
	}{
		{name: "newest first", history: history, want: "git://repo/ref/c"},
		{name: "empty history", history: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.history).Latest(context.Background())
			if tt.wantErr {
				require.ErrorIs(t, err, sourcecontrol.ErrNotFound)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsAncestor(t *testing.T) {
	tests := []struct {
		name       string
		ancestor   string
		descendant string
		want       bool
		wantErr    bool
	}{
		{name: "older is ancestor of newer", ancestor: "git://repo/ref/a", descendant: "git://repo/ref/c", want: true},
		{name: "newer is not ancestor of older", ancestor: "git://repo/ref/c", descendant: "git://repo/ref/a", want: false},
		{name: "equal is ancestor of itself", ancestor: "git://repo/ref/b", descendant: "git://repo/ref/b", want: true},
		{name: "unknown ancestor", ancestor: "git://repo/ref/x", descendant: "git://repo/ref/a", wantErr: true},
		{name: "unknown descendant", ancestor: "git://repo/ref/a", descendant: "git://repo/ref/x", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(history).IsAncestor(context.Background(), tt.ancestor, tt.descendant)
			if tt.wantErr {
				require.ErrorIs(t, err, sourcecontrol.ErrNotFound)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHistory(t *testing.T) {
	tests := []struct {
		name       string
		cursor     string
		limit      int
		wantItems  []string
		wantCursor string
		wantErr    bool
	}{
		{
			name:       "first page with next cursor",
			cursor:     "",
			limit:      2,
			wantItems:  []string{"git://repo/ref/c", "git://repo/ref/b"},
			wantCursor: "git://repo/ref/a",
		},
		{
			name:       "second page reaches end",
			cursor:     "git://repo/ref/a",
			limit:      2,
			wantItems:  []string{"git://repo/ref/a"},
			wantCursor: "",
		},
		{
			name:       "limit larger than remaining returns rest",
			cursor:     "",
			limit:      10,
			wantItems:  history,
			wantCursor: "",
		},
		{
			name:       "zero limit returns rest in one page",
			cursor:     "",
			limit:      0,
			wantItems:  history,
			wantCursor: "",
		},
		{
			name:    "unknown cursor",
			cursor:  "git://repo/ref/x",
			limit:   2,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(history).History(context.Background(), tt.cursor, tt.limit)
			if tt.wantErr {
				require.ErrorIs(t, err, sourcecontrol.ErrNotFound)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantItems, got.Items)
			assert.Equal(t, tt.wantCursor, got.NextCursor)
		})
	}
}
