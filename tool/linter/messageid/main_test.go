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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	tests := []struct {
		name  string
		src   string
		wantN int
		wantF string
	}{
		{
			name: "aliased import is caught",
			src: `package p
import entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
func f() { _ = entityqueue.NewMessage("id", nil, "part", nil) }`,
			wantN: 1,
			wantF: "entityqueue.NewMessage",
		},
		{
			name: "unaliased import is caught under its package name",
			src: `package p
import "github.com/uber/submitqueue/platform/base/messagequeue"
func f() { _ = messagequeue.NewMessage("id", nil, "part", nil) }`,
			wantN: 1,
			wantF: "messagequeue.NewMessage",
		},
		{
			name: "publishing through the helper passes",
			src: `package p
import "github.com/uber/submitqueue/platform/publish"
func f() { _ = publish.IntentID("batch-1", "landed") }`,
		},
		{
			name: "NewMessage of an unrelated package passes",
			src: `package p
import "example.com/other"
func f() { _ = other.NewMessage("id") }`,
		},
		{
			name: "the name in a comment or string passes",
			src: `package p
// entityqueue.NewMessage is named here but not called.
func f() string { return "entityqueue.NewMessage(id, nil, part, nil)" }`,
		},
		{
			name: "importing without constructing passes",
			src: `package p
import entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
func f(m entityqueue.Message) string { return m.ID }`,
		},
		{
			name: "every construction in a file is reported",
			src: `package p
import entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
func f() {
	_ = entityqueue.NewMessage("a", nil, "p", nil)
	_ = entityqueue.NewMessage("b", nil, "p", nil)
}`,
			wantN: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "src.go")
			require.NoError(t, os.WriteFile(path, []byte(tt.src), 0o600))

			got, err := check("src.go", path)
			require.NoError(t, err)
			require.Len(t, got, tt.wantN)
			if tt.wantF != "" {
				assert.Equal(t, tt.wantF, got[0].fn)
			}
		})
	}
}

func TestAllowed(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"platform/publish/publish.go", true},
		{"platform/extension/messagequeue/mysql/subscriber.go", true},
		{"submitqueue/orchestrator/controller/batch/batch.go", false},
		{"runway/controller/land/land.go", false},
		// A path merely prefixed by an allowed root's name is not inside it.
		{"platform/publishing/thing.go", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, allowed(tt.path))
		})
	}
}
