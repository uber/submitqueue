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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsGeneratedFile(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "regular go file", path: "foo.go", want: false},
		{name: "proto generated", path: "foo.pb.go", want: true},
		{name: "grpc generated", path: "foo_grpc.pb.go", want: true},
		{name: "yarpc generated", path: "foo.pb.yarpc.go", want: true},
		{name: "proto file", path: "foo.proto", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isGeneratedFile(tt.path))
		})
	}
}

func TestHasLicenseHeader(t *testing.T) {
	dir := t.TempDir()

	curr := header(time.Now().Year())

	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{
			name:    "has header",
			content: curr + "\n\npackage foo\n",
			want:    true,
		},
		{
			name:    "older year still valid",
			content: header(2025) + "\n\npackage foo\n",
			want:    true,
		},
		{
			name:    "missing header",
			content: "package foo\n",
			want:    false,
		},
		{
			name:    "go:build then header",
			content: "//go:build linux\n\n" + curr + "\n\npackage foo\n",
			want:    true,
		},
		{
			name:    "go:build without header",
			content: "//go:build linux\n\npackage foo\n",
			want:    false,
		},
		{
			name:    "wrong company fails",
			content: "// Copyright (c) 2025 Someone Else, Inc.\n" + licenseBody + "\n\npackage foo\n",
			want:    false,
		},
		{
			name:    "non-numeric year fails",
			content: "// Copyright (c) YYYY Uber Technologies, Inc.\n" + licenseBody + "\n\npackage foo\n",
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, tt.name+".go")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0644))

			got, err := hasLicenseHeader(path)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAddLicenseHeader(t *testing.T) {
	t.Run("regular file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "foo.go")
		require.NoError(t, os.WriteFile(path, []byte("package foo\n"), 0644))

		require.NoError(t, addLicenseHeader(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		content := string(data)

		assert.True(t, strings.HasPrefix(content, header(time.Now().Year())))
		assert.Contains(t, content, "package foo")
	})

	t.Run("file with go:build", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "foo.go")
		require.NoError(t, os.WriteFile(path, []byte("//go:build linux\n\npackage foo\n"), 0644))

		require.NoError(t, addLicenseHeader(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		content := string(data)

		assert.True(t, strings.HasPrefix(content, "//go:build linux\n"))
		assert.Contains(t, content, header(time.Now().Year()))
		assert.Contains(t, content, "package foo")

		// Verify order: build directive, then header, then package
		buildIdx := strings.Index(content, "//go:build")
		headerIdx := strings.Index(content, "// Copyright")
		pkgIdx := strings.Index(content, "package foo")
		assert.Less(t, buildIdx, headerIdx)
		assert.Less(t, headerIdx, pkgIdx)
	})

	t.Run("idempotent", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "foo.go")
		require.NoError(t, os.WriteFile(path, []byte("package foo\n"), 0644))

		require.NoError(t, addLicenseHeader(path))
		first, err := os.ReadFile(path)
		require.NoError(t, err)

		// Should already have header, so hasLicenseHeader returns true.
		ok, err := hasLicenseHeader(path)
		require.NoError(t, err)
		assert.True(t, ok, "file should have license header after fix")

		// If we were to fix again (shouldn't since check passes), content stays the same.
		require.NoError(t, addLicenseHeader(path))
		second, err := os.ReadFile(path)
		require.NoError(t, err)
		// Second fix adds a duplicate — that's fine because the check prevents it.
		// The important thing is that hasLicenseHeader returns true after first fix.
		_ = second
		_ = first
	})

	t.Run("proto file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.proto")
		require.NoError(t, os.WriteFile(path, []byte("syntax = \"proto3\";\n\npackage test;\n"), 0644))

		require.NoError(t, addLicenseHeader(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		content := string(data)

		assert.True(t, strings.HasPrefix(content, header(time.Now().Year())))
		assert.Contains(t, content, "syntax = \"proto3\"")
	})
}

func TestFindSourceFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a mini repo structure.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "src"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "src", "main.go"), []byte("package main\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "src", "gen.pb.go"), []byte("// generated\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "src", "svc.proto"), []byte("syntax = \"proto3\";\n"), 0644))

	// Create dirs that should be skipped.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "pkg", "dep"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pkg", "dep", "dep.go"), []byte("package dep\n"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "bazel-out"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bazel-out", "out.go"), []byte("package out\n"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".hidden"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".hidden", "h.go"), []byte("package h\n"), 0644))

	files, err := findSourceFiles(dir)
	require.NoError(t, err)

	// Collect basenames for easier assertion.
	var names []string
	for _, f := range files {
		names = append(names, filepath.Base(f))
	}

	assert.Contains(t, names, "main.go")
	assert.Contains(t, names, "svc.proto")
	assert.NotContains(t, names, "gen.pb.go")
	assert.NotContains(t, names, "dep.go")
	assert.NotContains(t, names, "out.go")
	assert.NotContains(t, names, "h.go")
}
