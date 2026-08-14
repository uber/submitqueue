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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsBinary(t *testing.T) {
	tests := []struct {
		name   string
		window []byte
		want   bool
	}{
		{name: "empty is text", window: nil, want: false},
		{name: "ascii source is text", window: []byte("package main\n"), want: false},
		{name: "utf-8 is text", window: []byte("// © Uber — naïve\n"), want: false},
		{name: "crlf is text", window: []byte("a\r\nb\r\n"), want: false},
		{name: "high bytes without NUL are text", window: []byte{0x80, 0xfe, 0xff}, want: false},
		{name: "leading NUL is binary", window: []byte{0x00, 'a'}, want: true},
		{name: "trailing NUL is binary", window: []byte{'a', 0x00}, want: true},
		{name: "elf header is binary", window: []byte{0x7f, 'E', 'L', 'F', 0x02, 0x00}, want: true},
		{name: "mach-o header is binary", window: []byte{0xcf, 0xfa, 0xed, 0xfe, 0x0c, 0x00}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isBinary(tt.window))
		})
	}
}

func TestIsBinaryFile(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name     string
		contents []byte
		want     bool
	}{
		{name: "empty file is text", contents: []byte{}, want: false},
		{name: "go source is text", contents: []byte("package main\n\nfunc main() {}\n"), want: false},
		{name: "executable is binary", contents: append([]byte{0x7f, 'E', 'L', 'F'}, make([]byte, 512)...), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, tt.name)
			require.NoError(t, os.WriteFile(path, tt.contents, 0o600))

			got, err := isBinaryFile(path)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBinaryFileOnlyReadsTheLeadingWindow(t *testing.T) {
	// A NUL past the sniff window is not reached, which is what bounds the
	// linter's cost on a large text file rather than a claim about the file.
	path := filepath.Join(t.TempDir(), "late-nul")
	contents := append(make([]byte, 0, sniffLen+2), []byte("package main\n")...)
	for len(contents) < sniffLen {
		contents = append(contents, 'x')
	}
	contents = append(contents, 0x00)
	require.NoError(t, os.WriteFile(path, contents, 0o600))

	got, err := isBinaryFile(path)
	require.NoError(t, err)
	assert.False(t, got)
}

func TestIsBinaryFileMissing(t *testing.T) {
	_, err := isBinaryFile(filepath.Join(t.TempDir(), "absent"))
	require.Error(t, err)
}
