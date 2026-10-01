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

package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/extension/gitworkspace"
)

func TestExec_Success(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "echo", Bin: "echo", Args: []string{"hello"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, "echo", outputs[0].Alias)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, "hello\n", outputs[0].Stdout)
}

func TestExec_MultipleCommands(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "first", Bin: "echo", Args: []string{"one"}},
		{Alias: "second", Bin: "echo", Args: []string{"two"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, int32(0), outputs[1].ExitCode)
	assert.Equal(t, "one\n", outputs[0].Stdout)
	assert.Equal(t, "two\n", outputs[1].Stdout)
}

func TestExec_SkipsAfterFailure(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "ok", Bin: "echo", Args: []string{"fine"}},
		{Alias: "fail", Bin: "false"},
		{Alias: "skipped", Bin: "echo", Args: []string{"never"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 3)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.NotEqual(t, int32(0), outputs[1].ExitCode)
	assert.Equal(t, int32(skippedExitCode), outputs[2].ExitCode)
	assert.Equal(t, "skipped", outputs[2].Alias)
}

func TestExec_StatePersistsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	ws, err := NewFactory(Params{Dir: dir}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	_, err = ws.Exec([]gitworkspace.Command{
		{Alias: "write", Bin: "sh", Args: []string{"-c", "echo content > testfile"}},
	})
	require.NoError(t, err)

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "read", Bin: "cat", Args: []string{"testfile"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, "content\n", outputs[0].Stdout)
}

func TestExec_Stdin(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "cat", Bin: "cat", Stdin: "from stdin"},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, "from stdin", outputs[0].Stdout)
}

func TestExec_Stderr(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "err", Bin: "sh", Args: []string{"-c", "echo oops >&2"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, "oops\n", outputs[0].Stderr)
}

func TestExec_InvalidBinary(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "bad", Bin: "nonexistent-binary-xyz"},
		{Alias: "skipped", Bin: "echo", Args: []string{"never"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	assert.Equal(t, int32(skippedExitCode), outputs[0].ExitCode)
	assert.NotEmpty(t, outputs[0].Stderr)
	assert.Equal(t, int32(skippedExitCode), outputs[1].ExitCode)
}

func TestExec_EmptyBatch(t *testing.T) {
	ws, err := NewFactory(Params{Dir: t.TempDir()}).For(context.Background(), "")
	require.NoError(t, err)
	defer ws.Close()

	outputs, err := ws.Exec(nil)
	require.NoError(t, err)
	assert.Empty(t, outputs)
}
