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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/extension/gitworkspace"
	gitexectest "github.com/uber/submitqueue/platform/git/exectest"
)

func newWorkspace(t *testing.T, ctx context.Context, p Params) gitworkspace.Workspace {
	t.Helper()
	f, err := NewFactory(p)
	require.NoError(t, err)
	ws, err := f.For(ctx, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

func pinnedRuntime(t *testing.T) Runtime {
	t.Helper()
	executable := gitexectest.Git(t)
	return Runtime{
		Executable:  executable,
		ExecPath:    filepath.Dir(executable),
		TemplateDir: filepath.Dir(gitexectest.Runfile(t, "SUBMITQUEUE_TEST_GIT_TEMPLATE_DESCRIPTION")),
	}
}

func TestNewFactory(t *testing.T) {
	tests := []struct {
		name    string
		params  Params
		wantErr bool
	}{
		{name: "directory only", params: Params{Dir: "/repo"}},
		{name: "committer identity", params: Params{Dir: "/repo", CommitterName: "Runway", CommitterEmail: "runway@example.com"}},
		{name: "committer name without email", params: Params{Dir: "/repo", CommitterName: "Runway"}, wantErr: true},
		{name: "committer email without name", params: Params{Dir: "/repo", CommitterEmail: "runway@example.com"}, wantErr: true},
		{name: "relative runtime path", params: Params{Dir: "/repo", Runtime: Runtime{Executable: "git", ExecPath: "/git-core", TemplateDir: "/templates"}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFactory(tt.params)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestExec_GitRunsOnPinnedRuntimeAsCommitter(t *testing.T) {
	// The ambient identity must not reach the commit: the pinned runtime scrubs
	// the environment, so only the configured committer can be recorded.
	t.Setenv("GIT_COMMITTER_NAME", "ambient")
	dir := t.TempDir()
	ws := newWorkspace(t, context.Background(), Params{
		Dir:            dir,
		Runtime:        pinnedRuntime(t),
		CommitterName:  "Runway",
		CommitterEmail: "runway@example.com",
	})

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "init", Bin: "git", Args: []string{"init", "-q", "-b", "main"}},
		{Alias: "commit", Bin: "git", Args: []string{"commit", "-q", "--allow-empty", "-m", "seed", "--author", "Jane Dev <jane@example.com>"}},
		{Alias: "idents", Bin: "git", Args: []string{"log", "-1", "--format=%an <%ae>|%cn <%ce>"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 3)
	for _, out := range outputs {
		require.Equal(t, int32(0), out.ExitCode, "%s: %s", out.Alias, out.Stderr)
	}
	assert.Equal(t, "Jane Dev <jane@example.com>|Runway <runway@example.com>\n", outputs[2].Stdout)
}

func TestExec_Success(t *testing.T) {
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

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
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

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
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "ok", Bin: "echo", Args: []string{"fine"}},
		{Alias: "fail", Bin: "false"},
		{Alias: "skipped", Bin: "echo", Args: []string{"never"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 3)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.NotEqual(t, int32(0), outputs[1].ExitCode)
	assert.False(t, outputs[1].Skipped)
	assert.True(t, outputs[2].Skipped)
	assert.Equal(t, "skipped", outputs[2].Alias)
}

func TestExec_StatePersistsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	ws := newWorkspace(t, context.Background(), Params{Dir: dir})

	_, err := ws.Exec([]gitworkspace.Command{
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
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "cat", Bin: "cat", Stdin: "from stdin"},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, "from stdin", outputs[0].Stdout)
}

func TestExec_Stderr(t *testing.T) {
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "err", Bin: "sh", Args: []string{"-c", "echo oops >&2"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, int32(0), outputs[0].ExitCode)
	assert.Equal(t, "oops\n", outputs[0].Stderr)
}

func TestExec_InvalidBinary(t *testing.T) {
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "bad", Bin: "nonexistent-binary-xyz"},
		{Alias: "skipped", Bin: "echo", Args: []string{"never"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	assert.NotEqual(t, int32(0), outputs[0].ExitCode)
	assert.False(t, outputs[0].Skipped)
	assert.NotEmpty(t, outputs[0].Stderr)
	assert.True(t, outputs[1].Skipped)
}

func TestExec_EmptyBatch(t *testing.T) {
	ws := newWorkspace(t, context.Background(), Params{Dir: t.TempDir()})

	outputs, err := ws.Exec(nil)
	require.NoError(t, err)
	assert.Empty(t, outputs)
}

func TestExec_CancelledContextStopsCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ws := newWorkspace(t, ctx, Params{Dir: t.TempDir()})

	cancel()
	outputs, err := ws.Exec([]gitworkspace.Command{
		{Alias: "first", Bin: "echo", Args: []string{"never"}},
		{Alias: "second", Bin: "echo", Args: []string{"never"}},
	})
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	assert.NotEqual(t, int32(0), outputs[0].ExitCode)
	assert.False(t, outputs[0].Skipped)
	assert.True(t, outputs[1].Skipped)
}
