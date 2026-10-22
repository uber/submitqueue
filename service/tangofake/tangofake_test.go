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

package tangofake

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tango/tangopb"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"

	"github.com/uber/submitqueue/platform/extension/gitworkspace/local"
	gitexectest "github.com/uber/submitqueue/platform/git/exectest"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango/tangoclient"
)

type fakeStream struct {
	tangopb.TangoServiceGetChangedTargetsYARPCServer
	sent []*tangopb.GetChangedTargetsResponse
}

func (s *fakeStream) Context() context.Context { return context.Background() }

func (s *fakeStream) Send(response *tangopb.GetChangedTargetsResponse, _ ...yarpc.StreamOption) error {
	s.sent = append(s.sent, response)
	return nil
}

type testRepo struct {
	t    *testing.T
	git  string
	work string
}

func (r *testRepo) run(args ...string) string {
	r.t.Helper()
	cmd := exec.Command(r.git, args...)
	cmd.Dir = r.work
	out, err := cmd.CombinedOutput()
	require.NoError(r.t, err, "git %v: %s", args, out)
	return strings.TrimSpace(string(out))
}

func (r *testRepo) write(file, content string) {
	r.t.Helper()
	full := filepath.Join(r.work, file)
	require.NoError(r.t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(r.t, os.WriteFile(full, []byte(content), 0o644))
}

func (r *testRepo) commit(message string) string {
	r.t.Helper()
	r.run("add", "-A")
	r.run("commit", "--no-gpg-sign", "-m", message)
	return r.run("rev-parse", "HEAD")
}

// newTestRepo commits three targets: //app depends on //lib, which depends on
// //base.
func newTestRepo(t *testing.T) (*testRepo, string) {
	r := &testRepo{t: t, git: gitexectest.Git(t), work: t.TempDir()}
	r.run("init", "-b", "main")
	r.run("config", "user.name", "Test")
	r.run("config", "user.email", "test@example.invalid")
	r.write("base/TARGET", "")
	r.write("base/base.txt", "a\n")
	r.write("lib/TARGET", "//base\n")
	r.write("lib/lib.txt", "a\n")
	r.write("app/TARGET", "# comment\n//lib\n//missing\n")
	r.write("app/app.txt", "a\n")
	r.write("docs/readme.txt", "a\n")
	return r, r.commit("seed")
}

func TestGetChangedTargets(t *testing.T) {
	tests := []struct {
		name   string
		change func(r *testRepo)
		want   map[string]struct {
			kind     tangopb.ChangeType
			distance int32
		}
	}{
		{
			name:   "a change to the lowest target reaches its dependents by distance",
			change: func(r *testRepo) { r.write("base/base.txt", "b\n") },
			want: map[string]struct {
				kind     tangopb.ChangeType
				distance int32
			}{
				"//base": {tangopb.CHANGE_TYPE_CHANGED, 0},
				"//lib":  {tangopb.CHANGE_TYPE_CHANGED, 1},
				"//app":  {tangopb.CHANGE_TYPE_CHANGED, 2},
			},
		},
		{
			name:   "a change to a leaf target reaches nothing else",
			change: func(r *testRepo) { r.write("app/app.txt", "b\n") },
			want: map[string]struct {
				kind     tangopb.ChangeType
				distance int32
			}{"//app": {tangopb.CHANGE_TYPE_CHANGED, 0}},
		},
		{
			name:   "a file outside every target changes nothing",
			change: func(r *testRepo) { r.write("docs/readme.txt", "b\n") },
		},
		{
			name: "a new target is new",
			change: func(r *testRepo) {
				r.write("extra/TARGET", "//lib\n")
				r.write("extra/extra.txt", "a\n")
			},
			want: map[string]struct {
				kind     tangopb.ChangeType
				distance int32
			}{"//extra": {tangopb.CHANGE_TYPE_NEW, 0}},
		},
		{
			name: "a deleted target is deleted and its dependents change",
			change: func(r *testRepo) {
				require.NoError(r.t, os.RemoveAll(filepath.Join(r.work, "lib")))
			},
			want: map[string]struct {
				kind     tangopb.ChangeType
				distance int32
			}{
				"//lib": {tangopb.CHANGE_TYPE_DELETED, 0},
				"//app": {tangopb.CHANGE_TYPE_CHANGED, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, base := newTestRepo(t)
			tt.change(repo)
			head := repo.commit("change")

			stream := &fakeStream{}
			err := New(Params{Workspaces: local.NewFactory(local.Params{Dir: repo.work})}).GetChangedTargets(
				&tangopb.GetChangedTargetsRequest{SecondRevision: &tangopb.BuildDescription{
					BaseSha:  base,
					Requests: []*tangopb.Request{{Url: "git://example.invalid/org/repo/refs%2Fheads%2Fmain/" + head}},
				}}, stream)
			require.NoError(t, err)
			require.Len(t, stream.sent, 2, "targets, then metadata")

			labels := stream.sent[1].GetMetadata().GetTargetIdMapping()
			got := map[string]struct {
				kind     tangopb.ChangeType
				distance int32
			}{}
			for _, target := range stream.sent[0].GetChangedTargets().GetChangedTargets() {
				node := target.GetNewTarget()
				if target.GetChangeType() == tangopb.CHANGE_TYPE_DELETED {
					node = target.GetOldTarget()
				}
				got[labels[node.GetId()]] = struct {
					kind     tangopb.ChangeType
					distance int32
				}{target.GetChangeType(), target.GetDistance()}
			}
			if len(tt.want) == 0 {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetChangedTargets_Rejects(t *testing.T) {
	repo, base := newTestRepo(t)
	server := New(Params{Workspaces: local.NewFactory(local.Params{Dir: repo.work})})

	tests := []struct {
		name    string
		request *tangopb.GetChangedTargetsRequest
	}{
		{name: "no base", request: &tangopb.GetChangedTargetsRequest{SecondRevision: &tangopb.BuildDescription{}}},
		{
			name: "change URI without a commit",
			request: &tangopb.GetChangedTargetsRequest{SecondRevision: &tangopb.BuildDescription{
				BaseSha: base, Requests: []*tangopb.Request{{Url: "git://example.invalid/org/repo/refs%2Fheads%2Fmain/main"}},
			}},
		},
		{
			name: "commit the repository does not hold",
			request: &tangopb.GetChangedTargetsRequest{SecondRevision: &tangopb.BuildDescription{
				BaseSha: base, Requests: []*tangopb.Request{{Url: "git://example.invalid/org/repo/refs%2Fheads%2Fmain/" + strings.Repeat("a", 40)}},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, server.GetChangedTargets(tt.request, &fakeStream{}))
		})
	}
}

func TestServedOverGRPC(t *testing.T) {
	repo, base := newTestRepo(t)
	repo.write("base/base.txt", "b\n")
	head := repo.commit("change")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "tango",
		Inbounds: yarpc.Inbounds{grpc.NewTransport().NewInbound(listener)},
	})
	dispatcher.Register(tangopb.BuildTangoYARPCProcedures(New(Params{Workspaces: local.NewFactory(local.Params{Dir: repo.work})})))
	require.NoError(t, dispatcher.Start())
	t.Cleanup(func() { _ = dispatcher.Stop() })

	connection, err := tangoclient.Dial(listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	client, err := connection.ChangedTargetsClient("org/repo", "")
	require.NoError(t, err)

	got, err := client.ChangedTargets(context.Background(), tango.ChangedTargetsRequest{
		BaseRevision: base,
		ChangeURIs:   []string{"git://example.invalid/org/repo/refs%2Fheads%2Fmain/" + head},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []tango.LabelDistance{
		{Label: "//base", Distance: 0},
		{Label: "//lib", Distance: 1},
		{Label: "//app", Distance: 2},
	}, got)
}
