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

// This suite runs the local demo's ANALYZER=tango stack: the git provider with
// the Tango conflict analyzer answered by the fake Tango server, which reads
// TARGET files from the orchestrator's copy of the repository. It asserts that
// batches whose paths never overlap still depend on each other when their
// build targets do, and that batches with no shared target stay independent.
package e2e_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	consumergatefile "github.com/uber/submitqueue/platform/extension/consumergate/file"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
	gitexectest "github.com/uber/submitqueue/platform/git/exectest"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

// tangoQueue is the queue profiles.tango.yaml gives the Tango analyzer.
const tangoQueue = "demo-queue"

// tangoTargets is the target graph the suite seeds: demo/00 and demo/01 are
// libraries, and every other folder depends on one of them. It is the same
// shape tool/gitsandbox commits for the demo.
var tangoTargets = map[string]string{
	"demo/00/TARGET": "",
	"demo/01/TARGET": "",
	"demo/03/TARGET": "//demo/01\n",
	"demo/04/TARGET": "//demo/00\n",
}

type TangoSuite struct {
	suite.Suite
	ctx           context.Context
	log           *testutil.TestLogger
	stack         *testutil.ComposeStack
	gatewayClient gatewaypb.SubmitQueueGatewayClient
	db            *sql.DB
	gate          *consumergatefile.Store

	git  string
	bare string
	work string
}

func TestTangoE2E(t *testing.T) {
	suite.Run(t, new(TangoSuite))
}

func (s *TangoSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.git = gitexectest.Git(t)
	t.Setenv("SQ_CONTAINER_USER", dockerContainerUser(t))

	gateDir := t.TempDir()
	t.Setenv("SQ_CONSUMER_GATE_DIR", gateDir)
	s.gate = consumergatefile.New(gateDir)

	sandbox := t.TempDir()
	s.bare = filepath.Join(sandbox, "sandbox.git")
	s.work = filepath.Join(t.TempDir(), "work")
	t.Setenv("SQ_GIT_SANDBOX_DIR", sandbox)
	t.Setenv("SQ_RUNWAY_CHECKOUT_DIR", t.TempDir())
	s.seedRepositoryWithTargets()

	staged := t.TempDir()
	for _, name := range []string{"merge.yaml", "profiles.tango.yaml"} {
		contents, err := os.ReadFile(testutil.Runfile("service/submitqueue/demo/provider/git/" + name))
		require.NoError(t, err, "reading example config %s", name)
		require.NoError(t, os.WriteFile(filepath.Join(staged, name), contents, 0o644))
	}
	t.Setenv("SQ_PROVIDER_CONFIG_DIR", staged)

	composeFile := testutil.Runfile("service/submitqueue/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-submitqueue-tango",
		testutil.WithOverlay(
			testutil.Runfile("service/submitqueue/docker-compose.git.yml"),
			testutil.Runfile("service/submitqueue/docker-compose.tango.yml"),
		),
		testutil.WithBuildContext(map[string]string{
			".docker-bin/gateway":                                "service/submitqueue/gateway/server/gateway_linux",
			".docker-bin/orchestrator":                           "service/submitqueue/orchestrator/server/orchestrator_linux",
			".docker-bin/runway":                                 "service/runway/server/runway_linux",
			".docker-bin/tangofake":                              "service/tangofake/server/tangofake_linux",
			"service/submitqueue/gateway/server/Dockerfile":      "service/submitqueue/gateway/server/Dockerfile",
			"service/submitqueue/gateway/server/queues.yaml":     "service/submitqueue/gateway/server/queues.yaml",
			"service/submitqueue/orchestrator/server/Dockerfile": "service/submitqueue/orchestrator/server/Dockerfile",
			"service/runway/server/Dockerfile":                   "service/runway/server/Dockerfile",
			"service/tangofake/server/Dockerfile":                "service/tangofake/server/Dockerfile",
		}))
	require.NoError(t, s.stack.Up(), "failed to start compose stack")

	var err error
	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err)
	queueDB, err := s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err)
	t.Cleanup(func() { _ = queueDB.Close() })

	testutil.ApplySubmitQueueStorageSchema(t, s.log, s.db)
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/extension/conflict/tango/mysql/schema"))
	testutil.ApplySchema(t, s.log, queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err)
	s.gatewayClient = gatewaypb.NewSubmitQueueGatewayClient(conn)
}

func (s *TangoSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *TangoSuite) TestBatchesDependOnSharedTargets() {
	// Hold the lead land so the batches below find it in flight; the gate must
	// be closed before the lead is submitted.
	key := consumergate.Key{
		ConsumerGroup: "runway-merge",
		Partition:     entityqueue.PartitionIdentity{Tenant: tangoQueue, PartitionKey: tangoQueue},
	}
	s.Require().NoError(s.gate.Close(s.ctx, key, consumergate.Metadata{
		Reason:      "e2e: keep the lead batch in flight while the others are analyzed",
		CreatedBy:   "e2e-suite",
		CreatedAtMs: time.Now().UnixMilli(),
	}))
	defer func() { _ = s.gate.Open(s.ctx, key) }()

	// A change to demo/04 affects //demo/04 alone.
	lead := s.landChange("feature/tango-lead", "demo/04/lead.txt")
	leadBatch := s.awaitBatchID(lead)
	s.awaitParkedLand(leadBatch)

	// A change to the library demo/00 affects //demo/00 and, one step away,
	// //demo/04, so it conflicts with the lead although no path overlaps.
	library := s.landChange("feature/tango-library", "demo/00/lib.txt")
	libraryBatch := s.awaitBatchID(library)
	s.Equal([]string{leadBatch}, s.dependenciesOnceAnalyzed(libraryBatch),
		"a change to a library must depend on the in-flight change to a target built on it")

	// A change to demo/03 affects //demo/03 alone, which neither batch reaches.
	unrelated := s.landChange("feature/tango-unrelated", "demo/03/other.txt")
	unrelatedBatch := s.awaitBatchID(unrelated)
	s.Empty(s.dependenciesOnceAnalyzed(unrelatedBatch),
		"a change sharing no target with an in-flight batch must stay independent")

	s.Require().NoError(s.gate.Open(s.ctx, key))
	for _, sqid := range []string{lead, library, unrelated} {
		s.requireLanded(sqid)
	}
}

// landChange pushes a branch adding one file and submits it to the queue,
// returning the request ID.
func (s *TangoSuite) landChange(branch, file string) string {
	s.runGit(s.work, "fetch", "origin")
	s.runGit(s.work, "checkout", "-B", branch, "origin/main")
	s.Require().NoError(os.MkdirAll(filepath.Join(s.work, filepath.Dir(file)), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(s.work, file), []byte(branch+"\n"), 0o644))
	s.runGit(s.work, "add", ".")
	s.runGit(s.work, "commit", "-m", "add "+file)
	s.runGit(s.work, "push", "-f", "origin", branch)
	head := s.runGit(s.work, "rev-parse", "HEAD")

	resp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue: tangoQueue,
		Change: &changepb.Change{Uris: []string{
			fmt.Sprintf("git://%s/sandbox/%s/%s", sandboxRemote, url.PathEscape("refs/heads/"+branch), head),
		}},
		Strategy: mergestrategypb.Strategy_SQUASH_REBASE,
	})
	s.Require().NoError(err, "Land failed for %s", branch)
	s.Require().NotEmpty(resp.Sqid)
	return resp.Sqid
}

// awaitBatchID waits for the request to be carried by a batch.
func (s *TangoSuite) awaitBatchID(sqid string) string {
	var batchID string
	pollUntil(persistPollInterval, func() bool {
		err := s.db.QueryRowContext(s.ctx,
			"SELECT id FROM batch WHERE queue = ? AND JSON_CONTAINS(contains, JSON_QUOTE(?)) LIMIT 1",
			tangoQueue, sqid).Scan(&batchID)
		return err == nil
	})
	return batchID
}

// dependenciesOnceAnalyzed returns the batch's dependency set after the batch
// has left creating, which is when the set and the new state are written
// together.
func (s *TangoSuite) dependenciesOnceAnalyzed(batchID string) []string {
	var raw string
	pollUntil(persistPollInterval, func() bool {
		var state string
		if err := s.db.QueryRowContext(s.ctx,
			"SELECT state, dependencies FROM batch WHERE queue = ? AND id = ?", tangoQueue, batchID,
		).Scan(&state, &raw); err != nil {
			return false
		}
		return entity.BatchState(state) != entity.BatchStateCreating
	})
	var dependencies []string
	s.Require().NoError(json.Unmarshal([]byte(raw), &dependencies))
	return dependencies
}

// awaitParkedLand waits until the gate has caught the batch's land delivery,
// which proves the batch finished analysis and is in flight.
func (s *TangoSuite) awaitParkedLand(batchID string) {
	pollUntil(persistPollInterval, func() bool {
		records, err := s.gate.ListParked(s.ctx, "runway-merge")
		s.Require().NoError(err)
		for _, r := range records {
			if r.Topic == runwaymq.TopicKeyMerge.String() && r.MessageID == batchID {
				return true
			}
		}
		return false
	})
}

func (s *TangoSuite) requireLanded(sqid string) {
	var got entity.RequestStatus
	pollUntil(persistPollInterval, func() bool {
		resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: sqid, Queue: tangoQueue})
		if err != nil || resp.Request == nil {
			return false
		}
		got = entity.RequestStatus(resp.Request.Status)
		return isTerminalStatus(got)
	})
	s.Equal(entity.RequestStatusLanded, got, "request %s reached the wrong terminal status", sqid)
}

// seedRepositoryWithTargets creates the bare repository Runway merges into and
// a working clone, with the TARGET files committed on main.
func (s *TangoSuite) seedRepositoryWithTargets() {
	s.runGit(filepath.Dir(s.bare), "init", "--bare", "-b", "main", s.bare)
	s.runGit(filepath.Dir(s.work), "clone", s.bare, s.work)
	for _, kv := range [][2]string{
		{"user.name", "E2E Author"},
		{"user.email", "author@example.com"},
		{"commit.gpgsign", "false"},
	} {
		s.runGit(s.work, "config", kv[0], kv[1])
	}
	for path, contents := range tangoTargets {
		s.Require().NoError(os.MkdirAll(filepath.Join(s.work, filepath.Dir(path)), 0o755))
		s.Require().NoError(os.WriteFile(filepath.Join(s.work, path), []byte(contents), 0o644))
	}
	s.runGit(s.work, "add", ".")
	s.runGit(s.work, "commit", "-m", "seed targets")
	s.runGit(s.work, "push", "origin", "main")
}

func (s *TangoSuite) runGit(dir string, args ...string) string {
	s.T().Helper()
	out, err := gitexec.Output(s.ctx, s.git, dir, args...)
	require.NoError(s.T(), err, "git %s in %s", strings.Join(args, " "), dir)
	return strings.TrimSpace(out)
}
