// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uber/submitqueue/sqsim"
	"github.com/uber/submitqueue/sqsim/model"
)

// LocalOptions configure a fresh local Compose run.
type LocalOptions struct {
	// ScenarioName is the selected public scenario name.
	ScenarioName string
	// Scenario is the immutable workload.
	Scenario sqsim.Scenario
	// Observer receives visible state changes.
	Observer Observer
	// Stdout receives build and Compose progress.
	Stdout io.Writer
	// Stderr receives service logs on failure.
	Stderr io.Writer
	// RepoRoot overrides repository discovery.
	RepoRoot string
	// PollInterval overrides the public API poll interval.
	PollInterval time.Duration
}

// RunLocal executes a scenario against one fresh local Compose stack.
func RunLocal(ctx context.Context, options LocalOptions) (report Report, retErr error) {
	if options.Stdout == nil {
		options.Stdout = io.Discard
	}
	if options.Stderr == nil {
		options.Stderr = io.Discard
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 250 * time.Millisecond
	}
	repoRoot := options.RepoRoot
	if repoRoot == "" {
		var err error
		repoRoot, err = findRepoRoot(ctx)
		if err != nil {
			return Report{}, err
		}
	}
	profile, err := model.Compile(options.ScenarioName, options.Scenario)
	if err != nil {
		return Report{}, err
	}
	profileDir, err := os.MkdirTemp("", "sqsim-profile-*")
	if err != nil {
		return Report{}, fmt.Errorf("create profile directory: %w", err)
	}
	defer os.RemoveAll(profileDir)
	if err := model.Write(filepath.Join(profileDir, "profile.json"), profile); err != nil {
		return Report{}, err
	}

	stack := newLocalStack(repoRoot, profileDir, options.Stdout, options.Stderr)
	if err := stack.Start(ctx); err != nil {
		stack.Logs(ctx)
		stack.Close(context.Background())
		return Report{}, err
	}
	defer func() {
		if retErr != nil || !report.Passed {
			stack.Logs(context.Background())
		}
		if err := stack.Close(context.Background()); err != nil && retErr == nil {
			retErr = err
		}
	}()

	gateway, err := waitForGateway(ctx, stack.GatewayAddress(ctx), options.Scenario.TimeoutMs)
	if err != nil {
		return Report{}, err
	}
	defer gateway.Close()

	return Run(ctx, Options{
		ScenarioName: options.ScenarioName,
		Scenario:     options.Scenario,
		Gateway:      gateway,
		Clock:        model.RealClock{},
		PollInterval: options.PollInterval,
		Observer:     options.Observer,
	})
}

type localStack struct {
	repoRoot string
	project  string
	compose  string
	stdout   io.Writer
	stderr   io.Writer
	exec     commandExecutor
	baseEnv  []string
	started  bool
}

type commandExecutor interface {
	Run(ctx context.Context, directory string, env []string, stdin io.Reader, stdout, stderr io.Writer, name string, args ...string) error
	Output(ctx context.Context, directory string, env []string, name string, args ...string) ([]byte, error)
}

type osExecutor struct{}

func (osExecutor) Run(ctx context.Context, directory string, env []string, stdin io.Reader, stdout, stderr io.Writer, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	command.Env = env
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	return command.Run()
}

func (osExecutor) Output(ctx context.Context, directory string, env []string, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	command.Env = env
	return command.Output()
}

func newLocalStack(repoRoot, profileDir string, stdout, stderr io.Writer) *localStack {
	project := fmt.Sprintf("sqsim-%x-%x", os.Getpid(), time.Now().UnixNano())
	return &localStack{
		repoRoot: repoRoot,
		project:  project,
		compose:  filepath.Join(repoRoot, "service/submitqueue/docker-compose.yml"),
		stdout:   stdout,
		stderr:   stderr,
		exec:     osExecutor{},
		baseEnv: append(os.Environ(),
			"REPO_ROOT="+repoRoot,
			"SQSIM_PROFILE_DIR="+profileDir,
			"SQSIM_SCENARIO_PATH=/sqsim/profile.json",
			"QUEUE_MYSQL_MAX_OPEN_CONNECTIONS=32",
		),
	}
}

func (s *localStack) Start(ctx context.Context) error {
	fmt.Fprintf(s.stdout, "Building local SubmitQueue binaries...\n")
	if err := s.exec.Run(ctx, s.repoRoot, s.baseEnv, nil, s.stdout, s.stderr, "make",
		"build-submitqueue-gateway-linux",
		"build-submitqueue-orchestrator-linux",
		"build-runway-linux",
	); err != nil {
		return fmt.Errorf("build local binaries: %w", err)
	}

	fmt.Fprintf(s.stdout, "Starting fresh stack %s...\n", s.project)
	if err := s.composeRun(ctx, nil, s.stdout, s.stderr, "up", "-d", "--wait", "mysql-app", "mysql-queue"); err != nil {
		return fmt.Errorf("start databases: %w", err)
	}
	s.started = true
	if err := s.applySchemas(ctx); err != nil {
		return err
	}
	if err := s.composeRun(ctx, nil, s.stdout, s.stderr, "up", "-d", "--build", "gateway-service", "orchestrator-service", "runway-service"); err != nil {
		return fmt.Errorf("start services: %w", err)
	}
	return nil
}

func (s *localStack) applySchemas(ctx context.Context) error {
	groups := []struct {
		service string
		dirs    []string
	}{
		{
			service: "mysql-app",
			dirs: []string{
				"submitqueue/extension/storage/mysql/schema",
				"platform/extension/counter/mysql/schema",
			},
		},
		{
			service: "mysql-queue",
			dirs:    []string{"platform/extension/messagequeue/mysql/schema"},
		},
	}
	for _, group := range groups {
		for _, directory := range group.dirs {
			files, err := filepath.Glob(filepath.Join(s.repoRoot, directory, "*.sql"))
			if err != nil {
				return fmt.Errorf("list schema files: %w", err)
			}
			sort.Strings(files)
			if len(files) == 0 {
				return fmt.Errorf("no schema files found in %s", directory)
			}
			for _, path := range files {
				content, err := os.ReadFile(path)
				if err != nil {
					return fmt.Errorf("read schema %s: %w", path, err)
				}
				fmt.Fprintf(s.stdout, "Applying %s to %s...\n", filepath.Base(path), group.service)
				if err := s.composeRun(ctx, bytes.NewReader(content), s.stdout, s.stderr,
					"exec", "-T", group.service, "mysql", "-uroot", "-proot", "submitqueue",
				); err != nil {
					return fmt.Errorf("apply schema %s: %w", path, err)
				}
			}
		}
	}
	return nil
}

func (s *localStack) GatewayAddress(ctx context.Context) string {
	output, err := s.composeOutput(ctx, "port", "gateway-service", "8080")
	if err != nil {
		return ""
	}
	port, err := parsePort(string(output))
	if err != nil {
		return ""
	}
	return "localhost:" + strconv.Itoa(port)
}

func (s *localStack) Logs(ctx context.Context) {
	if !s.started {
		return
	}
	_ = s.composeRun(ctx, nil, s.stderr, s.stderr, "logs", "--no-color")
}

func (s *localStack) Close(ctx context.Context) error {
	if !s.started {
		return nil
	}
	s.started = false
	if err := s.composeRun(ctx, nil, s.stdout, s.stderr, "down", "-v", "--rmi", "local"); err != nil {
		return fmt.Errorf("tear down stack %s: %w", s.project, err)
	}
	return nil
}

func (s *localStack) composeRun(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) error {
	base := []string{"compose", "-f", s.compose, "-p", s.project}
	return s.exec.Run(ctx, s.repoRoot, s.baseEnv, stdin, stdout, stderr, "docker", append(base, args...)...)
}

func (s *localStack) composeOutput(ctx context.Context, args ...string) ([]byte, error) {
	base := []string{"compose", "-f", s.compose, "-p", s.project}
	return s.exec.Output(ctx, s.repoRoot, s.baseEnv, "docker", append(base, args...)...)
}

func waitForGateway(ctx context.Context, address string, timeoutMs int64) (*GRPCGateway, error) {
	if address == "" {
		return nil, fmt.Errorf("gateway port is unavailable")
	}
	gateway, err := NewGRPCGateway(address)
	if err != nil {
		return nil, err
	}
	deadline := time.NewTimer(min(time.Duration(timeoutMs)*time.Millisecond, 30*time.Second))
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := gateway.Ping(pingCtx)
		cancel()
		if err == nil {
			return gateway, nil
		}
		select {
		case <-ctx.Done():
			gateway.Close()
			return nil, ctx.Err()
		case <-deadline.C:
			gateway.Close()
			return nil, fmt.Errorf("gateway %s did not become ready", address)
		case <-ticker.C:
		}
	}
}

func parsePort(output string) (int, error) {
	value := strings.TrimSpace(output)
	index := strings.LastIndex(value, ":")
	if index < 0 {
		return 0, fmt.Errorf("invalid port output %q", value)
	}
	port, err := strconv.Atoi(value[index+1:])
	if err != nil {
		return 0, fmt.Errorf("invalid port output %q: %w", value, err)
	}
	return port, nil
}

func findRepoRoot(ctx context.Context) (string, error) {
	for _, key := range []string{"REPO_ROOT", "BUILD_WORKSPACE_DIRECTORY"} {
		if root := os.Getenv(key); root != "" {
			return root, nil
		}
	}
	command := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("find repository root: %w", err)
	}
	root := strings.TrimSpace(string(output))
	if root == "" {
		return "", fmt.Errorf("repository root is empty")
	}
	return root, nil
}
