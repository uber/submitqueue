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

package main

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/sqsim/runner"
	"github.com/uber/submitqueue/sqsim/tui"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantOutput string
	}{
		{name: "list", args: []string{"list"}, wantCode: 0, wantOutput: "happy-path\n"},
		{name: "validate", args: []string{"validate", "happy-path"}, wantCode: 0, wantOutput: "happy-path is valid\n"},
		{name: "unknown scenario", args: []string{"validate", "missing"}, wantCode: 1},
		{name: "unknown command", args: []string{"missing"}, wantCode: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := run(tt.args, &bytes.Buffer{}, &stdout, &stderr)
			assert.Equal(t, tt.wantCode, code)
			if tt.wantOutput != "" {
				assert.Equal(t, tt.wantOutput, stdout.String())
			}
		})
	}
}

func TestRunScenarioCommandExitCodes(t *testing.T) {
	original := runLocal
	t.Cleanup(func() { runLocal = original })

	tests := []struct {
		name   string
		report runner.Report
		err    error
		want   int
	}{
		{name: "success", report: runner.Report{Passed: true}, want: 0},
		{name: "expectation failure", report: runner.Report{Passed: false}, want: 1},
		{name: "infrastructure failure", err: assert.AnError, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runLocal = func(context.Context, runner.LocalOptions) (runner.Report, error) {
				return tt.report, tt.err
			}
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			assert.Equal(t, tt.want, runScenarioCommand([]string{"happy-path", "--headless"}, &bytes.Buffer{}, &stdout, &stderr))
		})
	}
}

func TestRunScenarioCommandUsesInteractiveTUIByDefault(t *testing.T) {
	originalLocal := runLocal
	originalTUI := runTUI
	t.Cleanup(func() {
		runLocal = originalLocal
		runTUI = originalTUI
	})

	runTUI = func(_ context.Context, scenario string, _ io.Reader, _ io.Writer, execute tui.Execute) (runner.Report, error) {
		assert.Equal(t, "happy-path", scenario)
		return runner.Report{Passed: true}, nil
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	assert.Equal(t, 0, runScenarioCommand([]string{"happy-path"}, &bytes.Buffer{}, &stdout, &stderr))
}
