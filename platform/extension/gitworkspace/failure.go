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

package gitworkspace

import (
	"context"
	"fmt"
	"strings"

	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

// CommandFailure builds the error for a command that exited non-zero, in the
// form the git error classifier recognises. The diagnostic keeps all of stderr
// and stdout, since a classifying fragment can sit behind advice text. An ended
// ctx takes precedence, so a command killed by cancellation reads as
// cancellation rather than as a git failure.
func CommandFailure(ctx context.Context, cmd Command, out Output) error {
	return gitexec.CommandFailure(ctx, cmd.Args, diagnostic(out), nil)
}

func diagnostic(out Output) string {
	var streams []string
	for _, stream := range []string{out.Stderr, out.Stdout} {
		if detail := strings.TrimSpace(stream); detail != "" {
			streams = append(streams, detail)
		}
	}
	text := fmt.Sprintf("exit %d", out.ExitCode)
	if len(streams) > 0 {
		text += ": " + strings.Join(streams, "\n")
	}
	return text
}
