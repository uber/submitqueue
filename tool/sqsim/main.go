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
	"fmt"
	"io"
	"os"

	"github.com/uber/submitqueue/sqsim/scenarios"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "list":
		if len(args) != 1 {
			printUsage(stderr)
			return 2
		}
		for _, name := range scenarios.Names() {
			fmt.Fprintln(stdout, name)
		}
		return 0
	case "validate":
		if len(args) != 2 {
			printUsage(stderr)
			return 2
		}
		if _, err := scenarios.Build(args[1]); err != nil {
			fmt.Fprintf(stderr, "invalid scenario: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "%s is valid\n", args[1])
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  sqsim list")
	fmt.Fprintln(w, "  sqsim validate <scenario>")
}
