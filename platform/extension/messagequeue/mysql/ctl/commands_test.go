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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/extension/messagequeue/mysql/ctl/lib"
)

func TestListCommandsRequireExplicitTenantScope(t *testing.T) {
	var store *lib.AdminStore
	jsonOut := false
	commandFactories := map[string]func() *cobra.Command{
		"list topics":  func() *cobra.Command { return newListTopicsCmd(&store, &jsonOut) },
		"list offsets": func() *cobra.Command { return newListOffsetsCmd(&store, &jsonOut) },
		"list leases":  func() *cobra.Command { return newListLeasesCmd(&store, &jsonOut) },
		"stale leases": func() *cobra.Command { return newStaleLeasesCmd(&store, &jsonOut) },
	}

	for name, commandFactory := range commandFactories {
		t.Run(name, func(t *testing.T) {
			t.Run("missing scope", func(t *testing.T) {
				cmd := commandFactory()
				cmd.SetArgs(nil)
				cmd.SilenceErrors = true
				cmd.SilenceUsage = true
				require.Error(t, cmd.Execute())
			})

			t.Run("conflicting scope", func(t *testing.T) {
				cmd := commandFactory()
				cmd.SetArgs([]string{"--tenant", "acme", "--all-tenants"})
				cmd.SilenceErrors = true
				cmd.SilenceUsage = true
				require.Error(t, cmd.Execute())
			})
		})
	}
}

func TestMessageCommandsRequirePartition(t *testing.T) {
	var store *lib.AdminStore
	jsonOut := false
	noInteractive := true
	commandFactories := map[string]func() *cobra.Command{
		"inspect": func() *cobra.Command { return newInspectMessageCmd(&store, &jsonOut) },
		"delete":  func() *cobra.Command { return newDeleteMessageCmd(&store, &noInteractive) },
		"requeue": func() *cobra.Command { return newRequeueDLQCmd(&store) },
	}

	for name, commandFactory := range commandFactories {
		t.Run(name, func(t *testing.T) {
			cmd := commandFactory()
			cmd.SetArgs([]string{"--tenant", "acme", "--topic", "orders", "--message-id", "msg-1"})
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			require.Error(t, cmd.Execute())
		})
	}
}
