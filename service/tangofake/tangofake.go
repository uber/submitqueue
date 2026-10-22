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

// Package tangofake is a Tango server that answers GetChangedTargets from the
// contents of a git repository instead of a build graph, so a demo or a test can
// exercise the real wire protocol without a build system.
//
// A directory holding a file named TARGET is a target, labelled //<directory>
// (// for the repository root). Each non-blank line of the file that does not
// start with # is the label of a target it depends on; a dependency that names
// no target is ignored. The graph a change is read against holds the edges of
// the base and of every change head. A change affects the nearest target above
// each file it touches, at distance 0, and every target that depends on one, at
// its distance along the dependency edges. Adding the TARGET file makes a target
// new, and deleting it makes the target deleted.
//
// A change URI is resolved by the commit SHA in its last path segment, which
// both the git:// and github:// forms end in, so the repository must hold that
// commit. The request's remote and strategy are not consulted.
package tangofake

import (
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/uber/tango/tangopb"

	"github.com/uber/submitqueue/platform/extension/gitworkspace"
)

// targetFile is the name of the file that makes its directory a target.
const targetFile = "TARGET"

// Params are the dependencies of a Server.
type Params struct {
	// Workspaces opens the workspace of the repository holding the targets.
	Workspaces gitworkspace.Factory
}

type server struct {
	Params
}

// New returns a Tango server that serves GetChangedTargets from the repository
// in params. The other methods return an error.
func New(params Params) tangopb.TangoYARPCServer {
	return &server{Params: params}
}

// graph maps a target label to the labels it depends on.
type graph map[string][]string

func (s *server) GetTargetGraph(*tangopb.GetTargetGraphRequest, tangopb.TangoServiceGetTargetGraphYARPCServer) error {
	return fmt.Errorf("GetTargetGraph is not implemented by the fake server")
}

func (s *server) GetChangedTargetGraph(*tangopb.GetChangedTargetGraphRequest, tangopb.TangoServiceGetChangedTargetGraphYARPCServer) error {
	return fmt.Errorf("GetChangedTargetGraph is not implemented by the fake server")
}

func (s *server) GetChangedTargets(request *tangopb.GetChangedTargetsRequest, stream tangopb.TangoServiceGetChangedTargetsYARPCServer) error {
	base := request.GetSecondRevision().GetBaseSha()
	if base == "" {
		return fmt.Errorf("second revision has no base_sha")
	}
	var heads []string
	for _, r := range request.GetSecondRevision().GetRequests() {
		heads = append(heads, r.GetUrl())
	}

	workspace, err := s.Workspaces.For(stream.Context(), "")
	if err != nil {
		return fmt.Errorf("failed to open a workspace: %w", err)
	}
	defer workspace.Close()

	changed, err := changedTargets(workspace, base, heads)
	if err != nil {
		return err
	}

	ids := make(map[string]int32, len(changed.labels))
	for i, label := range changed.labels {
		ids[label] = int32(i + 1)
	}
	targets := make([]*tangopb.ChangedTarget, 0, len(changed.labels))
	mapping := make(map[int32]string, len(changed.labels))
	for _, label := range changed.labels {
		mapping[ids[label]] = label
		target := &tangopb.ChangedTarget{ChangeType: changed.kinds[label], Distance: changed.distances[label]}
		if node := optimized(ids, label, changed.deps); changed.kinds[label] != tangopb.CHANGE_TYPE_DELETED {
			target.NewTarget = node
		}
		if changed.kinds[label] != tangopb.CHANGE_TYPE_NEW {
			target.OldTarget = optimized(ids, label, changed.deps)
		}
		targets = append(targets, target)
	}

	if err := stream.Send(&tangopb.GetChangedTargetsResponse{
		Item: &tangopb.GetChangedTargetsResponse_ChangedTargets{
			ChangedTargets: &tangopb.ChangedTargets{ChangedTargets: targets},
		},
	}); err != nil {
		return fmt.Errorf("failed to send changed targets: %w", err)
	}
	return stream.Send(&tangopb.GetChangedTargetsResponse{
		Item: &tangopb.GetChangedTargetsResponse_Metadata{
			Metadata: &tangopb.Metadata{TargetIdMapping: mapping},
		},
	})
}

func optimized(ids map[string]int32, label string, deps graph) *tangopb.OptimizedTarget {
	node := &tangopb.OptimizedTarget{Id: ids[label]}
	for _, dep := range deps[label] {
		if id, ok := ids[dep]; ok {
			node.DirectDependencies = append(node.DirectDependencies, id)
		}
	}
	return node
}

type changeSet struct {
	// labels lists the changed targets in sorted order.
	labels    []string
	kinds     map[string]tangopb.ChangeType
	distances map[string]int32
	deps      graph
}

// changedTargets computes the targets that the changes, each named by a URI
// ending in its head commit, affect on top of base.
func changedTargets(workspace gitworkspace.Workspace, base string, changeURIs []string) (changeSet, error) {
	union, err := readGraph(workspace, base)
	if err != nil {
		return changeSet{}, err
	}

	kinds := map[string]tangopb.ChangeType{}
	var touched []touchedFile
	for _, uri := range changeURIs {
		head, err := commitOf(uri)
		if err != nil {
			return changeSet{}, err
		}
		headGraph, err := readGraph(workspace, head)
		if err != nil {
			return changeSet{}, err
		}
		for label, deps := range headGraph {
			if _, existed := union[label]; !existed {
				kinds[label] = tangopb.CHANGE_TYPE_NEW
			}
			union[label] = withDeps(union[label], deps)
		}
		files, err := changedFiles(workspace, base, head)
		if err != nil {
			return changeSet{}, err
		}
		touched = append(touched, files...)
	}

	for _, file := range touched {
		if path.Base(file.path) == targetFile && file.deleted {
			label := labelOf(path.Dir(file.path))
			if _, ok := union[label]; ok && kinds[label] != tangopb.CHANGE_TYPE_NEW {
				kinds[label] = tangopb.CHANGE_TYPE_DELETED
			}
		}
	}
	for _, file := range touched {
		if label, ok := owner(union, file.path); ok {
			if _, set := kinds[label]; !set {
				kinds[label] = tangopb.CHANGE_TYPE_CHANGED
			}
		}
	}

	distances := map[string]int32{}
	var frontier []string
	for label := range kinds {
		distances[label] = 0
		frontier = append(frontier, label)
	}
	dependents := map[string][]string{}
	for label, deps := range union {
		for _, dep := range deps {
			dependents[dep] = append(dependents[dep], label)
		}
	}
	for distance := int32(1); len(frontier) > 0; distance++ {
		var next []string
		for _, label := range frontier {
			for _, dependent := range dependents[label] {
				if _, seen := distances[dependent]; seen {
					continue
				}
				distances[dependent] = distance
				kinds[dependent] = tangopb.CHANGE_TYPE_CHANGED
				next = append(next, dependent)
			}
		}
		frontier = next
	}

	labels := make([]string, 0, len(distances))
	for label := range distances {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return changeSet{labels: labels, kinds: kinds, distances: distances, deps: union}, nil
}

// withDeps returns deps plus each of extra it does not already hold, so an edge
// that only the base or only a change has still carries a change to its target.
func withDeps(deps, extra []string) []string {
	merged := append([]string(nil), deps...)
	for _, dep := range extra {
		if !slices.Contains(merged, dep) {
			merged = append(merged, dep)
		}
	}
	return merged
}

type touchedFile struct {
	path    string
	deleted bool
}

// owner returns the label of the nearest target at or above file's directory.
func owner(targets graph, file string) (string, bool) {
	for dir := path.Dir(file); ; dir = path.Dir(dir) {
		label := labelOf(dir)
		if _, ok := targets[label]; ok {
			return label, true
		}
		if dir == "." {
			return "", false
		}
	}
}

func labelOf(dir string) string {
	if dir == "." {
		return "//"
	}
	return "//" + dir
}

func commitOf(changeURI string) (string, error) {
	sha := changeURI[strings.LastIndex(changeURI, "/")+1:]
	if len(sha) != 40 && len(sha) != 64 {
		return "", fmt.Errorf("change URI %q does not end in a full commit SHA", changeURI)
	}
	for _, c := range sha {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("change URI %q does not end in a full commit SHA", changeURI)
		}
	}
	return sha, nil
}

func run(workspace gitworkspace.Workspace, commands []gitworkspace.Command) ([]gitworkspace.Output, error) {
	outputs, err := workspace.Exec(commands)
	if err != nil {
		return nil, fmt.Errorf("failed to run git: %w", err)
	}
	for _, out := range outputs {
		if out.ExitCode != 0 {
			return nil, fmt.Errorf("git %s failed with exit code %d: %s", out.Alias, out.ExitCode, strings.TrimSpace(out.Stderr))
		}
	}
	return outputs, nil
}

// readGraph returns the targets of the tree at commit.
func readGraph(workspace gitworkspace.Workspace, commit string) (graph, error) {
	listing, err := run(workspace, []gitworkspace.Command{
		{Alias: "ls-tree", Bin: "git", Args: []string{"ls-tree", "-r", "--name-only", commit}},
	})
	if err != nil {
		return nil, err
	}
	var commands []gitworkspace.Command
	var dirs []string
	for _, file := range strings.Split(listing[0].Stdout, "\n") {
		if file != "" && path.Base(file) == targetFile {
			dirs = append(dirs, path.Dir(file))
			commands = append(commands, gitworkspace.Command{
				Alias: file, Bin: "git", Args: []string{"show", commit + ":" + file},
			})
		}
	}

	targets := graph{}
	if len(commands) == 0 {
		return targets, nil
	}
	outputs, err := run(workspace, commands)
	if err != nil {
		return nil, err
	}
	for i, out := range outputs {
		var deps []string
		for _, line := range strings.Split(out.Stdout, "\n") {
			if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
				deps = append(deps, line)
			}
		}
		targets[labelOf(dirs[i])] = deps
	}
	for label, deps := range targets {
		known := deps[:0]
		for _, dep := range deps {
			if _, ok := targets[dep]; ok {
				known = append(known, dep)
			}
		}
		targets[label] = known
	}
	return targets, nil
}

// changedFiles lists the files head changes relative to where it diverged from
// base.
func changedFiles(workspace gitworkspace.Workspace, base, head string) ([]touchedFile, error) {
	outputs, err := run(workspace, []gitworkspace.Command{
		{Alias: "diff", Bin: "git", Args: []string{"diff", "--name-status", "--no-renames", base + "..." + head}},
	})
	if err != nil {
		return nil, err
	}
	var files []touchedFile
	for _, line := range strings.Split(outputs[0].Stdout, "\n") {
		status, file, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		files = append(files, touchedFile{path: file, deleted: status == "D"})
	}
	return files, nil
}
