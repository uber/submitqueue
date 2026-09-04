// Copyright (c) 2025 Uber Technologies, Inc.
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

package git

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	landstrategypb "github.com/uber/submitqueue/api/base/landstrategy/protopb"
)

// uriPR builds a change URI for a specific pull request number, so a test can
// carry several distinct changes in one request.
func uriPR(n int, sha string) string {
	return fmt.Sprintf("github://github.example.com/uber/submitqueue/pull/%d/%s", n, sha)
}

// branchSHA returns the SHA at refs/heads/<branch> on the bare remote, and
// whether that branch exists at all.
func (f gitFixture) branchSHA(t *testing.T, branch string) (string, bool) {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "--verify", "refs/heads/"+branch)
	cmd.Dir = f.remoteDir
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

// landerWithHeadBranchUpdates builds a lander that moves change head branches
// after a committing land.
func (f gitFixture) landerWithHeadBranchUpdates(t *testing.T, strategy landstrategypb.Strategy) *gitLander {
	t.Helper()
	m := f.newLanderWith(t, func(p *Params) {
		p.DefaultStrategy = strategy
		p.UpdateHeadBranch = true
	})
	return m.(*gitLander)
}

func TestLand_Rebase_MovesHeadBranchToLandedCommit(t *testing.T) {
	// The rebased commit is a new object, so nothing on the target names the
	// change's original head. Moving the branch to the commit the change became
	// is what lets a provider see it as merged.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	res, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)

	landed := res.GetSteps()[0].GetOutputs()[0].GetId()
	assert.NotEqual(t, head, landed, "rebase should have produced a new commit")
	assert.Equal(t, landed, f.remoteHEAD(t))

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, landed, got)
}

func TestLand_HeadBranchUntouchedWhenDisabled(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")

	m := f.newLander(t, landstrategypb.Strategy_REBASE)
	_, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, head, got, "branch must not move unless the deployment opted in")
}

func TestLand_SquashRebase_MovesHeadBranchToSquashedCommit(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/sq",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
	)

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_SQUASH_REBASE)
	res, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_SQUASH_REBASE, "s1", uri(head))))
	require.NoError(t, err)

	outputs := res.GetSteps()[0].GetOutputs()
	require.Len(t, outputs, 1, "squash collapses the change to one commit")
	squashed := outputs[0].GetId()

	got, ok := f.branchSHA(t, "feature/sq")
	require.True(t, ok)
	assert.Equal(t, squashed, got)
}

func TestLand_Rebase_MovesEachHeadBranchOfAStack(t *testing.T) {
	// Each change in a stack keeps its own identity on the target, so each head
	// branch has to land on its own commit rather than all on the final tip.
	f := setupGitFixture(t)
	first := f.pushPRCommit(t, "feature/one", "one.txt", "one\n", "add one")
	second := f.pushPRCommit(t, "feature/two", "two.txt", "two\n", "add two")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	res, err := m.Land(context.Background(), req("b",
		stepOf(landstrategypb.Strategy_REBASE, "s1", uriPR(1, first), uriPR(2, second)),
	))
	require.NoError(t, err)

	outputs := res.GetSteps()[0].GetOutputs()
	require.Len(t, outputs, 2)
	landedFirst, landedSecond := outputs[0].GetId(), outputs[1].GetId()

	gotFirst, ok := f.branchSHA(t, "feature/one")
	require.True(t, ok)
	assert.Equal(t, landedFirst, gotFirst)

	gotSecond, ok := f.branchSHA(t, "feature/two")
	require.True(t, ok)
	assert.Equal(t, landedSecond, gotSecond)

	assert.NotEqual(t, gotFirst, gotSecond, "each change lands on its own commit")
	assert.Equal(t, landedSecond, f.remoteHEAD(t))
}

func TestLand_HeadBranchSkippedForForkChange(t *testing.T) {
	// A change proposed from a fork has no branch in this repository — only the
	// provider's read-only change ref. The land must still succeed, and nothing
	// here is ours to move.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/forked", "f.txt", "f\n", "add f")
	f.publishPRRef(t, 1, head)
	mustGit(t, f.authorDir, "push", "origin", "--delete", "feature/forked")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	res, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)

	assert.Equal(t, res.GetSteps()[0].GetOutputs()[0].GetId(), f.remoteHEAD(t))
	_, ok := f.branchSHA(t, "feature/forked")
	assert.False(t, ok, "no branch should be resurrected for a fork change")
}

func TestLand_HeadBranchSkippedWhenAmbiguous(t *testing.T) {
	// Two branches sit on the change's head and the URI does not say which one
	// it was proposed from. Rewriting a guess could clobber an unrelated branch.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	mustGit(t, f.authorDir, "push", "origin", head+":refs/heads/feature/a-copy")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	_, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)

	for _, branch := range []string{"feature/a", "feature/a-copy"} {
		got, ok := f.branchSHA(t, branch)
		require.True(t, ok)
		assert.Equal(t, head, got, "%s must be left alone", branch)
	}
}

func TestLand_Merge_LeavesHeadBranchAlone(t *testing.T) {
	// MERGE keeps the change's own head reachable from the target through
	// second-parent history, so the branch already satisfies the provider.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_MERGE)
	_, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_MERGE, "s1", uri(head))))
	require.NoError(t, err)

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, head, got)
}

func TestLand_HeadBranchUntouchedForAlreadyLandedChange(t *testing.T) {
	// The change contributed no commits, so there is nothing for its branch to
	// move to. Redelivery must not disturb it.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	f.advanceMain(t, head)

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	res, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, head, got)
}

func TestCheckLandability_DoesNotMoveHeadBranch(t *testing.T) {
	// A dry run commits nothing, so it has nothing to point a branch at.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	mainBefore := f.remoteHEAD(t)

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	_, err := m.CheckLandability(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)

	assert.Equal(t, mainBefore, f.remoteHEAD(t))
	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, head, got)
}

func TestUpdateHeadBranchFor_StaleLeaseFailsAndLeavesBranchAlone(t *testing.T) {
	// The guard against the window between reading the remote's branches and
	// pushing: if the author moved the branch in between, the lease must refuse
	// rather than discard their push. Driven directly, since the race is not
	// reproducible through Land.
	f := setupGitFixture(t)
	pinned := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	moved := f.pushPRCommit(t, "feature/a", "a.txt", "a2\n", "author pushed again")
	require.NotEqual(t, pinned, moved)

	landed := f.pushPRCommit(t, "feature/other", "z.txt", "z\n", "some landed commit")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	// A stale view of the remote: it claims feature/a is still at the pinned
	// SHA, which is exactly what a racing push invalidates.
	stale := map[string][]string{pinned: {"refs/heads/feature/a"}}
	err := m.updateHeadBranchFor(context.Background(),
		headUpdate{ref: changeRef{SHA: pinned, Label: "uber/submitqueue#1"}, newSHA: landed},
		stale,
		nil,
	)
	require.Error(t, err, "a refused lease must fail the land, not be swallowed")

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, moved, got, "the author's push must survive")
}

func TestLand_HeadBranchMovesBeforeTheTargetIsPushed(t *testing.T) {
	// Ordering is the whole mechanism: a provider compares a change's recorded
	// head against the target push while processing it, so the head has to be
	// recorded first. Observed by failing the target push and checking the head
	// branch moved anyway — which can only be true if it moved first.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	mainBefore := f.remoteHEAD(t)
	race := f.pushPRCommit(t, "race", "race.txt", "race\n", "race commit")
	f.installRaceHook(t, []string{race})

	m := f.newLanderWith(t, func(p *Params) {
		p.DefaultStrategy = landstrategypb.Strategy_REBASE
		p.UpdateHeadBranch = true
		p.MaxPushAttempts = 1
	})
	_, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.Error(t, err, "the target push was rejected, so the land failed")

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.NotEqual(t, head, got, "the head branch moved before the target push was attempted")
	assert.NotEqual(t, mainBefore, f.remoteHEAD(t), "the hook moved the target out from under us")
}

func TestLand_HeadBranchMovedAgainAfterTargetContention(t *testing.T) {
	// An attempt that moved the branch and then lost the target push leaves it on
	// a commit that never landed. The retry has to move it on to the commit that
	// did — it can no longer be found by matching the SHA the URI pinned.
	f := setupGitFixture(t)
	race := f.pushPRCommit(t, "race", "race.txt", "race\n", "race commit")
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	f.installRaceHook(t, []string{race})

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	res, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Equal(t, 2, f.hookInvocations(t), "first target push rejected, second allowed through")

	landed := res.GetSteps()[0].GetOutputs()[0].GetId()
	assert.Equal(t, landed, f.remoteHEAD(t))

	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, landed, got, "the branch follows the commit that actually landed")
}

func TestLand_HeadBranchPushFailureFailsTheLand(t *testing.T) {
	// Landing a change while knowing its head could not be moved produces exactly
	// the half-landed state the option exists to prevent, so the land stops
	// before the target is pushed rather than completing into it.
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	mainBefore := f.remoteHEAD(t)
	f.installRefRejectHook(t, "refs/heads/feature/a")

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	_, err := m.Land(context.Background(), req("b", stepOf(landstrategypb.Strategy_REBASE, "s1", uri(head))))
	require.Error(t, err)

	assert.Equal(t, mainBefore, f.remoteHEAD(t), "the target must not have been pushed")
	got, ok := f.branchSHA(t, "feature/a")
	require.True(t, ok)
	assert.Equal(t, head, got)
}

func TestRemoteBranchTips(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	mustGit(t, f.authorDir, "push", "origin", head+":refs/heads/feature/a-copy")
	f.publishPRRef(t, 7, head)

	m := f.landerWithHeadBranchUpdates(t, landstrategypb.Strategy_REBASE)
	tips, err := m.remoteBranchTips(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t,
		[]string{"refs/heads/feature/a", "refs/heads/feature/a-copy"},
		tips[head],
		"both branches at the head are reported, and the read-only change ref is not a branch",
	)

	// The target is excluded: a change whose head happened to equal the target
	// tip would otherwise make the lander rewrite the branch it just landed on.
	for sha, refs := range tips {
		assert.NotContains(t, refs, "refs/heads/main", "target must never be a candidate (sha %s)", sha)
	}
}
