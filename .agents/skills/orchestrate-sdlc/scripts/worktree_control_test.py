#!/usr/bin/env python3
"""Behavioral tests for canonical worktree creation and guarded removal."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from worktree_control import create_worktree, remove_worktree, worktree_path

GIT = ("git", "-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false")
FAKE_BAZEL = '#!/bin/sh\necho "$(pwd -P) $@" >> "$FAKE_BAZEL_LOG"\nexit "${FAKE_BAZEL_EXIT:-0}"\n'


def run(cwd, *args):
    return subprocess.run((*GIT, *args), cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class WorktreeControlTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name).resolve()
        self.remote = base / "remote.git"
        self.repo = base / "repo"
        self.bazel_log = base / "bazel.log"
        run(base, "init", "--bare", "-b", "main", str(self.remote))
        run(base, "init", "-b", "main", str(self.repo))
        (self.repo / "file.txt").write_text("one\n")
        (self.repo / ".gitignore").write_text("ignored.txt\nMODULE.bazel.lock\n/bazel-*\n")
        (self.repo / "tool").mkdir()
        (self.repo / "tool" / "bazel").write_text(FAKE_BAZEL)
        (self.repo / "tool" / "bazel").chmod(0o755)
        run(self.repo, "add", ".")
        run(self.repo, "commit", "-m", "initial")
        run(self.repo, "remote", "add", "origin", str(self.remote))
        run(self.repo, "push", "origin", "main")
        patcher = mock.patch.dict(os.environ, {"FAKE_BAZEL_LOG": str(self.bazel_log), "FAKE_BAZEL_EXIT": "0"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def assert_kept(self, path):
        self.assertTrue(path.exists())
        self.assertIn(str(path), run(self.repo, "worktree", "list"))
        self.assertFalse(self.bazel_log.exists())

    def test_create_resolves_the_same_path_from_a_linked_worktree(self):
        first = create_worktree(self.repo, "alpha", branch="topic/alpha")
        self.assertEqual(first, self.repo / ".sdlc" / "worktree" / "alpha")
        self.assertEqual(run(first, "branch", "--show-current"), "topic/alpha")
        second = create_worktree(first, "beta")
        self.assertEqual(second, self.repo / ".sdlc" / "worktree" / "beta")
        self.assertEqual(run(second, "branch", "--show-current"), "")

    def test_create_refuses_existing_path_branch_and_bad_names(self):
        create_worktree(self.repo, "alpha", branch="topic/alpha")
        for agent, branch in (("alpha", None), ("beta", "topic/alpha"), ("../escape", None), ("", None)):
            with self.subTest(agent=agent, branch=branch), self.assertRaises(ValueError):
                create_worktree(self.repo, agent, branch=branch)
        self.assertFalse((self.repo / ".sdlc" / "worktree" / "beta").exists())

    def test_create_fetches_the_remote_base_first(self):
        other = self.repo.parent / "other"
        run(self.repo.parent, "clone", str(self.remote), str(other))
        (other / "file.txt").write_text("two\n")
        run(other, "commit", "-am", "second")
        run(other, "push", "origin", "main")
        path = create_worktree(self.repo, "alpha")
        self.assertEqual(run(path, "rev-parse", "HEAD"), run(other, "rev-parse", "HEAD"))

    def test_remove_cleans_bazel_and_deletes_a_pushed_branch_worktree_and_local_branch(self):
        path = create_worktree(self.repo, "alpha", branch="topic/alpha")
        (path / "file.txt").write_text("changed\n")
        run(path, "commit", "-am", "work")
        run(path, "push", "-u", "origin", "topic/alpha")
        remove_worktree(self.repo, "alpha")
        self.assertFalse(path.exists())
        self.assertIn(f"{path} clean --expunge", self.bazel_log.read_text())
        self.assertEqual(run(self.repo, "branch", "--list", "topic/alpha"), "")
        self.assertEqual(create_worktree(self.repo, "alpha", branch="topic/alpha"), path)

    def test_remove_accepts_a_detached_worktree_only_at_a_remote_commit(self):
        path = create_worktree(self.repo, "alpha")
        remove_worktree(self.repo, "alpha")
        self.assertFalse(path.exists())
        self.bazel_log.unlink()
        path = create_worktree(self.repo, "beta")
        (path / "file.txt").write_text("local\n")
        run(path, "commit", "-am", "local only")
        with self.assertRaises(ValueError):
            remove_worktree(self.repo, "beta")
        self.assert_kept(path)

    def test_remove_refuses_a_branch_with_no_upstream_or_unpushed_commits(self):
        path = create_worktree(self.repo, "alpha", branch="topic/alpha")
        (path / "file.txt").write_text("changed\n")
        run(path, "commit", "-am", "local only")
        with self.assertRaises(ValueError):
            remove_worktree(self.repo, "alpha")
        run(path, "push", "-u", "origin", "topic/alpha")
        (path / "file.txt").write_text("again\n")
        run(path, "commit", "-am", "second local commit")
        with self.assertRaises(ValueError):
            remove_worktree(self.repo, "alpha")
        self.assertIn(str(path), run(self.repo, "worktree", "list"))

    def test_remove_refuses_when_the_remote_branch_was_deleted_after_the_push(self):
        path = create_worktree(self.repo, "alpha", branch="topic/alpha")
        (path / "file.txt").write_text("changed\n")
        run(path, "commit", "-am", "work")
        run(path, "push", "-u", "origin", "topic/alpha")
        run(self.repo, "push", "origin", "--delete", "topic/alpha")
        with self.assertRaises(ValueError):
            remove_worktree(self.repo, "alpha")
        self.assert_kept(path)

    def land_on_main_and_delete_remote_branch(self, branch, *, squash=False):
        other = self.repo.parent / f"other-{branch.replace('/', '-')}"
        run(self.repo.parent, "clone", "-q", str(self.remote), str(other))
        if squash:
            run(other, "merge", "--squash", f"origin/{branch}")
            run(other, "commit", "-m", f"squash {branch}")
        else:
            commits = run(other, "rev-list", "--reverse", f"origin/main..origin/{branch}").split()
            run(other, "cherry-pick", *commits)
        run(other, "push", "origin", "main")
        run(other, "push", "origin", "--delete", branch)

    def push_branch_commits(self, path, branch, count):
        for index in range(count):
            (path / f"change{index}.txt").write_text(f"{branch} {index}\n")
            run(path, "add", ".")
            run(path, "commit", "-m", f"{branch} {index}")
        run(path, "push", "-u", "origin", branch)

    def test_remove_accepts_work_landed_by_rebase_or_single_commit_squash(self):
        for name, count, squash in (("rebased", 2, False), ("squashed", 1, True)):
            with self.subTest(name=name):
                path = create_worktree(self.repo, name, branch=f"topic/{name}")
                self.push_branch_commits(path, f"topic/{name}", count)
                self.land_on_main_and_delete_remote_branch(f"topic/{name}", squash=squash)
                remove_worktree(self.repo, name)
                self.assertFalse(path.exists())

    def test_remove_refuses_work_that_was_not_landed_or_cannot_be_proven(self):
        for name, count, land in (("unlanded", 1, False), ("multisquash", 2, True)):
            with self.subTest(name=name):
                path = create_worktree(self.repo, name, branch=f"topic/{name}")
                self.push_branch_commits(path, f"topic/{name}", count)
                if land:
                    self.land_on_main_and_delete_remote_branch(f"topic/{name}", squash=True)
                else:
                    run(self.repo, "push", "origin", "--delete", f"topic/{name}")
                with self.assertRaises(ValueError):
                    remove_worktree(self.repo, name)
                self.assertTrue(path.exists())

    def test_remove_refuses_when_the_caller_is_inside_the_worktree(self):
        path = create_worktree(self.repo, "alpha")
        (path / "subdir").mkdir()
        for caller in (path, path / "subdir"):
            with self.subTest(caller=caller), self.assertRaises(ValueError):
                remove_worktree(caller, "alpha")
        self.assertTrue(path.exists())

    def test_remove_refuses_dirty_untracked_and_ignored_files(self):
        for name in ("scratch.txt", "ignored.txt"):
            with self.subTest(name=name):
                path = create_worktree(self.repo, "alpha")
                (path / name).write_text("x")
                with self.assertRaises(ValueError):
                    remove_worktree(self.repo, "alpha")
                self.assert_kept(path)
                (path / name).unlink()
                remove_worktree(self.repo, "alpha")
                self.bazel_log.unlink()
        path = create_worktree(self.repo, "alpha")
        (path / "file.txt").write_text("edited\n")
        with self.assertRaises(ValueError):
            remove_worktree(self.repo, "alpha")
        self.assert_kept(path)

    def test_remove_allows_bazel_generated_ignored_artifacts(self):
        path = create_worktree(self.repo, "alpha")
        (path / "MODULE.bazel.lock").write_text("{}")
        (path / "bazel-bin").symlink_to(self.repo.parent)
        (path / "bazel-out").symlink_to(self.repo.parent)
        remove_worktree(self.repo, "alpha")
        self.assertFalse(path.exists())

    def test_remove_keeps_the_worktree_when_bazel_clean_fails(self):
        path = create_worktree(self.repo, "alpha")
        with mock.patch.dict(os.environ, {"FAKE_BAZEL_EXIT": "1"}), self.assertRaises(ValueError):
            remove_worktree(self.repo, "alpha")
        self.assertTrue(path.exists())
        self.assertIn(str(path), run(self.repo, "worktree", "list"))

    def test_remove_refuses_unregistered_paths(self):
        (self.repo / ".sdlc" / "worktree" / "stray").mkdir(parents=True)
        with self.assertRaises(ValueError):
            remove_worktree(self.repo, "stray")
        self.assertTrue(worktree_path(self.repo, "stray").exists())


if __name__ == "__main__":
    unittest.main()
