#!/usr/bin/env python3
"""Create and remove one canonical worktree per agent; keeps no state, the orchestrator's ledger does."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

AGENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def git(cwd: Path, *args: str) -> str:
    result = subprocess.run(("git", *args), cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise ValueError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def sdlc_root(cwd: Path) -> Path:
    return Path(git(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir")).parent / ".sdlc"


def worktree_path(cwd: Path, agent: str) -> Path:
    if not AGENT_NAME.fullmatch(agent):
        raise ValueError("agent name must be letters, digits, '.', '_' or '-' and start with a letter or digit")
    return sdlc_root(cwd) / "worktree" / agent


def create_worktree(cwd: Path, agent: str, base: str = "origin/main", branch: str | None = None) -> Path:
    path = worktree_path(cwd, agent)
    if path.exists():
        raise ValueError(f"{path} already exists")
    if branch is not None and subprocess.run(
        ("git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"), cwd=cwd
    ).returncode == 0:
        raise ValueError(f"branch {branch} already exists")
    remote, _, remote_ref = base.partition("/")
    if remote_ref and remote in git(cwd, "remote").split():
        git(cwd, "fetch", remote, remote_ref)
    add = ("worktree", "add", "-b", branch, str(path), base) if branch else ("worktree", "add", "--detach", str(path), base)
    git(cwd, *add)
    return path


def blocking_entries(path: Path) -> list[str]:
    entries = git(path, "status", "--porcelain", "--ignored").splitlines()
    # Bazel's top-level convenience symlinks and lockfile are ignored, regenerated artifacts.
    return [
        entry
        for entry in entries
        if not (entry == "!! MODULE.bazel.lock" or (entry.startswith("!! bazel-") and "/" not in entry[3:] and (path / entry[3:]).is_symlink()))
    ]


def pushed_or_landed(path: Path) -> bool:
    if git(path, "for-each-ref", "--contains", "HEAD", "--format=%(refname)", "refs/remotes/origin"):
        return True
    # Rebase and squash merges land new commits whose patches match; the merged branch is deleted from origin.
    probe = subprocess.run(("git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"), cwd=path, capture_output=True, text=True)
    default_branch = probe.stdout.strip() if probe.returncode == 0 else "origin/main"
    return not any(line.startswith("+") for line in git(path, "cherry", default_branch, "HEAD").splitlines())


def remove_worktree(cwd: Path, agent: str) -> None:
    path = worktree_path(cwd, agent)
    registered = {Path(line.split(" ", 1)[1]).resolve() for line in git(cwd, "worktree", "list", "--porcelain").splitlines() if line.startswith("worktree ")}
    if path.resolve() not in registered:
        raise ValueError(f"{path} is not a registered worktree")
    caller = cwd.resolve()
    if caller == path.resolve() or path.resolve() in caller.parents:
        raise ValueError(f"run from outside {path}; removing the caller's own directory breaks the command")
    if blocking_entries(path):
        raise ValueError(f"{path} has uncommitted, untracked, or ignored files")
    git(path, "fetch", "--prune", "origin")
    if not pushed_or_landed(path):
        raise ValueError(f"{path} has commits that are on no origin ref and whose patches have not landed on the default branch")
    wrapper = path / "tool" / "bazel"
    if not wrapper.exists():
        raise ValueError(f"{wrapper} is missing, so the Bazel output base cannot be cleaned")
    if subprocess.run((str(wrapper), "clean", "--expunge"), cwd=path).returncode != 0:
        raise ValueError("bazel clean --expunge failed; the worktree was kept")
    branch = git(path, "branch", "--show-current")
    git(cwd, "worktree", "remove", str(path))
    if branch:
        deleted = subprocess.run(("git", "branch", "-d", branch), cwd=cwd, capture_output=True, text=True)
        if deleted.returncode != 0:
            print(f"worktree control: kept local branch {branch}: {deleted.stderr.strip()}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("CREATE")
    create.add_argument("agent")
    create.add_argument("--base", default="origin/main", help="ref to start from; a remote ref is fetched first")
    create.add_argument("--branch", help="create this new branch (default is a detached HEAD)")
    remove = commands.add_parser("REMOVE")
    remove.add_argument("agent")
    args = parser.parse_args(argv)
    cwd = Path(os.environ.get("BUILD_WORKING_DIRECTORY", "."))
    try:
        if args.command == "CREATE":
            print(create_worktree(cwd, args.agent, args.base, args.branch))
        else:
            remove_worktree(cwd, args.agent)
        return 0
    except (ValueError, OSError) as error:
        print(f"worktree control: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
