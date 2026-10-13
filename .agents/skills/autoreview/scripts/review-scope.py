#!/usr/bin/env python3
"""Print the read-only review scope, affected packages, and candidate smells."""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys


KINDS = (
    "go",
    "tests",
    "python",
    "web",
    "proto",
    "sql",
    "build",
    "shell",
    "config",
    "generated",
    "docs",
    "other",
)
GENERATED_SUFFIXES = ("_mock.go", ".pb.go", ".pb.yarpc.go", "_pb.ts", "_pb.js", "_connect.ts")
BUILD_FILES = {
    "BUILD",
    "BUILD.bazel",
    "Dockerfile",
    "Makefile",
    "MODULE.bazel",
    "go.mod",
    "go.sum",
}
CONFIG_SUFFIXES = (".json", ".toml", ".yaml", ".yml")
WEB_SUFFIXES = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".css")
WEB_TEST_PATTERN = re.compile(r"\.(test|spec)\.[cm]?[jt]sx?$")
# Uber-internal names that must not appear in this public repository.
INTERNAL_REFERENCE_PATTERN = re.compile(
    r"uberinternal|code\.uber\.internal|\b(go|web|java)-code\b|\buber-code\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Change:
    status: str
    path: str
    old_path: str | None = None

    def display(self) -> str:
        if self.old_path:
            return f"{escape_path(self.old_path)} -> {escape_path(self.path)}"
        return escape_path(self.path)

    def package_paths(self) -> tuple[str, ...]:
        if self.old_path:
            return (self.old_path, self.path)
        return (self.path,)


def die(message: str) -> None:
    print(f"review-scope: {message}", file=sys.stderr)
    raise SystemExit(2)


def run_git(*args: str, check: bool = True) -> str:
    process = subprocess.run(
        ("git", *args),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )
    if check and process.returncode != 0:
        detail = process.stderr.strip() or "git command failed"
        die(detail)
    return process.stdout


def run_git_bytes(*args: str, check: bool = True) -> bytes:
    process = subprocess.run(
        ("git", *args),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )
    if check and process.returncode != 0:
        detail = process.stderr.decode(errors="replace").strip() or "git command failed"
        die(detail)
    return process.stdout


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="review-scope",
        description=(
            "Print the target, stated intent, changed files by kind, affected "
            "Bazel packages, and candidate smells in added lines."
        ),
    )
    modes = result.add_mutually_exclusive_group()
    modes.add_argument(
        "--base",
        metavar="REF",
        help="review HEAD and dirty work against the merge-base with REF",
    )
    modes.add_argument(
        "--range",
        metavar="REF",
        help="review committed changes from the merge-base with REF through HEAD",
    )
    modes.add_argument("--commit", metavar="REV", help="review one commit only")
    modes.add_argument(
        "--uncommitted",
        action="store_true",
        help="review staged, unstaged, and untracked files only",
    )
    return result


def is_generated(path: str) -> bool:
    parts = PurePosixPath(path).parts
    return (
        path.endswith(".go") and ("protopb" in parts or "mock" in parts)
        or path.endswith(GENERATED_SUFFIXES)
    )


def kind_of(
    path: str, repo: Path | None = None, treeish: str | None = None
) -> str:
    name = PurePosixPath(path).name
    if (
        name in BUILD_FILES
        or name.startswith(("Dockerfile.", "Makefile."))
        or path.endswith((".bzl", ".bazelrc", ".bazelignore"))
    ):
        return "build"
    if path.endswith(".sql"):
        return "sql"
    if path.endswith(".proto"):
        return "proto"
    if (
        path.endswith(("_test.go", "_test.py"))
        or name.startswith("test_") and path.endswith(".py")
        or WEB_TEST_PATTERN.search(path)
    ):
        return "tests"
    if is_generated(path):
        return "generated"
    if path.endswith(".go"):
        return "go"
    if path.endswith(".py"):
        return "python"
    if path.endswith(WEB_SUFFIXES):
        return "web"
    if path.endswith((".sh", ".bash", ".zsh")):
        return "shell"
    if path.endswith(".md") or path.startswith("doc/"):
        return "docs"
    if path.endswith(CONFIG_SUFFIXES) or name.startswith("."):
        return "config"
    if treeish:
        content = run_git("show", f"{treeish}:{path}", check=False).encode()
        first_line = content.splitlines()[:1]
        if first_line and first_line[0].startswith(b"#!"):
            if b"python" in first_line[0]:
                return "python"
            return "shell"
    elif repo:
        candidate = repo / path
        if not candidate.is_symlink() and candidate.is_file():
            first_line = candidate.read_bytes().splitlines()[:1]
            if first_line and first_line[0].startswith(b"#!"):
                if b"python" in first_line[0]:
                    return "python"
                return "shell"
    return "other"


def parse_name_status(output: bytes) -> list[Change]:
    fields = output.split(b"\0")
    changes: list[Change] = []
    index = 0
    while index < len(fields) and fields[index]:
        status = fields[index].decode("ascii")
        index += 1
        if status.startswith(("R", "C")):
            if index + 1 >= len(fields):
                die("malformed rename or copy record from git")
            old_path = os.fsdecode(fields[index])
            path = os.fsdecode(fields[index + 1])
            changes.append(Change(status[0], path, old_path))
            index += 2
        else:
            if index >= len(fields):
                die("malformed name-status record from git")
            changes.append(Change(status, os.fsdecode(fields[index])))
            index += 1
    return changes


def git_name_status(*args: str) -> list[Change]:
    return parse_name_status(
        run_git_bytes("diff", "--name-status", "--find-renames", "-z", *args)
    )


def classify_changes(
    changes: list[Change],
    repo: Path | None = None,
    treeish: str | None = None,
) -> list[tuple[str, Change]]:
    latest_changes: dict[str, Change] = {}
    for change in changes:
        latest_changes[change.path] = change

    classified: list[tuple[str, Change]] = []
    for change in latest_changes.values():
        changed_kinds = {
            kind_of(path, repo, treeish) for path in change.package_paths()
        }
        classified.extend(
            (kind, change) for kind in KINDS if kind in changed_kinds
        )
    return classified


def escape_path(path: str) -> str:
    return path.encode("unicode_escape", errors="backslashreplace").decode("ascii")


def git_diff(*args: str) -> str:
    return run_git(
        "-c",
        "core.safecrlf=false",
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--unified=0",
        *args,
    )


def bazel_package(repo: Path, path: str) -> str | None:
    directory = PurePosixPath(path).parent
    while True:
        relative = "" if str(directory) == "." else str(directory)
        candidate = repo / relative
        if (candidate / "BUILD.bazel").is_file() or (candidate / "BUILD").is_file():
            return f"//{relative}" if relative else "//"
        if not relative:
            return None
        directory = directory.parent


def narration_comment(line: str) -> bool:
    match = re.match(r"^\s*//\s*(.*)$", line)
    if not match:
        return False
    text = match.group(1)
    if re.match(r"^(Check if|Loop over)\s", text):
        return True
    opener = re.match(
        r"^(Fetch|Get|Read|Deserialize|Parse|Publish|Send|Persist|Store|Save|"
        r"Create|Build|Return)\s+(\S+)",
        text,
    )
    return bool(opener and opener.group(2) != "returns")


def formatted_log(line: str) -> bool:
    if re.search(r"\.(Infof|Debugf|Warnf|Fatalf|Panicf|DPanicf)\s*\(", line):
        return True
    return bool(re.search(r"\.Errorf\s*\(", re.sub(r"fmt\.Errorf\s*\(", "", line)))


def analyze_added_line(path: str, number: int, text: str) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    generated = is_generated(path)
    is_test = path.endswith("_test.go") and not generated
    is_go = path.endswith(".go") and not generated

    if is_test and re.search(r"time\.Sleep\s*\(", text):
        results.append(("SMELL", "sleep"))
    if (
        is_test
        and ("assert." in text or "require." in text)
        and ".Error()" in text
    ):
        results.append(("SMELL", "error-string-assert"))
    if is_test and re.match(r"^\s*package\s+[A-Za-z0-9_]+_test\s*$", text):
        results.append(("SMELL", "external-test-package"))
    if is_test and ("show-toplevel" in text or re.search(r"[Ff]indRepoRoot", text)):
        results.append(("SMELL", "repo-root"))
    if is_go and formatted_log(text):
        results.append(("SMELL", "formatted-log"))
    if path.endswith(".sql") and re.search(r"key\s+`?idx_", text, re.IGNORECASE):
        results.append(("SMELL", "secondary-index"))
    if is_go and "extension" in PurePosixPath(path).parts and "NewFactory" in text:
        results.append(("SMELL", "extension-factory"))
    if not generated and INTERNAL_REFERENCE_PATTERN.search(text):
        results.append(("SMELL", "internal-reference"))
    if is_go and narration_comment(text):
        results.append(("SMELL", "narration-comment"))
    if (
        is_go
        and not is_test
        and re.match(r"^\s*type\s+[A-Za-z_][A-Za-z0-9_]*\s+interface\s*\{", text)
    ):
        results.append(("HINT", "interface"))

    clean_text = text.replace("\t", " ")[:200]
    display_path = escape_path(path)
    return [
        (kind, f"{display_path}:{number}: {identifier}: {clean_text}")
        for kind, identifier in results
    ]


def analyze_diff(diff: str) -> list[tuple[str, str]]:
    hits: list[tuple[str, str]] = []
    path = ""
    new_line = 0
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            path = ""
        elif line.startswith("+++ "):
            path = line[4:]
            if path.startswith('"'):
                try:
                    quoted_path = ast.literal_eval(path)
                    path = os.fsdecode(quoted_path.encode("latin1"))
                except (SyntaxError, UnicodeEncodeError, ValueError):
                    path = path[1:-1]
            if path.startswith("b/"):
                path = path[2:]
        elif line.startswith("@@ "):
            match = re.search(r"\+(\d+)", line)
            if match:
                new_line = int(match.group(1))
        elif line.startswith("+") and not line.startswith("+++"):
            if path and path != "/dev/null":
                hits.extend(analyze_added_line(path, new_line, line[1:]))
            new_line += 1
        elif line.startswith(" "):
            new_line += 1
    return hits


def read_untracked(repo: Path, path: str) -> list[tuple[str, str]]:
    candidate = repo / path
    if candidate.is_symlink() or not candidate.is_file():
        return []
    content = candidate.read_bytes()
    if b"\0" in content:
        return []
    hits: list[tuple[str, str]] = []
    for number, line in enumerate(content.decode(errors="replace").splitlines(), start=1):
        hits.extend(analyze_added_line(path, number, line))
    return hits


def mock_source_hints(
    repo: Path, changes: list[Change], treeish: str | None = None
) -> list[tuple[str, str]]:
    hints: list[tuple[str, str]] = []
    for path in {change.path for change in changes}:
        candidate = repo / path
        if (
            not path.endswith(".go")
            or path.endswith("_test.go")
            or is_generated(path)
        ):
            continue
        if treeish:
            content = run_git("show", f"{treeish}:{path}", check=False)
        elif candidate.is_symlink() or not candidate.is_file():
            continue
        else:
            content = candidate.read_text(errors="replace")
        for number, line in enumerate(
            content.splitlines(), start=1
        ):
            if "//go:generate" in line and "mockgen" in line:
                hints.append(
                    (
                        "HINT",
                        f"{escape_path(path)}:{number}: mock-source: {line[:200]}",
                    )
                )
                break
    return hints


def new_workflow_hints(changes: list[Change]) -> list[tuple[str, str]]:
    return [
        (
            "SMELL",
            f"{escape_path(change.path)}:1: new-workflow: "
            "confirm this cannot be a job in .github/workflows/ci.yml",
        )
        for change in sorted(changes, key=lambda change: change.path)
        if change.status in {"A", "?"}
        and change.path.startswith(".github/workflows/")
    ]


def shell_script_hints(
    repo: Path, changes: list[Change], treeish: str | None = None
) -> list[tuple[str, str]]:
    return [
        (
            "SMELL",
            f"{escape_path(path)}:1: shell-script: "
            "confirm this is only a thin launcher and move logic to a Bazel Python or Go target",
        )
        for path in sorted({change.path for change in changes})
        if kind_of(path, repo, treeish) == "shell"
    ]


def print_section(name: str, values: list[str]) -> None:
    print(f"\n## {name}")
    if values:
        print("\n".join(values))
    else:
        print("none")


def main() -> None:
    args = parser().parse_args()
    workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if workspace:
        os.chdir(workspace)
    repo_output = run_git("rev-parse", "--show-toplevel", check=False).strip()
    if not repo_output:
        die("not inside a git repository")
    repo = Path(repo_output)
    os.chdir(repo)

    head = run_git("rev-parse", "--short=12", "HEAD").strip()
    dirty = bool(run_git("status", "--porcelain"))
    changes: list[Change] = []
    diff = ""
    base_ref = args.base or args.range or "origin/main"
    base_sha = ""
    commit = ""

    if args.commit:
        mode = "commit"
        verified = run_git(
            "rev-parse", "--verify", "--quiet", f"{args.commit}^{{commit}}", check=False
        ).strip()
        if not verified:
            die(f"unknown commit: {args.commit}")
        commit = run_git("rev-parse", "--short=12", verified).strip()
        changes.extend(
            parse_name_status(
                run_git_bytes(
                    "diff-tree",
                    "--root",
                    "--no-commit-id",
                    "--name-status",
                    "--find-renames",
                    "-z",
                    "-r",
                    commit,
                )
            )
        )
        diff = run_git(
            "show",
            "--format=",
            "--unified=0",
            "--no-ext-diff",
            "--no-textconv",
            commit,
        )
    elif args.uncommitted:
        mode = "uncommitted"
        changes.extend(git_name_status("HEAD"))
        diff = git_diff("HEAD")
    else:
        mode = "range" if args.range else "branch"
        verified = run_git(
            "rev-parse", "--verify", "--quiet", f"{base_ref}^{{commit}}", check=False
        ).strip()
        if not verified:
            die(
                f"base {base_ref} is not available locally; pass --base. "
                "This script does not fetch."
            )
        base_sha = run_git("merge-base", "HEAD", base_ref).strip()
        if not base_sha:
            die(f"no merge-base between HEAD and {base_ref}")
        if mode == "range":
            changes.extend(git_name_status(base_sha, "HEAD"))
            diff = git_diff(base_sha, "HEAD")
        else:
            changes.extend(git_name_status(base_sha))
            diff = git_diff(base_sha)

    hits = analyze_diff(diff)
    if mode not in {"commit", "range"}:
        untracked = [
            path
            for path in run_git(
                "ls-files", "--others", "--exclude-standard", "-z"
            ).split("\0")
            if path
        ]
        for path in untracked:
            changes.append(Change("?", path))
            hits.extend(read_untracked(repo, path))

    target_tree = commit if mode == "commit" else "HEAD" if mode == "range" else None
    hits.extend(mock_source_hints(repo, changes, target_tree))
    hits.extend(shell_script_hints(repo, changes, target_tree))
    hits.extend(new_workflow_hints(changes))
    classified = classify_changes(changes, repo, target_tree)

    print(f"target: {mode}")
    if mode in {"branch", "range"}:
        base = run_git("rev-parse", "--short=12", base_sha).strip()
        print(f"base: {base} ({base_ref})")
        print(f"head: {head}")
        if mode == "range":
            print(f"range: {base}..HEAD; dirty work excluded")
        else:
            print(f"range: {base}..HEAD plus staged, unstaged, and untracked")
    elif mode == "commit":
        print(f"commit: {commit}")
        print(f"head: {head}")
        print(f"range: commit {commit} only")
    else:
        print(f"head: {head}")
        print("range: staged, unstaged, and untracked")
    print(f"dirty: {str(dirty).lower()}")
    if mode in {"commit", "range"} and dirty:
        print("note: dirty work is excluded from this review")

    if mode in {"branch", "range"}:
        subjects = run_git("log", "--format=- %s", f"{base_sha}..HEAD").splitlines()
        base = run_git("rev-parse", "--short=12", base_sha).strip()
        print_section("intent", subjects or [f"- (no commits since {base})"])
    elif mode == "commit":
        print_section("intent", [run_git("log", "-1", "--format=- %s", commit).strip()])
    else:
        print_section("intent", ["- uncommitted work"])

    print("\n## files")
    for kind in KINDS:
        print(f"{kind}:")
        entries = sorted(
            f"  {change.status} {change.display()}"
            for item_kind, change in classified
            if item_kind == kind
        )
        print("\n".join(entries) if entries else "  none")

    packages = sorted(
        {
            package
            for kind, change in classified
            if kind != "docs"
            for path in change.package_paths()
            for package in [bazel_package(repo, path)]
            if package
        }
    )
    print_section("packages", packages)

    smells = sorted({message for kind, message in hits if kind == "SMELL"})
    print("\n## smells")
    print("note: a hit is a candidate, not a finding")
    print("\n".join(smells) if smells else "none")

    hints = sorted({message for kind, message in hits if kind == "HINT"})
    print_section("hints", hints)


if __name__ == "__main__":
    main()
