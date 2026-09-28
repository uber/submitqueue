from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("review-scope.py")
SPEC = importlib.util.spec_from_file_location("review_scope", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
review_scope = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = review_scope
SPEC.loader.exec_module(review_scope)


class ReviewScopeTest(unittest.TestCase):
    def test_kind_of_classifies_tooling_and_build_files(self) -> None:
        cases = {
            "mock/BUILD.bazel": "build",
            "tool.py": "python",
            "test_tool.py": "tests",
            "run.sh": "shell",
            "config.yaml": "config",
            "service/Dockerfile.debug": "build",
            "generated/mock/example.go": "generated",
            "generated/mock/README.md": "docs",
        }

        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(expected, review_scope.kind_of(path))

    def test_kind_of_reads_an_extensionless_script_shebang(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            (repo / "tool").write_text("#!/usr/bin/env bash\n")

            self.assertEqual("shell", review_scope.kind_of("tool", repo))

    def test_parse_name_status_preserves_rename_copy_and_paths(self) -> None:
        changes = review_scope.parse_name_status(
            b"R100\0old\tname\0new\nname\0C075\0source\0copy\0M\0plain\0"
        )

        self.assertEqual(
            [
                review_scope.Change("R", "new\nname", "old\tname"),
                review_scope.Change("C", "copy", "source"),
                review_scope.Change("M", "plain"),
            ],
            changes,
        )

    def test_cross_kind_rename_is_classified_at_both_endpoints(self) -> None:
        change = review_scope.Change("R", "guide.md", "tool.py")

        self.assertEqual(
            [("python", change), ("docs", change)],
            review_scope.classify_changes([change]),
        )

    def test_bazel_package_formats_the_root_package(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            (repo / "BUILD.bazel").write_text("")

            self.assertEqual("//", review_scope.bazel_package(repo, "MODULE.bazel"))

    def test_analyze_diff_escapes_control_characters_in_paths(self) -> None:
        hits = review_scope.analyze_diff(
            'diff --git "a/name\\t.go" "b/name\\t.go"\n'
            '+++ "b/name\\t.go"\n'
            "@@ -0,0 +1 @@\n"
            "+// Get value\n"
        )

        self.assertEqual(
            [("SMELL", r"name\t.go:1: narration-comment: // Get value")],
            hits,
        )

    def test_untracked_symlink_contents_are_not_scanned(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            target = repo / "target.go"
            target.write_text("// Get value\n")
            (repo / "linked.go").symlink_to(target)

            self.assertEqual([], review_scope.read_untracked(repo, "linked.go"))

    def test_changed_mock_source_emits_a_hint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            source = repo / "store.go"
            source.write_text(
                "//go:generate mockgen -source=store.go -destination=mock/store.go\n"
                "package store\n"
            )

            self.assertEqual(
                [
                    (
                        "HINT",
                        "store.go:1: mock-source: "
                        "//go:generate mockgen -source=store.go "
                        "-destination=mock/store.go",
                    )
                ],
                review_scope.mock_source_hints(
                    repo, [review_scope.Change("M", "store.go")]
                ),
            )

    def test_changed_shell_script_emits_a_hermeticness_smell(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)

            self.assertEqual(
                [
                    (
                        "SMELL",
                        "tool/run.sh:1: shell-script: "
                        "confirm this is only a thin launcher and move logic "
                        "to a Bazel Python or Go target",
                    )
                ],
                review_scope.shell_script_hints(
                    repo, [review_scope.Change("A", "tool/run.sh")]
                ),
            )

    def test_branch_mode_uses_the_net_worktree_diff(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            self.run_git(repo, "init")
            self.run_git(repo, "config", "user.email", "review@example.com")
            self.run_git(repo, "config", "user.name", "Review Test")
            (repo / "BUILD.bazel").write_text("")
            source = repo / "review.go"
            source.write_text("package review\n")
            self.run_git(repo, "add", ".")
            self.run_git(repo, "commit", "-m", "initial")
            base = self.run_git(repo, "rev-parse", "HEAD").stdout.strip()

            source.write_text("package review\n// Get value\n")
            self.run_git(repo, "add", "review.go")
            self.run_git(repo, "commit", "-m", "add narration")
            source.write_text("package review\n")

            env = os.environ.copy()
            env.pop("BUILD_WORKSPACE_DIRECTORY", None)
            result = subprocess.run(
                (sys.executable, str(SCRIPT), "--base", base),
                cwd=repo,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )

            self.assertNotIn("narration-comment", result.stdout)
            self.assertIn("go:\n  none", result.stdout)

            range_result = subprocess.run(
                (sys.executable, str(SCRIPT), "--range", base),
                cwd=repo,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )

            self.assertIn("narration-comment", range_result.stdout)
            self.assertIn("dirty work is excluded from this review", range_result.stdout)

    @staticmethod
    def run_git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ("git", *args),
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )


if __name__ == "__main__":
    unittest.main()
