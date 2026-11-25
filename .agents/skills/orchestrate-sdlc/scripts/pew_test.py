#!/usr/bin/env python3
"""Behavioral tests for the PEW Markdown renderer."""

import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import subprocess

from pew import ci_verdict, gh_pr_fetcher, main, pr_marker, render_pew, run_gh

NOW = datetime(2026, 11, 25, 20, 50, tzinfo=timezone.utc)


def agent(**fields) -> dict:
    base = {
        "role": "implementer",
        "model": "opus-5.5",
        "issue": "ISSUE-1",
        "prs": [],
        "artifacts": {},
        "state": "idle",
        "archived": False,
        "next_step": "await review",
        "last_update": "2026-11-25T18:52:00Z",
        "last_action": "pushed",
        "author_action": None,
    }
    base.update(fields)
    return base


def ledger(agents: dict, **fields) -> dict:
    state = {"parameters": {"display_timezone": "America/Los_Angeles"}, "agents": agents}
    state.update(fields)
    return state


def pull(state="OPEN", draft=False, required=("pass",), checks=(), review=""):
    return {
        "url": "https://github.com/o/r/pull/N",
        "state": state,
        "isDraft": draft,
        "reviewDecision": review,
        "required_checks": [{"name": f"req{i}", "bucket": bucket} for i, bucket in enumerate(required)],
        "checks": [{"name": name, "bucket": bucket} for name, bucket in checks],
    }


def fetcher(pulls: dict):
    def fetch(number: str) -> dict:
        if number not in pulls:
            raise RuntimeError("not found")
        result = copy.deepcopy(pulls[number])
        result["url"] = result["url"].replace("/N", "/" + number)
        result["checks"] = result["required_checks"] + result["checks"]
        return result
    return fetch


def table_rows(output: str) -> list[list[str]]:
    rows = [line for line in output.splitlines() if line.startswith("| ")][1:]
    return [[cell.strip() for cell in row.strip("|").split(" | ")] for row in rows]


def footnotes(output: str) -> list[str]:
    return [line[2:] for line in output.splitlines() if line.startswith("- ")]


class RenderPewTest(unittest.TestCase):
    def test_rows(self):
        cases = [
            {
                "name": "newest update first, missing update last",
                "agents": {
                    "old": agent(last_update="2026-11-24T10:00:00Z"),
                    "none": agent(last_update=None),
                    "new": agent(last_update="2026-11-25T19:00:00Z"),
                },
                "pulls": {},
                "check": lambda rows, notes: self.assertEqual(
                    [row[0].split()[1] for row in rows], ["new", "old", "none"]
                ),
            },
            {
                "name": "draft with skipped required checks",
                "agents": {"a": agent(prs=["7"], role="implementer", state="working")},
                "pulls": {"7": pull(draft=True, required=("skipping",))},
                "check": lambda rows, notes: self.assertEqual(
                    rows[0],
                    [
                        "🧑‍💻 a (opus-5.5) ⚙️ ISSUE-1",
                        "[#7](https://github.com/o/r/pull/7)",
                        "🟡 Draft · ⚪ skipped",
                        "await review · pushed 10:52",
                    ],
                ),
            },
            {
                "name": "merged row renders without strikethrough and keeps its link",
                "agents": {"a": agent(prs=["8"])},
                "pulls": {"8": pull(state="MERGED", review="APPROVED", checks=(("license/cla", "pending"),))},
                "check": lambda rows, notes: (
                    self.assertFalse(any("~" in cell for cell in rows[0])),
                    self.assertEqual(rows[0][1], "[#8](https://github.com/o/r/pull/8)"),
                    self.assertEqual(rows[0][2], "🟣 Merged · 🟢 pass"),
                    self.assertEqual(notes, []),
                ),
            },
            {
                "name": "closed row renders without strikethrough",
                "agents": {"a": agent(prs=["6"])},
                "pulls": {"6": pull(state="CLOSED")},
                "check": lambda rows, notes: (
                    self.assertFalse(any("~" in cell for cell in rows[0])),
                    self.assertEqual(rows[0][2], "⚫ Closed · 🟢 pass"),
                ),
            },
            {
                "name": "artifact-only investigator",
                "agents": {
                    "inv": agent(
                        role="investigator",
                        artifacts={
                            "spec": {"path": "/tmp/campaign/X-spec.md", "at": "2026-11-25T18:00:00Z"},
                            "report": {"path": "/tmp/campaign/X report.md", "at": "2026-11-25T17:00:00Z"},
                        },
                    )
                },
                "pulls": {},
                "check": lambda rows, notes: (
                    self.assertTrue(rows[0][0].startswith("🔎 inv ")),
                    self.assertEqual(
                        rows[0][1],
                        "[report](file:///tmp/campaign/X%20report.md) · [spec](file:///tmp/campaign/X-spec.md)",
                    ),
                    self.assertEqual(rows[0][2], "—"),
                ),
            },
            {
                "name": "multiple PRs each get a state entry",
                "agents": {"a": agent(prs=["1", "2"])},
                "pulls": {"1": pull(state="MERGED"), "2": pull(required=("pass", "pending"))},
                "check": lambda rows, notes: (
                    self.assertEqual(rows[0][1], "[#1](https://github.com/o/r/pull/1) · [#2](https://github.com/o/r/pull/2)"),
                    self.assertEqual(rows[0][2], "#1 🟣 Merged · 🟢 pass; #2 🟢 Ready · 🟡 pending"),
                ),
            },
            {
                "name": "archived agents are counted, not listed",
                "agents": {"live": agent(), "gone1": agent(role="reviewer", archived=True), "gone2": agent(archived=True)},
                "pulls": {},
                "check": lambda rows, notes: (
                    self.assertEqual([row[0].split()[1] for row in rows], ["live"]),
                    self.assertEqual(notes, ["2 archived agents"]),
                ),
            },
            {
                "name": "missing values are explicit",
                "agents": {"a": agent(model=None, issue=None, next_step="", last_action=None, last_update=None, state=None, role=None)},
                "pulls": {},
                "check": lambda rows, notes: self.assertEqual(
                    rows[0], ["unknown a (model?) state? unknown", "—", "—", "unknown · unknown unknown"]
                ),
            },
            {
                "name": "author action, review state, qualifiers and GitHub failures become footnotes",
                "agents": {"a": agent(prs=["3", "4"], author_action="review the Draft")},
                "pulls": {"3": pull(review="REVIEW_REQUIRED", checks=(("license/cla", "pending"), ("Lint", "skipping")))},
                "check": lambda rows, notes: (
                    self.assertTrue(rows[0][3].startswith("🙋 await review")),
                    self.assertEqual(rows[0][2], "#3 🟢 Ready · 🟢 pass; #4 ⚪ Unknown · ⚪ unknown"),
                    self.assertIn("#4", rows[0][1]),
                    self.assertEqual(
                        notes,
                        [
                            "🙋 a: review the Draft",
                            "#3: review required",
                            "#3: license/cla pending",
                            "#4: GitHub unavailable: not found",
                        ],
                    ),
                ),
            },
        ]
        for case in cases:
            with self.subTest(case["name"]):
                output = render_pew(ledger(case["agents"]), NOW, fetcher(case["pulls"]))
                rows = table_rows(output)
                for row in rows:
                    self.assertEqual(len(row), 4)
                    self.assertTrue(all(row), row)
                case["check"](rows, footnotes(output))

    def test_every_row_has_four_cells_and_required_fields(self):
        agents = {
            "impl": agent(prs=["5"], last_action="fix | pipe\nnewline"),
            "inv": agent(role="investigator", artifacts={"report": {"path": "/r.md", "at": "2026-11-25T00:00:00Z"}}),
            "rev": agent(role="reviewer", state="working"),
        }
        output = render_pew(ledger(agents), NOW, fetcher({"5": pull()}))
        rows = table_rows(output)
        self.assertEqual(len(rows), 3)
        for row in rows:
            self.assertEqual(len(row), 4, row)
            self.assertRegex(row[0], r"^(🔎|🧑‍💻|🧐) \S+ \(opus-5\.5\)( ⚙️)? ISSUE-1$")
            self.assertRegex(row[3], r" · .+ \d\d:\d\d$")
        self.assertIn("fix \\| pipe newline", output)

    def test_header_timezone_and_watch(self):
        cases = [
            ("winter zone with active watch", "America/Los_Angeles", NOW, {"phase": "watching", "deadline_at": "2026-11-26T04:49:55Z"}, "PEW · 2026-11-25 12:50 PST · watch on until 11-25 20:49"),
            ("summer daylight time", "America/Los_Angeles", datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc), None, "PEW · 2026-07-01 05:00 PDT · watch off"),
            ("expired watch is off", "UTC", NOW, {"phase": "watching", "deadline_at": "2026-11-25T20:00:00Z"}, "PEW · 2026-11-25 20:50 UTC · watch off"),
            ("stop requested", "Asia/Tokyo", NOW, {"phase": "stopping", "deadline_at": "2026-11-26T20:00:00Z"}, "PEW · 2026-11-26 05:50 JST · watch stopping"),
        ]
        for name, zone, now, watch, header in cases:
            with self.subTest(name):
                state = ledger({}, watch=watch)
                state["parameters"]["display_timezone"] = zone
                self.assertEqual(render_pew(state, now, fetcher({})).splitlines()[0], header)

    def test_row_times_use_display_zone_and_date(self):
        agents = {
            "today": agent(last_update="2026-11-25T08:05:00Z"),
            "yesterday": agent(last_update="2026-11-25T07:59:00Z"),
        }
        rows = table_rows(render_pew(ledger(agents), NOW, fetcher({})))
        self.assertEqual([row[3].rsplit(" ", 2)[-2:] for row in rows], [["pushed", "00:05"], ["11-24", "23:59"]])

    def test_invalid_ledger_fails(self):
        cases = [
            ("no display timezone", {"agents": {}}),
            ("relative artifact path", ledger({"a": agent(artifacts={"report": {"path": "r.md", "at": ""}})})),
            ("artifact without an object", ledger({"a": agent(artifacts={"report": "/r.md"})})),
            ("agents not an object", ledger([])),
            ("agent not an object", ledger({"a": "implementer"})),
            ("prs not a list", ledger({"a": agent(prs="1138")})),
            ("watch without deadline", ledger({}, watch={"phase": "watching"})),
            ("unknown timezone", {"parameters": {"display_timezone": "Mars/Olympus"}, "agents": {}}),
        ]
        for name, state in cases:
            with self.subTest(name), self.assertRaises(ValueError):
                render_pew(state, NOW, fetcher({}))

    def test_ci_and_pr_markers(self):
        cases = [
            ("fail", ["pass", "fail"], "fail"),
            ("cancel", ["cancel", "skipping"], "fail"),
            ("fail beats pending", ["fail", "pending"], "fail"),
            ("no required checks", [], "absent"),
            ("pass", ["pass", "pass"], "pass"),
            ("pass and skipping", ["pass", "skipping"], "skipped"),
            ("all skipping", ["skipping"], "skipped"),
            ("pending and skipping", ["pending", "skipping"], "pending"),
            ("unknown bucket", ["pass", "neutral"], "unknown"),
            ("unknown bucket with skipping", ["skipping", "stale"], "unknown"),
        ]
        for name, buckets, verdict in cases:
            with self.subTest(name):
                self.assertEqual(ci_verdict({"required_checks": [{"name": "c", "bucket": b} for b in buckets]}), verdict)
        self.assertEqual(ci_verdict(None), "unknown")
        markers = [
            ("draft", pull(draft=True), "🟡 Draft"),
            ("ready", pull(), "🟢 Ready"),
            ("merged", pull(state="MERGED"), "🟣 Merged"),
            ("closed", pull(state="CLOSED"), "⚫ Closed"),
            ("unrecognized state", pull(state="LOCKED"), "⚪ Unknown"),
            ("unavailable", None, "⚪ Unknown"),
        ]
        for name, pr, marker in markers:
            with self.subTest(name):
                self.assertEqual(pr_marker(pr), marker)

    def test_run_gh(self):
        completed = lambda code, stdout="", stderr="": subprocess.CompletedProcess([], code, stdout, stderr)
        cases = [
            ("pending checks exit 8 with JSON", completed(8, '[{"bucket":"pending"}]'), '[{"bucket":"pending"}]', None),
            ("failing checks exit 1 with JSON", completed(1, '[{"bucket":"fail"}]'), '[{"bucket":"fail"}]', None),
            ("no checks reported", completed(1, stderr="no checks reported on the 'x' branch"), "", None),
            ("no required checks reported", completed(1, stderr="no required checks reported on the 'x' branch"), "", None),
            ("non-zero with empty stdout", completed(1, stderr="HTTP 502\nbad gateway"), None, RuntimeError),
            ("empty output", completed(0), "", None),
            ("gh missing", FileNotFoundError("gh"), None, OSError),
        ]
        for name, result, expected, error in cases:
            with self.subTest(name):
                side_effect = result if isinstance(result, Exception) else [result]
                with mock.patch("pew.subprocess.run", side_effect=side_effect):
                    if error:
                        with self.assertRaises(error):
                            run_gh(Path("/"), "pr", "checks", "1")
                    else:
                        self.assertEqual(run_gh(Path("/"), "pr", "checks", "1"), expected)

    def test_gh_pr_fetcher_without_required_checks_is_absent(self):
        outputs = [
            subprocess.CompletedProcess([], 0, '{"url": "u", "state": "OPEN", "isDraft": true, "reviewDecision": ""}', ""),
            subprocess.CompletedProcess([], 1, "", "no required checks reported on the 'x' branch"),
            subprocess.CompletedProcess([], 8, '[{"name": "license/cla", "bucket": "pending"}]', ""),
        ]
        with mock.patch("pew.subprocess.run", side_effect=outputs):
            pr = gh_pr_fetcher(Path("/"))("1")
        self.assertEqual(ci_verdict(pr), "absent")
        self.assertEqual(pr["checks"], [{"name": "license/cla", "bucket": "pending"}])

    def test_footnotes_keep_multiline_text_on_one_line(self):
        def failing(number):
            raise RuntimeError("HTTP 502\nbad gateway")
        notes = footnotes(render_pew(ledger({"a": agent(prs=["1"])}), NOW, failing))
        self.assertEqual(notes, ["#1: GitHub unavailable: HTTP 502 bad gateway"])

    def test_main_reads_ledger_and_reports_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "campaign.json"
            path.write_text(json.dumps(ledger({"a": agent(prs=["9"])})))
            stdout = io.StringIO()
            with mock.patch("sys.stdout", stdout):
                code = main(["--state", str(path), "--now", "2026-11-25T20:50:00Z"], lambda cwd: fetcher({"9": pull()}))
            self.assertEqual(code, 0)
            self.assertTrue(stdout.getvalue().startswith("PEW · 2026-11-25 12:50 PST · watch off\n"))

            path.write_text("[]")
            with mock.patch("sys.stderr", io.StringIO()):
                self.assertEqual(main(["--state", str(path)], lambda cwd: fetcher({})), 1)

    def test_main_defaults_to_the_main_checkout_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory).resolve()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / ".sdlc").mkdir()
            (repo / ".sdlc" / "campaign.json").write_text(json.dumps(ledger({"a": agent()})))
            (repo / "sub").mkdir()
            stdout = io.StringIO()
            with mock.patch.dict("os.environ", {"BUILD_WORKING_DIRECTORY": str(repo / "sub")}), mock.patch("sys.stdout", stdout):
                code = main(["--now", "2026-11-25T20:50:00Z"], lambda cwd: fetcher({}))
            self.assertEqual(code, 0)
            self.assertEqual([row[0] for row in table_rows(stdout.getvalue())], ["🧑‍💻 a (opus-5.5) ISSUE-1"])


if __name__ == "__main__":
    unittest.main()
