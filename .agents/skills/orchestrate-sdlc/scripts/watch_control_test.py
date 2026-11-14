#!/usr/bin/env python3
"""Behavioral tests for watch deadlines, stop handshakes, and stale-run fences."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from watch_control import control_watch, main, parse_duration, parse_timestamp


class WatchControlTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_duration_and_default_window(self):
        self.assertEqual(parse_duration("30m"), timedelta(minutes=30))
        for invalid in ("0h", "-1h", "NaN", "inf", "1.5h", "48", "1w", "9" * 100 + "d"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_duration(invalid)
        state = control_watch({}, "STARTWATCH", self.now)
        self.assertEqual(parse_timestamp(state["watch"]["deadline_at"]), self.now + timedelta(hours=48))

    def test_active_start_keeps_run_deadline_and_campaign_data(self):
        original = {"agents": {"owner": {"phase": "dormant"}}, "cursors": {"events": 17}, "attempts": {"issue": 2}}
        state = control_watch(original, "STARTWATCH", self.now)
        later = self.now + timedelta(hours=1)
        self.assertEqual(control_watch(state, "STARTWATCH", later, "48h"), state)
        shortened = control_watch(state, "STARTWATCH", later, "1h")
        self.assertEqual(shortened["watch"]["run_id"], state["watch"]["run_id"])
        self.assertEqual(parse_timestamp(shortened["watch"]["deadline_at"]), self.now + timedelta(hours=2))
        for key in original:
            self.assertEqual(shortened[key], original[key])
        self.assertNotIn("watch", original)

    def test_stop_handshake_and_stale_actions_after_new_user_start(self):
        state = control_watch({}, "STARTWATCH", self.now, "1h")
        run_id = state["watch"]["run_id"]
        with self.assertRaises(ValueError):
            control_watch(state, "CHECK", self.now, run_id=run_id)
        state = control_watch(state, "ARM", self.now, run_id=run_id)
        self.assertEqual(control_watch(state, "CHECK", self.now, run_id=run_id), state)
        stopped = control_watch(state, "STOPWATCH", self.now)
        self.assertEqual(control_watch(stopped, "STOPWATCH", self.now + timedelta(seconds=1)), stopped)
        for command in ("CHECK", "ARM", "STARTWATCH"):
            with self.subTest(command=command), self.assertRaises(ValueError):
                control_watch(stopped, command, self.now, run_id=run_id)
        stopped = control_watch(stopped, "CONFIRMSTOP", self.now, run_id=run_id)
        restarted = control_watch(stopped, "STARTWATCH", self.now + timedelta(hours=2), "2h")
        self.assertNotEqual(restarted["watch"]["run_id"], run_id)
        self.assertEqual(restarted["watch_history"], [stopped["watch"]])
        for command in ("CHECK", "ARM", "CONFIRMSTOP", "STOPWATCH"):
            with self.subTest(command=command), self.assertRaises(ValueError):
                control_watch(restarted, command, self.now, run_id=run_id)

    def test_deadline_cannot_be_revived_by_arm_or_handoff(self):
        state = control_watch({}, "STARTWATCH", self.now, "1h")
        run_id = state["watch"]["run_id"]
        for command in ("ARM", "CHECK", "STARTWATCH"):
            with self.subTest(command=command), self.assertRaises(ValueError):
                control_watch(state, command, self.now + timedelta(hours=1), run_id=run_id)
        stopped = control_watch(state, "STOPWATCH", self.now + timedelta(hours=2))
        self.assertEqual(control_watch(stopped, "CONFIRMSTOP", self.now + timedelta(hours=2), run_id=run_id)["watch"]["phase"], "stopped")

    def test_cli_persists_control_and_does_not_rewrite_on_check(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "campaign.json"
            self.assertEqual(main(["--state", str(path), "STARTWATCH", "1h"]), 0)
            run_id = json.loads(path.read_text())["watch"]["run_id"]
            args = ["--state", str(path), "--run-id", run_id]
            self.assertEqual(main(args + ["ARM"]), 0)
            before = path.stat().st_mtime_ns
            self.assertEqual(main(args + ["CHECK"]), 0)
            self.assertEqual(path.stat().st_mtime_ns, before)
            self.assertEqual(main(["--state", str(path), "STOPWATCH"]), 0)
            self.assertEqual(main(args + ["CHECK"]), 1)
            self.assertEqual(main(args + ["CONFIRMSTOP"]), 0)


if __name__ == "__main__":
    unittest.main()
