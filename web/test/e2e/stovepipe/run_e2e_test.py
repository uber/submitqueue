import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from web.test.e2e.stovepipe.run_e2e import main


class RunnerCleanupTest(unittest.TestCase):
    def test_termination_during_playwright_cleans_up_the_stack(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "_main"
                test_dir = root / "web/test/e2e/stovepipe"
                test_dir.mkdir(parents=True)
                for name in ("package.json", "playwright.config.ts", "stovepipe.spec.ts"):
                    (test_dir / name).touch()
                commands = []

                def run(command, **kwargs):
                    commands.append(command)
                    if command[0] == str(root / "playwright"):
                        signal.raise_signal(signum)
                    return subprocess.CompletedProcess(command, 0)

                handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
                try:
                    with patch.dict(os.environ, {"RUNFILES_DIR": directory, "TEST_UNDECLARED_OUTPUTS_DIR": directory, "SKIP_CLEANUP": "false"}), patch(
                        "sys.argv", ["runner", "backend-loader", "mysql-loader", "web-loader", "chromium", "playwright"]
                    ), patch("subprocess.run", side_effect=run), patch("subprocess.check_output", return_value="127.0.0.1:12345"):
                        with self.assertRaises(SystemExit) as exit:
                            main()
                        self.assertEqual(exit.exception.code, 128 + signum)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)

                self.assertTrue(any(command[-3:] == ["down", "-v", "--remove-orphans"] for command in commands))


if __name__ == "__main__":
    unittest.main()
