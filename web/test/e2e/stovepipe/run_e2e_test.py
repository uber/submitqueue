import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from web.test.e2e.stovepipe.run_e2e import main


class RunnerLifecycleTest(unittest.TestCase):
    def test_startup_order_and_cleanup(self):
        for signum in (None, signal.SIGTERM, signal.SIGINT):
            with self.subTest(
                signal=signum
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "_main"
                test_dir = root / "web/test/e2e/stovepipe"
                test_dir.mkdir(parents=True)
                for name in (
                    "package.json",
                    "playwright.config.ts",
                    "stovepipe.spec.ts",
                ):
                    (test_dir / name).touch()
                commands = []
                schema_application = []

                def record_schema_application(_groups, *, env):
                    schema_application.append(len(commands))

                def run(command, **kwargs):
                    commands.append(command)
                    if command[0] == str(root / "playwright") and signum is not None:
                        signal.raise_signal(signum)
                    return subprocess.CompletedProcess(command, 0)

                handlers = {
                    sig: signal.getsignal(sig)
                    for sig in (signal.SIGTERM, signal.SIGINT)
                }
                try:
                    with patch.dict(
                        os.environ,
                        {
                            "RUNFILES_DIR": directory,
                            "TEST_UNDECLARED_OUTPUTS_DIR": directory,
                            "SKIP_CLEANUP": "false",
                        },
                    ), patch(
                        "sys.argv",
                        [
                            "runner",
                            "backend-loader",
                            "mysql-loader",
                            "web-loader",
                            "chromium",
                            "playwright",
                        ],
                    ), patch(
                        "subprocess.run", side_effect=run
                    ), patch(
                        "subprocess.check_output", return_value="127.0.0.1:12345"
                    ), patch(
                        "web.test.e2e.stovepipe.run_e2e.apply_schemas",
                        side_effect=record_schema_application,
                    ):
                        if signum is None:
                            self.assertEqual(main(), 0)
                        else:
                            with self.assertRaises(SystemExit) as exit:
                                main()
                            self.assertEqual(exit.exception.code, 128 + signum)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)

                startups = [
                    (index, command)
                    for index, command in enumerate(commands)
                    if "up" in command
                ]
                self.assertEqual(len(startups), 2)
                self.assertEqual(startups[0][1][-2:], ["mysql-app", "mysql-queue"])
                self.assertEqual(len(schema_application), 1)
                self.assertLess(startups[0][0], schema_application[0])
                self.assertLess(schema_application[0], startups[1][0])
                self.assertEqual(
                    commands[schema_application[0]][:3], ["docker", "exec", "-i"]
                )
                self.assertTrue(
                    any(
                        command[-3:] == ["down", "-v", "--remove-orphans"]
                        for command in commands
                    )
                )


if __name__ == "__main__":
    unittest.main()
