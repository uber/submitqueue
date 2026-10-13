import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from web.test.e2e.run_e2e import _capture_compose_logs, _compose_command


class CaptureComposeLogsTest(unittest.TestCase):
    def test_writes_failure_output_and_bazel_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as output_dir:
            stderr = io.StringIO()
            result = subprocess.CompletedProcess(
                args=["docker-compose", "logs"],
                returncode=0,
                stdout="web-service | gateway readiness check failed\n",
            )

            with patch("web.test.e2e.run_e2e.subprocess.run", return_value=result), patch(
                "web.test.e2e.run_e2e.sys.stderr", stderr
            ):
                _capture_compose_logs(
                    ["docker-compose", "-p", "e2e-submitqueue-web-test"],
                    env={"TEST_UNDECLARED_OUTPUTS_DIR": output_dir},
                )

            expected = "web-service | gateway readiness check failed\n"
            self.assertEqual(stderr.getvalue(), expected)
            self.assertEqual((Path(output_dir) / "compose.log").read_text(), expected)


class ComposeCommandTest(unittest.TestCase):
    def test_prefers_the_compose_plugin(self) -> None:
        with patch("web.test.e2e.run_e2e.shutil.which", return_value="/usr/bin/docker"), patch(
            "web.test.e2e.run_e2e.subprocess.run", return_value=subprocess.CompletedProcess(args=[], returncode=0)
        ):
            self.assertEqual(_compose_command(env={}), ["docker", "compose"])

    def test_falls_back_to_the_standalone_binary(self) -> None:
        with patch("web.test.e2e.run_e2e.shutil.which", return_value="/usr/bin/docker"), patch(
            "web.test.e2e.run_e2e.subprocess.run", return_value=subprocess.CompletedProcess(args=[], returncode=1)
        ):
            self.assertEqual(_compose_command(env={}), ["docker-compose"])


if __name__ == "__main__":
    unittest.main()
