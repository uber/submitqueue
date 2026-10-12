import os
import subprocess
import unittest

from web.test.testutil.compose import compose_command


class ComposeCommandTest(unittest.TestCase):
    def test_reports_version_without_a_local_docker_installation(self) -> None:
        version = subprocess.check_output(
            [*compose_command(), "version", "--short"],
            env={**os.environ, "PATH": ""},
            text=True,
        )
        self.assertRegex(version.strip(), r"^v?\d+\.\d+\.\d+")


if __name__ == "__main__":
    unittest.main()
