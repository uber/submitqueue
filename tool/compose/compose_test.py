"""Exercise the pinned Compose executable in runfiles without a Docker daemon."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from python.runfiles import runfiles


class ComposeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compose = runfiles.Create().Rlocation(
            "rules_docker_compose/docker_compose/current_docker_compose_toolchain/docker-compose"
        )

    def test_project_directory_and_environment(self):
        with tempfile.TemporaryDirectory() as project:
            Path(project, "compose.yaml").write_text(
                "services:\n"
                "  demo:\n"
                "    image: ${COMPOSE_TEST_IMAGE}\n"
                "    volumes:\n"
                "      - ./content:/data\n"
            )
            for absolute_config in (False, True):
                with self.subTest(absolute_config=absolute_config):
                    env = {**os.environ, "COMPOSE_TEST_IMAGE": "busybox:1.37.0"}
                    config_file = str(Path(project, "compose.yaml")) if absolute_config else "compose.yaml"
                    result = subprocess.run(
                        [self.compose, "-f", config_file, "-p", "compose-test", "config", "--format", "json"],
                        cwd=os.path.dirname(project) if absolute_config else project,
                        env=env,
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    config = json.loads(result.stdout)
                    self.assertEqual(config["name"], "compose-test")
                    self.assertEqual(config["services"]["demo"]["image"], "busybox:1.37.0")
                    self.assertEqual(
                        config["services"]["demo"]["volumes"][0]["source"],
                        str(Path(project, "content").resolve()),
                    )

    def test_invalid_config_returns_failure(self):
        with tempfile.TemporaryDirectory() as project:
            Path(project, "compose.yaml").write_text("services: []\n")
            result = subprocess.run(
                [self.compose, "-p", "compose-test", "config"],
                cwd=project,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(result.stderr)


if __name__ == "__main__":
    unittest.main()
