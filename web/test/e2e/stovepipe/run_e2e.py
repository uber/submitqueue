import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile

from web.test.testutil.compose import (
    compose_command,
    load_project_images,
    apply_schemas,
    published_port,
)


def run(command, env, **kwargs):
    return subprocess.run(command, env=env, check=True, text=True, **kwargs)


def exit_on_signal(signum, _frame):
    raise SystemExit(128 + signum)


def _pagination_fixture_sql() -> str:
    # Retained projections exercise pagination independently of the fake source's single HEAD.
    statements = []
    for i in range(1, 52):
        uri = f"git://web/pagination/{i}" + (
            "?file=src%2Fmain#fragment" if i == 51 else ""
        )
        statements.append(
            f"INSERT INTO request_summary VALUES ('web/pagination','{i}','{uri}','','succeeded',1,{1000+i},1,{1000+i},'');"
        )
        statements.append(
            f"INSERT INTO request_acceptance VALUES ('web/pagination',{1000+i},'{i}');"
        )
    statements.append(
        "INSERT INTO request_log VALUES ('web/pagination','51','accepted',1000,'accepted','',1,'','{}');"
    )
    return "\n".join(statements)


def main():
    root = Path(os.environ["RUNFILES_DIR"]) / "_main"
    project = f"sq-test-e2e-stovepipe-web-{os.getpid()}"
    with tempfile.TemporaryDirectory(prefix="stovepipe-web-") as temporary:
        context = Path(temporary)
        env = dict(
            os.environ,
            REPO_ROOT=str(context),
            SQ_DOCKER_IMAGE_PREFIX=project,
            STOVEPIPE_WEB_IMAGE=f"{project}-web-service:latest",
            SQ_MYSQL_INITDB_SKIP_TZINFO="1",
            MQ_TENANTS="monorepo/main,web/pagination,web/empty",
        )
        env["STOVEPIPE_WEB_QUEUES"] = json.dumps(
            [
                {"name": "monorepo/main"},
                {"name": "web/pagination"},
                {"name": "web/empty"},
            ]
        )
        compose = [
            *compose_command(),
            "-p",
            project,
            "-f",
            str(root / "service/stovepipe/docker-compose.yml"),
            "-f",
            str(root / "web/service/stovepipe/docker-compose.yml"),
            "-f",
            str(root / "web/test/e2e/stovepipe/docker-compose.yml"),
        ]
        signal.signal(signal.SIGTERM, exit_on_signal)
        signal.signal(signal.SIGINT, exit_on_signal)
        try:
            load_project_images(
                [root / loader for loader in sys.argv[1:4]],
                {
                    "stovepipe-web-e2e-service:latest": f"{project}-stovepipe-service:latest",
                    "submitqueue-web-e2e-mysql:8.0": f"{project}-mysql:8.0",
                    "stovepipe-web-service:latest": f"{project}-web-service:latest",
                },
                env=env,
            )
            run([*compose, "up", "-d", "--no-build", "--pull", "never", "--wait"], env)
            apply_schemas(
                [
                    (
                        root / "platform/extension/counter/mysql/schema",
                        f"{project}-mysql-app-1",
                    ),
                    (
                        root / "stovepipe/extension/storage/mysql/schema",
                        f"{project}-mysql-app-1",
                    ),
                    (
                        root / "platform/extension/messagequeue/mysql/schema",
                        f"{project}-mysql-queue-1",
                    ),
                ],
                env=env,
            )
            run(
                [
                    "docker",
                    "exec",
                    "-i",
                    f"{project}-mysql-app-1",
                    "mysql",
                    "-uroot",
                    "-proot",
                    "submitqueue",
                ],
                env,
                input=_pagination_fixture_sql(),
            )
            for key, service, port in [
                ("STOVEPIPE_E2E_WEB_URL", "web-service", "3000"),
                ("STOVEPIPE_E2E_URL", "stovepipe-service", "8080"),
            ]:
                port_number = published_port(f"{project}-{service}-1", port, env=env)
                env[key] = "http://127.0.0.1:" + port_number
            env["JS_BINARY__PATCH_NODE_FS"] = "0"
            env["PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH"] = str(
                root / sys.argv[4] / "chrome-linux/chrome"
            )
            browser_context = context / "playwright"
            browser_context.mkdir()
            for name in ["package.json", "playwright.config.ts", "stovepipe.spec.ts"]:
                shutil.copy2(
                    root / "web/test/e2e/stovepipe" / name, browser_context / name
                )
            (browser_context / "node_modules").symlink_to(
                root / "web/test/e2e/stovepipe/node_modules", target_is_directory=True
            )
            run(
                [
                    str(root / sys.argv[5]),
                    "test",
                    "--config",
                    str(browser_context / "playwright.config.ts"),
                ],
                env,
            )
        finally:
            output = Path(os.environ["TEST_UNDECLARED_OUTPUTS_DIR"]) / "compose.log"
            with output.open("w") as logs:
                subprocess.run(
                    [*compose, "logs", "--no-color"],
                    env=env,
                    stdout=logs,
                    stderr=subprocess.STDOUT,
                )
            if env.get("SKIP_CLEANUP") != "true":
                subprocess.run(
                    [*compose, "down", "-v", "--remove-orphans"], env=env, check=False
                )
                subprocess.run(
                    [
                        "docker",
                        "image",
                        "rm",
                        "-f",
                        f"{project}-stovepipe-service:latest",
                        f"{project}-mysql:8.0",
                        f"{project}-web-service:latest",
                    ],
                    env=env,
                    check=False,
                )
            else:
                print(
                    f"Retained stack: {project}; web URL: {env.get('STOVEPIPE_E2E_WEB_URL', 'not started')}"
                )
    return 0


if __name__ == "__main__":
    sys.exit(main())
