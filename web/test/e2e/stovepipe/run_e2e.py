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
        state = "failed" if i == 51 else "succeeded"
        project_count, broken_project_count = (51, 2) if i == 51 else (0, 0)
        statements.append(
            "INSERT INTO request_summary "
            "(queue,request_id,uri,base_uri,state,request_version,"
            "state_timestamp_ms,version,accepted_at_ms,outcome_reason,project_count,broken_project_count) "
            f"VALUES ('web/pagination','{i}','{uri}','','{state}',1,{1000+i},1,{1000+i},'',{project_count},{broken_project_count});"
        )
        statements.append(
            f"INSERT INTO request_acceptance VALUES ('web/pagination',{1000+i},'{i}');"
        )
    statements.append(
        "INSERT INTO request_log VALUES ('web/pagination','51','accepted',1000,'accepted','',1,'','{}');"
    )
    history_details = [
        ("triggered", "", "build_triggered", {"build_id": "runner/build/51?attempt=2"}),
        ("finished", "", "build_finished", {"build_id": "runner/build/51?attempt=2"}),
        ("validation", "", "validation_fact_recorded", {"fact_degree": "0.25"}),
        ("green", "", "validation_fact_recorded", {"fact_degree": "0"}),
        ("projects", "", "project_facts_recorded", {"project_fact_count": "51"}),
        ("empty-projects", "", "project_facts_recorded", {"project_fact_count": "0"}),
        ("abandoned", "", "build_analysis_abandoned", {"build_id": "runner/build/51?attempt=2"}),
        ("failed", "failed", "", {"build_id": "runner/build/51?attempt=2"}),
    ]
    for index, (event_id, state, event, metadata) in enumerate(history_details):
        statements.append(
            "INSERT INTO request_log VALUES "
            f"('web/pagination','51','{event_id}',{1100+index},'{state}','{event}',1,'','{json.dumps(metadata)}');"
        )
    uri = "git://web/pagination/51?file=src%2Fmain#fragment"
    statements.append(
        f"INSERT INTO validation_fact (queue,uri,project,degree,request_id,created_at) VALUES ('web/pagination','{uri}','',0.25,'51',2000);"
    )
    for i in range(1, 52):
        degree = 1.0 if i == 50 else 0.25 if i == 1 else 0
        statements.append(
            f"INSERT INTO request_log VALUES ('web/pagination','51','fact-{i}',{2000+i},'','validation_fact_recorded',1,'','{{}}');"
        )
        statements.append(
            f"INSERT INTO validation_fact (queue,uri,project,degree,request_id,created_at) VALUES ('web/pagination','{uri}','project-{i:02d}',{degree},'51',{2000+i});"
        )
        statements.append(
            f"INSERT INTO project_fact_by_degree VALUES ('web/pagination','{uri}',{degree},'project-{i:02d}');"
        )
    for i in range(53):
        task = "buildtest" if i < 52 else "expensive"
        unit = "" if i in (0, 52) else f"//packages/test-{i:02d}:go_default_test"
        statements.append(
            "INSERT INTO issue_summary "
            "(queue,task_id,unit_id,status,first_seen_build_id,request_id,build_id,changed_at_ms,version) "
            f"VALUES ('web/pagination','{task}','{unit}','open','web/build/51','51','web/build/51',1000,1);"
        )
        statements.append(
            f"INSERT INTO open_issue VALUES ('web/pagination','{task}','{unit}','web/build/51');"
        )
    for event in ("issue_tracking_started", "issues_reconciled"):
        statements.append(
            f"INSERT INTO request_log VALUES ('web/pagination','51','{event}',3000,'','{event}',1,'','{{}}');"
        )
    for i in range(51):
        kind = ("new", "existing", "closed")[i % 3]
        unit = "" if i == 0 else f"//packages/test-{i:02d}:go_default_test"
        statements.append(
            "INSERT INTO request_issue_summary "
            "(queue,request_id,task_id,unit_id,kind,build_id,first_seen_build_id,version) "
            f"VALUES ('web/pagination','51','buildtest','{unit}','{kind}','web/build/51','web/build/50',1);"
        )
    statements.append(
        "INSERT INTO request_log VALUES ('web/pagination','51','project-issues',3001,'','project_issues_recorded',1,'','{}');"
    )
    for i in range(1, 52):
        for table in ("request_projects_by_issue", "request_issues_by_project"):
            statements.append(
                f"INSERT INTO {table} (queue,request_id,task_id,unit_id,project) "
                f"VALUES ('web/pagination','51','buildtest','//packages/test-01:go_default_test','project-{i:02d}');"
            )
    for i in range(50):
        for table in ("request_projects_by_issue", "request_issues_by_project"):
            statements.append(
                f"INSERT INTO {table} (queue,request_id,task_id,unit_id,project) "
                f"VALUES ('web/pagination','51','expensive','//packages/extra-{i:02d}:test','project-01');"
            )
    closed_issue_count = 500
    for i in range(closed_issue_count + 1):
        closed = i < closed_issue_count
        task = "a-closed" if closed else "z-open"
        unit = f"unit-{i:03d}" if closed else ""
        status = "closed" if closed else "open"
        statements.append(
            "INSERT INTO issue_summary "
            "(queue,task_id,unit_id,status,first_seen_build_id,request_id,build_id,changed_at_ms,version) "
            f"VALUES ('web/empty','{task}','{unit}','{status}','web/build/1','1','web/build/1',1000,1);"
        )
        statements.append(
            f"INSERT INTO open_issue VALUES ('web/empty','{task}','{unit}','web/build/1');"
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
            run(
                [
                    *compose,
                    "up",
                    "-d",
                    "--no-build",
                    "--pull",
                    "never",
                    "--wait",
                    "mysql-app",
                    "mysql-queue",
                ],
                env,
            )
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
            run([*compose, "up", "-d", "--no-build", "--pull", "never", "--wait"], env)
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
