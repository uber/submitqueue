import fcntl
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path


_PROJECT = f"e2e-submitqueue-web-{os.getpid()}"
_TOKEN = "test"


def _runfile(path: str) -> Path:
    runfiles_dir = os.environ.get("RUNFILES_DIR")
    if runfiles_dir:
        return Path(runfiles_dir) / "_main" / path

    manifest_path = os.environ.get("RUNFILES_MANIFEST_FILE")
    if not manifest_path:
        raise RuntimeError("Bazel runfiles are unavailable")

    key = f"_main/{path}"
    with open(manifest_path, encoding="utf-8") as manifest:
        for line in manifest:
            logical, separator, physical = line.rstrip("\n").partition(" ")
            if logical == key:
                return Path(physical if separator else logical)
    raise RuntimeError(f"Bazel runfile not found: {path}")


def _copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def _link(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.symlink_to(source, target_is_directory=True)


def _run(command: list[str], *, env: dict[str, str], stdin=None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, env=env, stdin=stdin, text=True)


def _output(command: list[str], *, env: dict[str, str]) -> str:
    return subprocess.check_output(command, env=env, text=True).strip()


def _published_port(container: str, container_port: str, *, env: dict[str, str]) -> str:
    address = _output(["docker", "port", container, container_port], env=env)
    return address.splitlines()[0].rsplit(":", 1)[-1]


def _stage_provider_configuration(workspace: Path, context: Path) -> None:
    copies = {
        "service/submitqueue/demo/provider/fake/merge.yaml": "provider/merge.yaml",
        "service/submitqueue/demo/provider/fake/profiles.yaml": "provider/profiles.yaml",
    }
    for source, destination in copies.items():
        _copy(workspace / source, context / destination)


def _stage_playwright(workspace: Path, context: Path) -> Path:
    playwright_root = context / "playwright"
    _copy(workspace / "web/package.json", playwright_root / "package.json")
    _copy(workspace / "web/playwright.config.ts", playwright_root / "playwright.config.ts")
    _copy(workspace / "web/test/e2e/submitqueue.spec.ts", playwright_root / "test/e2e/submitqueue.spec.ts")

    links = {
        "web/node_modules/@axe-core/playwright": "node_modules/@axe-core/playwright",
        "web/node_modules/@bufbuild/protobuf": "node_modules/@bufbuild/protobuf",
        "web/node_modules/@connectrpc/connect": "node_modules/@connectrpc/connect",
        "web/node_modules/@connectrpc/connect-node": "node_modules/@connectrpc/connect-node",
        "web/node_modules/@playwright/test": "node_modules/@playwright/test",
        "web/test/e2e/node_modules/@submitqueue/api": "node_modules/@submitqueue/api",
    }
    for source, destination in links.items():
        _link(workspace / source, playwright_root / destination)
    return playwright_root


def _stage_docker_config(destination: Path) -> None:
    destination.mkdir()
    source = Path.home() / ".docker"
    config = source / "config.json"
    if config.is_file():
        _copy(config, destination / "config.json")
    contexts = source / "contexts"
    if contexts.is_dir():
        shutil.copytree(contexts, destination / "contexts", symlinks=True)
    # Docker finds per-user CLI plugins such as Compose through DOCKER_CONFIG.
    plugins = source / "cli-plugins"
    if plugins.is_dir():
        (destination / "cli-plugins").symlink_to(plugins, target_is_directory=True)


def _compose_command(*, env: dict[str, str]) -> list[str]:
    # Mirrors test/testutil/compose.go: prefer the Compose plugin, which CI runners ship,
    # and fall back to the standalone binary.
    if shutil.which("docker", path=env.get("PATH")) is None:
        return ["docker-compose"]
    probe = subprocess.run(
        ["docker", "compose", "version"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return ["docker", "compose"] if probe.returncode == 0 else ["docker-compose"]


def _load_project_images(image_loaders: list[Path], *, env: dict[str, str]) -> None:
    lock_path = Path(tempfile.gettempdir()) / "submitqueue-web-e2e-image.lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for image_loader in image_loaders:
            _run([str(image_loader)], env=env)
        _run(
            [
                "docker",
                "tag",
                "submitqueue-web-e2e-mysql:8.0",
                f"{_PROJECT}-mysql:8.0",
            ],
            env=env,
        )
        for service in ("gateway", "orchestrator", "runway", "web"):
            _run(
                [
                    "docker",
                    "tag",
                    f"submitqueue-web-e2e-{service}-service:latest"
                    if service != "web"
                    else "submitqueue-web-service:latest",
                    f"{_PROJECT}-{service}-service:latest",
                ],
                env=env,
            )


def _apply_schemas(workspace: Path, *, env: dict[str, str]) -> None:
    schema_groups = (
        (workspace / "submitqueue/gateway/extension/storage/mysql/schema", f"{_PROJECT}-mysql-app-1"),
        (workspace / "submitqueue/orchestrator/extension/storage/mysql/schema", f"{_PROJECT}-mysql-app-1"),
        (workspace / "platform/extension/counter/mysql/schema", f"{_PROJECT}-mysql-app-1"),
        (workspace / "platform/extension/messagequeue/mysql/schema", f"{_PROJECT}-mysql-queue-1"),
    )
    for schema_dir, container in schema_groups:
        for schema in sorted(schema_dir.glob("*.sql")):
            with schema.open("r", encoding="utf-8") as statements:
                _run(
                    ["docker", "exec", "-i", container, "mysql", "-uroot", "-proot", "submitqueue"],
                    env=env,
                    stdin=statements,
                )


def _chromium_executable(chromium: Path) -> Path:
    candidates = (
        chromium / "chrome-linux/chrome",
        chromium / "chrome-mac/Chromium.app/Contents/MacOS/Chromium",
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError(f"unable to find the Bazel-managed Chromium executable under {chromium}")


def _capture_compose_logs(compose: list[str], *, env: dict[str, str]) -> None:
    try:
        result = subprocess.run(
            [*compose, "logs", "--no-color"],
            check=False,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as cause:
        print(f"Unable to collect Compose logs: {cause}", file=sys.stderr)
        return
    sys.stderr.write(result.stdout)
    output_dir = env.get("TEST_UNDECLARED_OUTPUTS_DIR")
    if output_dir:
        destination = Path(output_dir) / "compose.log"
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(result.stdout, encoding="utf-8")
        except OSError as cause:
            print(f"Unable to preserve Compose logs at {destination}: {cause}", file=sys.stderr)


def main() -> int:
    if len(sys.argv) != 8:
        raise RuntimeError("expected five image loaders, Playwright, and Chromium runfile paths")

    image_loaders = [_runfile(path) for path in sys.argv[1:6]]
    playwright = _runfile(sys.argv[6])
    chromium = _runfile(sys.argv[7])
    workspace = _runfile("web/package.json").parents[1]
    context = Path(tempfile.mkdtemp(prefix="submitqueue-web-e2e."))
    gate_dir = context / "consumer-gate"
    gate_dir.mkdir()

    env = os.environ.copy()
    docker_config = context / "docker-config"
    _stage_docker_config(docker_config)
    env["DOCKER_CONFIG"] = str(docker_config)

    compose = [
        *_compose_command(env=env),
        "-f",
        str(workspace / "service/submitqueue/docker-compose.yml"),
        "-f",
        str(workspace / "service/submitqueue/docker-compose.fake.yml"),
        "-f",
        str(workspace / "service/submitqueue/docker-compose.web.yml"),
        "-f",
        str(workspace / "web/test/e2e/docker-compose.e2e.yml"),
        "-p",
        _PROJECT,
    ]

    def handle_signal(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        _stage_provider_configuration(workspace, context)
        playwright_root = _stage_playwright(workspace, context)
        _load_project_images(image_loaders, env=env)

        env.update(
            {
                "REPO_ROOT": str(context),
                "SQ_PROVIDER_CONFIG_DIR": str(context / "provider"),
                "SQ_CONSUMER_GATE_DIR": str(gate_dir),
                "SQ_DOCKER_IMAGE_PREFIX": _PROJECT,
                "SUBMITQUEUE_WEB_TOKEN": _TOKEN,
                "SUBMITQUEUE_WEB_CURSOR_SECRET": "e2e-cursor-secret",
                "SQ_MYSQL_INITDB_SKIP_TZINFO": "1",
            }
        )
        security_options = _output(["docker", "info", "--format", "{{json .SecurityOptions}}"], env=env)
        env["SQ_CONTAINER_USER"] = "0:0" if "name=rootless" in security_options else f"{os.getuid()}:{os.getgid()}"

        _run([*compose, "up", "-d", "--no-build", "--pull", "never", "--wait"], env=env)
        _apply_schemas(workspace, env=env)

        gateway_port = _published_port(f"{_PROJECT}-gateway-service-1", "8080/tcp", env=env)
        web_port = _published_port(f"{_PROJECT}-web-service-1", "3000/tcp", env=env)
        env.update(
            {
                "SUBMITQUEUE_E2E_GATEWAY_URL": f"http://127.0.0.1:{gateway_port}",
                "SUBMITQUEUE_E2E_WEB_URL": f"http://127.0.0.1:{web_port}",
                "PLAYWRIGHT_OUTPUT_DIR": str(Path(env.get("TEST_UNDECLARED_OUTPUTS_DIR", context)) / "playwright"),
                "PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH": str(_chromium_executable(chromium)),
                "JS_BINARY__PATCH_NODE_FS": "0",
            }
        )
        _run([str(playwright), "test", "--config", str(playwright_root / "playwright.config.ts")], env=env)
        return 0
    except BaseException:
        _capture_compose_logs(compose, env=env)
        raise
    finally:
        if env.get("SKIP_CLEANUP") != "true":
            _run([*compose, "down", "-v", "--remove-orphans"], env=env, check=False)
            _run(
                [
                    "docker",
                    "image",
                    "rm",
                    "-f",
                    f"{_PROJECT}-gateway-service:latest",
                    f"{_PROJECT}-orchestrator-service:latest",
                    f"{_PROJECT}-runway-service:latest",
                    f"{_PROJECT}-web-service:latest",
                    f"{_PROJECT}-mysql:8.0",
                ],
                env=env,
                check=False,
            )
            shutil.rmtree(context, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
