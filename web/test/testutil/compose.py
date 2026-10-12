import fcntl
import subprocess
import tempfile
from pathlib import Path

from python.runfiles import runfiles


def compose_command() -> list[str]:
    executable = runfiles.Create().Rlocation(
        "rules_docker_compose/docker_compose/current_docker_compose_toolchain/docker-compose"
    )
    if executable is None:
        raise RuntimeError("The pinned Compose executable is missing from Bazel runfiles")
    return [executable]


def load_project_images(
    loaders: list[Path], tags: dict[str, str], *, env: dict[str, str]
) -> None:
    # Shared loader tags are locked until each stack has its own image tags.
    with (Path(tempfile.gettempdir()) / "submitqueue-web-e2e-image.lock").open(
        "w", encoding="utf-8"
    ) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for loader in loaders:
            subprocess.run([str(loader)], env=env, check=True)
        for source, destination in tags.items():
            subprocess.run(["docker", "tag", source, destination], env=env, check=True)


def apply_schemas(groups: list[tuple[Path, str]], *, env: dict[str, str]) -> None:
    for folder, container in groups:
        for schema in sorted(folder.glob("*.sql")):
            with schema.open(encoding="utf-8") as statements:
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        "-i",
                        container,
                        "mysql",
                        "-uroot",
                        "-proot",
                        "submitqueue",
                    ],
                    env=env,
                    stdin=statements,
                    check=True,
                    text=True,
                )


def published_port(container: str, port: str, *, env: dict[str, str]) -> str:
    address = subprocess.check_output(
        ["docker", "port", container, port], env=env, text=True
    )
    return address.splitlines()[0].rsplit(":", 1)[-1]
