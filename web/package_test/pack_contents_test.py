import sys
import json
import tarfile
from pathlib import Path


_EXPECTED_API_FILES = {
    "package/dist/api/submitqueue/gateway/proto/gateway_pb.d.ts",
    "package/dist/api/submitqueue/gateway/proto/gateway_pb.js",
    "package/README.md",
}

_EXPECTED_WEB_FILES = {
    "package/dist/index.d.ts",
    "package/dist/server.d.ts",
    "package/dist/testing.d.ts",
    "package/README.md",
}


def _missing_files(archive_path: Path, expected: set[str]) -> list[str]:
    with tarfile.open(archive_path) as archive:
        files = {member.name for member in archive.getmembers()}
    return sorted(expected - files)


def main() -> int:
    if len(sys.argv) != 3:
        raise RuntimeError("expected API and web package archives")

    missing = {
        str(archive): absent
        for archive, expected in (
            (Path(sys.argv[1]), _EXPECTED_API_FILES),
            (Path(sys.argv[2]), _EXPECTED_WEB_FILES),
        )
        if (absent := _missing_files(archive, expected))
    }
    if missing:
        for archive, files in missing.items():
            print(f"{archive} is missing: {', '.join(files)}", file=sys.stderr)
        return 1
    with tarfile.open(Path(sys.argv[2])) as archive:
        package_file = archive.extractfile("package/package.json")
        package = json.load(package_file)
        if "next" in package.get("peerDependencies", {}) or "next" in package.get("dependencies", {}):
            print("The presentation library must not require Next.js.", file=sys.stderr)
            return 1
        for member in archive.getmembers():
            if member.name.endswith((".js", ".d.ts")):
                source = archive.extractfile(member).read().decode("utf-8")
                if '"next/' in source or "'next/" in source:
                    print(f"{member.name} imports host-owned Next.js.", file=sys.stderr)
                    return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
