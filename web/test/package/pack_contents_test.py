import json
import posixpath
import re
import sys
import tarfile
from pathlib import Path


# The module reads the gateway through a structural contract; the transport,
# generated protos, and web framework belong to the host.
_FORBIDDEN_IMPORTS = ("@submitqueue/api", "@submitqueue/web-submitqueue", "@connectrpc/", "@bufbuild/", "next")

# Modules the client-safe root entry must never reach, directly or transitively.
_CLIENT_FORBIDDEN_MODULES = ("package/dist/controller/", "package/dist/module.js", "package/dist/server.js")
_CLIENT_FORBIDDEN_IMPORTS = ("node:",)

# Static `from`, side-effect `import "x"`, dynamic `import("x")`, and `require("x")` specifiers.
_IMPORT_PATTERN = re.compile(r"""(?:\bfrom|\bimport|\bimport\s*\(|\brequire\s*\()\s*["']([^"']+)["']""")


def _client_entry_violations(archive: tarfile.TarFile) -> list[str]:
    sources = {
        member.name: archive.extractfile(member).read().decode("utf-8")
        for member in archive.getmembers()
        if member.name.endswith(".js")
    }
    violations = []
    pending, seen = ["package/dist/index.js"], set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        if name.startswith(_CLIENT_FORBIDDEN_MODULES):
            violations.append(name)
            continue
        for specifier in _IMPORT_PATTERN.findall(sources.get(name, "")):
            if specifier.startswith("."):
                pending.append(posixpath.normpath(posixpath.join(posixpath.dirname(name), specifier)))
            elif specifier.startswith(_CLIENT_FORBIDDEN_IMPORTS):
                violations.append(f"{name} -> {specifier}")
    return violations


_EXPECTED_FILES = {
    "package/view/styles.css",
    "package/dist/index.d.ts",
    "package/dist/server.d.ts",
    "package/dist/extension/cursor/hmac/index.d.ts",
    "package/dist/extension/gateway/mock/index.d.ts",
    "package/README.md",
}


def _imports_forbidden(specifier: str) -> bool:
    return any(
        specifier.startswith(forbidden) if forbidden.endswith("/") else specifier == forbidden or specifier.startswith(forbidden + "/")
        for forbidden in _FORBIDDEN_IMPORTS
    )


def main() -> int:
    if len(sys.argv) not in (2, 3):
        raise RuntimeError("expected the module package archive")

    with tarfile.open(Path(sys.argv[1])) as archive:
        expected = _EXPECTED_FILES if len(sys.argv) == 2 else {"package/dist/index.d.ts", "package/dist/server.d.ts", "package/README.md"}
        missing = sorted(expected - {member.name for member in archive.getmembers()})
        if missing:
            print(f"The package is missing: {', '.join(missing)}", file=sys.stderr)
            return 1
        package = json.load(archive.extractfile("package/package.json"))
        if any(_imports_forbidden(name) for group in ("peerDependencies", "dependencies", "optionalDependencies") for name in package.get(group, {})):
            print("The core module must not require framework or transport dependencies.", file=sys.stderr)
            return 1
        stylesheet = package.get("exports", {}).get("./styles.css")
        if stylesheet != "./view/styles.css" or stylesheet not in package.get("sideEffects", []):
            print("The module must export and preserve its default stylesheet.", file=sys.stderr)
            return 1
        if violations := _client_entry_violations(archive):
            print(f"The client entry reaches server-only code: {', '.join(violations)}", file=sys.stderr)
            return 1
        for member in archive.getmembers():
            if member.name.endswith((".js", ".d.ts")):
                source = archive.extractfile(member).read().decode("utf-8")
                for specifier in _IMPORT_PATTERN.findall(source):
                    if _imports_forbidden(specifier):
                        print(f"{member.name} imports {specifier}, which the host owns.", file=sys.stderr)
                        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
