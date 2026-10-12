load("@aspect_rules_js//npm:defs.bzl", "npm_package")
load("@aspect_rules_ts//ts:defs.bzl", "ts_config", "ts_project")

def web_library(package, vitest, eslint, deps, test_deps = [], assets = []):
    """Builds, packages and checks a TypeScript library with the workspace toolchain."""
    sources = native.glob(
        ["**/*.ts", "**/*.tsx"],
        exclude = [
            "**/*.test.ts",
            "**/*.test.tsx",
            "dist/**",
            "node_modules/**",
            "test-setup.ts",
            "vitest.config.ts",
            "test/**",
        ],
    )
    tests = native.glob(
        ["**/*.test.ts", "**/*.test.tsx", "test/**"],
        allow_empty = True,
    )
    config = native.glob(["test-setup.ts"], allow_empty = True) + ["vitest.config.ts"]
    ts_config(name = "tsconfig", src = "tsconfig.json")
    ts_config(
        name = "tsconfig_build",
        src = "tsconfig.build.json",
        deps = [":tsconfig"],
    )
    ts_project(
        name = "lib",
        srcs = sources,
        declaration = True,
        declaration_map = True,
        no_emit = False,
        out_dir = "dist",
        root_dir = ".",
        transpiler = "tsc",
        tsconfig = ":tsconfig_build",
        deps = deps,
    )
    ts_project(
        name = "typecheck",
        srcs = sources + tests + config,
        declaration = True,
        no_emit = True,
        tsconfig = ":tsconfig",
        deps = deps + test_deps,
    )
    npm_package(
        name = "pkg",
        package = package,
        packable = True,
        srcs = [":lib", ":lib_types", "package.json"] + assets,
    )
    vitest.vitest_test(
        name = "test",
        args = ["run", "--config", "vitest.config.ts"],
        chdir = native.package_name(),
        data = sources + tests + config + [
            ":node_modules",
            ":node_modules/vitest",
            "//web:node_modules/react",
            "//web:node_modules/react-dom",
            # jest-dom resolves its vitest peer from the workspace root.
            "//web:node_modules/vitest",
            "//web:node_modules/@opentelemetry/api",
        ] + deps,
    )
    eslint.eslint_test(
        name = "lint",
        args = ["."],
        chdir = native.package_name(),
        data = sources + tests + config + [
            "eslint.config.mjs",
            "//web/tool:eslint_config",
            "//web/service/submitqueue:node_modules/eslint",
            "//web/service/submitqueue:node_modules/eslint-config-next",
            "//web:node_modules/next",
        ],
    )
