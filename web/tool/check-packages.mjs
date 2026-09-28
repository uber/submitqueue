import { spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const temporaryRoot = mkdtempSync(join(tmpdir(), "submitqueue-web-packages-"));

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: "utf8",
    env: options.env ?? process.env,
    stdio: options.capture ? "pipe" : "inherit",
  });
  if (result.status !== 0) {
    if (options.capture) {
      process.stderr.write(result.stdout ?? "");
      process.stderr.write(result.stderr ?? "");
    }
    throw new Error(`${command} ${args.join(" ")} failed`);
  }
}

try {
  run("corepack", [
    "pnpm@10.17.1",
    "--filter",
    "@submitqueue/api",
    "pack",
    "--pack-destination",
    temporaryRoot,
  ]);
  run("corepack", [
    "pnpm@10.17.1",
    "--filter",
    "@submitqueue/web-submitqueue",
    "pack",
    "--pack-destination",
    temporaryRoot,
  ]);

  const archives = readdirSync(temporaryRoot).filter((name) => name.endsWith(".tgz"));
  const apiArchive = archives.find((name) => name.includes("submitqueue-api-"));
  const libraryArchive = archives.find((name) => name.includes("web-submitqueue-"));
  if (!apiArchive || !libraryArchive) {
    throw new Error(`expected API and library archives, found: ${archives.join(", ")}`);
  }

  writeFileSync(
    join(temporaryRoot, "package.json"),
    `${JSON.stringify(
      {
        private: true,
        type: "module",
        dependencies: {
          "@bufbuild/protobuf": "2.15.0",
          "@connectrpc/connect": "2.1.1",
          "@submitqueue/api": `file:${join(temporaryRoot, apiArchive)}`,
          "@submitqueue/web-submitqueue": `file:${join(temporaryRoot, libraryArchive)}`,
          "@types/react": "19.1.16",
          "@types/react-dom": "19.1.9",
          next: "16.3.6",
          react: "19.2.0",
          "react-dom": "19.2.0",
          typescript: "5.9.3",
        },
        pnpm: {
          overrides: {
            "@submitqueue/api": `file:${join(temporaryRoot, apiArchive)}`,
          },
        },
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(
    join(temporaryRoot, "consumer.ts"),
    [
      'import { RequestList } from "@submitqueue/web-submitqueue";',
      'import { loadRequestList } from "@submitqueue/web-submitqueue/server";',
      'import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/testing";',
      "void RequestList;",
      "void loadRequestList;",
      "void createFakeGatewayReader;",
      "",
    ].join("\n"),
  );
  writeFileSync(
    join(temporaryRoot, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          module: "ESNext",
          moduleResolution: "Bundler",
          noEmit: true,
          skipLibCheck: true,
          strict: true,
          target: "ES2023",
        },
        include: ["consumer.ts"],
      },
      null,
      2,
    )}\n`,
  );

  run(
    "corepack",
    ["pnpm@10.17.1", "install", "--ignore-scripts", "--no-frozen-lockfile"],
    {
      cwd: temporaryRoot,
      capture: true,
      env: { ...process.env, npm_config_registry: "https://registry.npmjs.org" },
    },
  );
  run("corepack", ["pnpm@10.17.1", "exec", "tsc"], { cwd: temporaryRoot });
  console.log("package consumer imports passed");
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}
