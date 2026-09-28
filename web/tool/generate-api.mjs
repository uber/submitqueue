import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const webRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = resolve(webRoot, "..");
const generatedRoot = join(webRoot, "api", "src", "gen");
const check = process.argv.includes("--check");
const temporaryRoot = mkdtempSync(join(tmpdir(), "submitqueue-web-api-"));
const outputRoot = check ? join(temporaryRoot, "api", "src", "gen") : generatedRoot;
const buf = join(webRoot, "node_modules", ".bin", process.platform === "win32" ? "buf.cmd" : "buf");
const plugin = join(webRoot, "node_modules", ".bin", process.platform === "win32" ? "protoc-gen-es.cmd" : "protoc-gen-es");
const protoFiles = [
  "api/base/change/proto/change.proto",
  "api/base/mergestrategy/proto/mergestrategy.proto",
  "api/submitqueue/gateway/proto/gateway.proto",
];

function listFiles(root, current = root) {
  if (!existsSync(current)) {
    return [];
  }
  return readdirSync(current, { withFileTypes: true }).flatMap((entry) => {
    const path = join(current, entry.name);
    return entry.isDirectory() ? listFiles(root, path) : [relative(root, path)];
  }).sort();
}

function generate() {
  if (!existsSync(buf) || !existsSync(plugin)) {
    throw new Error("Buf and protoc-gen-es are not installed; run pnpm install from web/");
  }
  const template = check ? join(temporaryRoot, "buf.gen.yaml") : join(webRoot, "buf.gen.yaml");
  if (check) {
    writeFileSync(template, `version: v2\nclean: true\nplugins:\n  - local: protoc-gen-es\n    out: ${JSON.stringify(outputRoot)}\n    opt:\n      - target=ts\n      - import_extension=js\n`);
  }
  const path = [join(webRoot, "node_modules", ".bin"), process.env.PATH].filter(Boolean).join(process.platform === "win32" ? ";" : ":");
  const result = spawnSync(buf, [
    "generate",
    repositoryRoot,
    `--template=${template}`,
    ...protoFiles.flatMap((protoFile) => ["--path", join("..", protoFile)]),
  ], {
    cwd: webRoot,
    encoding: "utf8",
    env: { ...process.env, PATH: path },
  });
  if (result.status !== 0) {
    process.stderr.write(result.stderr);
    process.exit(result.status ?? 1);
  }
}

function verifyDrift() {
  const expectedFiles = listFiles(outputRoot);
  const actualFiles = listFiles(generatedRoot);
  const mismatches = [];
  if (expectedFiles.join("\n") !== actualFiles.join("\n")) {
    mismatches.push("generated file list differs");
  }
  for (const path of expectedFiles) {
    const actualPath = join(generatedRoot, path);
    if (!existsSync(actualPath) || readFileSync(join(outputRoot, path), "utf8") !== readFileSync(actualPath, "utf8")) {
      mismatches.push(path);
    }
  }
  if (mismatches.length > 0) {
    process.stderr.write(`Generated API is stale (${mismatches.join(", ")}). Run pnpm api:generate.\n`);
    process.exitCode = 1;
  }
}

try {
  generate();
  if (check) {
    verifyDrift();
  }
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}
