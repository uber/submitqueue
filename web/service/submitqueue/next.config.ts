import type { NextConfig } from "next";
import path from "node:path";

const nextConfig: NextConfig = {
  experimental: {
    authInterrupts: true,
  },
  // Pages already wait for their data, so resolve titles before streaming rather
  // than leaving the document untitled until streamed metadata arrives.
  htmlLimitedBots: /.*/,
  output: "standalone",
  outputFileTracingRoot: path.join(import.meta.dirname, "../.."),
  outputFileTracingExcludes: {
    "*": ["**/node_modules/**"],
  },
  transpilePackages: ["@submitqueue/api", "@submitqueue/web-submitqueue"],
};

export default nextConfig;
