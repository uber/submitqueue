import type { NextConfig } from "next";
import path from "node:path";

const nextConfig: NextConfig = {
  experimental: {
    authInterrupts: true,
  },
  output: "standalone",
  outputFileTracingRoot: path.join(import.meta.dirname, "../.."),
  outputFileTracingExcludes: {
    "*": ["**/node_modules/**"],
  },
  transpilePackages: ["@submitqueue/api", "@submitqueue/web-submitqueue"],
};

export default nextConfig;
