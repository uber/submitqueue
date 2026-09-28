import type { NextConfig } from "next";
import path from "node:path";

const nextConfig: NextConfig = {
  experimental: {
    authInterrupts: true,
  },
  output: "standalone",
  outputFileTracingRoot: path.join(import.meta.dirname, "../.."),
  transpilePackages: ["@submitqueue/api", "@submitqueue/web-submitqueue"],
};

export default nextConfig;
