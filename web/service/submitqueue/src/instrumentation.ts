// Starts the metrics exporter at server startup so it is scrapeable before the first page request.
export async function register(): Promise<void> {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { resolveReferenceMeter } = await import("./server/observability");
    resolveReferenceMeter();
  }
}
