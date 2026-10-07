import { SpanStatusCode, type Meter, type Tracer } from "@opentelemetry/api";
import type { Logger } from "pino";
import { beginOperation, defaultMeter, defaultTracer, silentLogger } from "./core/observability.js";
import { WebPaths } from "./core/paths.js";
import { parseRoute, type SubmitQueueWebRoute, type WebSearchParams } from "./core/route.js";
import { loadChangePage } from "./controller/change.js";
import { loadDirectoryPage, loadQueueDirectory } from "./controller/directory.js";
import type { GatewayLoadOptions } from "./controller/gateway-call.js";
import { notFound, type PageDependencies, type SubmitQueueWebResult } from "./controller/page.js";
import { loadQueuePage } from "./controller/queue.js";
import { loadRequestPage } from "./controller/request.js";
import type { LoadResult, QueueModel } from "./entity/index.js";
import type { CursorCodec } from "./extension/cursor/index.js";
import type { GatewayErrorClassifier, SubmitQueueGateway } from "./extension/gateway/index.js";

export type { SubmitQueueWebResult } from "./controller/page.js";

/** Per-request values the host passes to every load. */
export interface SubmitQueueRequestContext {
  /** Identity the host authenticated; opaque to the module. */
  principal?: unknown;
  /** Correlation ID for logs and traces. */
  requestId?: string;
  /** Aborted when the viewer's request is abandoned. */
  signal?: AbortSignal;
}

/** One page request, in framework-neutral terms. */
export interface SubmitQueueWebRequest {
  /** URL path from the host root, still percent-encoded, starting with `/`. */
  path: string;
  search: WebSearchParams;
  principal?: unknown;
  /** Correlation ID; generated when absent. */
  requestId?: string;
  signal?: AbortSignal;
}

/** Caches successful catalogs per key; the key must separate viewers whose gateway visibility differs. */
export interface QueueCatalogCache {
  ttlMs: number;
  key(context: SubmitQueueRequestContext): string;
}

export interface SubmitQueueWebOptions {
  /** The gateway, or a resolver called once per load; resolve per request when visibility depends on the viewer. */
  gateway: SubmitQueueGateway | ((context: SubmitQueueRequestContext) => SubmitQueueGateway);
  /** Signs snapshot-page cursors. */
  cursors: CursorCodec;
  /** Builds links; its base path is where the host mounts the UI. */
  paths?: WebPaths;
  /** Decides whether the principal may see a route; `false` is reported as `forbidden`. */
  authorize?(principal: unknown, route: SubmitQueueWebRoute): boolean | Promise<boolean>;
  /** Maps transport errors to codes; defaults to gRPC status codes (Connect, grpc-js). */
  classifyError?: GatewayErrorClassifier;
  /** Caches the queue catalog (`ListQueues`) across loads; absent means every load reads it. */
  queuesCache?: QueueCatalogCache;
  logger?: Logger;
  meter?: Meter;
  tracer?: Tracer;
  now?: () => number;
  newRequestId?: () => string;
}

export interface SubmitQueueWeb {
  /** Serves one page request; never throws for expected outcomes such as missing resources. */
  handle(request: SubmitQueueWebRequest): Promise<SubmitQueueWebResult>;
}

type QueueCatalogLoader = (gateway: SubmitQueueGateway, context: SubmitQueueRequestContext) => Promise<LoadResult<QueueModel[]>>;

function cacheQueueCatalog(load: QueueCatalogLoader, cache: QueueCatalogCache, now: () => number): QueueCatalogLoader {
  const entries = new Map<string, { expiresAtMs: number; catalog: LoadResult<QueueModel[]> }>();
  return async (gateway, context) => {
    const key = cache.key(context);
    const cached = entries.get(key);
    if (cached && cached.expiresAtMs > now()) {
      return cached.catalog;
    }
    const catalog = await load(gateway, context);
    const loadedAtMs = now();
    for (const [cachedKey, entry] of entries) {
      if (entry.expiresAtMs <= loadedAtMs) {
        entries.delete(cachedKey);
      }
    }
    if (catalog.ok) {
      entries.set(key, { expiresAtMs: loadedAtMs + cache.ttlMs, catalog });
    } else {
      entries.delete(key);
    }
    return catalog;
  };
}

function pathSegments(path: string): string[] {
  return path.split("/").filter((segment, index, all) => index > 0 && !(segment === "" && index === all.length - 1));
}

/**
 * Builds the SubmitQueue web UI. The host routes every page path to
 * `handle` and renders a `render` result with `SubmitQueueApp`. Nothing is
 * cached unless `queuesCache` is set.
 */
export function createSubmitQueueWeb(options: SubmitQueueWebOptions): SubmitQueueWeb {
  const paths = options.paths ?? new WebPaths();
  const mount = paths.basePath.split("/").filter(Boolean);
  const now = () => options.now?.() ?? Date.now();
  const logger = options.logger ?? silentLogger;
  const meter = options.meter ?? defaultMeter();
  const tracer = options.tracer ?? defaultTracer();
  const newRequestId = options.newRequestId ?? (() => globalThis.crypto.randomUUID());
  const baseGatewayOptions: GatewayLoadOptions = {
    ...(options.classifyError ? { classifyError: options.classifyError } : {}),
    meter,
    tracer,
  };
  const readQueues: QueueCatalogLoader = (gateway, context) =>
    loadQueueDirectory(gateway, { ...baseGatewayOptions, logger: logger.child({ requestId: context.requestId }) });
  const loadQueues = options.queuesCache ? cacheQueueCatalog(readQueues, options.queuesCache, now) : readQueues;

  async function load(route: SubmitQueueWebRoute, context: SubmitQueueRequestContext): Promise<SubmitQueueWebResult> {
    const gateway = typeof options.gateway === "function" ? options.gateway(context) : options.gateway;
    const gatewayOptions = { ...baseGatewayOptions, logger: logger.child({ requestId: context.requestId }) };
    const catalog = await loadQueues(gateway, context);
    const deps: PageDependencies = { gateway, cursors: options.cursors, paths, now, gatewayOptions, catalog };
    if (route.kind === "queues") {
      return loadDirectoryPage(deps);
    }
    if (!route.queue || (catalog.ok && !catalog.data.some(queue => queue.name === route.queue))) {
      return notFound;
    }
    const search = route.search ?? {};
    switch (route.kind) {
      case "queue":
        return loadQueuePage(deps, route.queue, search);
      case "request":
        return route.sqid ? loadRequestPage(deps, route.queue, route.sqid, search) : notFound;
      case "change":
        return loadChangePage(deps, route.queue, route.reference, search);
    }
  }

  return {
    async handle(request) {
      const segments = pathSegments(request.path);
      if (!mount.every((segment, index) => segments[index] === segment)) {
        return notFound;
      }
      const route = parseRoute(segments.slice(mount.length), request.search);
      if (route === null) {
        return notFound;
      }
      const requestId = request.requestId ?? newRequestId();
      const requestLogger = logger.child({ requestId });
      if (options.authorize && !(await options.authorize(request.principal, route))) {
        requestLogger.info({ path: request.path }, "Web page request was not authorized");
        return { kind: "forbidden" };
      }
      const context: SubmitQueueRequestContext = {
        requestId,
        ...(request.principal === undefined ? {} : { principal: request.principal }),
        ...(request.signal ? { signal: request.signal } : {}),
      };
      const operation = beginOperation(meter, "web.page_load", { page: route.kind });
      return tracer.startActiveSpan("web.page_load", { attributes: { "web.page": route.kind } }, async (span): Promise<SubmitQueueWebResult> => {
        try {
          const result = await load(route, context);
          operation.end(result.kind);
          span.setAttribute("web.outcome", result.kind);
          return result;
        } catch (cause) {
          operation.end("exception");
          span.recordException(cause instanceof Error ? cause : String(cause));
          span.setStatus({ code: SpanStatusCode.ERROR });
          requestLogger.error({ path: request.path, err: cause }, "Web page load failed");
          throw cause;
        } finally {
          span.end();
        }
      });
    },
  };
}
