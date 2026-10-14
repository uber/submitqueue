import { AsyncLocalStorage } from "node:async_hooks";
import { context, trace, ROOT_CONTEXT, SpanStatusCode, type Context, type ContextManager, type Span, type Tracer } from "@opentelemetry/api";
import { afterEach, describe, expect, it, vi } from "vitest";
import { observeTelemetry, withSpan } from "./instrumentation.js";

function createSpanFixture() {
  return { setAttribute: vi.fn(), setStatus: vi.fn(), recordException: vi.fn(), end: vi.fn() };
}

afterEach(() => { context.disable(); });

describe("reference host OpenTelemetry spans", () => {
  it("preserves results and errors while ending each span once", async () => {
    const span = createSpanFixture();
    const tracer = { startSpan: vi.fn(() => span) } as unknown as Tracer;
    const value = { page: "queue" };
    const execute = vi.fn(async () => value);
    expect(await withSpan(tracer, "page", execute)).toBe(value);
    expect(execute).toHaveBeenCalledOnce();
    expect(span.end).toHaveBeenCalledOnce();

    const error = new Error("original failure");
    await expect(withSpan(tracer, "page", async () => { throw error; })).rejects.toBe(error);
    expect(span.recordException).toHaveBeenCalledWith(error);
    expect(span.setStatus).toHaveBeenCalledWith({ code: SpanStatusCode.ERROR });
    expect(span.end).toHaveBeenCalledTimes(2);
  });

  it("continues exactly once when starting or finishing telemetry fails", async () => {
    const failure = () => { throw new Error("telemetry unavailable"); };
    const span = createSpanFixture();
    span.end.mockImplementation(failure);
    span.recordException.mockImplementation(failure);
    span.setStatus.mockImplementation(failure);
    const tracer = { startSpan: vi.fn(() => span).mockImplementationOnce(failure) } as unknown as Tracer;
    const execute = vi.fn(async () => "page");
    expect(await withSpan(tracer, "page", execute)).toBe("page");
    expect(await withSpan(tracer, "page", execute)).toBe("page");
    expect(execute).toHaveBeenCalledTimes(2);
    const error = new Error("business failure");
    await expect(withSpan(tracer, "page", async () => { throw error; })).rejects.toBe(error);
    expect(() => observeTelemetry(failure)).not.toThrow();
  });

  it("keeps overlapping requests in their native active span across awaits", async () => {
    const storage = new AsyncLocalStorage<Context>();
    const manager: ContextManager = {
      active: () => storage.getStore() ?? ROOT_CONTEXT,
      with: (ctx, fn, thisArg, ...args) => storage.run(ctx, () => fn.call(thisArg, ...args)),
      bind: (_ctx, target) => target,
      enable: () => manager,
      disable: () => { storage.disable(); return manager; },
    };
    context.setGlobalContextManager(manager);
    const spans = [createSpanFixture(), createSpanFixture()];
    const tracer = { startSpan: vi.fn().mockReturnValueOnce(spans[0]).mockReturnValueOnce(spans[1]) } as unknown as Tracer;
    let release!: () => void;
    const ready = new Promise<void>(resolve => { release = resolve; });
    const pending = spans.map((span, index) => withSpan(tracer, "page", async active => {
      expect(active).toBe(span);
      expect(trace.getSpan(context.active())).toBe(span as unknown as Span);
      await ready;
      expect(trace.getSpan(context.active())).toBe(span as unknown as Span);
      return index;
    }));
    release();
    expect(await Promise.all(pending)).toEqual([0, 1]);
    expect(trace.getSpan(context.active())).toBeUndefined();
  });
});
