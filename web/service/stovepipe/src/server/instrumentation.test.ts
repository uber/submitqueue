import { SpanStatusCode, type Meter, type Tracer } from "@opentelemetry/api";
import { createStovepipeWeb } from "@submitqueue/web-stovepipe/server";
import { describe, expect, it, vi } from "vitest";
import { instrumentStovepipeWeb } from "./instrumentation";

function createHostFixture() {
  const list = vi.fn().mockResolvedValue({ requests: [], nextPageToken: "" });
  const getRequestHistoryByID = vi.fn().mockRejectedValue(new Error("private failure"));
  const web = createStovepipeWeb({ queues: [{ name: "demo" }], service: { list, getRequestHistoryByID } });
  const record = vi.fn();
  const add = vi.fn();
  const span = { setAttribute: vi.fn(), setStatus: vi.fn(), recordException: vi.fn(), end: vi.fn() };
  const createHistogram = vi.fn(() => ({ record }));
  const createCounter = vi.fn(() => ({ add }));
  const host = instrumentStovepipeWeb(web,
    { createHistogram, createCounter } as unknown as Meter,
    { startSpan: () => span } as unknown as Tracer);
  return { host, list, web, record, add, span, createHistogram, createCounter };
}

describe("Stovepipe host instrumentation", () => {
  it("records successful page loads under the existing metric names", async () => {
    const fixture = createHostFixture();
    const result = await fixture.host.handle({ path: "/demo", search: new URLSearchParams() });
    expect(result).toMatchObject({ kind: "render", model: { requests: { ok: true } } });
    expect(fixture.createHistogram).toHaveBeenCalledWith("stovepipe.web.page_load_duration", expect.objectContaining({ unit: "ms" }));
    expect(fixture.createCounter).toHaveBeenCalledWith("stovepipe.web.page_loads");
    expect(fixture.record).toHaveBeenCalledWith(expect.any(Number), { outcome: "render" });
    expect(fixture.add).toHaveBeenCalledWith(1, { outcome: "render" });
    expect(fixture.span.end).toHaveBeenCalledOnce();
  });

  it.each(["/demo", "/demo/request/1"])("marks handled section failures at %s as errors", async path => {
    const fixture = createHostFixture();
    fixture.list.mockRejectedValue(new Error("private failure"));
    const result = await fixture.host.handle({ path, search: new URLSearchParams() });
    expect(result.kind).toBe("render");
    expect(fixture.span.setStatus).toHaveBeenCalledWith({ code: SpanStatusCode.ERROR });
    expect(fixture.record).toHaveBeenCalledWith(expect.any(Number), { outcome: "error" });
    expect(JSON.stringify(result)).not.toContain("private failure");
  });

  it("does not repeat a load or replace its result when metrics fail", async () => {
    const fixture = createHostFixture();
    fixture.record.mockImplementation(() => { throw new Error("metrics unavailable"); });
    fixture.add.mockImplementation(() => { throw new Error("metrics unavailable"); });
    const result = await fixture.host.handle({ path: "/demo", search: new URLSearchParams() });
    expect(result.kind).toBe("render");
    expect(fixture.list).toHaveBeenCalledOnce();
    expect(fixture.span.end).toHaveBeenCalledOnce();
  });

  it("preserves unexpected failures even when recording fails", async () => {
    const fixture = createHostFixture();
    const error = new Error("business failure");
    const handle = vi.spyOn(fixture.web, "handle").mockRejectedValue(error);
    fixture.span.recordException.mockImplementation(() => { throw new Error("tracing unavailable"); });
    await expect(fixture.host.handle({ path: "/demo", search: new URLSearchParams() })).rejects.toBe(error);
    expect(handle).toHaveBeenCalledOnce();
    expect(fixture.record).toHaveBeenCalledWith(expect.any(Number), { outcome: "exception" });
  });
});
