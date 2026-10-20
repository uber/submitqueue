import { Code, ConnectError, type UnaryRequest, type UnaryResponse } from "@connectrpc/connect";
import { SpanStatusCode, type Meter, type Tracer } from "@opentelemetry/api";
import { createSubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";
import { describe, expect, it, vi } from "vitest";
import { instrumentSubmitQueueWeb, measureGatewayRequests } from "./instrumentation";

function createHostFixture() {
  const gateway = createFakeGatewayReader();
  const web = createSubmitQueueWeb({ gateway });
  const span = { setAttribute: vi.fn(), setStatus: vi.fn(), recordException: vi.fn(), end: vi.fn() };
  const record = vi.fn();
  const createHistogram = vi.fn(() => ({ record }));
  const host = instrumentSubmitQueueWeb(web, { createHistogram } as unknown as Meter, { startSpan: () => span } as unknown as Tracer);
  return { gateway, web, host, span, record, createHistogram };
}

describe("SubmitQueue host instrumentation", () => {
  it("records page outcomes with the existing metric name, units and page tag", async () => {
    const fixture = createHostFixture();
    const result = await fixture.host.handle({ path: "/", search: {} });
    expect(result.kind).toBe("render");
    expect(fixture.createHistogram).toHaveBeenCalledWith("web.page_load.duration", expect.objectContaining({ unit: "ms" }));
    expect(fixture.record).toHaveBeenCalledWith(expect.any(Number), { page: "queues", outcome: "render" });
    expect(fixture.span.setAttribute).toHaveBeenCalledWith("web.page", "queues");
    expect(fixture.span.end).toHaveBeenCalledOnce();
  });

  it("marks handled gateway failures without exposing their raw cause in the model", async () => {
    const fixture = createHostFixture();
    vi.spyOn(fixture.gateway, "listQueues").mockRejectedValue(new ConnectError("private failure", Code.Unavailable));
    const result = await fixture.host.handle({ path: "/", search: {} });
    expect(result).toMatchObject({ kind: "render", model: { result: { ok: false } } });
    expect(fixture.record).toHaveBeenCalledWith(expect.any(Number), { page: "queues", outcome: "transient" });
    expect(fixture.span.setStatus).toHaveBeenCalledWith({ code: SpanStatusCode.ERROR });
    expect(JSON.stringify(result)).not.toContain("private failure");
  });

  it("preserves the result and executes once when telemetry delivery fails", async () => {
    const fixture = createHostFixture();
    fixture.record.mockImplementation(() => { throw new Error("metrics unavailable"); });
    fixture.span.setAttribute.mockImplementation(() => { throw new Error("tracing unavailable"); });
    const handle = vi.spyOn(fixture.web, "handle");
    expect((await fixture.host.handle({ path: "/", search: {} })).kind).toBe("render");
    expect(handle).toHaveBeenCalledOnce();
    expect(fixture.span.end).toHaveBeenCalledOnce();
  });
});

describe("SubmitQueue transport metrics", () => {
  it.each([
    ["List", "list"],
    ["ListQueues", "queues"],
    ["GetRequestSummaryByID", "summary"],
    ["GetRequestHistoryByID", "history"],
    ["GetRequestHistoryByChangeURI", "change"],
  ])("measures %s at the transport boundary", async (method, operation) => {
    const record = vi.fn();
    const meter = { createHistogram: vi.fn(() => ({ record })) } as unknown as Meter;
    const request = { method: { name: method } } as unknown as UnaryRequest;
    const response = {} as UnaryResponse;
    const next = vi.fn(async () => response);
    expect(await measureGatewayRequests(meter)(next)(request)).toBe(response);
    expect(next).toHaveBeenCalledExactlyOnceWith(request);
    expect(record).toHaveBeenCalledWith(expect.any(Number), { operation, outcome: "success" });
  });

  it("preserves RPC errors and records their classification even if metrics fail", async () => {
    const error = new ConnectError("private failure", Code.Unavailable);
    const record = vi.fn(() => { throw new Error("metrics unavailable"); });
    const meter = { createHistogram: () => ({ record }) } as unknown as Meter;
    const next = vi.fn(async () => { throw error; });
    const request = { method: { name: "List" } } as unknown as UnaryRequest;
    await expect(measureGatewayRequests(meter)(next)(request)).rejects.toBe(error);
    expect(next).toHaveBeenCalledOnce();
    expect(record).toHaveBeenCalledWith(expect.any(Number), { operation: "list", outcome: "unavailable" });
  });

  it("does not mix readiness probes into the read-operation metrics", async () => {
    const createHistogram = vi.fn();
    const meter = { createHistogram } as unknown as Meter;
    const request = { method: { name: "Ping" } } as unknown as UnaryRequest;
    const response = {} as UnaryResponse;
    expect(await measureGatewayRequests(meter)(async () => response)(request)).toBe(response);
    expect(createHistogram).not.toHaveBeenCalled();
  });
});
