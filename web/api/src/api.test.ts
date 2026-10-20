import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  ListRequestSchema,
  SubmitQueueGateway,
} from "./gen/api/submitqueue/gateway/proto/gateway_pb.js";

describe("generated gateway API", () => {
  it("constructs list requests without losing int64 values", () => {
    const request = create(ListRequestSchema, {
      queue: "demo-queue",
      receivedLowerBound: { case: "receivedAtOrAfterMs", value: 1_700_000_000_000n },
      receivedUpperBound: { case: "receivedBeforeMs", value: 1_700_086_400_000n },
      pageSize: 50,
    });

    expect(request.queue).toBe("demo-queue");
    expect(request.receivedLowerBound).toEqual({ case: "receivedAtOrAfterMs", value: 1_700_000_000_000n });
    expect(request.receivedUpperBound).toEqual({ case: "receivedBeforeMs", value: 1_700_086_400_000n });
    expect(create(ListRequestSchema, { queue: "demo-queue", pageToken: "token" }).receivedLowerBound.case).toBeUndefined();
    expect(SubmitQueueGateway.methods.some((method) => method.localName === "list")).toBe(true);
  });
});
