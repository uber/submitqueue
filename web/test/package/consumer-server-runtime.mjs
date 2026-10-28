import assert from "node:assert/strict";
import { createSubmitQueueWeb } from "@submitqueue/web-submitqueue/server";
import { createFakeGatewayReader } from "@submitqueue/web-submitqueue/extension/gateway/mock";
import { createStovepipeWeb } from "@submitqueue/web-stovepipe/server";

const submitqueue = createSubmitQueueWeb({ gateway: createFakeGatewayReader() });
const requests = await submitqueue.handle({ path: "/queue/demo-queue", search: {} });
assert.equal(requests.kind, "render");
assert.equal(requests.model.result.ok, true);
assert.doesNotThrow(() => JSON.stringify(requests.model));

const stovepipe = createStovepipeWeb({
  queues: [{ name: "demo-queue" }],
  service: {
    list: async () => ({ requests: [], nextPageToken: "" }),
    getRequestHistoryByID: async () => { throw new Error("unexpected history lookup"); },
    getProjectStatusByID: async () => { throw new Error("unexpected project lookup"); },
  },
});
const queue = await stovepipe.handle({ path: "/demo-queue", search: new URLSearchParams() });
assert.equal(queue.kind, "render");
assert.equal(queue.model.requests.ok, true);
assert.deepEqual(queue.model.requests.data.requests, []);
assert.doesNotThrow(() => JSON.stringify(queue.model));
