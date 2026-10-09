import { describe, expect, it } from "vitest";
import { loadConfiguration } from "./config";
const environment = { STOVEPIPE_URL: "http://127.0.0.1:8080", STOVEPIPE_ALLOW_PLAINTEXT: "true" };
describe("Stovepipe host configuration", () => {
  it("preserves queue and project identities", () => {
    const queues = [{ name: "repo/main", projects: ["project/a"] }, { name: "repo/empty", projects: ["project/b"] }];
    expect(loadConfiguration({ ...environment, STOVEPIPE_WEB_QUEUES: JSON.stringify(queues) }).queues).toEqual(queues);
  });
  it("rejects missing or ambiguous queue/project configuration", () => {
    for (const queues of [[], [{ name: "q", projects: [] }], [{ name: "q", projects: ["a", "a"] }], [{ name: "q", projects: ["a"] }, { name: "q", projects: ["a"] }], [{ name: "q", projects: [7] }]]) {
      expect(() => loadConfiguration({ ...environment, STOVEPIPE_WEB_QUEUES: JSON.stringify(queues) })).toThrow();
    }
  });
});
