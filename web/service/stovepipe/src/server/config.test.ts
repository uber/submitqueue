import { describe, expect, it } from "vitest";
import { loadConfiguration } from "./config";
const environment = {
  STOVEPIPE_URL: "http://127.0.0.1:8080",
  STOVEPIPE_ALLOW_PLAINTEXT: "true",
};
describe("Stovepipe host configuration", () => {
  it("preserves queue identities", () => {
    const queues = [
      { name: "repo/main" },
      { name: "repo/empty" },
    ];
    expect(
      loadConfiguration({
        ...environment,
        STOVEPIPE_WEB_QUEUES: JSON.stringify(queues),
      }).queues,
    ).toEqual(queues);
  });
  it("rejects missing or ambiguous queue configuration", () => {
    for (const queues of [
      [],
      [{}],
      [{ name: "" }],
      [{ name: 7 }],
      [{ name: "q" }, { name: "q" }],
      [null],
    ]) {
      expect(() =>
        loadConfiguration({
          ...environment,
          STOVEPIPE_WEB_QUEUES: JSON.stringify(queues),
        }),
      ).toThrow();
    }
  });
});
