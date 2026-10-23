import { cleanup, fireEvent, render, screen } from "./test/rtl.js";
import { afterEach, expect, it, vi } from "vitest";
import { Timestamp, TimeZoneProvider, TimeZoneToggle } from "./timestamp.js";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  window.localStorage.clear();
});

it("defaults to the browser zone, respects daylight saving, and switches every timestamp without changing its instant", () => {
  const options = new Intl.DateTimeFormat().resolvedOptions();
  vi.spyOn(Intl.DateTimeFormat.prototype, "resolvedOptions").mockReturnValue({ ...options, timeZone: "America/New_York" });
  const instants = ["2026-11-01T05:30:00.000Z", "2026-11-01T06:30:00.000Z"];
  const { container } = render(<TimeZoneProvider>
    <TimeZoneToggle />
    {instants.map(instant => <Timestamp key={instant} value={Date.parse(instant)} />)}
  </TimeZoneProvider>);
  const browser = screen.getByRole("option", { name: "Browser" });
  expect(browser).toHaveAttribute("aria-selected", "true");
  expect(browser).toHaveAttribute("title", "America/New_York");
  expect(screen.getByText("Nov 1, 2026, 1:30:00 AM EDT")).toBeVisible();
  expect(screen.getByText("Nov 1, 2026, 1:30:00 AM EST")).toBeVisible();
  fireEvent.click(screen.getByRole("option", { name: "UTC" }));
  expect(screen.getByRole("option", { name: "UTC" })).toHaveAttribute("aria-selected", "true");
  expect(screen.getByText("Nov 1, 2026, 5:30:00 AM UTC")).toBeVisible();
  expect(screen.getByText("Nov 1, 2026, 6:30:00 AM UTC")).toBeVisible();
  expect([...container.querySelectorAll("time")].map(time => time.dateTime)).toEqual(instants);
  fireEvent.click(browser);
  expect(screen.getByText("Nov 1, 2026, 1:30:00 AM EST")).toBeVisible();
});

it("preserves invalid timestamp values without inventing a date", () => {
  render(<TimeZoneProvider><Timestamp value="unknown" /></TimeZoneProvider>);
  expect(screen.getByText("unknown")).not.toHaveAttribute("datetime");
});

it("remembers UTC when a new page mounts", () => {
  const page = <TimeZoneProvider><TimeZoneToggle /><Timestamp value={0} /></TimeZoneProvider>;
  const first = render(page);
  fireEvent.click(screen.getByRole("option", { name: "UTC" }));
  first.unmount();
  render(page);
  expect(screen.getByRole("option", { name: "UTC" })).toHaveAttribute("aria-selected", "true");
  expect(screen.getByText("Jan 1, 1970, 12:00:00 AM UTC")).toBeVisible();
});

it("allows switching when browser storage is unavailable", () => {
  vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => { throw new Error("Storage unavailable"); });
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new Error("Storage unavailable"); });
  render(<TimeZoneProvider><TimeZoneToggle /><Timestamp value={0} /></TimeZoneProvider>);
  fireEvent.click(screen.getByRole("option", { name: "UTC" }));
  expect(screen.getByRole("option", { name: "UTC" })).toHaveAttribute("aria-selected", "true");
});
