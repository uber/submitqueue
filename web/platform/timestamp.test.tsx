import { act, cleanup, fireEvent, render, screen, waitFor, within } from "./test/rtl.js";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { Timestamp, TimeZoneProvider } from "./timestamp.js";

const value = Date.parse("2026-11-01T05:30:00.000Z");
const isoTimestamp = "2026-11-01T05:30:00.000Z";
const preferenceKey = "submitqueue.time-zone";

beforeEach(() => {
  const options = new Intl.DateTimeFormat().resolvedOptions();
  vi.spyOn(Intl.DateTimeFormat.prototype, "resolvedOptions").mockReturnValue({ ...options, timeZone: "America/New_York" });
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  window.localStorage.clear();
});

function renderTimestamps() {
  return render(<TimeZoneProvider><Timestamp value={value} /><Timestamp value={value + 3_600_000} /></TimeZoneProvider>);
}

async function openTimestamp(index = 0) {
  const trigger = screen.getAllByRole("button", { name: /timestamp options$/ })[index]!;
  await act(async () => { fireEvent.click(trigger); });
  const dialog = screen.getByRole("dialog", { name: "Timestamp" });
  await waitFor(() => expect(dialog).toBeVisible());
  return { trigger, dialog };
}

it("defaults to local time without saving a preference before the user chooses one", async () => {
  renderTimestamps();
  const { trigger, dialog } = await openTimestamp();
  expect(trigger).toHaveTextContent("Nov 1, 2026, 1:30:00 AM EDT");
  expect(within(dialog).getByRole("radio", { name: "Local" })).toBeChecked();
  expect(window.localStorage.getItem(preferenceKey)).toBeNull();
  expect(dialog).toHaveAccessibleDescription("Applies to all timestamps.");
});

it("shows local, UTC and ISO values without changing the selected display timezone", async () => {
  renderTimestamps();
  const { trigger, dialog } = await openTimestamp();
  expect(trigger).toHaveAttribute("aria-expanded", "true");
  expect(within(dialog).getByText("Local · America/New_York")).toBeVisible();
  expect(within(dialog).getByText("Nov 1, 2026, 5:30:00 AM UTC")).toBeVisible();
  expect(within(dialog).getByText(isoTimestamp)).toBeVisible();
  expect(within(dialog).getByRole("radio", { name: "Local" })).toBeChecked();
});

it("switches all timestamps through the existing preference and preserves daylight-saving instants", async () => {
  const { container } = renderTimestamps();
  const { dialog } = await openTimestamp();
  expect(screen.getByRole("button", { name: /1:30:00 AM EST — timestamp options$/ })).toBeVisible();
  fireEvent.click(within(dialog).getByRole("radio", { name: "UTC" }));
  expect(screen.getByRole("button", { name: /5:30:00 AM UTC — timestamp options$/ })).toBeVisible();
  expect(screen.getByRole("button", { name: /6:30:00 AM UTC — timestamp options$/ })).toBeVisible();
  expect(window.localStorage.getItem(preferenceKey)).toBe("utc");
  expect([...container.querySelectorAll("time")].map(time => time.dateTime)).toEqual([
    isoTimestamp, "2026-11-01T06:30:00.000Z",
  ]);
  fireEvent.click(within(dialog).getByRole("radio", { name: "Local" }));
  expect(screen.getByRole("button", { name: /1:30:00 AM EST — timestamp options$/ })).toBeVisible();
  expect(window.localStorage.getItem(preferenceKey)).toBe("browser");
});

it("honors an existing saved UTC preference when a new page mounts", async () => {
  window.localStorage.setItem(preferenceKey, "utc");
  const view = renderTimestamps();
  expect(screen.getByRole("button", { name: /5:30:00 AM UTC — timestamp options$/ })).toBeVisible();
  view.unmount();
  renderTimestamps();
  const { dialog } = await openTimestamp();
  expect(within(dialog).getByRole("radio", { name: "UTC" })).toBeChecked();
});

it("allows switching when browser storage is unavailable", async () => {
  vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => { throw new Error("Storage unavailable"); });
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new Error("Storage unavailable"); });
  renderTimestamps();
  const { dialog } = await openTimestamp();
  expect(within(dialog).getByRole("radio", { name: "Local" })).toBeChecked();
  fireEvent.click(within(dialog).getByRole("radio", { name: "UTC" }));
  expect(within(dialog).getByRole("radio", { name: "UTC" })).toBeChecked();
  expect(screen.getByRole("button", { name: /5:30:00 AM UTC — timestamp options$/ })).toBeVisible();
});

it("dismisses with Escape and returns focus to the timestamp", async () => {
  renderTimestamps();
  const { trigger } = await openTimestamp();
  fireEvent.keyUp(document, { key: "Escape", keyCode: 27 });
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(trigger).toHaveFocus();
  expect(trigger).toHaveAttribute("aria-expanded", "false");
});

it("closes from the borderless close control and restores focus", async () => {
  renderTimestamps();
  const { trigger, dialog } = await openTimestamp();
  fireEvent.click(within(dialog).getByRole("button", { name: "Close timestamp options" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(trigger).toHaveFocus();
});

it("dismisses when clicking outside or tabbing away without stealing focus", async () => {
  render(<TimeZoneProvider><Timestamp value={value} /><button>Other action</button></TimeZoneProvider>);
  await openTimestamp();
  fireEvent.mouseDown(document.body);
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  const { dialog } = await openTimestamp();
  act(() => within(dialog).getByRole("radio", { name: "Local" }).focus());
  const otherAction = screen.getByRole("button", { name: "Other action" });
  act(() => otherAction.focus());
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(otherAction).toHaveFocus();
});

it("keeps only the newly selected timestamp popover open", async () => {
  renderTimestamps();
  await openTimestamp();
  const next = screen.getAllByRole("button", { name: /timestamp options$/ })[1]!;
  fireEvent.mouseDown(next);
  fireEvent.click(next);
  await waitFor(() => expect(screen.getAllByRole("dialog")).toHaveLength(1));
  expect(next).toHaveAttribute("aria-expanded", "true");
  await waitFor(() => expect(within(screen.getByRole("dialog")).getByText("2026-11-01T06:30:00.000Z")).toBeVisible());
});

it("copies the ISO instant and restores the copy icon two seconds after showing success", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  renderTimestamps();
  const { dialog } = await openTimestamp();
  vi.useFakeTimers();
  await act(async () => {
    fireEvent.click(within(dialog).getByRole("button", { name: "Copy ISO timestamp" }));
  });
  expect(writeText).toHaveBeenCalledWith(isoTimestamp);
  expect(within(dialog).getByRole("button", { name: "Copied" })).toBeInTheDocument();
  expect(within(dialog).getByRole("status")).toHaveTextContent("Copied");
  act(() => vi.advanceTimersByTime(1999));
  expect(within(dialog).getByRole("button", { name: "Copied" })).toBeInTheDocument();
  act(() => vi.advanceTimersByTime(1));
  expect(within(dialog).getByRole("button", { name: "Copy ISO timestamp" })).toBeInTheDocument();
  expect(within(dialog).getByRole("status")).toBeEmptyDOMElement();
});

it("shows a selectable ISO value and useful feedback when clipboard access fails", async () => {
  renderTimestamps();
  const { dialog } = await openTimestamp();
  fireEvent.click(within(dialog).getByRole("button", { name: "Copy ISO timestamp" }));
  await waitFor(() => expect(within(dialog).getByRole("status")).toHaveTextContent("Unable to copy"));
  expect(within(dialog).getByText(isoTimestamp)).toBeVisible();
  expect(within(dialog).queryByRole("button", { name: "Copied" })).not.toBeInTheDocument();
});

it("does not carry copy feedback into a newly opened popover", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
  renderTimestamps();
  const { dialog } = await openTimestamp();
  fireEvent.click(within(dialog).getByRole("button", { name: "Copy ISO timestamp" }));
  await waitFor(() => expect(within(dialog).getByRole("button", { name: "Copied" })).toBeInTheDocument());
  fireEvent.click(within(dialog).getByRole("button", { name: "Close timestamp options" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  const reopened = await openTimestamp();
  expect(within(reopened.dialog).getByRole("button", { name: "Copy ISO timestamp" })).toBeInTheDocument();
  expect(within(reopened.dialog).getByRole("status")).toBeEmptyDOMElement();
});

it("preserves invalid values as noninteractive timestamps", () => {
  render(<TimeZoneProvider><Timestamp value="unknown" /></TimeZoneProvider>);
  expect(screen.getByText("unknown")).not.toHaveAttribute("datetime");
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});

it("retains the UTC fallback when rendered outside a timezone provider", () => {
  render(<Timestamp value={value} />);
  expect(screen.getByText("Nov 1, 2026, 5:30:00 AM UTC")).toHaveAttribute("datetime", isoTimestamp);
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
