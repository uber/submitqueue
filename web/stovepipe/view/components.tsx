"use client";

import { useState, type ReactNode } from "react";
import { useWebNavigation, type WebLinkProps } from "./navigation.js";
import type { ReadResult } from "../entity/index.js";

export function Link(props: WebLinkProps) {
  const { Link: HostLink } = useWebNavigation();
  return HostLink ? <HostLink {...props} /> : <a {...props} />;
}
export function Degree({ value }: { value: number | null }) {
  return <span className="sq-status" data-tone={value === null ? "neutral" : value === 0 ? "success" : "danger"}>{value === null ? "No recorded result" : value === 0 ? "Green (0)" : `Breakage degree: ${value}`}</span>;
}
export function Table({ label, headers, children }: { label: string; headers: string[]; children: ReactNode }) {
  return <div className="sq-table-scroll"><table aria-label={label}><thead><tr>{headers.map(header => <th key={header}>{header}</th>)}</tr></thead><tbody>{children}</tbody></table></div>;
}
export function Section<T>({ result, children }: { result: ReadResult<T>; children(data: T): ReactNode }) {
  const [previous, setPrevious] = useState<T | null>(result.ok ? result.data : null);
  if (result.ok && result.data !== previous) setPrevious(result.data);
  const data = result.ok ? result.data : previous;
  return <>{!result.ok && <div role="alert" className="sq-error">{result.error}{previous !== null && <p>Showing the last successful result.</p>}</div>}{data !== null ? children(data) : null}</>;
}
