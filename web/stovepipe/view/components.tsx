"use client";

import { useState, type ReactNode } from "react";
import { useWebNavigation, type WebLinkProps } from "@submitqueue/web-platform";
import type { ReadResult } from "../entity/index.js";
import { Notification, KIND } from "baseui/notification";
import { StyledLink } from "baseui/link";
import { ParagraphSmall } from "baseui/typography";

export { Table } from "@submitqueue/web-platform";

export function Link({ className = "", scroll, ...props }: WebLinkProps) {
  const { Link: HostLink } = useWebNavigation();
  return <StyledLink $as={HostLink ?? "a"} className={className} {...(HostLink ? { scroll } : {})} {...props} />;
}
export function Section<T>({
  result,
  cacheKey,
  children,
}: {
  result: ReadResult<T>;
  cacheKey?: string;
  children(data: T | null): ReactNode;
}) {
  const [previous, setPrevious] = useState({ cacheKey, data: result.ok ? result.data : null });
  const data = result.ok ? result.data : previous.cacheKey === cacheKey ? previous.data : null;
  if (previous.cacheKey !== cacheKey || previous.data !== data) setPrevious({ cacheKey, data });
  return (
    <>
      {!result.ok && (
        <Notification kind={KIND.negative} overrides={{ Body: { props: { role: "alert" }, style: { width: "auto" } } }}>
          {result.error}
          {data !== null && <ParagraphSmall>Showing the last successful result.</ParagraphSmall>}
        </Notification>
      )}
      {children(data)}
    </>
  );
}
