"use client";

import { useState, type ReactNode } from "react";
import { useWebNavigation, type WebLinkProps } from "@submitqueue/web-platform";
import type { ReadResult } from "../entity/index.js";
import { Notification, KIND } from "baseui/notification";
import { StyledLink } from "baseui/link";
import { ParagraphSmall } from "baseui/typography";

export { Table } from "@submitqueue/web-platform";

export function Link({ className = "", ...props }: WebLinkProps) {
  const { Link: HostLink } = useWebNavigation();
  return <StyledLink $as={HostLink ?? "a"} className={className} {...props} />;
}
export function Section<T>({
  result,
  children,
}: {
  result: ReadResult<T>;
  children(data: T | null): ReactNode;
}) {
  const [previous, setPrevious] = useState<T | null>(
    result.ok ? result.data : null,
  );
  if (result.ok && result.data !== previous) setPrevious(result.data);
  const data = result.ok ? result.data : previous;
  return (
    <>
      {!result.ok && (
        <Notification kind={KIND.negative} overrides={{ Body: { props: { role: "alert" }, style: { width: "auto" } } }}>
          {result.error}
          {previous !== null && <ParagraphSmall>Showing the last successful result.</ParagraphSmall>}
        </Notification>
      )}
      {children(data)}
    </>
  );
}
