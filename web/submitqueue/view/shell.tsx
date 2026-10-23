"use client";

import type { ReactNode } from "react";
import { TimeZoneProvider, TimeZoneToggle } from "@submitqueue/web-platform";
import { Block } from "baseui/block";
import { StyledLink } from "baseui/link";
import { Tag } from "baseui/tag";
import { LabelMedium, LabelXSmall } from "baseui/typography";

export interface WebShellProps {
  children: ReactNode;
  homeHref?: string;
  /** Accessible name of the home link. */
  homeLabel?: string;
  brand?: ReactNode;
  /** Short monogram shown beside the brand; omitted when empty. */
  brandMark?: string;
  environmentLabel?: string;
  footer?: ReactNode;
  controls?: ReactNode;
}

export function WebShell({
  children, homeHref = "/", homeLabel, brand, brandMark = "", environmentLabel = "Read-only",
  footer = "Read-only", controls,
}: WebShellProps) {
  return <TimeZoneProvider><Block className="sq-app" backgroundColor="backgroundPrimary" color="contentPrimary" font="ParagraphSmall"
    minHeight="100vh" display="grid" gridTemplateColumns="minmax(0, 1fr)" gridTemplateRows="auto 1fr auto"
    paddingLeft="scale600" paddingRight="scale600">
    <Block as="header" display="flex" alignItems="center" justifyContent="space-between" flexWrap gridGap="scale600"
      paddingTop="scale300" paddingBottom="scale300">
      <Block as={StyledLink} display="inline-flex" alignItems="center" gridGap="scale300" href={homeHref} {...(homeLabel ? { "aria-label": homeLabel } : {})}>
        {brandMark ? <Tag closeable={false} kind="neutral" size="small"><span aria-hidden="true">{brandMark}</span></Tag> : null}<LabelMedium as="span">{brand}</LabelMedium>
      </Block>
      <Block display="flex" alignItems="center" flexWrap gridGap="scale600">
        {environmentLabel ? <LabelXSmall as="span" color="contentSecondary">{environmentLabel}</LabelXSmall> : null}
        <TimeZoneToggle />
        {controls}
      </Block>
    </Block>
    {children}
    {footer ? <Block as="footer" paddingTop="scale300" paddingBottom="scale300" font="ParagraphXSmall" color="contentSecondary">{footer}</Block> : null}
  </Block></TimeZoneProvider>;
}
