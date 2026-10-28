"use client";

import type { ReactNode } from "react";
import { TimeZoneProvider, TimeZoneToggle, useWebNavigation } from "@submitqueue/web-platform";
import { styled, withStyle } from "baseui";
import { Block } from "baseui/block";
import { LabelXSmall } from "baseui/typography";
import { AccentLink } from "./web-app.js";

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

/** Header, page, and footer share one centered column so their edges line up at every width. */
const Column = styled("div", {
  width: "min(1200px, calc(100% - 48px))",
  marginLeft: "auto",
  marginRight: "auto",
});

const Header = withStyle(Column, ({ $theme }) => ({
  minHeight: "80px",
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  flexWrap: "wrap",
  gap: $theme.sizing.scale600,
  borderBottom: `1px solid ${$theme.colors.borderOpaque}`,
}));

const Footer = withStyle(Column, ({ $theme }) => ({
  ...$theme.typography.ParagraphXSmall,
  color: $theme.colors.contentSecondary,
  paddingTop: $theme.sizing.scale600,
  paddingBottom: $theme.sizing.scale600,
  borderTop: `1px solid ${$theme.colors.borderOpaque}`,
}));

const BrandLink = withStyle(AccentLink, ({ $theme }) => ({
  display: "inline-flex",
  alignItems: "center",
  gap: $theme.sizing.scale300,
  color: $theme.colors.contentPrimary,
  fontSize: "18px",
  fontWeight: 600,
  ":visited": { color: $theme.colors.contentPrimary },
  ":hover": { color: $theme.colors.contentPrimary, textDecoration: "none" },
}));

const BrandMark = styled("span", ({ $theme }) => ({
  backgroundColor: $theme.colors.contentPrimary,
  color: $theme.colors.backgroundPrimary,
  borderRadius: "6px",
  padding: "5px 7px",
  fontSize: "12px",
  lineHeight: 1,
}));

export function WebShell({
  children, homeHref = "/", homeLabel, brand, brandMark = "", environmentLabel, footer, controls,
}: WebShellProps) {
  const { Link } = useWebNavigation();
  return <TimeZoneProvider><Block className="sq-app" backgroundColor="backgroundPrimary" color="contentPrimary" font="ParagraphSmall"
    minHeight="100vh" display="grid" gridTemplateColumns="minmax(0, 1fr)" gridTemplateRows="auto 1fr auto">
    <Header $as="header">
      <BrandLink $as={Link ?? "a"} href={homeHref} {...(homeLabel ? { "aria-label": homeLabel } : {})}>
        {brandMark ? <BrandMark aria-hidden="true">{brandMark}</BrandMark> : null}<span>{brand}</span>
      </BrandLink>
      <Block display="flex" alignItems="center" flexWrap gridGap="scale600">
        {environmentLabel ? <LabelXSmall as="span" color="contentSecondary">{environmentLabel}</LabelXSmall> : null}
        <TimeZoneToggle />
        {controls}
      </Block>
    </Header>
    <Column>{children}</Column>
    {footer ? <Footer $as="footer">{footer}</Footer> : null}
  </Block></TimeZoneProvider>;
}
