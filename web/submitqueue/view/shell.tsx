"use client";

import type { ReactNode } from "react";
import { AppBrand, AppHeader, TimeZoneProvider } from "@submitqueue/web-platform";
import { styled, withStyle } from "baseui";
import { Block } from "baseui/block";
import { LabelXSmall } from "baseui/typography";

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
const columnStyles = {
  width: "min(1200px, calc(100% - 48px))",
  marginLeft: "auto",
  marginRight: "auto",
};

const Column = styled("div", columnStyles);
const Header = withStyle(AppHeader, columnStyles);

const Footer = withStyle(Column, ({ $theme }) => ({
  ...$theme.typography.ParagraphXSmall,
  color: $theme.colors.contentSecondary,
  paddingTop: $theme.sizing.scale600,
  paddingBottom: $theme.sizing.scale600,
  borderTop: `1px solid ${$theme.colors.borderOpaque}`,
}));

export function WebShell({
  children, homeHref = "/", homeLabel, brand, brandMark = "", environmentLabel, footer, controls,
}: WebShellProps) {
  return <TimeZoneProvider><Block className="sq-app" backgroundColor="backgroundPrimary" color="contentPrimary" font="ParagraphSmall"
    minHeight="100vh" display="grid" gridTemplateColumns="minmax(0, 1fr)" gridTemplateRows="auto 1fr auto">
    <Header>
      <AppBrand href={homeHref} {...(homeLabel ? { homeLabel } : {})} mark={brandMark}>{brand}</AppBrand>
      <Block display="flex" alignItems="center" flexWrap gridGap="scale600">
        {environmentLabel ? <LabelXSmall as="span" color="contentSecondary">{environmentLabel}</LabelXSmall> : null}
        {controls}
      </Block>
    </Header>
    <Column>{children}</Column>
    {footer ? <Footer $as="footer">{footer}</Footer> : null}
  </Block></TimeZoneProvider>;
}
