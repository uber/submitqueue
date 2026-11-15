"use client";

import type { ReactNode } from "react";
import { styled } from "baseui";
import { StyledLink } from "baseui/link";
import { useWebNavigation } from "./navigation.js";

const BrandLink = styled(StyledLink, ({ $theme }) => ({
  display: "inline-flex",
  alignItems: "center",
  gap: $theme.sizing.scale300,
  color: $theme.colors.contentPrimary,
  fontSize: "18px",
  fontWeight: 600,
  textDecoration: "none",
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

const BrandSymbol = styled("span", {
  display: "inline-flex",
  alignItems: "center",
  flexShrink: 0,
  lineHeight: 0,
});

export interface AppBrandProps {
  href: string;
  /** Accessible name of the home link. */
  homeLabel?: string;
  children: ReactNode;
  /** Decorative mark; text is rendered as a monogram badge. */
  mark?: ReactNode;
}

export function AppBrand({ href, homeLabel, children, mark = "" }: AppBrandProps) {
  const { Link } = useWebNavigation();
  return <BrandLink $as={Link ?? "a"} href={href} {...(homeLabel ? { "aria-label": homeLabel } : {})}>
    {mark ? typeof mark === "string"
      ? <BrandMark aria-hidden="true">{mark}</BrandMark>
      : <BrandSymbol aria-hidden="true">{mark}</BrandSymbol>
      : null}
    <span>{children}</span>
  </BrandLink>;
}
