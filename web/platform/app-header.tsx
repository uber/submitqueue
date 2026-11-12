"use client";

import { styled } from "baseui";

export const AppHeader = styled("header", ({ $theme }) => ({
  minHeight: "64px",
  boxSizing: "border-box",
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  flexWrap: "wrap",
  gap: $theme.sizing.scale600,
  paddingTop: $theme.sizing.scale300,
  paddingBottom: $theme.sizing.scale300,
  borderBottom: `1px solid ${$theme.colors.borderOpaque}`,
}));
