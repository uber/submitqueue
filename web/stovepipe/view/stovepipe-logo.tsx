"use client";

import { styled } from "baseui";

const Logo = styled("svg", ({ $theme }) => ({
  width: "28px",
  height: "28px",
  padding: "2px",
  boxSizing: "border-box",
  borderRadius: "50%",
  border: `1px solid ${$theme.colors.contentPrimary}`,
  backgroundColor: $theme.colors.contentPrimary,
  color: $theme.colors.backgroundPrimary,
}));

export function StovepipeLogo() {
  return <Logo viewBox="-4 -4 40 40" fill="none" focusable="false">
    <path d="M23 1v5H12a5 5 0 0 0 0 10h8a5 5 0 0 1 0 10H9v5"
      stroke="currentColor" strokeWidth="5" strokeLinejoin="round" />
    <ellipse cx="23" cy="1" rx="3.5" ry="1.5" fill="currentColor" />
    <ellipse cx="9" cy="31" rx="3.5" ry="1.5" fill="currentColor" />
  </Logo>;
}
