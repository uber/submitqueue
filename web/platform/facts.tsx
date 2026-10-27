"use client";

import type { ReactNode } from "react";
import { styled } from "baseui";
import { LabelXSmall, ParagraphSmall } from "baseui/typography";

const FactRow = styled<"div", { $compact: boolean }>("div", ({ $theme, $compact }) => ({
  minWidth: 0,
  paddingTop: $compact ? $theme.sizing.scale200 : $theme.sizing.scale500,
  paddingBottom: $compact ? $theme.sizing.scale200 : $theme.sizing.scale500,
  borderBottom: $compact ? "none" : `1px solid ${$theme.colors.borderOpaque}`,
}));

export function Fact({ label, children, compact = false }: { label: string; children: ReactNode; compact?: boolean }) {
  return <FactRow $compact={compact}>
    <LabelXSmall as="dt" color="contentSecondary" marginBottom={compact ? "scale100" : "scale300"}>{label}</LabelXSmall>
    <ParagraphSmall as="dd" margin="0">{children}</ParagraphSmall>
  </FactRow>;
}
