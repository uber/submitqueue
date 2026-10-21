"use client";

import type { ReactNode } from "react";
import { styled } from "baseui/index.js";
import { LabelXSmall, ParagraphSmall } from "baseui/typography/index.js";

const FactRow = styled("div", ({ $theme }) => ({
  paddingTop: $theme.sizing.scale500,
  paddingBottom: $theme.sizing.scale500,
  borderBottom: `1px solid ${$theme.colors.borderOpaque}`,
}));

export function Fact({ label, children }: { label: string; children: ReactNode }) {
  return <FactRow>
    <LabelXSmall as="dt" color="contentSecondary" marginBottom="scale300">{label}</LabelXSmall>
    <ParagraphSmall as="dd" margin="0">{children}</ParagraphSmall>
  </FactRow>;
}
