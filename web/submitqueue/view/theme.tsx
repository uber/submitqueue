"use client";

import { useStyletron } from "baseui";
import type { CSSProperties, ReactNode } from "react";

/** Applies the host's Base Web theme to SubmitQueue's domain layouts. */
export function SubmitQueueFrame({ children, className = "" }: { children: ReactNode; className?: string }) {
  const [, theme] = useStyletron();
  const style = {
    "--sq-surface": theme.colors.backgroundPrimary,
    "--sq-surface-muted": theme.colors.backgroundSecondary,
    "--sq-text": theme.colors.contentPrimary,
    "--sq-muted": theme.colors.contentSecondary,
    "--sq-border": theme.colors.borderOpaque,
    "--sq-accent": theme.colors.linkText,
    "--sq-danger": theme.colors.contentNegative,
    "--sq-danger-soft": theme.colors.backgroundNegativeLight,
    "--sq-font": theme.typography.ParagraphSmall.fontFamily,
    "--sq-font-mono": theme.typography.MonoParagraphSmall.fontFamily,
    "--sq-font-size": theme.typography.ParagraphSmall.fontSize,
    "--sq-line-height": theme.typography.ParagraphSmall.lineHeight,
  } as CSSProperties;
  return <div className={`sq-app ${className}`.trim()} style={style}>{children}</div>;
}
