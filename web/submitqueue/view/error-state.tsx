"use client";

import { HeadingXSmall } from "baseui/typography";

import type { WebError } from "../entity/index.js";

export function ErrorState({ error, compact = false }: { error: WebError; compact?: boolean }) {
  return <section className="sq-error" data-error-kind={error.kind} role={compact ? "status" : "alert"}>
    <HeadingXSmall as="h2" marginTop="0" marginBottom="scale400">{error.title}</HeadingXSmall><p>{error.message}</p>
  </section>;
}
