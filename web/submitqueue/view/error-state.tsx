"use client";

import type { WebError } from "../entity/index.js";

export function ErrorState({ error, compact = false }: { error: WebError; compact?: boolean }) {
  return <section className="sq-error" data-error-kind={error.kind} role={compact ? "status" : "alert"}>
    <h2>{error.title}</h2><p>{error.message}</p>
  </section>;
}
