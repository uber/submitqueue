"use client";

import type { WebError } from "../entity/index.js";
import { KIND, Notification } from "baseui/notification";
import { HeadingSmall, ParagraphSmall } from "baseui/typography";

export function ErrorState({ error, compact = false }: { error: WebError; compact?: boolean }) {
  return <Notification kind={KIND.negative} overrides={{ Body: {
    props: { "data-error-kind": error.kind, role: compact ? "status" : "alert" },
    style: { width: "auto", marginLeft: 0, marginRight: 0 },
  } }}>
    <HeadingSmall as="h2" marginTop="0">{error.title}</HeadingSmall><ParagraphSmall>{error.message}</ParagraphSmall>
  </Notification>;
}
