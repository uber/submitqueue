import type { ReactNode } from "react";
import { SubmitQueueShell } from "@submitqueue/web-submitqueue";

import { requireAuthorization } from "../../next/authorize";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function ProtectedLayout({
  children,
}: Readonly<{ children: ReactNode }>) {
  await requireAuthorization();

  return (
    <SubmitQueueShell environmentLabel="Local demo · read-only"
      footer="Read-only demo · updates automatically">{children}</SubmitQueueShell>
  );
}
