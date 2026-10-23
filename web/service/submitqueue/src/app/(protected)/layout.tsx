import type { ReactNode } from "react";

import { requireAuthorization } from "../../next/authorize";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function ProtectedLayout({
  children,
}: Readonly<{ children: ReactNode }>) {
  await requireAuthorization();

  return children;
}
