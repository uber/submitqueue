import type { ReactNode } from "react";
import Link from "next/link";

import { requireAuthorization } from "../../server/request-auth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function ProtectedLayout({
  children,
}: Readonly<{ children: ReactNode }>) {
  await requireAuthorization();

  return (
    <div className="site-frame">
      <header className="site-header">
        <Link className="brand" href="/requests" aria-label="SubmitQueue requests">
          <span className="brand-mark" aria-hidden="true">
            SQ
          </span>
          <span>SubmitQueue</span>
        </Link>
        <span className="environment-pill">demo-queue</span>
      </header>
      {children}
      <footer className="site-footer">
        Read-only demo · updates automatically
      </footer>
    </div>
  );
}
