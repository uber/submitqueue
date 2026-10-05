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
        <Link className="brand" href="/" aria-label="SubmitQueue queues">
          <span className="brand-mark" aria-hidden="true">
            SQ
          </span>
          <span>SubmitQueue</span>
        </Link>
        <span className="environment-pill">Local demo · read-only</span>
      </header>
      {children}
      <footer className="site-footer">
        Read-only demo · updates automatically
      </footer>
    </div>
  );
}
