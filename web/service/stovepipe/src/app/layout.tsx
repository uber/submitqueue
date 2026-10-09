import type { ReactNode } from "react";
import "@submitqueue/web-stovepipe/styles.css";
export const metadata = { title: "Stovepipe", description: "Read-only Stovepipe requests, project results, and history." };
export default function Layout({ children }: { children: ReactNode }) {
  return <html lang="en"><body style={{ margin: 0 }}>{children}</body></html>;
}
