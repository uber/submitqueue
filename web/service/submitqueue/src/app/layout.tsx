import type { Metadata } from "next";
import type { ReactNode } from "react";

import "@submitqueue/web-submitqueue/styles.css";

export const metadata: Metadata = {
  title: {
    default: "SubmitQueue",
    template: "%s · SubmitQueue",
  },
  description: "Read-only request status for the SubmitQueue demo queue.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0 }}>{children}</body>
    </html>
  );
}
