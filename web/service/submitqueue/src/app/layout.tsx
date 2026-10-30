import type { Metadata } from "next";
import type { ReactNode } from "react";
import { ThemeProvider } from "@submitqueue/web-service-shared/theme";
import { QueueProvider } from "../next/queue-provider";

import "@submitqueue/web-submitqueue/styles.css";

export const metadata: Metadata = {
  title: {
    default: "SubmitQueue",
    template: "%s · SubmitQueue",
  },
  description: "Request status for the SubmitQueue demo queue.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0 }}><ThemeProvider><QueueProvider>{children}</QueueProvider></ThemeProvider></body>
    </html>
  );
}
