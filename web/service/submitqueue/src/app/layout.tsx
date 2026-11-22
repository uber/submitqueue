import type { Metadata } from "next";
import type { ReactNode } from "react";
import { ThemeProvider } from "@submitqueue/web-service-shared/theme";
import { getAppearanceDocumentStyle, getAppearancePreferences } from "@submitqueue/web-service-shared/appearance";
import { QueueProvider } from "../next/queue-provider";

import "@submitqueue/web-submitqueue/styles.css";

export const metadata: Metadata = {
  title: {
    default: "SubmitQueue",
    template: "%s · SubmitQueue",
  },
  description: "Request status for the SubmitQueue demo queue.",
};

export default async function RootLayout({ children }: { children: ReactNode }) {
  const appearance = await getAppearancePreferences();
  return (
    <html lang="en" style={getAppearanceDocumentStyle(appearance)}>
      <body style={{ margin: 0 }}><ThemeProvider {...appearance}><QueueProvider>{children}</QueueProvider></ThemeProvider></body>
    </html>
  );
}
