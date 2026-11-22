import type { ReactNode } from "react";
import { ThemeProvider } from "@submitqueue/web-service-shared/theme";
import { getAppearanceDocumentStyle, getAppearancePreferences } from "@submitqueue/web-service-shared/appearance";
import { QueueProvider } from "../next/queue-provider";
import "@submitqueue/web-stovepipe/styles.css";
export const metadata = {
  title: "Stovepipe",
  description: "Stovepipe request status and history.",
};
export default async function Layout({ children }: { children: ReactNode }) {
  const appearance = await getAppearancePreferences();
  return (
    <html lang="en" style={getAppearanceDocumentStyle(appearance)}>
      <body style={{ margin: 0 }}><ThemeProvider {...appearance}><QueueProvider>{children}</QueueProvider></ThemeProvider></body>
    </html>
  );
}
