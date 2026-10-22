import type { ReactNode } from "react";
import { ThemeProvider } from "@submitqueue/web-service-shared/theme";
import { QueueProvider } from "../next/queue-provider";
import "@submitqueue/web-stovepipe/styles.css";
export const metadata = {
  title: "Stovepipe",
  description: "Stovepipe request status and history.",
};
export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0 }}><ThemeProvider><QueueProvider>{children}</QueueProvider></ThemeProvider></body>
    </html>
  );
}
