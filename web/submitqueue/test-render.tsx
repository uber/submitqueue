import { BaseProvider, LightTheme } from "baseui";
import { render as renderReact, type RenderResult, type RenderOptions } from "@testing-library/react";
import type { ReactElement, ReactNode } from "react";
import { Client } from "styletron-engine-atomic";
import { Provider } from "styletron-react";

export function render(ui: ReactElement, options?: RenderOptions): RenderResult {
  const engine = new Client();
  function Host({ children }: { children: ReactNode }) {
    return <Provider value={engine}><BaseProvider theme={LightTheme}>{children}</BaseProvider></Provider>;
  }
  return renderReact(ui, { wrapper: Host, ...options });
}
