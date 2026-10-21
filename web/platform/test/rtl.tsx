import { render as renderRTL, type RenderOptions, type RenderResult } from "@testing-library/react";
import { BaseProvider, LightTheme } from "baseui";
import { useId, useState, type ReactElement, type ReactNode } from "react";
import { Client } from "styletron-engine-atomic";
import { Provider } from "styletron-react";

export * from "@testing-library/react";

export function render(ui: ReactElement, options?: Omit<RenderOptions, "wrapper">): RenderResult {
  function Wrapper({ children }: { children: ReactNode }) {
    const prefix = useId().replaceAll(":", "");
    const [engine] = useState(() => new Client({ prefix: `sq-test-${prefix}-` }));
    return <Provider value={engine}><BaseProvider theme={LightTheme}>{children}</BaseProvider></Provider>;
  }
  return renderRTL(ui, { wrapper: Wrapper, ...options });
}
