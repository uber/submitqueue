"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState, useSyncExternalStore, type ReactNode } from "react";
import { Button, KIND, MIN_HIT_AREA, SHAPE, SIZE } from "baseui/button";
import { Icon } from "baseui/icon";
import { StatefulTooltip } from "baseui/tooltip";
import { appearanceCookieName, readAppearanceMode, type AppearanceMode, type ResolvedAppearance } from "./appearance-preference.js";

export type { AppearanceMode, ResolvedAppearance } from "./appearance-preference.js";

const darkQuery = "(prefers-color-scheme: dark)";
const AppearanceContext = createContext<{
  mode: AppearanceMode;
  resolvedMode: ResolvedAppearance;
  toggleAppearance(): void;
} | null>(null);

function subscribeToSystem(listener: () => void) {
  const media = window.matchMedia?.(darkQuery);
  media?.addEventListener("change", listener);
  return () => media?.removeEventListener("change", listener);
}

function systemPrefersDark() {
  return window.matchMedia?.(darkQuery).matches ?? false;
}

export interface AppearanceProviderProps {
  children: ReactNode;
  /** Server-known saved choice; defaults to following the system. */
  initialMode?: AppearanceMode;
  /** Server-known system preference, when supplied by the host. */
  initialSystemMode?: ResolvedAppearance;
}

/**
 * Remembers appearance choices without installing a theme or styling provider.
 * The application supplies its own theme using the selected and resolved modes.
 */
export function AppearanceProvider({
  children, initialMode, initialSystemMode = "light",
}: AppearanceProviderProps) {
  const [mode, setModeState] = useState(initialMode ?? "system");
  const dark = useSyncExternalStore(subscribeToSystem, systemPrefersDark, () => initialSystemMode === "dark");
  useEffect(() => {
    if (initialMode !== undefined) return;
    try {
      setModeState(readAppearanceMode(document.cookie));
    } catch {
      // Keep the server-supplied choice when browser storage is blocked.
    }
  }, [initialMode]);
  const toggleAppearance = useCallback(() => {
    const currentlyDark = mode === "system" ? systemPrefersDark() : mode === "dark";
    const next = currentlyDark ? "light" : "dark";
    setModeState(next);
    try {
      document.cookie = `${appearanceCookieName}=${next}; Path=/; Max-Age=31536000; SameSite=Lax`;
    } catch {
      // The choice still applies for this page when cookies are blocked.
    }
  }, [mode]);
  const resolvedMode: ResolvedAppearance = mode === "system" ? (dark ? "dark" : "light") : mode;
  const value = useMemo(() => ({ mode, resolvedMode, toggleAppearance }), [mode, resolvedMode, toggleAppearance]);
  return <AppearanceContext.Provider value={value}>{children}</AppearanceContext.Provider>;
}

/** Reads appearance preferences so the application can choose its own theme. */
export function useAppearance() {
  const appearance = useContext(AppearanceContext);
  if (!appearance) throw new Error("useAppearance requires an AppearanceProvider");
  return appearance;
}

/** Hidden until the host opts into the shared appearance provider. */
export function AppearanceControl() {
  const appearance = useContext(AppearanceContext);
  if (!appearance) return null;
  const action = appearance.resolvedMode === "dark" ? "Switch to light mode" : "Switch to dark mode";
  return <StatefulTooltip content={action} placement="bottomRight" onMouseEnterDelay={150} onMouseLeaveDelay={0}>
    <Button type="button" kind={KIND.tertiary} size={SIZE.mini} shape={SHAPE.circle} minHitArea={MIN_HIT_AREA.tap}
      aria-label={action} onClick={appearance.toggleAppearance}>
      <Icon size={16} viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="1.75" />
        <path d="M12 3a9 9 0 0 1 0 18Z" fill="currentColor" />
      </Icon>
    </Button>
  </StatefulTooltip>;
}
