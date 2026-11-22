import type { Theme } from "baseui";
import { DarkTheme, LightTheme } from "baseui/themes";
import type { AppearanceMode, ResolvedAppearance } from "./appearance-preference.js";

function createSystemAppearanceTheme(theme: Theme): Theme {
  return {
    ...theme,
    colors: Object.fromEntries(Object.entries(theme.colors).map(([key, value]) => {
      const light = LightTheme.colors[key];
      const dark = DarkTheme.colors[key];
      // Ramp units are numbers encoded as strings, not CSS colors.
      return [key, light === dark || /^\d+$/.test(value) ? value : `light-dark(${light}, ${dark})`];
    })) as Theme["colors"],
  };
}

const systemLightTheme = createSystemAppearanceTheme(LightTheme);
const systemDarkTheme = createSystemAppearanceTheme(DarkTheme);

/** Native color selection keeps System mode correct even before hydration. */
export function getAppearanceTheme(mode: AppearanceMode, systemMode: ResolvedAppearance) {
  if (mode === "system") return systemMode === "dark" ? systemDarkTheme : systemLightTheme;
  return mode === "dark" ? DarkTheme : LightTheme;
}

export function getAppearanceColorScheme(mode: AppearanceMode) {
  return mode === "system" ? "light dark" : mode;
}
