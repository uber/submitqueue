export type AppearanceMode = "system" | "light" | "dark";
export type ResolvedAppearance = Exclude<AppearanceMode, "system">;

export const appearanceCookieName = "submitqueue.appearance";

/** Reads the shared browser preference from a request's Cookie header. */
export function readAppearanceMode(cookieHeader: string): AppearanceMode {
  const value = cookieHeader.split(";")
    .map(cookie => cookie.trim())
    .find(cookie => cookie.startsWith(`${appearanceCookieName}=`))
    ?.slice(appearanceCookieName.length + 1);
  return value === "light" || value === "dark" ? value : "system";
}
