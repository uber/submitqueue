"use client";

import { useCallback, useState } from "react";
import { AutoRefresh, useWebNavigation } from "@submitqueue/web-platform";
import { ParagraphXSmall } from "baseui/typography";

export function RefreshControls() {
  const navigation = useWebNavigation();
  const [error, setError] = useState("");
  const refresh = useCallback(async () => {
    setError("");
    if (!navigation.refresh) {
      return;
    }
    try {
      await navigation.refresh(null);
    } catch (cause) {
      setError("Refresh failed. Showing the last successful result.");
      throw cause;
    }
  }, [navigation]);
  return (
    <div>
      <AutoRefresh
        refresh={refresh}
        available={Boolean(navigation.refresh)}
        intervalMs={10_000}
        jitterMs={0}
        maxBackoffMs={10_000}
      />
      {error && <ParagraphXSmall role="alert" color="contentNegative">{error}</ParagraphXSmall>}
    </div>
  );
}
