"use client";

import { useCallback, useState } from "react";
import { AutoRefresh, useWebNavigation } from "@submitqueue/web-platform";
import { Checkbox } from "baseui/checkbox";
import { ParagraphXSmall } from "baseui/typography";

export function RefreshControls() {
  const navigation = useWebNavigation();
  const [automatic, setAutomatic] = useState(false);
  const [error, setError] = useState("");
  const refresh = useCallback(async () => {
    setError("");
    if (!navigation.refresh) {
      window.location.reload();
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
        automatic={automatic && Boolean(navigation.refresh)}
        intervalMs={10_000}
        jitterMs={0}
        maxBackoffMs={10_000}
      />
      {navigation.refresh && (
        <Checkbox
          checked={automatic}
          onChange={(event) => setAutomatic(event.currentTarget.checked)}
        >
          Auto refresh every 10s
        </Checkbox>
      )}
      {error && <ParagraphXSmall role="alert" color="contentNegative">{error}</ParagraphXSmall>}
    </div>
  );
}
